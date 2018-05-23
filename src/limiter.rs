// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use algorithms::{bucket::Bucket, Id, Token};
use error::Result;
use futures::{prelude::*, task::{self, Task}};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::{atomic::{AtomicUsize, Ordering}, Arc}};
use std::time::{Duration, Instant};
use tokio_executor::Executor;
use tokio_timer::Interval;

type Tasks = Arc<Mutex<HashMap<Id, Task>>>;

/// A `Limiter` maintains rate-limiting invariants over a set
/// of `Limited` resources.
#[derive(Clone)]
pub struct Limiter {
    bucket: Arc<Bucket>,
    tasks: Tasks,
}

impl Limiter {
    /// Create a new limiter which caps the transfer rate to the given
    /// maximum of bytes per second.
    pub fn new<E: Executor>(e: &mut E, max: usize) -> Result<Limiter> {
        let bucket = Arc::new(Bucket::new(max));
        let clock = Arc::new(AtomicUsize::new(0));
        let tasks = Arc::new(Mutex::new(HashMap::<Id, Task>::new()));
        e.spawn(Box::new(timer(clock, bucket.clone(), tasks.clone())))?;
        Ok(Limiter { bucket, tasks })
    }

    pub(crate) fn get(&self, id: Id, hint: usize) -> Result<Token> {
        self.bucket.get(id, hint)
    }

    pub(crate) fn release(&self, t: Token) {
        self.bucket.release(t)
    }

    pub(crate) fn enqueue(&self, id: Id) {
        self.tasks.lock().insert(id, task::current());
    }

    pub(crate) fn register(&self) -> Result<Id> {
        self.bucket.add_part()
    }

    pub(crate) fn deregister(&self, id: Id) {
        self.tasks.lock().remove(&id);
        self.bucket.remove_part(id)
    }
}

fn timer(clock: Arc<AtomicUsize>, bucket: Arc<Bucket>, tasks: Tasks) -> impl Future<Item=(), Error=()> {
    Interval::new(Instant::now(), Duration::from_secs(1))
        .for_each(move |_| {
            bucket.reset(clock.fetch_add(1, Ordering::Relaxed));
            let mut tt = tasks.lock();
            for t in tt.drain() {
                t.1.notify()
            }
            Ok(())
        })
        .map_err(|e| {
            // TODO: Restart on error
            error!("interval error: {}", e)
        })
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use log::LevelFilter;
    use std::{cmp::max, io, str, thread};
    use limited::Limited;
    use super::*;
    use tokio::{
        self,
        io::{copy, read_exact},
        net::{TcpListener, TcpStream},
        prelude::*,
        runtime::Runtime,
        timer::Delay
    };

    fn echo_server(
        addr: &str,
        lr: Option<Limiter>,
        lw: Limiter,
    ) -> Box<Future<Item = (), Error = ()> + Send> {
        let srv = TcpListener::bind(&addr.parse().unwrap())
            .unwrap()
            .incoming()
            .map_err(|e| error!("accept failed = {:?}", e))
            .for_each(move |s| {
                let (r, w) = s.split();
                let reader = match lr {
                    Some(ref lim) => {
                        Box::new(Limited::new(r, lim.clone()).unwrap()) as Box<AsyncRead + Send>
                    }
                    None => Box::new(r) as Box<AsyncRead + Send>,
                };
                let writer = Limited::new(w, lw.clone()).unwrap();
                let future = copy(reader, writer)
                    .map(|_| ())
                    .map_err(|e| error!("copy failed = {:?}", e));
                tokio::spawn(future)
            });
        Box::new(srv)
    }

    fn echo_client(addr: &str) -> Box<Future<Item = (), Error = ()> + Send> {
        let clt = TcpStream::connect(&addr.parse().unwrap())
            .and_then(|stream| copy(&b"0123456789"[..], stream))
            .and_then(|(n, _, stream)| read_exact(stream, vec![0; n as usize]))
            .map(|(_stream, buf)| info!("{:?}", str::from_utf8(&buf)))
            .map_err(|e| error!("client error: {:?}", e));
        Box::new(clt)
    }

    fn echo_rate_client(addr: &str, size: usize) -> Box<Future<Item = (), Error = ()> + Send> {
        let clt = TcpStream::connect(&addr.parse().unwrap())
            .and_then(move |stream| {
                Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| Ok(stream))
            })
            .and_then(move |stream| {
                copy(io::repeat(1).take(size as u64), stream)
                    .and_then(move |(_, _, stream)| {
                        let t = Instant::now();
                        read_exact(stream, vec![0; size]).map(move |(_stream, buf)| (buf, t))
                    })
                    .and_then(move |(buf, t)| {
                        assert_eq!(size, buf.len());
                        let delta = Instant::now().duration_since(t);
                        let read_rate = size as f64 / max(1, delta.as_secs()) as f64;
                        info!("duration = {} s, r = {:.3} b/s", delta.as_secs(), read_rate);
                        Ok(())
                    })
            })
            .map_err(|e| error!("client error: {:?}", e));
        Box::new(clt)
    }

    #[test]
    fn test1() {
        let _ = env_logger::Builder::from_default_env()
            .filter(Some("aio_limited"), LevelFilter::Trace)
            .try_init();

        let mut rt = Runtime::new().unwrap();
        let mut ex = rt.executor();

        let num_clients = 30;
        let rate = 100;
        let read_limiter = Limiter::new(&mut ex, rate).unwrap();
        let write_limiter = Limiter::new(&mut ex, rate).unwrap();

        thread::spawn(move || {
            ex.spawn(echo_server(
                "127.0.0.1:12345",
                Some(read_limiter),
                write_limiter,
            ))
        });

        thread::sleep(Duration::from_secs(1));

        for _ in 0..num_clients {
            rt.spawn(echo_client("127.0.0.1:12345"));
        }

        thread::sleep(Duration::from_secs((3 + 10 * num_clients / rate) as u64));
        rt.shutdown_now().wait().unwrap()
    }

    #[test]
    fn test2() {
        let _ = env_logger::Builder::from_default_env()
            .filter(None, LevelFilter::Info)
            .try_init();

        let mut rt = Runtime::new().unwrap();
        let mut ex = rt.executor();

        let data_size = 10_000;
        let num_clients = 30;
        let srv_write_rate = 10_000;
        let srv_write_lim = Limiter::new(&mut ex, srv_write_rate).unwrap();

        thread::spawn(move || ex.spawn(echo_server("127.0.0.1:23456", None, srv_write_lim)));

        thread::sleep(Duration::from_secs(1));

        info!(
            "spawning {} clients, data size = {} b, duration = {} s",
            num_clients,
            data_size,
            data_size * num_clients / srv_write_rate
        );

        for _ in 0..num_clients {
            rt.spawn(echo_rate_client("127.0.0.1:23456", data_size));
        }

        thread::sleep(Duration::from_secs(
            (3 + data_size * num_clients / srv_write_rate) as u64,
        ));

        info!(
            "spawn 1 more client, data size = {} b, duration = {} s",
            data_size,
            data_size / srv_write_rate
        );
        rt.spawn(echo_rate_client("127.0.0.1:23456", data_size));
        thread::sleep(Duration::from_secs((3 + data_size / srv_write_rate) as u64));

        rt.shutdown_now().wait().unwrap()
    }
}
