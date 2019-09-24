// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{algorithms::{bucket::Bucket, Id, Token}, error::{Error, Result}};
use futures::{prelude::*, task::{self, Task}};
use log::error;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc},
    time::{Duration, Instant}
};
use tokio_executor::Executor;
use tokio_timer::Interval;

type Tasks = Arc<Mutex<HashMap<Id, Task>>>;

/// A `Limiter` maintains rate-limiting invariants over a set
/// of `Limited` resources.
#[derive(Clone, Debug)]
pub struct Limiter {
    bucket: Arc<Bucket>,
    tasks: Tasks,
    error: Arc<AtomicBool>
}

impl Limiter {
    /// Create a new limiter which caps the transfer rate to the given
    /// maximum of bytes per second.
    pub fn new<E: Executor>(e: &mut E, max: usize) -> Result<Limiter> {
        let bucket = Arc::new(Bucket::new(max));
        let clock = Arc::new(AtomicUsize::new(0));
        let tasks = Arc::new(Mutex::new(HashMap::<Id, Task>::new()));
        let error = Arc::new(AtomicBool::new(false));
        let limiter = Limiter {
            bucket: bucket.clone(),
            tasks: tasks.clone(),
            error: error.clone()
        };
        let timer = Interval::new(Instant::now(), Duration::from_secs(1))
            .for_each(move |_| {
                bucket.reset(clock.fetch_add(1, Ordering::Relaxed));
                let mut tt = tasks.lock();
                for t in tt.drain() {
                    t.1.notify()
                }
                Ok(())
            })
            .map_err(move |e| {
                error!("interval error: {}", e);
                error.store(true, Ordering::Release)
            });
        e.spawn(Box::new(timer))?;
        Ok(limiter)
    }

    pub(crate) fn get(&self, id: Id, hint: usize) -> Result<Token> {
        if self.error.load(Ordering::Acquire) {
            return Err(Error::TimerError)
        }
        self.bucket.get(id, hint)
    }

    pub(crate) fn release(&self, t: Token) {
        self.bucket.release(t)
    }

    pub(crate) fn enqueue(&self, id: Id) -> Result<()> {
        if self.error.load(Ordering::Acquire) {
            return Err(Error::TimerError)
        }
        self.tasks.lock().insert(id, task::current());
        Ok(())
    }

    pub(crate) fn register(&self) -> Result<Id> {
        if self.error.load(Ordering::Acquire) {
            return Err(Error::TimerError)
        }
        self.bucket.add_part()
    }

    pub(crate) fn deregister(&self, id: Id) {
        self.tasks.lock().remove(&id);
        self.bucket.remove_part(id)
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use log::{info, LevelFilter};
    use std::{cmp::max, io, str, thread};
    use crate::limited::Limited;
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
    ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let srv = TcpListener::bind(&addr.parse().unwrap())
            .unwrap()
            .incoming()
            .map_err(|e| error!("accept failed = {:?}", e))
            .for_each(move |s| {
                let (r, w) = s.split();
                let reader = match lr {
                    Some(ref lim) => {
                        Box::new(Limited::new(r, lim.clone()).unwrap()) as Box<dyn AsyncRead + Send>
                    }
                    None => Box::new(r) as Box<dyn AsyncRead + Send>,
                };
                let writer = Limited::new(w, lw.clone()).unwrap();
                let future = copy(reader, writer)
                    .map(|_| ())
                    .map_err(|e| error!("copy failed = {:?}", e));
                tokio::spawn(future)
            });
        Box::new(srv)
    }

    fn echo_client(addr: &str) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let clt = TcpStream::connect(&addr.parse().unwrap())
            .and_then(|stream| copy(&b"0123456789"[..], stream))
            .and_then(|(n, _, stream)| read_exact(stream, vec![0; n as usize]))
            .map(|(_stream, buf)| info!("{:?}", str::from_utf8(&buf)))
            .map_err(|e| error!("client error: {:?}", e));
        Box::new(clt)
    }

    fn echo_rate_client(addr: &str, size: usize) -> Box<dyn Future<Item = (), Error = ()> + Send> {
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
