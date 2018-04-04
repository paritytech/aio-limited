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
use parking_lot::Mutex;
use std::{
	collections::HashMap,
	sync::Arc,
	sync::atomic::{AtomicUsize, Ordering},
	time::{Duration, Instant}
};
use tokio::{
	executor::Executor,
	prelude::*,
	prelude::task::Task,
	timer::Interval
};

type Tasks = Arc<Mutex<HashMap<Id, Task>>>;
type TimerFuture = Box<Future<Item=(), Error=()> + Send>;

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
		e.spawn(Limiter::timer(clock, bucket.clone(), tasks.clone())?)?;
		Ok(Limiter { bucket, tasks })
	}

	fn timer(clock: Arc<AtomicUsize>, bucket: Arc<Bucket>, tasks: Tasks) -> Result<TimerFuture> {
		let i = Interval::new(Instant::now(), Duration::from_secs(1))
			.for_each(move |_| {
				bucket.reset(clock.fetch_add(1, Ordering::Relaxed));
				let mut tt = tasks.lock();
				trace!("notifying {} tasks", tt.len());
				for t in tt.drain() {
					t.1.notify()
				}
				Ok(())
			})
			.map_err(|e| { // TODO: Restart on error
				error!("interval error: {}", e)
			});
		Ok(Box::new(i))
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

#[cfg(test)]
mod tests {
	extern crate env_logger;

	use log::LevelFilter;
	use std::{str, thread};
	use limited::Limited;
	use super::*;
	use tokio::{
		self,
		io::{copy, read_exact},
		net::{TcpListener, TcpStream},
		runtime::Runtime
	};

	const ADDRESS: &str = "127.0.0.1:12345";

	fn echo_server(lr: Limiter, lw: Limiter) -> Box<Future<Item=(), Error=()> + Send> {
		let srv = TcpListener::bind(&ADDRESS.parse().unwrap()).unwrap()
			.incoming()
			.map_err(|e| {
				eprintln!("accept failed = {:?}", e)
			})
			.for_each(move |s| {
				let (r, w) = s.split();
				let reader = Limited::new(r, lr.clone()).unwrap();
				let writer = Limited::new(w, lw.clone()).unwrap();
				let future = copy(reader, writer)
					.map(|_| ()) // TODO: print transfer rate
					.map_err(|e| {
						eprintln!("copy failed = {:?}", e)
					});
				tokio::spawn(future)
			});
		Box::new(srv)
	}

	fn echo_client() -> Box<Future<Item=(), Error=()> + Send> {
		let clt = TcpStream::connect(&ADDRESS.parse().unwrap())
			.and_then(|stream| {
				copy(&b"0123456789"[..], stream)
			})
			.and_then(|(n, _, stream)| {
				read_exact(stream, vec![0; n as usize])
			})
			.map(|(_stream, buf)| {
				println!("{:?}", str::from_utf8(&buf))
			})
			.map_err(|e| {
				eprintln!("client error: {:?}", e)
			});
		Box::new(clt)
	}

	#[test]
	fn test1() {
		let _ = env_logger::Builder::from_default_env()
			.filter(Some("aio_limited"), LevelFilter::Trace)
			.try_init();

		let mut rt = Runtime::new().unwrap();
		let mut ex = rt.executor();

		let read_limiter = Limiter::new(&mut ex, 100).unwrap();
		let read_limiter2 = read_limiter.clone();

		let write_limiter = Limiter::new(&mut ex, 100).unwrap();
		let write_limiter2 = write_limiter.clone();

		thread::spawn(move || {
			ex.spawn(echo_server(read_limiter2, write_limiter2))
		});

		thread::sleep(Duration::from_secs(1));

		for _ in 0 .. 100 {
			rt.spawn(echo_client());
		}

		thread::sleep(Duration::from_secs(30));
		rt.shutdown_now().wait().unwrap()
	}
}
