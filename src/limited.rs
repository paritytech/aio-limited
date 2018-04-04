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

use algorithms::Id;
use error::{Error, Result};
use limiter::Limiter;
use std::cmp::min;
use std::io;
use tokio::prelude::*;

/// A rate-limited resource.
pub struct Limited<T> {
    id: Id,
    io: T,
    lim: Limiter,
}

impl<T> Limited<T> {
    pub fn new(io: T, lim: Limiter) -> Result<Limited<T>> {
        let id = lim.register()?;
        Ok(Limited { id, io, lim })
    }
}

impl<T> Drop for Limited<T> {
    fn drop(&mut self) {
        self.lim.deregister(self.id)
    }
}

impl<T: AsyncRead> io::Read for Limited<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("{}: read, buf len = {}", self.id, buf.len());
        match self.lim.get(self.id, buf.len()) {
            Ok(mut t) => {
                let n = t.get();
                let k = min(buf.len(), n);
                trace!("{}: reading {} bytes (allowed {})", self.id, k, n);
                match self.io.read(&mut buf[0..k]) {
                    Err(e) => {
                        trace!("{}: read i/o error: {}", self.id, e);
                        Err(e)
                    }
                    Ok(m) => {
                        t.set(n - m);
                        trace!("{}: read {} bytes (returning {})", self.id, m, n - m);
                        self.lim.release(t);
                        Ok(m)
                    }
                }
            }
            Err(Error::NoCapacity) => {
                trace!("{}: read throttled", self.id);
                self.lim.enqueue(self.id);
                Err(io::Error::new(io::ErrorKind::WouldBlock, "rate limited"))
            }
            Err(Error::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

impl<T: AsyncRead> AsyncRead for Limited<T> {}

impl<T: io::Write> io::Write for Limited<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("{}: write, buf len = {}", self.id, buf.len());
        match self.lim.get(self.id, buf.len()) {
            Ok(mut t) => {
                let n = t.get();
                let k = min(buf.len(), n);
                trace!("{}: writing {} bytes (allowed {})", self.id, k, n);
                match self.io.write(&buf[0..k]) {
                    Err(e) => {
                        trace!("{}: write i/o error: {}", self.id, e);
                        Err(e)
                    }
                    Ok(m) => {
                        t.set(n - m);
                        trace!("{}: wrote {} bytes (returning {})", self.id, m, n - m);
                        self.lim.release(t);
                        Ok(m)
                    }
                }
            }
            Err(Error::NoCapacity) => {
                trace!("{}: write throttled", self.id);
                self.lim.enqueue(self.id);
                Err(io::Error::new(io::ErrorKind::WouldBlock, "rate limited"))
            }
            Err(Error::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.io.flush()
    }
}

impl<T: AsyncWrite> AsyncWrite for Limited<T> {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.io.shutdown()
    }
}
