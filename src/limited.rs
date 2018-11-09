// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{algorithms::Id, error::{Error, Result}, limiter::Limiter};
use futures::prelude::*;
use std::{cmp::min, io};
use tokio_io::{AsyncRead, AsyncWrite};

/// A rate-limited resource.
#[derive(Clone, Debug)]
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
        match self.lim.get(self.id, buf.len()) {
            Ok(mut t) => {
                let n = t.get();
                let k = min(buf.len(), n);
                match self.io.read(&mut buf[0..k]) {
                    Err(e) => Err(e),
                    Ok(m) => {
                        t.set(n - m);
                        self.lim.release(t);
                        Ok(m)
                    }
                }
            }
            Err(Error::NoCapacity) => {
                self.lim.enqueue(self.id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
        match self.lim.get(self.id, buf.len()) {
            Ok(mut t) => {
                let n = t.get();
                let k = min(buf.len(), n);
                match self.io.write(&buf[0..k]) {
                    Err(e) => Err(e),
                    Ok(m) => {
                        t.set(n - m);
                        self.lim.release(t);
                        Ok(m)
                    }
                }
            }
            Err(Error::NoCapacity) => {
                self.lim.enqueue(self.id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
