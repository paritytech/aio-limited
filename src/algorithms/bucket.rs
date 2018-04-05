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

use error::{Error, Result};
use parking_lot::Mutex;
use std::cmp::min;
use std::sync::atomic::{AtomicUsize, Ordering};
use super::{Id, Token};

/// A bucket has a certain capacity which is made available as `Token`s
/// containing quantities equal to capacity divided by parts.
///
/// With every part added, future `get` calls will return tokens with a
/// quantity equal to capacity / (parts + 1).
///
/// While the available capacity can not be blocked by inactive parts, i.e.
/// those which do not call `get`, it requires more `get` calls to retrieve
/// all available capacity which slows down active parts.
///
/// TODO: In order to avoid continuous slowdown in the rate limiter itself,
/// track usage per part and remove stale parts if necessary.
pub struct Bucket {
    maximum: usize,     // maximum capacity
    idgen: AtomicUsize, // id generator
    capacity: Mutex<Capacity>,
}

#[derive(Debug)]
struct Capacity {
    index: usize, // time index
    value: usize, // capacity value
    parts: usize, // parts over which to spread the available capacity
}

impl Bucket {
    /// Create a new bucket with the given maximum capacity.
    pub fn new(capacity: usize) -> Bucket {
        Bucket {
            maximum: capacity,
            idgen: AtomicUsize::new(1),
            capacity: Mutex::new(Capacity {
                index: 0,
                value: capacity,
                parts: 0,
            }),
        }
    }

    /// Get a `Token` which contains as quantity the number of items of
    /// the remaining capacity divided by parts.
    pub fn get(&self, id: Id, hint: usize) -> Result<Token> {
        let mut cap = self.capacity.lock();

        // no parts => always at full capacity
        if cap.parts == 0 {
            return Ok(Token::new(cap.index, self.maximum));
        }

        let quant = match cap.value / cap.parts {
            0 if cap.value > 0 => 1,
            x => min(x, hint),
        };

        trace!(
            "{}: {:?}, hint = {}, quantity = {}",
            id.0,
            *cap,
            hint,
            quant
        );

        if quant == 0 {
            return Err(Error::NoCapacity);
        }

        cap.value -= quant;
        let t = Token::new(cap.index, quant);
        cap.unlock_fair();
        Ok(t)
    }

    /// Give back the reviously retrieved `Token` which increases available
    /// capacity. Tokens which have expired will not be considered.
    pub fn release(&self, t: Token) {
        let mut cap = self.capacity.lock();
        if t.index == cap.index {
            cap.value += t.get()
        }
    }

    /// Reset the time index and make the maximum capacity available again.
    pub fn reset(&self, i: usize) {
        let mut cap = self.capacity.lock();
        cap.index = i;
        cap.value = self.maximum
    }

    /// Attempt to increase the number of parts by one.
    /// This can fail if it would result in more parts than the maximum capacity.
    pub fn add_part(&self) -> Result<Id> {
        let mut cap = self.capacity.lock();
        if cap.parts >= self.maximum {
            return Err(Error::NoCapacity);
        }
        cap.parts += 1;
        Ok(Id(self.idgen.fetch_add(1, Ordering::Relaxed)))
    }

    /// Remove a previously added part again.
    pub fn remove_part(&self, _id: Id) {
        let mut cap = self.capacity.lock();
        if cap.parts > 0 {
            cap.parts -= 1
        }
    }
}
