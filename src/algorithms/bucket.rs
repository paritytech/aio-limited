// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{algorithms::{Id, Token}, error::{Error, Result}};
use parking_lot::Mutex;
use std::{cmp::min, sync::atomic::{AtomicUsize, Ordering}};

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
// TODO: In order to avoid continuous slowdown in the rate limiter itself,
// track usage per part and remove stale parts if necessary.
#[derive(Debug)]
pub struct Bucket {
    maximum: usize, // maximum capacity
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
    pub fn get(&self, _id: Id, hint: usize) -> Result<Token> {
        let mut cap = self.capacity.lock();

        // no parts => always at full capacity
        if cap.parts == 0 {
            return Ok(Token::new(cap.index, self.maximum));
        }

        let quant = match cap.value / cap.parts {
            0 if cap.value > 0 => 1,
            x => min(x, hint),
        };

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
