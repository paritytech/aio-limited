// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use std::fmt;

pub mod bucket;

/// An opaque ID used for registration purposes.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(usize);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A Token represents an indexed quantity.
pub struct Token {
    index: usize,
    quant: usize,
}

impl Token {
    /// Create a new token with the given index and quantity
    fn new(index: usize, quant: usize) -> Token {
        Token { index, quant }
    }

    /// Get this token's quantity.
    pub fn get(&self) -> usize {
        self.quant
    }

    /// Reduce this token's quantity to the given value.
    ///
    /// If the argument is greater than or equal to the current quantity,
    /// this will be a no-op.
    pub fn set(&mut self, q: usize) {
        if q < self.quant {
            self.quant = q
        }
    }
}
