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

use std::fmt;

pub mod bucket;

/// An opaque ID used for registration purposes.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
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

