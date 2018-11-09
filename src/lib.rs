// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

extern crate futures;
extern crate log;
extern crate parking_lot;
extern crate tokio_executor;
extern crate tokio_io;
extern crate tokio_timer;

#[cfg(test)]
extern crate tokio;

mod algorithms;
mod error;
mod limited;
mod limiter;

pub use crate::error::Error;
pub use crate::limited::Limited;
pub use crate::limiter::Limiter;
