// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use quick_error::quick_error;
use std::io;
use tokio_executor::SpawnError;

pub type Result<T> = std::result::Result<T, Error>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(e: io::Error) {
            cause(e)
            display("i/o error: {}", e)
            from()
        }
        Exec(e: SpawnError) {
            display("spawn error: {:?}", e)
            from()
        }
        NoCapacity {
            description("no capacity left")
        }
        TimerError {
            description("error executing background timer")
        }
        #[doc(hidden)]
        __Nonexhaustive
    }
}
