// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Panic boundary for FFI entry points.
//!
//! Rust panics unwinding into C++ are undefined behavior. Every `extern "C"`
//! function in this crate that returns a `RustResult` MUST run its body
//! through `catch_ffi`; the helper turns a panic into `RustResult::err("rust
//! panic: ...")` so C++ sees a well-formed error instead of an abort.
//!
//! Free functions (returning unit) wrap their body manually with
//! `std::panic::catch_unwind` and ignore the result — a panic during free is
//! swallowed because there's no place to report it and aborting is worse.

use crate::ffi::result::RustResult;

/// Run `f` in a panic-catching boundary. Returns `f`'s `RustResult` on normal
/// completion; on panic, returns a `RustResult::err` with the panic payload.
///
/// `AssertUnwindSafe` is sound here because the FFI ABI dictates that a
/// `success=false` result invalidates everything the call could have produced
/// — the C++ caller must not observe partially-mutated Rust state.
pub fn catch_ffi<F: FnOnce() -> RustResult>(f: F) -> RustResult {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                format!("rust panic: {s}")
            } else if let Some(s) = payload.downcast_ref::<String>() {
                format!("rust panic: {s}")
            } else {
                "rust panic: <opaque payload>".to_string()
            };
            RustResult::err(msg)
        }
    }
}
