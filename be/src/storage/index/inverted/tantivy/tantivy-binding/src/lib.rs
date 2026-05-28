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

//! StarRocks ⇄ tantivy FFI binding crate.
//!
//! Exposes a C ABI that the StarRocks BE (C++17) consumes to drive tantivy
//! indexes. surface: create writer → batch add strings → commit → load
//! reader → term / match-any / match-all / phrase queries → free handles.
//!
//! Module layout:
//!   - `error`  — internal error type and `Result<T>` alias.
//!   - `safe`   — safe Rust wrappers around tantivy. No `unsafe` here.
//!   - `ffi`    — extern "C" thunks. All `unsafe` lives here.
//!
//! Memory accounting: this crate intentionally pins the global allocator to
//! `std::alloc::System` so every Rust allocation routes through libc
//! malloc/free, which BE rebinds to its own `my_malloc/my_free` via mem_hook
//! `__attribute__((alias))`. That hook updates `tls_thread_status`, so the
//! BE-side `SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER` set before each FFI call
//! transparently captures every byte allocated in here. **Do not** introduce
//! `jemallocator` / `mimalloc` / `tikv-jemallocator` — they replace the Rust
//! allocator and break the integration. CI's `deny.toml` blocks them.
#[global_allocator]
static GLOBAL: std::alloc::System = std::alloc::System;

pub mod error;
pub mod ffi;
pub mod safe;
