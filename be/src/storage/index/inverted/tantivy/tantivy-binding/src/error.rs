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

//! Internal error type for the binding crate.
//!
//! Anything fallible inside `safe::` returns `Result<T>`; the FFI layer
//! converts to a `RustResult` at the boundary so C++ never sees a Rust enum.

use std::fmt;

#[derive(Debug)]
pub enum TantivyBindingError {
    InvalidArgument(String),
    Io(std::io::Error),
    Tantivy(tantivy::TantivyError),
    Internal(String),
}

impl fmt::Display for TantivyBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Tantivy(e) => write!(f, "tantivy error: {e}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for TantivyBindingError {}

impl From<std::io::Error> for TantivyBindingError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<tantivy::TantivyError> for TantivyBindingError {
    fn from(value: tantivy::TantivyError) -> Self {
        Self::Tantivy(value)
    }
}

impl From<tantivy::directory::error::OpenDirectoryError> for TantivyBindingError {
    fn from(value: tantivy::directory::error::OpenDirectoryError) -> Self {
        Self::Internal(format!("open directory: {value}"))
    }
}

pub type Result<T> = std::result::Result<T, TantivyBindingError>;
