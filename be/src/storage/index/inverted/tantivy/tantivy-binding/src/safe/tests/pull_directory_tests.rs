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

//! Boundary-check tests for `PullFileHandle::read_bytes`. The handle calls
//! into BE via FFI, so it cannot be exercised end-to-end here — instead we
//! test the pure `validate_read_range` helper that guards the FFI call.

use std::io::ErrorKind;

use crate::safe::pull_directory::validate_read_range;

#[test]
fn empty_range_at_zero_is_ok() {
    assert!(validate_read_range(&(0..0), 0).is_ok());
    assert!(validate_read_range(&(0..0), 100).is_ok());
}

#[test]
fn empty_range_at_end_is_ok() {
    assert!(validate_read_range(&(100..100), 100).is_ok());
}

#[test]
fn full_range_is_ok() {
    assert!(validate_read_range(&(0..100), 100).is_ok());
}

#[test]
fn middle_range_is_ok() {
    assert!(validate_read_range(&(10..50), 100).is_ok());
}

#[test]
fn end_past_file_len_rejected() {
    let err = validate_read_range(&(0..101), 100).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("out of bounds"));
}

#[test]
fn start_past_file_len_rejected() {
    let err = validate_read_range(&(101..200), 100).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("out of bounds"));
}

#[test]
fn inverted_range_rejected() {
    // Range::end < Range::start would underflow `range.end - range.start`.
    let range = std::ops::Range { start: 50usize, end: 10usize };
    let err = validate_read_range(&range, 100).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("inverted range"));
}

#[test]
fn empty_file_rejects_nonzero_read() {
    let err = validate_read_range(&(0..1), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}
