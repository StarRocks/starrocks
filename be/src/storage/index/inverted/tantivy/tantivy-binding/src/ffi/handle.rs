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

//! Generic opaque-handle helpers.
//!
//! `create_binding(value)` heap-boxes the value and returns an erased pointer
//! to be handed back across the FFI; `free_binding::<T>(ptr)` reverses the
//! operation. Pair these on every `tantivy_create_*` / `tantivy_free_*` FFI
//! function with the matching concrete type `T`.

use std::ffi::c_void;

/// Heap-allocate `value` and return a type-erased pointer the C++ side can
/// hand back to us later. Must be paired with `free_binding::<T>`.
pub fn create_binding<T>(value: T) -> *mut c_void {
    Box::into_raw(Box::new(value)) as *mut c_void
}

/// SAFETY:
///   - `ptr` MUST have been produced by `create_binding::<T>` for the same `T`.
///   - `ptr` MUST NOT have been freed already.
///   - After this call, `ptr` is dangling; the caller must not dereference it.
///   - A NULL `ptr` is a no-op (defensive, matches C double-free idioms).
pub unsafe fn free_binding<T>(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    drop(Box::from_raw(ptr as *mut T));
}

/// Cast an opaque FFI pointer back to a `&mut T`. Used by writer FFI.
/// Returns `None` for NULL.
///
/// SAFETY: same constraints as `free_binding<T>` regarding provenance.
pub unsafe fn as_mut<'a, T>(ptr: *mut c_void) -> Option<&'a mut T> {
    if ptr.is_null() {
        None
    } else {
        Some(&mut *(ptr as *mut T))
    }
}

/// Cast an opaque FFI pointer back to a shared `&T`. Used by reader/query FFI
/// where concurrent access is expected. Returns `None` for NULL.
///
/// SAFETY: same provenance constraints as `as_mut`. The caller must ensure
/// that no `&mut T` alias exists while the returned `&T` is live.
pub unsafe fn as_ref<'a, T>(ptr: *const c_void) -> Option<&'a T> {
    if ptr.is_null() {
        None
    } else {
        Some(&*(ptr as *const T))
    }
}
