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

//! C ABI shim wiring tantivy's background work onto a BE-owned thread pool.
//!
//! tantivy normally spawns its own OS threads for indexing workers and its own
//! rayon pools for the segment-updater serial queue and merges. That both
//! explodes the thread count (one set per `IndexWriter`) and breaks the BE's
//! per-thread memory accounting (documents are allocated on the caller thread
//! but freed on tantivy's worker thread, so `consume`/`release` land on
//! different mem trackers).
//!
//! With the [`tantivy::executor`] hook, all such work is instead submitted here
//! and forwarded to the shared BE `ThreadPool` via C callbacks. The BE side
//! captures the caller's thread-local mem tracker at submit time and restores it
//! on the pool thread before running the task, so allocation and deallocation
//! land on the same tracker (capture-at-submit + set-at-run).
//!
//! Ownership across the boundary: a tantivy task is a `Box<dyn FnOnce() + Send>`
//! (a fat pointer). We box it once more so it fits a single `*mut c_void`, hand
//! that raw pointer to the BE pool, and the BE pool later calls
//! [`tantivy_binding_run_pool_task`] on a pool thread to run + drop it. If the
//! pool is shut down with tasks still queued, the BE side must call
//! [`tantivy_binding_drop_pool_task`] on each to avoid leaking the closure.

use std::ffi::c_void;
use std::sync::Arc;

use tantivy::executor::{set_global_spawner, PooledJoinHandle, ThreadSpawner};

/// Submit a joinable task to the BE pool. Takes ownership of `task` (opaque
/// boxed closure). Returns an opaque handle to be passed to the join callback.
pub type TantivyPoolSubmitFn = unsafe extern "C" fn(task: *mut c_void) -> *mut c_void;

/// Submit a fire-and-forget task to the BE pool. Takes ownership of `task`.
pub type TantivyPoolSubmitDetachedFn = unsafe extern "C" fn(task: *mut c_void);

/// Block until the joinable task behind `handle` completes, then free `handle`.
pub type TantivyPoolJoinFn = unsafe extern "C" fn(handle: *mut c_void);

/// The boxed tantivy closure, double-boxed so a `Box<dyn FnOnce()>` fat pointer
/// fits in a single thin `*mut c_void`.
type BoxedTask = Box<dyn FnOnce() + Send>;

/// Run and drop a task previously handed to the BE pool. Called by the BE pool
/// on a pool thread (with the captured mem tracker already installed).
///
/// SAFETY: `task` must be a pointer produced by this crate's spawner and not
/// previously run or dropped.
#[no_mangle]
pub unsafe extern "C" fn tantivy_binding_run_pool_task(task: *mut c_void) {
    if task.is_null() {
        return;
    }
    // Never let a Rust panic unwind across the FFI boundary into C++.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let boxed: Box<BoxedTask> = Box::from_raw(task as *mut BoxedTask);
        (*boxed)();
    }));
}

/// Drop a queued task without running it. Called by the BE pool for tasks still
/// pending when the pool shuts down, so the boxed closure is not leaked.
///
/// SAFETY: same as [`tantivy_binding_run_pool_task`].
#[no_mangle]
pub unsafe extern "C" fn tantivy_binding_drop_pool_task(task: *mut c_void) {
    if task.is_null() {
        return;
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(Box::from_raw(task as *mut BoxedTask));
    }));
}

/// A [`ThreadSpawner`] backed by the BE C++ thread pool via C callbacks.
struct BeSpawner {
    submit: TantivyPoolSubmitFn,
    submit_detached: TantivyPoolSubmitDetachedFn,
    join: TantivyPoolJoinFn,
}

// The callbacks are plain C function pointers; sending/sharing them is safe.
unsafe impl Send for BeSpawner {}
unsafe impl Sync for BeSpawner {}

/// Join handle wrapping the opaque handle returned by the BE submit callback.
struct BeJoinHandle {
    handle: *mut c_void,
    join: TantivyPoolJoinFn,
}

// The opaque handle is owned by exactly one `BeJoinHandle` and only touched via
// the join callback, so moving it across threads is safe.
unsafe impl Send for BeJoinHandle {}

impl PooledJoinHandle for BeJoinHandle {
    fn join(self: Box<Self>) {
        unsafe { (self.join)(self.handle) }
    }
}

fn into_raw_task(task: BoxedTask) -> *mut c_void {
    Box::into_raw(Box::new(task)) as *mut c_void
}

impl ThreadSpawner for BeSpawner {
    fn spawn(&self, _name: String, task: BoxedTask) -> Box<dyn PooledJoinHandle> {
        let raw = into_raw_task(task);
        let handle = unsafe { (self.submit)(raw) };
        Box::new(BeJoinHandle {
            handle,
            join: self.join,
        })
    }

    fn spawn_detached(&self, _name: String, task: BoxedTask) {
        let raw = into_raw_task(task);
        unsafe { (self.submit_detached)(raw) }
    }
}

/// Install the BE thread pool as tantivy's global spawner. Call once at BE
/// startup, after the pool exists and before any tantivy index writer is
/// created. A later call replaces the previous spawner.
///
/// SAFETY: the three callbacks must be valid for the entire process lifetime
/// and implement the ownership contract described in this module.
#[no_mangle]
pub unsafe extern "C" fn tantivy_binding_init_thread_pool(
    submit: TantivyPoolSubmitFn,
    submit_detached: TantivyPoolSubmitDetachedFn,
    join: TantivyPoolJoinFn,
) {
    let _ = std::panic::catch_unwind(|| {
        set_global_spawner(Arc::new(BeSpawner {
            submit,
            submit_detached,
            join,
        }));
    });
}
