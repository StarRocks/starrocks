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

#pragma once

namespace starrocks {

class ThreadPool;

// Register the BE thread pools that back tantivy's background-work executor.
//
// Call exactly once at BE startup, after both pools are built and before any
// tantivy index writer is created. This installs C submit/join callbacks into
// the Rust binding (via `tantivy_binding_init_thread_pool`) so tantivy's
// background work runs on these pools instead of tantivy-spawned threads. Each
// task runs with the submitting thread's mem tracker restored, keeping
// `consume`/`release` on the same tracker.
//
// Two pools are required because tantivy mixes two incompatible task classes:
//
//   * `build_pool` runs the RESIDENT indexing workers (joinable submits) that
//     turn documents into in-memory segments. Each worker holds its thread from
//     writer creation until commit and blocks on a maintenance task, so it must
//     be an ELASTIC pool that grows a thread per worker on demand — putting these
//     on a bounded pool deadlocks once the number of live workers reaches the
//     pool size (a queued worker never runs, so the commit that joins it, and
//     the flush that feeds it, hang forever).
//
//   * `merge_pool` runs the TRANSIENT segment-updater serial queue (add_segment)
//     and merges (detached submits). These never block on another task in the
//     pool, so a bounded pool always drains and safely caps merge concurrency.
//
// Both pools must outlive all tantivy index writing (they are owned by ExecEnv).
void register_tantivy_index_thread_pool(ThreadPool* build_pool, ThreadPool* merge_pool);

} // namespace starrocks
