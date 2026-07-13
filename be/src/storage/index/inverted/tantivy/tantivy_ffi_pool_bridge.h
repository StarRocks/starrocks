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

// Register the shared BE thread pool as tantivy's background-work executor.
//
// Call exactly once at BE startup, after `pool` is built and before any tantivy
// index writer is created. This installs C submit/join callbacks into the Rust
// binding (via `tantivy_binding_init_thread_pool`) so tantivy's indexing
// workers, serial segment-updater queue, and merges all run on `pool` instead
// of tantivy-spawned threads. Each task runs with the submitting thread's mem
// tracker restored, keeping `consume`/`release` on the same tracker.
//
// `pool` must outlive all tantivy index writing (it is owned by ExecEnv).
void register_tantivy_index_thread_pool(ThreadPool* pool);

} // namespace starrocks
