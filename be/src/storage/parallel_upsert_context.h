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

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "base/string/slice.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

namespace lake {
class ParallelTaskRunner;
}

// Declared rather than included: persistent_index.h forward-declares ParallelUpsertContext, so
// including it back here would be circular. Only used through a pointer.
struct IndexValue;

// Scratch for one chunk of a publish-phase index lookup.
//
// `pk_column` owns the encoded key bytes that `keys` points into, so the slot has to outlive every
// task that reads them -- which is why callers keep a vector of slots alive across the whole loop
// rather than reusing one. Only the fields a given path needs are populated: the upsert chain fills
// all four, a read-only lookup fills `pk_column` and `old_values`.
struct ParallelPublishSlot {
    MutableColumnPtr pk_column;       // Encoded primary key column for this chunk
    Buffer<Slice> keys;               // Slices into `pk_column`'s bytes
    std::vector<uint64_t> values;     // New rss_rowids being written
    std::vector<uint64_t> old_values; // rss_rowids this upsert replaced, or NullIndexValue
};

// Where a parallel publish's upsert chain sends the rowids it replaces.
//
// The upsert chain is the one parallel step whose fan-out cannot be a plain local
// lake::ParallelTaskRunner. The caller -- LakePersistentIndex::parallel_upsert, or the condition-update
// upsert phase -- drives the chunk loop and the join, but the work is submitted four layers down in
// LakePersistentIndex::upsert, because that is where the SST / inactive-memtable lookup happens and
// therefore where the replaced old values first become known. This object is what those two ends
// share.
//
// It carries exactly two things: the runner to submit to, and the delete map to append to. It owns
// its own mutex instead of borrowing one from a caller's stack frame, and it has no Status -- the
// runner aggregates that.
class ParallelUpsertContext {
public:
    using DeletesMap = std::unordered_map<uint32_t, std::vector<uint32_t>>;

    // `runner` may be null, meaning the lookup runs synchronously inside the upsert call and the
    // caller neither joins nor flushes. Both `runner` and `deletes` must outlive this context.
    ParallelUpsertContext(lake::ParallelTaskRunner* runner, DeletesMap* deletes) : _runner(runner), _deletes(deletes) {}

    ParallelUpsertContext(const ParallelUpsertContext&) = delete;
    ParallelUpsertContext& operator=(const ParallelUpsertContext&) = delete;

    // The single source of truth for "did the lookup go async?". Two things hang off it and MUST
    // agree: whether the caller has to join() and flush_memtable() afterwards, and who appends the
    // replaced rowids -- the task does when deferred, the synchronous path does when not. They used
    // to be two independent `token == nullptr` checks several layers apart, and disagreeing would
    // have appended every replaced rowid twice.
    bool defers_lookup() const { return _runner != nullptr; }

    lake::ParallelTaskRunner* runner() const { return _runner; }

    // Append every non-null rss_rowid to the delete map. Safe to call from a worker thread.
    //
    // EXACTLY ONE caller may do this per upsert, and it is PersistentIndex::upsert -- the layer that
    // fills old_values, and therefore the only one that knows when they are complete. The local
    // implementation appends before returning; LakePersistentIndex appends from inside its deferred
    // task when defers_lookup(). Appending again from a caller double-counts every replaced row,
    // which surfaces as "delvec inconsistent ... #add:2N #new:N" at publish.
    void add_replaced(const std::vector<uint64_t>& old_values);
    void add_replaced(const IndexValue* old_values, size_t n);

    // Append one (rssid, rowid) directly, for callers that already decoded it.
    void add_delete(uint32_t rssid, uint32_t rowid);

private:
    lake::ParallelTaskRunner* _runner;
    DeletesMap* _deletes;
    std::mutex _mutex;
};

} // namespace starrocks
