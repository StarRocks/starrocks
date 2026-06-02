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
#include <optional>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "storage/primitive/chunk_iterator.h"
#include "storage/primitive/primary_key_encoding_types.h"

namespace starrocks::lake {

// One chunk emitted by SegmentPKIterator::current(), carrying chunk[0]'s
// position in the SOURCE SEGMENT FILE:
//
//   physical_rowid_offset — chunk[0]'s row position in the segment file;
//   chunk[i]'s physical rowid = physical_rowid_offset + i. For shared
//   (post-split) segments this skips the iterator's range_start; for
//   non-shared segments it is 0.
//
// Used by PK-index upserts, delete-bitmap writers, and any consumer that reads
// the full segment via Segment::open + fetch_values_by_rowid. The field is
// by-value so the ref is async-capture safe — lambdas that outlive iter.next()
// can rely on it without holding the iterator.
struct SegmentPKChunkRef {
    ChunkPtr chunk;
    uint32_t physical_rowid_offset = 0;
};

class SegmentPKIterator {
public:
    SegmentPKIterator() = default;
    ~SegmentPKIterator() { close(); }
    // If defer_data_load is true, only save parameters without loading the first chunk.
    // The first chunk will be loaded lazily on the first done()/next() call.
    // This avoids memory spikes when many iterators are created upfront.
    Status init(const ChunkIteratorPtr& iter, const Schema& pkey_schema, bool lazy_load,
                PrimaryKeyEncodingType encoding_type, bool defer_data_load = false);
    void next();
    bool done();
    Status status();
    void close();
    // Returns the most-recently-loaded chunk plus chunk[0]'s physical position
    // in the source segment (see SegmentPKChunkRef doc). The returned chunk
    // is moved out — done() will report empty until the next _load().
    SegmentPKChunkRef current();

    // The segment-wide physical rowid base: the first physical rowid the
    // underlying iterator emits (= range_start for shared post-split segments,
    // 0 otherwise, and 0 if the iterator emitted nothing). Constant — set once
    // on the first non-empty emit and never changed thereafter, in contrast to
    // the per-chunk SegmentPKChunkRef::physical_rowid_offset which advances by
    // chunk. Lets callers translate a row's logical emit offset to its physical
    // position in the segment file.
    uint32_t physical_rowid_base() const { return _physical_rowid_base.value_or(0); }

    // Return the memory usage of this encode pk column.
    // If _lazy_load is true, return 0, because memory allocation is lazy.
    size_t memory_usage() const { return _memory_usage; }

    // Encode `pk_column_chunk` to the given |pk_column|
    StatusOr<MutableColumnPtr> encoded_pk_column(const Chunk* chunk);

    const MutableColumnPtr& standalone_pk_column() const { return _standalone_pk_column; }

private:
    Status _load();

    // Iterator of this segment file.
    ChunkIteratorPtr _iter;
    // The PK schema of this segment file.
    Schema _pkey_schema;
    // status
    Status _status = Status::OK();
    // The current pk column index.
    size_t _current_pk_column_idx = 0;
    // The rowid offsets of each piece.
    // E.g. if we have column vec : 100 rows, 101 rows, 200 rows,
    // offset will be [0, 100, 201, 401]
    std::vector<size_t> _begin_rowid_offsets;
    // Current loaded row count of the segment.
    size_t _current_rows = 0;
    // If true, we will load segment peice by piece when needed.
    bool _lazy_load = false;
    // If true, first _load() is deferred until done() is first called.
    bool _defer_data_load = false;
    // If enable lazy load, `_memory_usage` will record first piece of pk column memory usage.
    size_t _memory_usage = 0;
    // For large segment, we need to load segment file piece by piece.
    ChunkUniquePtr _pk_column_chunk;
    // For no lazy load, we can load whole pk column and encode at once.
    MutableColumnPtr _standalone_pk_column;
    // First physical rowid emitted by the underlying iterator for this segment
    // (= iterator's range_start). Set once on the first non-empty emit and never
    // reset across subsequent _load() / next() calls. std::optional disambiguates
    // "not yet set" from the legitimate value 0 (non-shared segment iterating from
    // physical row 0).
    std::optional<uint32_t> _physical_rowid_base;
    // The encoding type of primary key.
    PrimaryKeyEncodingType _encoding_type = PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE;
};

using SegmentPKIteratorPtr = std::unique_ptr<SegmentPKIterator>;

} // namespace starrocks::lake
