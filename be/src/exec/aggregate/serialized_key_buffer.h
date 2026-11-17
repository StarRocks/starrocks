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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>

#include "base/bit/bit_util.h"
#include "base/string/slice.h"
#include "column/column.h"
#include "column/column_hash.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "runtime/mem_pool.h"

namespace starrocks {

// Staging buffer a serialized-key aggregate hash map/set builds one chunk's keys in.
//
// Rows are packed back to back. A size pass records each row's exact encoded size
// (Column::serialize_size_compact_batch()), an exclusive prefix sum turns the sizes into row
// offsets, and each key column's serialize_batch() then writes at those offsets with
// max_one_row_size = 0. The buffer therefore holds the chunk's actual key bytes plus SIMD
// padding, instead of max_row_size * chunk_size: one long row no longer spreads every other row
// of the chunk -- and, with a running maximum, of every later chunk -- over a buffer sized for it.
class SerializedKeyBuffer {
public:
    // A chunk whose keys exceed this goes through the caller's per-row path instead. It keeps every
    // batched row below the uint32 sizes serialize_batch() works in, and keeps keys + padding below
    // 2^31 so MemPool's power-of-two rounding never turns a 2 GiB batch into a 4 GiB chunk.
    static constexpr size_t kMaxBatchBytes = std::numeric_limits<int32_t>::max() - SLICE_MEMEQUAL_OVERFLOW_PADDING;

    SerializedKeyBuffer() : _pool(std::make_unique<MemPool>()) {}

    // Serializes rows [0, chunk_size) of `key_columns`. Returns false when the chunk must take the
    // per-row path; key() is then not valid for this chunk, and max_row_size() tells the caller
    // how large its per-row buffer has to be.
    bool serialize(const Columns& key_columns, size_t chunk_size) {
        _batched = false;
        // All key columns fixed-width: every row has the same size, so skip the size pass and lay the
        // rows out at that stride -- exactly the layout the max-row-size stride used to produce.
        uint64_t fixed_row_size = 0;
        for (const auto& key_column : key_columns) {
            const uint32_t size = key_column->serialize_batch_fixed_row_size();
            if (size == 0) {
                fixed_row_size = 0;
                break;
            }
            fixed_row_size += size;
        }
        if (fixed_row_size != 0) {
            return _serialize_fixed_rows(key_columns, chunk_size, fixed_row_size);
        }

        // The size pass sums into 64 bits, so no row size can wrap and no separate
        // max_one_element_serialize_size() pass is needed to rule that out.
        _row_sizes.assign(chunk_size, 0);
        for (const auto& key_column : key_columns) {
            key_column->serialize_size_compact_batch(_row_sizes, chunk_size);
        }
        size_t total = 0;
        size_t max_row = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            total += _row_sizes[i];
            max_row = std::max<size_t>(max_row, _row_sizes[i]);
        }
        _max_row_size = max_row;
        if (UNLIKELY(total > _max_batch_bytes)) {
            return false;
        }

        // Exclusive prefix sum: _ends[i] becomes row i's start offset. serialize_batch() then
        // advances each entry by the bytes it writes, leaving row i's end offset behind. Every
        // value fits: total <= kMaxBatchBytes.
        _ends.resize(chunk_size);
        uint32_t offset = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            _ends[i] = offset;
            offset += static_cast<uint32_t>(_row_sizes[i]);
        }

        _reserve(total + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(_data, _ends, chunk_size, 0);
        }

#ifndef NDEBUG
        // A column whose serialize_size_compact_batch() disagrees with its serialize_batch()
        // would make adjacent rows overwrite each other; catch it where it happens.
        for (size_t i = 0; i < chunk_size; ++i) {
            const uint32_t begin = i == 0 ? 0 : static_cast<uint32_t>(_ends[i - 1]);
            DCHECK_EQ(_ends[i] - begin, _row_sizes[i]) << "row " << i << " size pass disagrees with serialize_batch()";
        }
#endif
        _batched = true;
        return true;
    }

    // The largest row of the chunk serialize() last saw, whether or not it was batched.
    size_t max_row_size() const { return _max_row_size; }

    // Whether the last serialize() accepted its chunk, i.e. whether key() is valid.
    bool batched() const { return _batched; }

    // Row i of the chunk serialize() last accepted.
    Slice key(size_t i) const {
        DCHECK(_batched);
        const uint32_t begin = i == 0 ? 0 : _ends[i - 1];
        return {_data + begin, _ends[i] - begin};
    }

    // A buffer for one row of at most `row_bytes`, for the caller's per-row path.
    uint8_t* row_buffer(size_t row_bytes) {
        _reserve(row_bytes + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        return _data;
    }

    size_t capacity() const { return _capacity; }

    // Lets a test reach the per-row path without a multi-GiB chunk.
    void set_max_batch_bytes_for_test(size_t bytes) { _max_batch_bytes = bytes; }

private:
    bool _serialize_fixed_rows(const Columns& key_columns, size_t chunk_size, uint64_t row_size) {
        _max_row_size = row_size;
        const uint64_t total = row_size * chunk_size;
        if (UNLIKELY(total > _max_batch_bytes)) {
            return false;
        }
        _ends.resize(chunk_size);
        const auto stride = static_cast<uint32_t>(row_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            _ends[i] = static_cast<uint32_t>(i) * stride;
        }
        _reserve(total + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(_data, _ends, chunk_size, 0);
        }
        DCHECK(chunk_size == 0 || _ends[chunk_size - 1] == total) << "fixed row size disagrees with serialize_batch()";
        _batched = true;
        return true;
    }

    void _reserve(size_t bytes) {
        if (LIKELY(bytes <= _capacity)) {
            return;
        }
        // Allocate the power of two MemPool would round the chunk up to anyway, so the whole chunk
        // is ours: that doubles capacity per step without rounding up a second time
        // (1.5 GiB -> 2.25 -> 4 GiB), and never touches reserved-but-unallocated bytes, which ASAN
        // keeps poisoned. free_all(), not clear(): clear() would keep every chunk ever reserved.
        const size_t capacity = BitUtil::RoundUpToPowerOfTwo(bytes);
        _pool->free_all();
        _data = _pool->allocate(static_cast<int64_t>(capacity));
        _capacity = capacity;
    }

    std::unique_ptr<MemPool> _pool;
    uint8_t* _data = nullptr;
    size_t _capacity = 0;
    size_t _max_batch_bytes = kMaxBatchBytes;
    bool _batched = false;
    size_t _max_row_size = 0;
    Buffer<uint64_t> _row_sizes;
    Buffer<uint32_t> _ends;
};

} // namespace starrocks
