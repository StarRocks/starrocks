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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/hll.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cmath>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "datasketches/hll.hpp"
#include "gutil/macros.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "types/constexpr.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class Slice;

// Hyperloglog distinct estimate algorithm.
// See these papers for more details.
// 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
// algorithm (2007)
// 2) HyperLogLog in Practice (paper from google with some improvements)

// Each HLL value is a set of values. To save space, StarRocks store HLL value
// in different format according to its cardinality.
//
// HLL_DATA_EMPTY: when set is empty.
//
// HLL_DATA_EXPLICIT: when there is only few values in set, store these values explicit.
// If the number of hash values is not greater than 160, set is encoded in this format.
// The max space occupied is (1 + 1 + 160 * 8) = 1282. I don't know why 160 is choosed,
// maybe can be other number. If you are interested, you can try other number and see
// if it will be better.
//
// HLL_DATA_SPARSE: only store non-zero registers. If the number of non-zero registers
// is not greater than 4096, set is encoded in this format. The max space occupied is
// (1 + 4 + 3 * 4096) = 12293.
//
// HLL_DATA_FULL: most space-consuming, store all registers
//
// A HLL value will change in the sequence empty -> explicit -> sparse -> full, and not
// allow reverse.
//
// NOTE: This values are persisted in storage devices, so don't change exist
// enum values.
enum HllDataType {
    HLL_DATA_EMPTY = 0,
    HLL_DATA_EXPLICIT = 1,
    HLL_DATA_SPARSE = 2,
    HLL_DATA_FULL = 3,
};

class HyperLogLog {
public:
    HyperLogLog() = default;
    HyperLogLog(const HyperLogLog& other);

    HyperLogLog& operator=(const HyperLogLog& other);

    HyperLogLog(HyperLogLog&& other) noexcept;

    HyperLogLog& operator=(HyperLogLog&& other) noexcept;

    explicit HyperLogLog(uint64_t hash_value);

    explicit HyperLogLog(const Slice& src);

    ~HyperLogLog();

    typedef uint8_t SetTypeValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // Add a hash value to this HLL value
    // NOTE: input must be a hash_value
    void update(uint64_t hash_value);

    void merge(const HyperLogLog& other);

    // Return max size of serialized binary
    size_t max_serialized_size() const;

    // Input slice should has enough capacity for serialize, which
    // can be get through max_serialized_size(). If insufficient buffer
    // is given, this will cause process crash.
    // Return actual size of serialized binary.
    size_t serialize(uint8_t* dst) const;

    // Now, only empty HLL support this funciton.
    bool deserialize(const Slice& slice);

    int64_t estimate_cardinality() const;

    static std::string empty();

    // Check if input slice is a valid serialized binary of HyperLogLog.
    // This function only check the encoded type in slice, whose complex
    // function is O(1).
    static bool is_valid(const Slice& slice);

    // only for debug
    std::string to_string() const;

    uint64_t serialize_size() const { return max_serialized_size(); }

    uint64_t mem_usage() const { return max_serialized_size(); }

    // common interface
    void clear();

private:
    using ElementSet = phmap::flat_hash_set<uint64_t>;

    HllDataType _type = HLL_DATA_EMPTY;
    // Use raw object instead of pointer to give a chance to create multiple
    // _hash_sets of a HLL column in only one memory allocation,
    // eg. by std::vector<ObjectColumn<HyperLogLog>>::resize/reserve.
    ElementSet _hash_set;

    // This field is much space consumming(HLL_REGISTERS_COUNT), we create
    // it only when it is really needed.
    // Allocate memory by MemChunkAllocator in order to reuse memory.
    MemChunk _registers;

private:
    void _convert_explicit_to_register();

    // update one hash value into this registers
    void _update_registers(uint64_t hash_value) {
        // Use the lower bits to index into the number of streams and then
        // find the first 1 bit after the index bits.
        int idx = hash_value % HLL_REGISTERS_COUNT;
        hash_value >>= HLL_COLUMN_PRECISION;
        // make sure max first_one_bit is HLL_ZERO_COUNT_BITS + 1
        hash_value |= ((uint64_t)1 << HLL_ZERO_COUNT_BITS);
        auto first_one_bit = (uint8_t)(__builtin_ctzl(hash_value) + 1);
        _registers.data[idx] = std::max((uint8_t)_registers.data[idx], first_one_bit);
    }
};

class DataSketchesHll {
public:
    // default lg_k value for HLL
    static const datasketches::target_hll_type DEFAULT_HLL_TGT_TYPE = datasketches::HLL_6;

    explicit DataSketchesHll(uint8_t log_k, datasketches::target_hll_type tgt_type) : _tgt_type(tgt_type) {
        this->_sketch_union = std::make_unique<datasketches::hll_union>(log_k);
    }

    DataSketchesHll(const DataSketchesHll& other) = delete;
    DataSketchesHll& operator=(const DataSketchesHll& other) = delete;

    DataSketchesHll(DataSketchesHll&& other) noexcept
            : _sketch_union(std::move(other._sketch_union)), _tgt_type(other._tgt_type) {}
    DataSketchesHll& operator=(DataSketchesHll&& other) noexcept {
        if (this != &other) {
            this->_sketch_union = std::move(other._sketch_union);
            this->_tgt_type = other._tgt_type;
        }
        return *this;
    }

    explicit DataSketchesHll(const Slice& src);

    ~DataSketchesHll() = default;

    // Returns sketch's configured lg_k value.
    uint8_t get_lg_config_k() const {
        if (UNLIKELY(_sketch_union == nullptr)) {
            return DEFAULT_HLL_LOG_K;
        }
        return _sketch_union->get_lg_config_k();
    }

    // Returns the sketch's target HLL mode (from #target_hll_type).
    datasketches::target_hll_type get_target_type() const {
        if (UNLIKELY(_sketch_union == nullptr)) {
            return DEFAULT_HLL_TGT_TYPE;
        }
        return _sketch_union->get_target_type();
    }

    // Add a hash value to this HLL value
    // NOTE: input must be a hash_value
    void update(uint64_t hash_value);

    // merge with other HLL value
    void merge(const DataSketchesHll& other);

    // Return max size of serialized binary
    size_t max_serialized_size() const;
    // use max_serialized_size as estimate memory usage
    uint64_t mem_usage() const { return max_serialized_size(); }

    // Input slice should have enough capacity for serialize, which
    // can be got through max_serialized_size(). If insufficient buffer
    // is given, this will cause process crash.
    // Return actual size of serialized binary.
    size_t serialize(uint8_t* dst) const;

    // Now, only empty HLL support this funciton.
    bool deserialize(const Slice& slice);

    int64_t estimate_cardinality() const;

    // No need to check is_valid for datasketches HLL,
    // return ture for compatibility.
    static bool is_valid(const Slice& slice);

    // only for debug
    std::string to_string() const;

    uint64_t serialize_size() const;

    // common interface
    void clear() { _sketch_union->reset(); }

    // get hll_sketch object which is lazy initialized
    datasketches::hll_sketch* get_hll_sketch() const {
        if (_is_changed) {
            _sketch = std::make_unique<datasketches::hll_sketch>(_sketch_union->get_result(_tgt_type));
            _is_changed = false;
        }
        return _sketch.get();
    }
    inline void mark_changed() { _is_changed = true; }

private:
    std::unique_ptr<datasketches::hll_union> _sketch_union = nullptr;
    datasketches::target_hll_type _tgt_type = DEFAULT_HLL_TGT_TYPE;
    // lazy value of union state
    mutable std::unique_ptr<datasketches::hll_sketch> _sketch = nullptr;
    mutable bool _is_changed = true;
};

} // namespace starrocks
