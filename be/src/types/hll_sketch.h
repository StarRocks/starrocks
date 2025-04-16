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

#include <memory>
#include <string>

#include "datasketches/hll.hpp"
#include "runtime/memory/counting_allocator.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/mem_chunk_allocator.h"

namespace starrocks {

class Slice;

class DataSketchesHll {
public:
    using alloc_type = STLCountingAllocator<uint8_t>;
    using hll_sketch_type = datasketches::hll_sketch_alloc<alloc_type>;
    using hll_union_type = datasketches::hll_union_alloc<alloc_type>;
    // default lg_k value for HLL
    static const datasketches::target_hll_type DEFAULT_HLL_TGT_TYPE = datasketches::HLL_6;

    explicit DataSketchesHll(uint8_t log_k, datasketches::target_hll_type tgt_type, int64_t* memory_usage)
            : _memory_usage(memory_usage), _tgt_type(tgt_type) {
        this->_sketch_union = std::make_unique<hll_union_type>(log_k, alloc_type(_memory_usage));
    }

    DataSketchesHll(const DataSketchesHll& other) = delete;
    DataSketchesHll& operator=(const DataSketchesHll& other) = delete;

    DataSketchesHll(DataSketchesHll&& other) noexcept
            : _memory_usage(std::move(other._memory_usage)),
              _sketch_union(std::move(other._sketch_union)),
              _tgt_type(other._tgt_type) {}
    DataSketchesHll& operator=(DataSketchesHll&& other) noexcept {
        if (this != &other) {
            this->_memory_usage = std::move(other._memory_usage);
            this->_sketch_union = std::move(other._sketch_union);
            this->_tgt_type = other._tgt_type;
        }
        return *this;
    }

    explicit DataSketchesHll(const Slice& src, int64_t* memory_usage);

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
    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

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
    void clear() {
        if (_sketch_union != nullptr) {
            _sketch_union->reset();
            _is_changed = true; // Mark as changed after reset
        }
    }

    // get hll_sketch object which is lazy initialized
    hll_sketch_type* get_hll_sketch() const {
        if (_is_changed) {
            if (_sketch_union == nullptr) {
                return nullptr;
            }
            _sketch = std::make_unique<hll_sketch_type>(_sketch_union->get_result(_tgt_type));
            _is_changed = false;
        }
        return _sketch.get();
    }

    inline void mark_changed() { _is_changed = true; }

private:
    int64_t* _memory_usage;
    std::unique_ptr<hll_union_type> _sketch_union = nullptr;
    datasketches::target_hll_type _tgt_type = DEFAULT_HLL_TGT_TYPE;
    // lazy value of union state
    mutable std::unique_ptr<hll_sketch_type> _sketch = nullptr;
    mutable bool _is_changed = true;
};

} // namespace starrocks
