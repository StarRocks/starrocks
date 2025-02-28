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

#include "runtime/memory/counting_allocator.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "util/slice.h"

#undef IS_BIG_ENDIAN
#include <datasketches/theta_sketch.hpp>
#include <datasketches/theta_union.hpp>

namespace starrocks {

class DataSketchesTheta {
public:
    using alloc_type = STLCountingAllocator<uint64_t>;
    using theta_sketch_type = datasketches::update_theta_sketch_alloc<alloc_type>;
    using theta_union_type = datasketches::theta_union_alloc<alloc_type>;
    using theta_wrapped_type = datasketches::wrapped_compact_theta_sketch_alloc<alloc_type>;
    using sketch_data_alloc_type = typename std::allocator_traits<alloc_type>::template rebind_alloc<uint8_t>;
    using sketch_data_type = std::vector<uint8_t, sketch_data_alloc_type>;

    explicit DataSketchesTheta(int64_t* memory_usage) : _memory_usage(memory_usage) {
        _sketch = std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
    }

    DataSketchesTheta(const DataSketchesTheta& other) = delete;
    DataSketchesTheta& operator=(const DataSketchesTheta& other) = delete;

    DataSketchesTheta(DataSketchesTheta&& other) noexcept
            : _memory_usage(std::move(other._memory_usage)), _sketch(std::move(other._sketch)) {
        if (other._sketch_union != nullptr) {
            this->_sketch_union = std::move(other._sketch_union);
        }
    }

    DataSketchesTheta& operator=(DataSketchesTheta&& other) noexcept {
        if (this != &other) {
            this->_memory_usage = std::move(other._memory_usage);
            this->_sketch = std::move(other._sketch);
            if (other._sketch_union != nullptr) {
                this->_sketch_union = std::move(other._sketch_union);
            }
        }
        return *this;
    }

    explicit DataSketchesTheta(const Slice& src, int64_t* memory_usage) : _memory_usage(memory_usage) {
        if (!deserialize(src)) {
            LOG(WARNING) << "Failed to init DataSketchesFrequent from slice, will be reset to 0.";
        }
    }

    ~DataSketchesTheta() = default;

    void update(uint64_t hash_value);

    void merge(const DataSketchesTheta& other);

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    size_t serialize(uint8_t* dst) const;

    uint64_t serialize_size() const;

    void serialize_if_needed() const;

    bool deserialize(const Slice& slice);

    static bool is_valid(const Slice& slice) {
        if (slice.size < 1) {
            return false;
        }
        return true;
    }

    int64_t estimate_cardinality() const;

    void clear();

private:
    int64_t* _memory_usage;
    mutable std::unique_ptr<theta_sketch_type> _sketch = nullptr;
    mutable std::unique_ptr<theta_union_type> _sketch_union = nullptr;
    mutable std::unique_ptr<sketch_data_type> _sketch_data = nullptr;
    mutable bool _is_changed = true;
};

} // namespace starrocks
