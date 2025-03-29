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

//  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };
#pragma push_macro("IS_BIG_ENDIAN")
#undef IS_BIG_ENDIAN
#include "datasketches/theta_sketch.hpp"
#include "datasketches/theta_union.hpp"
#pragma pop_macro("IS_BIG_ENDIAN")

namespace starrocks {

class DataSketchesTheta {
public:
    using alloc_type = STLCountingAllocator<uint64_t>;
    using theta_sketch_type = datasketches::update_theta_sketch_alloc<alloc_type>;
    using theta_union_type = datasketches::theta_union_alloc<alloc_type>;
    using theta_compact_sketch = datasketches::compact_theta_sketch_alloc<alloc_type>;
    using wrapped_compact_theta_sketch = datasketches::wrapped_compact_theta_sketch_alloc<alloc_type>;

    DataSketchesTheta(const DataSketchesTheta& other) = delete;
    DataSketchesTheta& operator=(const DataSketchesTheta& other) = delete;

    explicit DataSketchesTheta(int64_t* memory_usage) : _memory_usage(memory_usage) {}
    DataSketchesTheta& operator=(DataSketchesTheta&& other) noexcept {
        if (this != &other) {
            this->_memory_usage = other._memory_usage;
            this->_sketch_update = std::move(other._sketch_update);
            this->_sketch_union = std::move(other._sketch_union);
        }
        return *this;
    }
    explicit DataSketchesTheta(const Slice& src, int64_t* memory_usage);
    DataSketchesTheta(DataSketchesTheta&& other) noexcept : _memory_usage(other._memory_usage) {}

    ~DataSketchesTheta() = default;

    void update(uint64_t hash_value);
    void merge(const DataSketchesTheta& other);

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }
    size_t serialize(uint8_t* dst) const;
    uint64_t serialize_size() const;
    bool deserialize(const Slice& slice);

    int64_t estimate_cardinality() const;

    theta_sketch_type* get_sketch_update() {
        if (!_sketch_update) {
            _sketch_update =
                    std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
        }
        return _sketch_update.get();
    }

    theta_union_type* get_sketch_union() {
        if (!_sketch_union) {
            _sketch_union =
                    std::make_unique<theta_union_type>(theta_union_type::builder(alloc_type(_memory_usage)).build());
        }
        return _sketch_union.get();
    }

    void clear() {
        if (_sketch_update != nullptr) {
            _sketch_update->reset();
        }
        if (_sketch_union != nullptr) {
            _sketch_union.reset();
        }
    }

private:
    int64_t* _memory_usage;
    std::unique_ptr<theta_sketch_type> _sketch_update = nullptr;
    std::unique_ptr<theta_union_type> _sketch_union = nullptr;
};

} // namespace starrocks
