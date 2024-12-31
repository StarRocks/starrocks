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

#include <datasketches/frequent_items_sketch.hpp>
#include <memory>
#include <string>

#include "runtime/memory/counting_allocator.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "util/slice.h"

namespace starrocks {
template <typename T>
struct FrequentRow {
    T value;
    uint64_t count;
    uint64_t lower_bound;
    uint64_t upper_bound;
};

template <typename T>
class DataSketchesFrequent {
public:
    using alloc_type = STLCountingAllocator<T>;
    using frequent_sketch_type =
            datasketches::frequent_items_sketch<T, uint64_t, std::hash<T>, std::equal_to<T>, alloc_type>;

    explicit DataSketchesFrequent(uint8_t lg_max_map_size, uint8_t lg_start_map_size, int64_t* memory_usage)
            : _memory_usage(memory_usage), _lg_max_map_size(lg_max_map_size), _lg_start_map_size(lg_start_map_size) {
        _sketch = std::make_unique<frequent_sketch_type>(_lg_max_map_size, _lg_start_map_size, std::equal_to<T>(),
                                                         alloc_type(_memory_usage));
    }

    DataSketchesFrequent(const DataSketchesFrequent& other) = delete;
    DataSketchesFrequent& operator=(const DataSketchesFrequent& other) = delete;

    DataSketchesFrequent(DataSketchesFrequent&& other) noexcept
            : _memory_usage(std::move(other._memory_usage)),
              _lg_max_map_size(other._lg_max_map_size),
              _lg_start_map_size(other._lg_start_map_size),
              _sketch(std::move(other._sketch)) {}

    DataSketchesFrequent& operator=(DataSketchesFrequent&& other) noexcept {
        if (this != &other) {
            this->_memory_usage = std::move(other._memory_usage);
            this->_lg_max_map_size = other._lg_max_map_size;
            this->_lg_start_map_size = other._lg_start_map_size;
            this->_sketch = std::move(other._sketch);
        }
        return *this;
    }

    explicit DataSketchesFrequent(const Slice& src, uint8_t lg_max_map_size, uint8_t lg_start_map_size,
                                  int64_t* memory_usage)
            : _memory_usage(memory_usage), _lg_max_map_size(lg_max_map_size), _lg_start_map_size(lg_start_map_size) {
        if (!deserialize(src)) {
            LOG(WARNING) << "Failed to init DataSketchesFrequent from slice, will be reset to 0.";
        }
    }

    ~DataSketchesFrequent() = default;

    void update(T value);

    void merge(const DataSketchesFrequent& other);

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    uint64_t serialize_size() const;

    size_t serialize(uint8_t* dst) const;

    bool deserialize(const Slice& slice);

    std::vector<FrequentRow<T>> get_frequent_items(uint64_t threshold) const;

    static bool is_valid(const Slice& slice) {
        if (slice.size < 1) {
            return false;
        }
        return true;
    }

    void clear();

    std::string to_string() const;

private:
    int64_t* _memory_usage;
    uint8_t _lg_max_map_size;
    uint8_t _lg_start_map_size;
    mutable std::unique_ptr<frequent_sketch_type> _sketch = nullptr;
};

} // namespace starrocks