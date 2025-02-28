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

#include <datasketches/quantiles_sketch.hpp>

#include "runtime/memory/counting_allocator.h"
#include "util/slice.h"

namespace starrocks {
template <typename T>
class DataSketchesQuantile {
public:
    using alloc_type = STLCountingAllocator<T>;
    using quantile_sketch_type = datasketches::quantiles_sketch<T, std::less<T>, alloc_type>;

    explicit DataSketchesQuantile(uint16_t k, int64_t* memory_usage) : _memory_usage(memory_usage) {
        this->_sketch = std::make_unique<quantile_sketch_type>(k, std::less<T>(), alloc_type(_memory_usage));
    }

    DataSketchesQuantile(const DataSketchesQuantile& other) = delete;
    DataSketchesQuantile& operator=(const DataSketchesQuantile& other) = delete;

    DataSketchesQuantile(DataSketchesQuantile&& other) noexcept
            : _memory_usage(std::move(other._memory_usage)), _sketch(std::move(other._sketch)) {}
    DataSketchesQuantile& operator=(DataSketchesQuantile&& other) noexcept {
        if (this != &other) {
            this->_memory_usage = std::move(other._memory_usage);
            this->_sketch = std::move(other._sketch);
        }
        return *this;
    }

    explicit DataSketchesQuantile(const Slice& src, int64_t* memory_usage) : _memory_usage(memory_usage) {
        if (!deserialize(src)) {
            LOG(WARNING) << "Failed to init DataSketchesQuantile from slice, will be reset to 0.";
        }
    }

    ~DataSketchesQuantile() = default;

    uint16_t get_k() const { return _sketch->get_k(); }

    void update(T value);

    void merge(const DataSketchesQuantile& other);

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    size_t serialize(uint8_t* dst) const {
        if (_sketch == nullptr) {
            return 0;
        }
        auto serialize_compact = _sketch->serialize();
        std::copy(serialize_compact.begin(), serialize_compact.end(), dst);
        return _sketch->get_serialized_size_bytes();
    }

    uint64_t serialize_size() const;

    bool deserialize(const Slice& slice);

    std::vector<T> get_quantiles(const double* ranks, uint32_t size) const;

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
    mutable std::unique_ptr<quantile_sketch_type> _sketch = nullptr;
};
} // namespace starrocks