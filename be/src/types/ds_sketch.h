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
#include <datasketches/hll.hpp>
#include <datasketches/quantiles_sketch.hpp>
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

    void update(T value) { _sketch->update(value); }

    void merge(const DataSketchesQuantile& other) {
        if (UNLIKELY(_sketch == nullptr)) {
            _sketch = std::make_unique<quantile_sketch_type>(other._sketch->get_k(), std::less<T>(),
                                                             alloc_type(_memory_usage));
        }
        _sketch.get()->merge(*other._sketch);
    }

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    size_t serialize(uint8_t* dst) const {
        if (_sketch == nullptr) {
            return 0;
        }
        auto serialize_compact = _sketch->serialize();
        std::copy(serialize_compact.begin(), serialize_compact.end(), dst);
        return _sketch->get_serialized_size_bytes();
    }

    uint64_t serialize_size() const {
        if (_sketch == nullptr) {
            return 0;
        }
        return _sketch->get_serialized_size_bytes();
    }

    bool deserialize(const Slice& slice) {
        DCHECK(_sketch == nullptr);

        if (!is_valid(slice)) {
            return false;
        }
        try {
            _sketch = std::make_unique<quantile_sketch_type>(
                    quantile_sketch_type::deserialize((uint8_t*)slice.data, slice.size, datasketches::serde<T>(),
                                                      std::less<T>(), alloc_type(_memory_usage)));
        } catch (std::logic_error& e) {
            LOG(WARNING) << "DataSketchesQuantile deserialize error: " << e.what();
            return false;
        }
        return true;
    }

    std::vector<T> get_quantiles(const double* ranks, uint32_t size) const {
        std::vector<T> result;
        if (_sketch == nullptr) {
            return result;
        }
        try {
            std::vector<T, alloc_type> quantiles = _sketch->get_quantiles(ranks, size);
            for (T quantile : quantiles) {
                result.push_back(quantile);
            }
        } catch (std::logic_error& e) {
            LOG(WARNING) << "DataSketchesQuantile get_quantiles error: " << e.what();
            result.clear();
        }
        return result;
    }

    static bool is_valid(const Slice& slice) {
        if (slice.size < 1) {
            return false;
        }
        return true;
    }

    void clear() {
        *_memory_usage = 0;
        this->_sketch =
                std::make_unique<quantile_sketch_type>(_sketch->get_k(), std::less<T>(), alloc_type(_memory_usage));
    }

    std::string to_string() const {
        if (_sketch == nullptr) {
            return "";
        }
        datasketches::string<alloc_type> str = _sketch->to_string();
        return std::string(str.begin(), str.end());
    }

private:
    int64_t* _memory_usage;
    mutable std::unique_ptr<quantile_sketch_type> _sketch = nullptr;
};

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
            : _memory_usage(memory_usage), _lg_max_map_size(lg_max_map_size) , _lg_start_map_size(lg_start_map_size){
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

    void update(T value) {
        uint32_t old_active_items = _sketch->get_num_active_items();
        _sketch->update(value);
        uint32_t new_active_items = _sketch->get_num_active_items();
        if (old_active_items != new_active_items) {
            // *_memory_usage = *_memory_usage + sizeof(T);
        }
    }

    void merge(const DataSketchesFrequent& other) {
        if (UNLIKELY(_sketch == nullptr)) {
            _sketch = std::make_unique<frequent_sketch_type>(_lg_max_map_size, _lg_start_map_size, std::equal_to<T>(),
                                                             alloc_type(_memory_usage));
        }
        _sketch.get()->merge(*other._sketch);
    }

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    size_t serialize(uint8_t* dst) const {
        if (_sketch == nullptr) {
            return 0;
        }
        auto serialize_compact = _sketch->serialize();
        std::copy(serialize_compact.begin(), serialize_compact.end(), dst);
        return _sketch->get_serialized_size_bytes();
    }

    uint64_t serialize_size() const {
        if (_sketch == nullptr) {
            return 0;
        }
        return _sketch->get_serialized_size_bytes();
    }

    bool deserialize(const Slice& slice) {
        DCHECK(_sketch == nullptr);

        if (!is_valid(slice)) {
            return false;
        }
        try {
            _sketch = std::make_unique<frequent_sketch_type>(
                    frequent_sketch_type::deserialize((uint8_t*)slice.data, slice.size, datasketches::serde<T>(),
                                                      std::equal_to<T>(), alloc_type(_memory_usage)));
        } catch (std::logic_error& e) {
            LOG(WARNING) << "DataSketchesFrequent deserialize error: " << e.what();
            return false;
        }
        return true;
    }

    std::vector<FrequentRow<T>> get_frequent_items(uint64_t threshold) const {
        std::vector<FrequentRow<T>> result;
        if (_sketch == nullptr) {
            return result;
        }
        try {
            auto frequent_items = _sketch->get_frequent_items(datasketches::NO_FALSE_POSITIVES, threshold);
            for (auto item : frequent_items) {
                FrequentRow<T> frequent_row = FrequentRow<T>{item.get_item(), item.get_estimate(),
                                                             item.get_lower_bound(), item.get_upper_bound()};
                result.push_back(frequent_row);
            }
        } catch (std::logic_error& e) {
            LOG(WARNING) << "DataSketchesFrequent get_quantiles error: " << e.what();
            result.clear();
        }
        return result;
    }

    static bool is_valid(const Slice& slice) {
        if (slice.size < 1) {
            return false;
        }
        return true;
    }

    void clear() {
        *_memory_usage = 0;
        this->_sketch = std::make_unique<frequent_sketch_type>(_lg_max_map_size, _lg_start_map_size, std::equal_to<T>(),
                                                               alloc_type(_memory_usage));
    }

    std::string to_string() const {
        if (_sketch == nullptr) {
            return "";
        }
        datasketches::string<alloc_type> str = _sketch->to_string();
        return std::string(str.begin(), str.end());
    }

private:
    int64_t* _memory_usage;
    uint8_t _lg_max_map_size;
    uint8_t _lg_start_map_size;
    mutable std::unique_ptr<frequent_sketch_type> _sketch = nullptr;
};

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

    void update(uint64_t hash_value) {
        _sketch->update(hash_value);
        _is_changed = true;
    }

    void merge(const DataSketchesTheta& other) {
        if (_sketch_union == nullptr) {
            _sketch_union = 
                    std::make_unique<theta_union_type>(theta_union_type::builder(alloc_type(_memory_usage)).build());
        }
        _sketch_union->update(other._sketch->compact());
        if (other._sketch_union != nullptr) {
            _sketch_union->update(other._sketch_union->get_result());
        }
        _is_changed = true;
    }

    int64_t mem_usage() const { return _memory_usage == nullptr ? 0L : *_memory_usage; }

    size_t serialize(uint8_t* dst) const {
        serialize_if_needed();
        std::copy(_sketch_data->begin(), _sketch_data->end(), dst);
        return _sketch_data->size();
    }

    uint64_t serialize_size() const {
        serialize_if_needed();
        return _sketch_data->size();
    }

    void serialize_if_needed() const {
        if (UNLIKELY(_sketch == nullptr)) {
            _sketch = 
                    std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
        }
        if (_is_changed) {
            auto resultTheta_union = theta_union_type(theta_union_type::builder(alloc_type(_memory_usage)).build());
            resultTheta_union.update(_sketch->compact());
            if (_sketch_union != nullptr) {
                resultTheta_union.update(_sketch_union->get_result());
            }
            auto sketch_ser = resultTheta_union.get_result().serialize();
            _sketch_data = std::make_unique<sketch_data_type>(sketch_data_type(
                    sketch_ser.begin(),sketch_ser.end(), sketch_ser.get_allocator()));
            _is_changed = false;
        }
    }

    bool deserialize(const Slice& slice) {
        if (!is_valid(slice)) {
            return false;
        }
        DCHECK(_sketch == nullptr);
        _sketch = 
                std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
        try {
            auto sketch_warp = theta_wrapped_type::wrap((uint8_t*)slice.data, slice.size);
            if (_sketch_union == nullptr) {
                _sketch_union = std::make_unique<theta_union_type>(
                        theta_union_type::builder(alloc_type(_memory_usage)).build());
            }
            _sketch_union->update(sketch_warp);
        } catch (std::logic_error& e) {
            LOG(WARNING) << "DataSketchesFrequent deserialize error: " << e.what();
            return false;
        }
        return true;
    }

    static bool is_valid(const Slice& slice) {
        if (slice.size < 1) {
            return false;
        }
        return true;
    }

    int64_t estimate_cardinality() const {
        if (_sketch == nullptr && _sketch_union == nullptr) {
            return 0;
        }
        if (_sketch_union == nullptr) {
            return _sketch->get_estimate();
        } else {
            auto resultTheta_union = theta_union_type(theta_union_type::builder(alloc_type(_memory_usage)).build());
            resultTheta_union.update(_sketch_union->get_result());
            if (_sketch != nullptr) {
                resultTheta_union.update(_sketch->compact());
            }
            return resultTheta_union.get_result().get_estimate();
        }
    }

    void clear() {
        if (_sketch != nullptr) {
            _sketch->reset();
        }

        if (_sketch_union != nullptr) {
            _sketch_union.reset();
        }
    }

private:
    int64_t* _memory_usage;
    mutable std::unique_ptr<theta_sketch_type> _sketch = nullptr;
    mutable std::unique_ptr<theta_union_type> _sketch_union = nullptr;
    mutable std::unique_ptr<sketch_data_type> _sketch_data = nullptr;
    mutable bool _is_changed = true;
};

} // namespace starrocks
