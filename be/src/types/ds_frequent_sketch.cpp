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

#include "types/ds_frequent_sketch.h"

namespace starrocks {

template <typename T>
void DataSketchesFrequent<T>::update(T value) {
    uint32_t old_active_items = _sketch->get_num_active_items();
    _sketch->update(value);
    uint32_t new_active_items = _sketch->get_num_active_items();
}

template <typename T>
void DataSketchesFrequent<T>::merge(const DataSketchesFrequent<T>& other) {
    if (other._sketch == nullptr) {
        return;
    }
    if (UNLIKELY(_sketch == nullptr)) {
        _sketch = std::make_unique<frequent_sketch_type>(_lg_max_map_size, _lg_start_map_size, std::equal_to<T>(),
                                                         alloc_type(_memory_usage));
    }
    _sketch.get()->merge(*other._sketch);
}

template <typename T>
uint64_t DataSketchesFrequent<T>::serialize_size() const {
    if (_sketch == nullptr) {
        return 0;
    }
    return _sketch->get_serialized_size_bytes();
}

template <typename T>
size_t DataSketchesFrequent<T>::serialize(uint8_t* dst) const {
    if (_sketch == nullptr) {
        return 0;
    }
    auto serialize_compact = _sketch->serialize();
    std::copy(serialize_compact.begin(), serialize_compact.end(), dst);
    return _sketch->get_serialized_size_bytes();
}

template <typename T>
bool DataSketchesFrequent<T>::deserialize(const Slice& slice) {
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

template <typename T>
std::vector<FrequentRow<T>> DataSketchesFrequent<T>::get_frequent_items(uint64_t threshold) const {
    std::vector<FrequentRow<T>> result;
    if (_sketch == nullptr) {
        return result;
    }
    try {
        auto frequent_items = _sketch->get_frequent_items(datasketches::NO_FALSE_POSITIVES, threshold);
        for (auto item : frequent_items) {
            FrequentRow<T> frequent_row = FrequentRow<T>{item.get_item(), item.get_estimate(), item.get_lower_bound(),
                                                         item.get_upper_bound()};
            result.push_back(frequent_row);
        }
    } catch (std::logic_error& e) {
        LOG(WARNING) << "DataSketchesFrequent get_quantiles error: " << e.what();
        result.clear();
    }
    return result;
}

template <typename T>
void DataSketchesFrequent<T>::clear() {
    *_memory_usage = 0;
    this->_sketch = std::make_unique<frequent_sketch_type>(_lg_max_map_size, _lg_start_map_size, std::equal_to<T>(),
                                                           alloc_type(_memory_usage));
}

template <typename T>
std::string DataSketchesFrequent<T>::to_string() const {
    if (_sketch == nullptr) {
        return "";
    }
    datasketches::string<alloc_type> str = _sketch->to_string();
    return std::string(str.begin(), str.end());
}

} // namespace starrocks