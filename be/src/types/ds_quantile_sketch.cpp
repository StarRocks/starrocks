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

#include "types/ds_quantile_sketch.h"

namespace starrocks {

template <typename T>
void DataSketchesQuantile<T>::update(T value) {
    _sketch->update(value);
}

template <typename T>
void DataSketchesQuantile<T>::merge(const DataSketchesQuantile<T>& other) {
    if (other._sketch == nullptr) {
        return;
    }
    if (UNLIKELY(_sketch == nullptr)) {
        _sketch = std::make_unique<quantile_sketch_type>(other._sketch->get_k(), std::less<T>(),
                                                         alloc_type(_memory_usage));
    }
    _sketch.get()->merge(*other._sketch);
}
 
template <typename T>
uint64_t DataSketchesQuantile<T>::serialize_size() const {
    if (_sketch == nullptr) {
        return 0;
    }
    return _sketch->get_serialized_size_bytes();
}

template <typename T>
bool DataSketchesQuantile<T>::deserialize(const Slice& slice) {
    DCHECK(_sketch == nullptr);
    
    if (!is_valid(slice)) {
        return false;
    }
    try {
        _sketch = std::make_unique<quantile_sketch_type>(quantile_sketch_type::deserialize(
                (uint8_t*)slice.data, slice.size, datasketches::serde<T>(), std::less<T>(), alloc_type(_memory_usage)));
    } catch (std::logic_error& e) {
        LOG(WARNING) << "DataSketchesQuantile deserialize error: " << e.what();
        return false;
    }

    return true;
}

template <typename T>
std::vector<T> DataSketchesQuantile<T>::get_quantiles(const double* ranks, uint32_t size) const {
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

template <typename T>
void DataSketchesQuantile<T>::clear() {
    *_memory_usage = 0;
    this->_sketch = std::make_unique<quantile_sketch_type>(_sketch->get_k(), std::less<T>(), alloc_type(_memory_usage));
}

template <typename T>
std::string DataSketchesQuantile<T>::to_string() const {
    if (_sketch == nullptr) {
        return "";
    }  
    datasketches::string<alloc_type> str = _sketch->to_string();
    return std::string(str.begin(), str.end());
}

} // namespace starrocks
