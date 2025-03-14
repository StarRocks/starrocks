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

#include "types/ds_theta_sketch.h"

namespace starrocks {

void DataSketchesTheta::update(uint64_t hash_value) {
    _sketch->update(hash_value);
    _is_changed = true;
}

void DataSketchesTheta::merge(const DataSketchesTheta& other) {
    if (_sketch_union == nullptr) {
        _sketch_union =
                std::make_unique<theta_union_type>(theta_union_type::builder(alloc_type(_memory_usage)).build());
    }
    if (other._sketch != nullptr) {
        _sketch_union->update(other._sketch->compact());
    }
    if (other._sketch_union != nullptr) {
        _sketch_union->update(other._sketch_union->get_result());
    }
    _is_changed = true;
}

uint64_t DataSketchesTheta::serialize_size() const {
    serialize_if_needed();
    return _sketch_data->size();
}

void DataSketchesTheta::serialize_if_needed() const {
    if (UNLIKELY(_sketch == nullptr)) {
        _sketch = std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
    }
    if (_is_changed) {
        auto resultTheta_union = theta_union_type(theta_union_type::builder(alloc_type(_memory_usage)).build());
        resultTheta_union.update(_sketch->compact());
        if (_sketch_union != nullptr) {
            resultTheta_union.update(_sketch_union->get_result());
        }
        auto sketch_ser = resultTheta_union.get_result().serialize();
        _sketch_data = std::make_unique<sketch_data_type>(
                sketch_data_type(sketch_ser.begin(), sketch_ser.end(), sketch_ser.get_allocator()));
        _is_changed = false;
    }
}

size_t DataSketchesTheta::serialize(uint8_t* dst) const {
    serialize_if_needed();
    std::copy(_sketch_data->begin(), _sketch_data->end(), dst);
    return _sketch_data->size();
}

bool DataSketchesTheta::deserialize(const Slice& slice) {
    if (!is_valid(slice)) {
        return false;
    }
    DCHECK(_sketch == nullptr);
    _sketch = std::make_unique<theta_sketch_type>(theta_sketch_type::builder(alloc_type(_memory_usage)).build());
    try {
        auto sketch_warp = theta_wrapped_type::wrap((uint8_t*)slice.data, slice.size);
        if (_sketch_union == nullptr) {
            _sketch_union =
                    std::make_unique<theta_union_type>(theta_union_type::builder(alloc_type(_memory_usage)).build());
        }
        _sketch_union->update(sketch_warp);
    } catch (std::logic_error& e) {
        LOG(WARNING) << "DataSketchesFrequent deserialize error: " << e.what();
        return false;
    }
    return true;
}

int64_t DataSketchesTheta::estimate_cardinality() const {
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

void DataSketchesTheta::clear() {
    if (_sketch != nullptr) {
        _sketch->reset();
    }

    if (_sketch_union != nullptr) {
        _sketch_union.reset();
    }
}

} // namespace starrocks
