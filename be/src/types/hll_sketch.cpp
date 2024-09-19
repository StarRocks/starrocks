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

#include "types/hll_sketch.h"

#include "common/logging.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"

namespace starrocks {

DataSketchesHll::DataSketchesHll(const Slice& src, int64_t* memory_usage) : _memory_usage(memory_usage) {
    if (!deserialize(src)) {
        LOG(WARNING) << "Failed to init DataSketchHll from slice, will be reset to 0.";
    }
}

bool DataSketchesHll::is_valid(const Slice& slice) {
    if (slice.size < 1) {
        return false;
    }

    const uint8_t preInts = static_cast<const uint8_t*>((uint8_t*)slice.data)[0];
    if (preInts == datasketches::hll_constants::HLL_PREINTS ||
        preInts == datasketches::hll_constants::HASH_SET_PREINTS ||
        preInts == datasketches::hll_constants::LIST_PREINTS) {
        return true;
    } else {
        return false;
    }
}

void DataSketchesHll::update(uint64_t hash_value) {
    _sketch_union->update(hash_value);
    this->mark_changed();
}

void DataSketchesHll::merge(const DataSketchesHll& other) {
    if (UNLIKELY(_sketch_union == nullptr)) {
        _sketch_union = std::make_unique<hll_union_type>(other.get_lg_config_k(), alloc_type(_memory_usage));
    }
    auto o_sketch = other.get_hll_sketch();
    if (o_sketch == nullptr) {
        return;
    }
    _sketch_union->update(*o_sketch);
    this->mark_changed();
}

size_t DataSketchesHll::max_serialized_size() const {
    if (_sketch_union == nullptr) {
        return 0;
    }
    uint8_t log_k = get_lg_config_k();
    datasketches::target_hll_type tgt_type = get_target_type();
    return get_hll_sketch()->get_max_updatable_serialization_bytes(log_k, tgt_type);
}

size_t DataSketchesHll::serialize_size() const {
    if (_sketch_union == nullptr) {
        return 0;
    }
    return get_hll_sketch()->get_compact_serialization_bytes();
}

size_t DataSketchesHll::serialize(uint8_t* dst) const {
    if (_sketch_union == nullptr) {
        return 0;
    }
    auto serialize_compact = _sketch->serialize_compact();
    std::copy(serialize_compact.begin(), serialize_compact.end(), dst);
    return get_hll_sketch()->get_compact_serialization_bytes();
}

bool DataSketchesHll::deserialize(const Slice& slice) {
    // can be called only when _sketch_union is empty
    DCHECK(_sketch_union == nullptr);

    // check if input length is valid
    if (!is_valid(slice)) {
        return false;
    }

    try {
        auto sketch = std::make_unique<hll_sketch_type>(
                hll_sketch_type::deserialize((uint8_t*)slice.data, slice.size, alloc_type(_memory_usage)));
        _sketch_union = std::make_unique<hll_union_type>(sketch->get_lg_config_k(), alloc_type(_memory_usage));
        _sketch_union->update(*sketch);
        this->mark_changed();
    } catch (std::logic_error& e) {
        LOG(WARNING) << "DataSketchesHll deserialize error: " << e.what();
        return false;
    }

    return true;
}

int64_t DataSketchesHll::estimate_cardinality() const {
    if (_sketch_union == nullptr) {
        return 0;
    }
    return _sketch_union->get_estimate();
}

std::string DataSketchesHll::to_string() const {
    if (_sketch_union == nullptr) {
        return "";
    }
    datasketches::string<alloc_type> str = get_hll_sketch()->to_string();
    return std::string(str.begin(), str.end());
}

} // namespace starrocks
