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

#include "exprs/agg/data_sketch/ds_theta.h"

#include <memory>
#include <string>

#include "util/slice.h"

namespace starrocks {

DataSketchesTheta::DataSketchesTheta(const Slice& src, int64_t* memory_usage) : _memory_usage(memory_usage) {
    if (!deserialize(src)) {
        LOG(WARNING) << "Failed to init DataSketchesFrequent from slice, will be reset to 0.";
    }
}

void DataSketchesTheta::update(uint64_t hash_value) {
    auto* sk_update = get_sketch_update();
    sk_update->update(hash_value);
}

void DataSketchesTheta::merge(const DataSketchesTheta& other) {
    auto* sk_union = get_sketch_union();
    if (_sketch_update) {
        sk_union->update(*_sketch_update);
        _sketch_update.reset();
    }
    if (other._sketch_update != nullptr) {
        sk_union->update(*other._sketch_update);
    }
    if (other._sketch_union != nullptr) {
        sk_union->update(other._sketch_union->get_result());
    }
}

size_t DataSketchesTheta::serialize(uint8_t* dst) const {
    if (_sketch_update) {
        auto bytes = _sketch_update->compact().serialize();
        std::copy(bytes.begin(), bytes.end(), dst);
        return bytes.size();
    } else if (_sketch_union) {
        auto bytes = _sketch_union->get_result().serialize();
        std::copy(bytes.begin(), bytes.end(), dst);
        return bytes.size();
    } else {
        theta_compact_sketch::vector_bytes bytes;
        std::copy(bytes.begin(), bytes.end(), dst);
        return bytes.size();
    }
}

uint64_t DataSketchesTheta::serialize_size() const {
    if (_sketch_update) {
        auto bytes = _sketch_update->compact().serialize();
        return bytes.size();
    } else if (_sketch_union) {
        auto bytes = _sketch_union->get_result().serialize();
        return bytes.size();
    } else {
        theta_compact_sketch::vector_bytes bytes;
        return bytes.size();
    }
}

bool DataSketchesTheta::deserialize(const Slice& slice) {
    if (slice.empty()) {
        return true;
    }
    try {
        auto sk = wrapped_compact_theta_sketch::wrap(slice.get_data(), slice.get_size());
        get_sketch_union()->update(sk);
    } catch (std::logic_error& e) {
        LOG(WARNING) << "DataSketchesTheta deserialize error: " << e.what();
        return false;
    }
    return true;
}

int64_t DataSketchesTheta::estimate_cardinality() const {
    if (_sketch_update != nullptr) {
        return _sketch_update->get_estimate();
    } else if (_sketch_union != nullptr) {
        return _sketch_union->get_result().get_estimate();
    } else {
        return 0;
    }
}

} // namespace starrocks
