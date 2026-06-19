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

#include <glog/logging.h>

#include "runtime/mem_pool.h"

namespace starrocks {

DataSketchesHll::DataSketchesHll(const Slice& src, int64_t* memory_usage) : _memory_usage(memory_usage) {
    if (!deserialize(src)) {
        DLOG(INFO) << "Failed to init DataSketchesHll from slice, will be reset to 0.";
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

uint64_t DataSketchesHll::serialize_size() const {
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
        DLOG(INFO) << "DataSketchesHll deserialize error with exception:" << e.what();
        return false;
    }

    return true;
}

int64_t DataSketchesHll::estimate_cardinality() const {
    if (_sketch_union == nullptr) {
        return 0;
    }
    // Use the composite (non-HIP) estimator instead of get_estimate().
    //
    // get_estimate() returns the HIP (Historic Inverse Probability) estimate while the union's
    // internal gadget still has its out-of-order flag unset. The HIP accumulator is updated
    // incrementally using the running kxq value at the moment each register changes, so its final
    // value depends on the order in which coupons/sketches were merged. In distributed/parallel
    // aggregation the merge order is non-deterministic, which makes the HIP estimate fluctuate
    // across runs over the same data. This happens whenever each partial sketch stays in coupon
    // (LIST/SET) mode while the unioned cardinality crosses the SET->HLL promotion threshold: the
    // gadget is promoted to HLL via coupon replay but never sets the out-of-order flag, so it keeps
    // using the order-dependent HIP estimate.
    //
    // get_composite_estimate() is computed purely from the final register state, so it is
    // independent of merge order and reproducible. The trade-off is a slightly higher relative
    // standard error (non-HIP RSE factor 1.039 vs HIP 0.833, ~1.25x), which is acceptable here
    // since this object is fundamentally a merge accumulator where the HIP estimate is not valid.
    //
    // There is no meaningful performance concern: estimate_cardinality() is only called from the
    // finalize/output stage (once per group), never on the hot update/merge path. The extra cost
    // of the composite estimator over reading the cached HIP accumulator is O(1) and negligible.
    return _sketch_union->get_composite_estimate();
}

std::string DataSketchesHll::to_string() const {
    if (_sketch_union == nullptr) {
        return "";
    }
    datasketches::string<alloc_type> str = get_hll_sketch()->to_string();
    return std::string(str.begin(), str.end());
}

} // namespace starrocks
