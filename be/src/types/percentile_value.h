// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "types/tdigest.h"

namespace starrocks {
class PercentileValue {
public:
    PercentileValue() { _type = TDIGEST; }

    explicit PercentileValue(double compression) : _tdigest(compression) { _type = TDIGEST; }

    explicit PercentileValue(const Slice& src) {
        switch (*src.data) {
        case PercentileDataType::TDIGEST:
            _type = TDIGEST;
            break;
        default:
            DCHECK(false);
        }
        _tdigest.deserialize(src.data + 1);
    }

    void add(float value) { _tdigest.add(value); }

    void add(float value, int64_t weight) { _tdigest.add(value, static_cast<float>(weight)); }

    void add(float value, float weight) { _tdigest.add(value, weight); }

    void merge(const PercentileValue* other) { _tdigest.merge(&other->_tdigest); }

    // Fast-path probe for the very common shape produced by
    // PercentileApproxAggregateFunction::convert_to_serialize_format in
    // PASS_THROUGH / streaming: one unprocessed centroid, no processed.
    // Caller can then add(mean, weight) directly to the target and skip
    // the priority-queue merge path in TDigest::add(iter, end).
    bool try_extract_singleton(float* mean, float* weight) const {
        if (!_tdigest.processed().empty() || _tdigest.unprocessed().size() != 1) {
            return false;
        }
        const Centroid& c = _tdigest.unprocessed().front();
        *mean = c.mean();
        *weight = c.weight();
        return true;
    }

    uint64_t serialize_size() const {
        //_type 1 bytes
        return 1 + _tdigest.serialize_size();
    }

    uint64_t mem_usage() const { return 1 + _tdigest.serialize_size(); }

    size_t serialize(uint8_t* writer) const {
        *(writer) = _type;
        return _tdigest.serialize(writer + 1);
    }
    void deserialize(const char* type_reader) {
        switch (*type_reader) {
        case PercentileDataType::TDIGEST:
            _type = TDIGEST;
            break;
        default:
            DCHECK(false);
        }
        _tdigest.deserialize(type_reader + 1);
    }

    Value quantile(Value q) { return _tdigest.quantile(q); }

    // True iff the digest received zero total weight. Used by the
    // percentile_approx null predicate to return SQL NULL instead of a
    // silent NaN when the digest is empty (e.g. weighted variant with all
    // w <= 0, or input values rejected as non-finite by TDigest::add).
    // Test the centroid vectors directly rather than TDigest::totalWeight(),
    // which narrows the accumulated float weight to long (UB once a weighted
    // sum exceeds the long range). A digest holds a centroid iff some w > 0
    // was added, so emptiness == both vectors empty.
    bool is_empty() const { return _tdigest.processed().empty() && _tdigest.unprocessed().empty(); }

private:
    enum PercentileDataType { TDIGEST = 0 };
    TDigest _tdigest;
    PercentileDataType _type;
};
} // namespace starrocks
