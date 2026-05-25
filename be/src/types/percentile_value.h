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

#include <limits>

#include "common/logging.h"
#include "types/tdigest.h"

namespace starrocks {
class PercentileValue {
public:
    PercentileValue() { _type = TDIGEST; }

    explicit PercentileValue(double compression) : _tdigest(compression) { _type = TDIGEST; }

    explicit PercentileValue(const Slice& src) {
        _type = TDIGEST;
        (void)deserialize(src.data, src.size);
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

    // Heap footprint of the PercentileValue (capacity-based, so process()
    // shrinking _unprocessed.size() does not flip the delta negative in
    // FunctionContext::add_mem_usage).
    // byte_size_in_memory() already counts sizeof(TDigest), and since _tdigest is
    // an inline member it is also part of sizeof(PercentileValue); subtract it
    // once so the TDigest object is not counted twice.
    uint64_t mem_usage() const { return sizeof(PercentileValue) + _tdigest.byte_size_in_memory() - sizeof(TDigest); }

    size_t serialize(uint8_t* writer) const {
        // Must not mutate _tdigest here: callers size their buffer from
        // serialize_size() before this call, and compress() can grow the
        // serialized footprint (rebuilds _cumulative with M+1 weights), so
        // canonicalizing inside serialize would overflow the caller buffer.
        *(writer) = _type;
        return _tdigest.serialize(writer + 1);
    }
    // Bounded entry point. Returns false on truncation or unrecognized type
    // tag and leaves the digest in an empty state.
    bool deserialize(const char* data, size_t size) {
        if (size < 1) {
            LOG(WARNING) << "PercentileValue::deserialize: missing type tag";
            return false;
        }
        switch (*data) {
        case PercentileDataType::TDIGEST:
            _type = TDIGEST;
            break;
        default:
            LOG(WARNING) << "PercentileValue::deserialize: unknown type tag " << static_cast<int>(*data);
            return false;
        }
        return _tdigest.deserialize(data + 1, size - 1);
    }
    // Legacy unsafe entry point retained for callers that do not carry the
    // blob length; delegates with an unbounded size.
    void deserialize(const char* type_reader) { (void)deserialize(type_reader, std::numeric_limits<size_t>::max()); }

    Value quantile(Value q) { return _tdigest.quantile(q); }

    // True iff the digest received zero total weight. Used by the
    // percentile_approx null predicate to return SQL NULL instead of a
    // silent NaN when the digest is empty (e.g. weighted variant with all
    // w <= 0, or input values rejected as non-finite by TDigest::add).
    bool is_empty() const { return _tdigest.totalWeight() == 0; }

    // Compression the underlying digest was built with. Forwarded so the merge
    // path can adopt the compression carried inside a deserialized intermediate
    // blob instead of re-deriving it from the function context.
    double compression() const { return _tdigest.compression(); }

private:
    enum PercentileDataType { TDIGEST = 0 };
    TDigest _tdigest;
    PercentileDataType _type;
};
} // namespace starrocks
