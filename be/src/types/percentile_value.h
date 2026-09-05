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
        bool ok = deserialize(src.data, src.size);
        // A failure here leaves an empty digest; flag it in debug so a truncated
        // or corrupt persisted slice is not silently read as an empty value.
        DCHECK(ok) << "PercentileValue: failed to deserialize a " << src.size << "-byte slice";
    }

    void add(float value) { _tdigest.add(value); }

    void add(float value, int64_t weight) { _tdigest.add(value, static_cast<float>(weight)); }

    void merge(const PercentileValue* other) { _tdigest.merge(&other->_tdigest); }

    uint64_t serialize_size() const {
        //_type 1 bytes
        return 1 + _tdigest.serialize_size();
    }

    uint64_t mem_usage() const { return 1 + _tdigest.serialize_size(); }

    size_t serialize(uint8_t* writer) const {
        // Must not mutate _tdigest here: callers size their buffer from
        // serialize_size() before this call, and compress() can grow the
        // serialized footprint (rebuilds _cumulative with M+1 weights), so
        // canonicalizing inside serialize would overflow the caller buffer.
        *(writer) = _type;
        // Include the 1-byte type tag so the return matches serialize_size().
        // ObjectColumn::build_slices / serialize_batch use this value as the
        // slice length; without the +1 each percentile slice is one byte short
        // and bounded deserialize then rejects it as truncated.
        return 1 + _tdigest.serialize(writer + 1);
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

private:
    enum PercentileDataType { TDIGEST = 0 };
    TDigest _tdigest;
    PercentileDataType _type;
};
} // namespace starrocks
