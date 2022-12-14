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

#include "tdigest.h"

namespace starrocks {
class PercentileValue {
public:
    PercentileValue() { _type = TDIGEST; }

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

    void merge(const PercentileValue* other) { _tdigest.merge(&other->_tdigest); }

    uint64_t serialize_size() const {
        //_type 1 bytes
        return 1 + _tdigest.serialize_size();
    }

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

private:
    enum PercentileDataType { TDIGEST = 0 };
    TDigest _tdigest;
    PercentileDataType _type;
};
} // namespace starrocks
