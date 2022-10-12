// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
