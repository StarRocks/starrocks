// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "column/datum.h"
#include "column/schema.h"
#include "storage/short_key_index.h"

namespace starrocks::vectorized {

// SeekTuple represent the values of key columns, including NULL.
// The column id of columns specified in |_schema| must be continuously and started from zero.
class SeekTuple {
public:
    // default is infinite.
    SeekTuple() = default;

    SeekTuple(Schema schema, std::vector<Datum> values) : _schema(std::move(schema)), _values(std::move(values)) {
#ifndef NDEBUG
        // Ensure the columns are continuously and started from zero.
        for (size_t i = 0; i < _schema.num_fields(); i++) {
            CHECK_EQ(ColumnId(i), _schema.field(i)->id());
        }
#endif
    }

    bool empty() const { return _values.empty(); }

    const Schema& schema() const { return _schema; }

    size_t columns() const { return _values.size(); }

    // Return the value of i-th column.
    const Datum& get(int i) const { return _values[i]; }

    // Encode the first Min(|num_short_keys|, |columns|) values for short key index lookup.
    // if |num_short_keys| is greater than |columns|, one additional char |padding| will be
    // appended at the tail.
    std::string short_key_encode(size_t num_short_keys, uint8_t padding) const;

    void convert_to(SeekTuple* new_tuple, const std::vector<FieldType>& new_types) const;

    std::string debug_string() const;

private:
    Schema _schema;
    std::vector<Datum> _values;
};

inline std::string SeekTuple::short_key_encode(size_t num_short_keys, uint8_t padding) const {
    std::string output;
    size_t n = std::min(num_short_keys, _values.size());
    for (auto cid = 0; cid < n; cid++) {
        if (_values[cid].is_null()) {
            output.push_back(KEY_NULL_FIRST_MARKER);
        } else {
            output.push_back(KEY_NORMAL_MARKER);
            _schema.field(cid)->encode_ascending(_values[cid], &output);
        }
    }
    if (_values.size() < num_short_keys) {
        output.push_back(padding);
    }
    return output;
}

} // namespace starrocks::vectorized
