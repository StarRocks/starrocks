// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/seek_tuple.h"

#include <sstream>
#include <string>

#include "column/datum_convert.h"
#include "storage/convert_helper.h"

namespace starrocks::vectorized {

void SeekTuple::convert_to(SeekTuple* new_tuple, const std::vector<FieldType>& new_types) const {
    _schema.convert_to(&new_tuple->_schema, new_types);

    RowConverter converter;
    converter.init(_schema, new_tuple->_schema);
    converter.convert(&new_tuple->_values, _values);
}

std::string SeekTuple::debug_string() const {
    std::stringstream ss;
    ss << "(";
    for (int i = 0; i < _values.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << datum_to_string(_schema.field(i)->type().get(), _values[i]);
    }
    ss << ")";

    return ss.str();
}

} // namespace starrocks::vectorized
