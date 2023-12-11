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

#include "storage/seek_tuple.h"

#include <sstream>
#include <string>

#include "column/datum_convert.h"
#include "storage/convert_helper.h"

namespace starrocks {

void SeekTuple::convert_to(SeekTuple* new_tuple, const std::vector<LogicalType>& new_types) const {
    _schema.convert_to(&new_tuple->_schema, new_types);

    RowConverter converter;
    WARN_IF_ERROR(converter.init(_schema, new_tuple->_schema), "Cannot get field converter");
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

} // namespace starrocks
