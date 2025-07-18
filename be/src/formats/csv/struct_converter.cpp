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

#include "formats/csv/struct_converter.h"

#include "column/struct_column.h"
#include "common/logging.h"

namespace starrocks::csv {

Status StructConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                     const Options& options) const {
    auto* struct_column = down_cast<const StructColumn*>(&column);
    auto& fields = struct_column->fields();
    auto& field_names = struct_column->field_names();
    auto size = fields.size();

    RETURN_IF_ERROR(os->write('{'));
    for (size_t i = 0; i < size; i++) {
        RETURN_IF_ERROR(os->write(field_names[i]));
        RETURN_IF_ERROR(os->write(':'));
        RETURN_IF_ERROR(_child_converters[i]->write_quoted_string(os, *fields[i], row_num, options));
        if (i + 1 < size) {
            RETURN_IF_ERROR(os->write(','));
        }
    }
    return os->write('}');
}

Status StructConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                            const Options& options) const {
    return write_string(os, column, row_num, options);
}

bool StructConverter::read_string(Column* column, const Slice& s, const Options& options) const {
    // As we only consider read the Hive type so here we do not need to valid the slice
    auto* struct_column = down_cast<StructColumn*>(column);

    std::vector<Slice> fields;
    _split_struct_fields(s, fields, options);

    // Make sure the split size is same as the child conveters size
    if (fields.size() != _child_converters.size()) {
        return false;
    }

    auto& column_fields = struct_column->fields_column();

    Options sub_options = options;
    sub_options.hive_nested_level++;
    for (size_t i = 0; i < _child_converters.size(); i++) {
        if (!_child_converters[i]->read_string(column_fields[i].get(), fields[i], sub_options)) {
            // TODO: consider to implement the logic that just remove the data that already append in the column fields
            return false;
        }
    }
    return true;
}

void StructConverter::_split_struct_fields(Slice s, std::vector<Slice>& fields, const Options& options) const {
    auto delimiter = get_collection_delimiter(options.hive_collection_delimiter, options.hive_mapkey_delimiter,
                                              options.hive_nested_level);

    size_t left = 0;
    size_t right = 0;
    for (/**/; right < s.size; right++) {
        char c = s[right];
        if (c == delimiter) {
            fields.emplace_back(s.data + left, right - left);
            left = right + 1;
        }
    }
    if (right >= left) {
        fields.emplace_back(s.data + left, right - left);
    }
}

bool StructConverter::read_quoted_string(Column* column, const Slice& s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::csv
