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

#include "formats/avro/cpp/complex_column.h"

#include "column/array_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "formats/avro/cpp/nullable_column.h"
#include "formats/avro/cpp/utils.h"

namespace starrocks {

// ------ struct column ------

Status add_struct_column(const avro::GenericDatum& datum, const std::string& col_name, const TypeDescriptor& type_desc,
                         bool invalid_as_null, const cctz::time_zone& timezone, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_RECORD);

    auto struct_column = down_cast<StructColumn*>(column);
    const auto& record = datum.value<avro::GenericRecord>();

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        const auto& field_name = type_desc.field_names[i];
        auto& field_column = struct_column->field_column(field_name);

        if (record.hasField(field_name)) {
            const auto& field = record.field(field_name);
            RETURN_IF_ERROR(add_nullable_column(field, field_name, type_desc.children[i], invalid_as_null, timezone,
                                                field_column.get()));
        } else {
            field_column->append_nulls(1);
        }
    }
    return Status::OK();
}

// ------ array column ------

Status add_array_column(const avro::GenericDatum& datum, const std::string& col_name, const TypeDescriptor& type_desc,
                        bool invalid_as_null, const cctz::time_zone& timezone, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_ARRAY);

    auto array_column = down_cast<ArrayColumn*>(column);
    auto& elements_column = array_column->elements_column();
    auto& offsets_column = array_column->offsets_column();

    const auto& array = datum.value<avro::GenericArray>();
    const auto& array_values = array.value();

    uint32_t n = 0;
    for (auto& value : array_values) {
        RETURN_IF_ERROR(add_nullable_column(value, col_name, type_desc.children[0], invalid_as_null, timezone,
                                            elements_column.get()));
        ++n;
    }

    uint32_t sz = offsets_column->get_data().back() + n;
    offsets_column->append_numbers(&sz, sizeof(sz));
    return Status::OK();
}

// ------ map column ------

Status add_map_column(const avro::GenericDatum& datum, const std::string& col_name, const TypeDescriptor& type_desc,
                      bool invalid_as_null, const cctz::time_zone& timezone, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_MAP);

    auto map_column = down_cast<MapColumn*>(column);
    auto keys_column = down_cast<NullableColumn*>(map_column->keys_column().get());
    auto& keys_null_column = keys_column->null_column();
    auto keys_data_column = down_cast<BinaryColumn*>(keys_column->data_column().get());
    auto& values_column = map_column->values_column();
    auto& offsets_column = map_column->offsets_column();

    const auto& map = datum.value<avro::GenericMap>();
    const auto& map_values = map.value();

    uint32_t n = 0;
    for (auto& p : map_values) {
        const auto& key = p.first;
        keys_data_column->append(Slice(key));
        keys_null_column->append(0);

        const auto& value = p.second;
        RETURN_IF_ERROR(add_nullable_column(value, col_name, type_desc.children[1], invalid_as_null, timezone,
                                            values_column.get()));

        ++n;
    }
    offsets_column->append(offsets_column->get_data().back() + n);
    return Status::OK();
}

} // namespace starrocks
