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

#include "formats/avro/cpp/complex_column_reader.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "formats/avro/cpp/nullable_column_reader.h"
#include "formats/avro/cpp/utils.h"

namespace starrocks::avrocpp {

StructColumnReader::StructColumnReader(std::string_view col_name, const TypeDescriptor& type_desc,
                                       std::vector<ColumnReaderUniquePtr> field_readers)
        : ColumnReader(col_name, type_desc), _field_readers(std::move(field_readers)) {
    DCHECK_EQ(_type_desc.children.size(), _field_readers.size());
}

Status StructColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    // The reader is built from the table column type while the datum carries whatever the writer sent,
    // so a kind mismatch is reachable data, not a programming error: value<T>() on it would crash.
    if (UNLIKELY(datum.type() != avro::AVRO_RECORD)) {
        return Status::DataQualityError(
                fmt::format("Unsupported avro type {} to struct. column: {}", avro::toString(datum.type()), _col_name));
    }

    auto struct_column = down_cast<StructColumn*>(column);
    const auto& record = datum.value<avro::GenericRecord>();

    for (size_t i = 0; i < _type_desc.children.size(); ++i) {
        const auto& field_name = _type_desc.field_names[i];
        ASSIGN_OR_RETURN(auto* field_column, struct_column->field_column_raw_ptr(field_name));

        if (_field_readers[i] == nullptr) {
            field_column->append_nulls(1);
        } else if (record.hasField(field_name)) {
            const auto& field = record.field(field_name);
            auto* field_reader = down_cast<NullableColumnReader*>(_field_readers[i].get());
            RETURN_IF_ERROR(field_reader->read_datum(field, field_column));
        } else {
            field_column->append_nulls(1);
        }
    }
    return Status::OK();
}

Status ArrayColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    if (UNLIKELY(datum.type() != avro::AVRO_ARRAY)) {
        return Status::DataQualityError(
                fmt::format("Unsupported avro type {} to array. column: {}", avro::toString(datum.type()), _col_name));
    }

    auto array_column = down_cast<ArrayColumn*>(column);
    auto* elements_column = array_column->elements_column_raw_ptr();
    auto* offsets_column = array_column->offsets_column_raw_ptr();

    const auto& array = datum.value<avro::GenericArray>();
    const auto& array_values = array.value();

    uint32_t n = 0;
    for (auto& value : array_values) {
        if (_element_reader != nullptr) {
            auto* element_reader = down_cast<NullableColumnReader*>(_element_reader.get());
            RETURN_IF_ERROR(element_reader->read_datum(value, elements_column));
        } else {
            elements_column->append_nulls(1);
        }
        ++n;
    }

    uint32_t sz = offsets_column->get_data().back() + n;
    offsets_column->append_numbers(&sz, sizeof(sz));
    return Status::OK();
}

Status MapColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    if (UNLIKELY(datum.type() != avro::AVRO_MAP)) {
        return Status::DataQualityError(
                fmt::format("Unsupported avro type {} to map. column: {}", avro::toString(datum.type()), _col_name));
    }

    auto map_column = down_cast<MapColumn*>(column);
    auto keys_column = down_cast<NullableColumn*>(map_column->keys_column_raw_ptr());
    auto* values_column = map_column->values_column_raw_ptr();
    auto* offsets_column = map_column->offsets_column_raw_ptr();

    // Avro map keys are always strings, and the key path below appends raw bytes into a binary key
    // column. FILES() always types the key column VARCHAR (inferred schema), but routine load types it
    // straight from the table, so e.g. MAP<INT,...> reaches here: reject it instead of corrupting it.
    if (_key_reader != nullptr && UNLIKELY(!keys_column->data_column_raw_ptr()->is_binary())) {
        return Status::DataQualityError(
                fmt::format("avro map keys are strings; the key type of map column '{}' cannot hold them", _col_name));
    }

    const auto& map = datum.value<avro::GenericMap>();
    const auto& map_values = map.value();

    uint32_t n = 0;
    for (auto& p : map_values) {
        if (_key_reader != nullptr) {
            auto* keys_null_column = keys_column->null_column_raw_ptr();
            auto* keys_data_column = down_cast<BinaryColumn*>(keys_column->data_column_raw_ptr());
            const auto& key = p.first;
            if (UNLIKELY(key.size() > _type_desc.children[0].len)) {
                return Status::DataQualityError(
                        fmt::format("Value length is beyond the capacity. column: {}, capacity: {}", _col_name,
                                    _type_desc.children[0].len));
            }
            keys_data_column->append(Slice(key));
            keys_null_column->append(0);
        } else {
            keys_column->append_nulls(1);
        }

        if (_value_reader != nullptr) {
            auto* value_reader = down_cast<NullableColumnReader*>(_value_reader.get());
            const auto& value = p.second;
            RETURN_IF_ERROR(value_reader->read_datum(value, values_column));
        } else {
            values_column->append_nulls(1);
        }

        ++n;
    }
    offsets_column->append(offsets_column->get_data().back() + n);
    return Status::OK();
}

} // namespace starrocks::avrocpp
