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

StructColumnReader::StructColumnReader(const std::string& col_name, const TypeDescriptor& type_desc,
                                       std::vector<ColumnReaderUniquePtr> field_readers)
        : ColumnReader(col_name, type_desc), _field_readers(std::move(field_readers)) {
    DCHECK_EQ(_type_desc.children.size(), _field_readers.size());
}

Status StructColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_RECORD);

    auto struct_column = down_cast<StructColumn*>(column);
    const auto& record = datum.value<avro::GenericRecord>();

    for (size_t i = 0; i < _type_desc.children.size(); ++i) {
        const auto& field_name = _type_desc.field_names[i];
        auto field_column = struct_column->field_column_mutable(field_name);

        if (record.hasField(field_name)) {
            const auto& field = record.field(field_name);
            auto* field_reader = down_cast<NullableColumnReader*>(_field_readers[i].get());
            RETURN_IF_ERROR(field_reader->read_datum(field, field_column.get()));
        } else {
            field_column->append_nulls(1);
        }
    }
    return Status::OK();
}

Status ArrayColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_ARRAY);

    auto* element_reader = down_cast<NullableColumnReader*>(_element_reader.get());

    auto array_column = down_cast<ArrayColumn*>(column);
    auto elements_column = array_column->elements_column_mutable_ptr();
    auto offsets_column_mut = array_column->offsets_column_mutable_ptr();
    auto* offsets_column = offsets_column_mut.get();

    const auto& array = datum.value<avro::GenericArray>();
    const auto& array_values = array.value();

    uint32_t n = 0;
    for (auto& value : array_values) {
        RETURN_IF_ERROR(element_reader->read_datum(value, elements_column.get()));
        ++n;
    }

    uint32_t sz = offsets_column->get_data().back() + n;
    offsets_column->append_numbers(&sz, sizeof(sz));
    return Status::OK();
}

Status MapColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    DCHECK_EQ(datum.type(), avro::AVRO_MAP);

    auto* value_reader = down_cast<NullableColumnReader*>(_value_reader.get());

    auto map_column = down_cast<MapColumn*>(column);
    auto keys_column_mut = map_column->keys_column()->as_mutable_ptr();
    auto keys_column = down_cast<NullableColumn*>(keys_column_mut.get());
    auto keys_null_column = keys_column->null_column_mutable_ptr();
    auto keys_data_column_mut = keys_column->data_column_mutable_ptr();
    auto keys_data_column = down_cast<BinaryColumn*>(keys_data_column_mut.get());
    auto values_column = map_column->values_column()->as_mutable_ptr();
    auto offsets_column_mut = map_column->offsets_column()->as_mutable_ptr();
    auto* offsets_column = down_cast<UInt32Column*>(offsets_column_mut.get());

    const auto& map = datum.value<avro::GenericMap>();
    const auto& map_values = map.value();

    uint32_t n = 0;
    for (auto& p : map_values) {
        const auto& key = p.first;
        if (UNLIKELY(key.size() > _type_desc.children[0].len)) {
            return Status::DataQualityError(fmt::format("Value length is beyond the capacity. column: {}, capacity: {}",
                                                        _col_name, _type_desc.children[0].len));
        }
        keys_data_column->append(Slice(key));
        keys_null_column->append(0);

        const auto& value = p.second;
        RETURN_IF_ERROR(value_reader->read_datum(value, values_column.get()));

        ++n;
    }
    offsets_column->append(offsets_column->get_data().back() + n);
    return Status::OK();
}

} // namespace starrocks::avrocpp
