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

#include "formats/avro/cpp/nullable_column_reader.h"

#include "column/adaptive_nullable_column.h"
#include "column/nullable_column.h"

namespace starrocks::avrocpp {

Status NullableColumnReader::read_datum_for_adaptive_column(const avro::GenericDatum& datum, Column* column) {
    auto type = datum.type();
    if (type == avro::AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    } else if (type == avro::AVRO_UNION) {
        const auto& union_value = datum.value<avro::GenericUnion>();
        return read_datum_for_adaptive_column(union_value.datum(), column);
    }

    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto* data_column = nullable_column->mutable_begin_append_not_default_value();
    auto st = _base_reader->read_datum(datum, data_column);
    if (st.ok()) {
        nullable_column->finish_append_one_not_default_value();
    } else if (st.is_data_quality_error() && _invalid_as_null) {
        nullable_column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

Status NullableColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto type = datum.type();
    if (type == avro::AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    } else if (type == avro::AVRO_UNION) {
        const auto& union_value = datum.value<avro::GenericUnion>();
        return read_datum(union_value.datum(), column);
    }

    auto nullable_column = down_cast<NullableColumn*>(column);
    auto data_column = nullable_column->data_column_mutable_ptr();
    auto st = _base_reader->read_datum(datum, data_column.get());
    if (st.ok()) {
        auto null_col_mut = nullable_column->null_column_mutable_ptr();
        null_col_mut->append(0);
    } else if (st.is_data_quality_error() && _invalid_as_null) {
        nullable_column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

} // namespace starrocks::avrocpp
