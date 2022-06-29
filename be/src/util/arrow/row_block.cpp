// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/arrow/row_block.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/arrow/row_block.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/visitor_inline.h>

#include <utility>

#include "gutil/strings/substitute.h"
#include "storage/field.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {

using strings::Substitute;

Status convert_to_arrow_type(FieldType type, std::shared_ptr<arrow::DataType>* result) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case OLAP_FIELD_TYPE_INT:
        *result = arrow::int32();
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown FieldType($0)", type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(uint32_t cid, const vectorized::Field* field, std::shared_ptr<arrow::Field>* result) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(field->type()->type(), &type));
    *result = arrow::field(strings::Substitute("Col$0", cid), type, field->is_nullable());
    return Status::OK();
}

Status convert_to_arrow_schema(const vectorized::Schema& schema, std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    size_t num_fields = schema.num_fields();
    fields.resize(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        RETURN_IF_ERROR(convert_to_arrow_field(i, schema.field(i).get(), &fields[i]));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status convert_to_type_name(const arrow::DataType& type, std::string* name) {
    switch (type.id()) {
    case arrow::Type::INT8:
        *name = "TINYINT";
        break;
    case arrow::Type::INT16:
        *name = "SMALLINT";
        break;
    case arrow::Type::INT32:
        *name = "INT";
        break;
    case arrow::Type::INT64:
        *name = "BIGINT";
        break;
    case arrow::Type::FLOAT:
        *name = "FLOAT";
        break;
    case arrow::Type::DOUBLE:
        *name = "DOUBLE";
        break;
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown arrow type id($0)", type.id()));
    }
    return Status::OK();
}

Status convert_to_field(const arrow::Field& field, int32_t cid, std::shared_ptr<vectorized::Field>& output) {
    std::string type_name;
    RETURN_IF_ERROR(convert_to_type_name(*field.type(), &type_name));
    TypeInfoPtr type_info = nullptr;
    type_info = get_type_info(TabletColumn::get_field_type_by_string(type_name));
    output.reset(new vectorized::Field(cid, field.name(), type_info, field.nullable()));
    output->set_is_key(true);
    return Status::OK();
}

Status convert_to_starrocks_schema(const arrow::Schema& schema, std::shared_ptr<vectorized::Schema>* result) {
    auto num_fields = schema.num_fields();
    vectorized::Fields columns(num_fields);
    std::vector<ColumnId> col_ids(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        RETURN_IF_ERROR(convert_to_field(*schema.field(i), i, columns[i]));
    }
    // The KeysType::DUP_KEYS here meaningless, it is just a default value
    result->reset(new vectorized::Schema(columns, KeysType::DUP_KEYS));
    return Status::OK();
}

} // namespace starrocks
