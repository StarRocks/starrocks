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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/arrow/row_batch.cpp

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

#include "util/arrow/row_batch.h"

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>
#include <fmt/format.h>

#include <memory>

#include "common/logging.h"
#include "exprs/column_ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "util/arrow/utils.h"

namespace starrocks {

using strings::Substitute;

Status convert_to_arrow_type(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    case TYPE_TIME:
        *result = arrow::float64();
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_DECIMAL:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_JSON:
        *result = arrow::utf8();
        break;
    case TYPE_DECIMALV2:
        *result = std::make_shared<arrow::Decimal128Type>(27, 9);
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        *result = std::make_shared<arrow::Decimal128Type>(type.precision, type.scale);
        break;
    case TYPE_ARRAY: {
        std::shared_ptr<arrow::DataType> type0;
        convert_to_arrow_type(type.children[0], &type0);
        *result = arrow::list(type0);
        break;
    }
    case TYPE_MAP: {
        std::shared_ptr<arrow::DataType> type0;
        convert_to_arrow_type(type.children[0], &type0);
        std::shared_ptr<arrow::DataType> type1;
        convert_to_arrow_type(type.children[1], &type1);
        *result = arrow::map(type0, type1);
        break;
    }
    case TYPE_STRUCT: {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        if (type.field_names.size() != type.children.size()) {
            return Status::InternalError(
                    fmt::format("Struct filed names' size {} mismatch children size {} in convert_to_arrow_type()",
                                type.field_names.size(), type.children.size()));
        }
        for (auto i = 0; i < type.children.size(); ++i) {
            std::shared_ptr<arrow::DataType> type0;
            convert_to_arrow_type(type.children[i], &type0);
            fields.emplace_back(arrow::field(type.field_names[i], type0));
        }
        *result = arrow::struct_(fields);
        break;
    }
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown logical type($0)", type.type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(const TypeDescriptor& desc, const string& col_name, bool is_nullable,
                              std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc, &type));
    // we keep the col_name here just for compatibility, col_names are from the first RefSlot,
    // users of arrow should not adjust the order of columns based on the colname.
    *field = arrow::field(col_name, type, is_nullable);
    return Status::OK();
}

Status convert_to_arrow_schema(const RowDescriptor& row_desc,
                               const std::unordered_map<int64_t, std::string>& id_to_col_name,
                               std::shared_ptr<arrow::Schema>* result,
                               const std::vector<ExprContext*>& output_expr_ctxs) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& expr_context : output_expr_ctxs) {
        Expr* expr = expr_context->root();
        std::shared_ptr<arrow::Field> field;
        string col_name;
        ColumnRef* col_ref = expr->get_column_ref();
        DCHECK(col_ref != nullptr);
        int64_t slot_id = col_ref->slot_id();
        int64_t tuple_id = col_ref->tuple_id();
        int64_t id = tuple_id << 32 | slot_id;
        auto it = id_to_col_name.find(id);
        if (it == id_to_col_name.end()) {
            LOG(WARNING) << "Can't find the RefSlot in the row_desc.";
        } else {
            col_name = it->second;
        }

        RETURN_IF_ERROR(convert_to_arrow_field(expr->type(), col_name, expr->is_nullable(), &field));
        fields.push_back(field);
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result) {
    // create sink memory buffer outputstream with the computed capacity
    int64_t capacity;
    arrow::Status a_st = arrow::ipc::GetRecordBatchSize(record_batch, &capacity);
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "GetRecordBatchSize failure, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    auto sink_res = arrow::io::BufferOutputStream::Create(capacity, arrow::default_memory_pool());
    if (!sink_res.ok()) {
        std::stringstream msg;
        msg << "create BufferOutputStream failure, reason: " << sink_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    std::shared_ptr<arrow::io::BufferOutputStream> sink = sink_res.ValueOrDie();
    // create RecordBatch Writer
    auto writer_res = arrow::ipc::MakeStreamWriter(sink.get(), record_batch.schema());
    if (!writer_res.ok()) {
        std::stringstream msg;
        msg << "open RecordBatchStreamWriter failure, reason: " << writer_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer = writer_res.ValueOrDie();
    // write RecordBatch to memory buffer outputstream
    a_st = record_batch_writer->WriteRecordBatch(record_batch);
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "write record batch failure, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    [[maybe_unused]] auto wr_close_st = record_batch_writer->Close();
    auto finish_res = sink->Finish();
    if (!finish_res.ok()) {
        std::stringstream msg;
        msg << "allocate result buffer failure, reason: " << finish_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    std::shared_ptr<arrow::Buffer> buffer = finish_res.ValueOrDie();
    *result = buffer->ToString();
    // close the sink
    [[maybe_unused]] auto sk_close_st = sink->Close();
    return Status::OK();
}

} // namespace starrocks
