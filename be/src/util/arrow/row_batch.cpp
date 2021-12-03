// This file is made available under Elastic License 2.0.
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
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>

#include <cstdlib>
#include <ctime>
#include <memory>

#include "common/logging.h"
#include "exprs/slot_ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "util/arrow/utils.h"
#include "util/types.h"

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
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown primitive type($0)", type.type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(SlotDescriptor* desc, std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc->type(), &type));
    *field = arrow::field(desc->col_name(), type, desc->is_nullable());
    return Status::OK();
}

Status convert_to_arrow_schema(const RowDescriptor& row_desc, std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto tuple_desc : row_desc.tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            std::shared_ptr<arrow::Field> field;
            RETURN_IF_ERROR(convert_to_arrow_field(desc, &field));
            fields.push_back(field);
        }
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
    record_batch_writer->Close();
    auto finish_res = sink->Finish();
    if (!finish_res.ok()) {
        std::stringstream msg;
        msg << "allocate result buffer failure, reason: " << finish_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    std::shared_ptr<arrow::Buffer> buffer = finish_res.ValueOrDie();
    *result = buffer->ToString();
    // close the sink
    sink->Close();
    return Status::OK();
}

} // namespace starrocks
