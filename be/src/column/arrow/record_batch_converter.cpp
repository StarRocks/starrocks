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

// This file contains code originally based on Apache Doris'
// be/src/util/arrow/row_batch.cpp and starrocks_column_to_arrow.cpp under the Apache License 2.0.

#include "column/arrow/record_batch_converter.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <sstream>
#include <vector>

#include "column/arrow/column_to_arrow_converter.h"
#include "types/type_descriptor.h"

namespace starrocks {

Status convert_columns_to_arrow_batch(size_t num_rows, const Columns& columns, arrow::MemoryPool* pool,
                                      const TypeDescriptor* type_descs, const std::shared_ptr<arrow::Schema>& schema,
                                      std::shared_ptr<arrow::RecordBatch>* result) {
    size_t num_columns = columns.size();
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_columns);

    for (size_t i = 0; i < num_columns; ++i) {
        RETURN_IF_ERROR(
                convert_column_to_arrow_array(columns[i], type_descs[i], schema->field(i)->type(), pool, &arrays[i]));
    }
    *result = arrow::RecordBatch::Make(schema, num_rows, std::move(arrays));
    return Status::OK();
}

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result) {
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
    const auto& sink = sink_res.ValueOrDie();
    auto writer_res = arrow::ipc::MakeStreamWriter(sink.get(), record_batch.schema());
    if (!writer_res.ok()) {
        std::stringstream msg;
        msg << "open RecordBatchStreamWriter failure, reason: " << writer_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    const auto& record_batch_writer = writer_res.ValueOrDie();
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
    const auto& buffer = finish_res.ValueOrDie();
    *result = buffer->ToString();
    [[maybe_unused]] auto sk_close_st = sink->Close();
    return Status::OK();
}

Status serialize_arrow_schema(std::shared_ptr<arrow::Schema>* schema, std::string* result) {
    auto empty_arrow_record_batch = arrow::RecordBatch::MakeEmpty(*schema);
    if (!empty_arrow_record_batch.ok()) {
        return Status::InternalError("serialize_arrow_schema failed");
    }

    const auto& record_batch = empty_arrow_record_batch.ValueOrDie();
    return serialize_record_batch(*record_batch, result);
}

} // namespace starrocks
