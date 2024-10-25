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

#include "udf/python/callstub.h"

#include <cstring>
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/flight/client.h"
#include "arrow/type.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/base64.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/types.h"
#include "udf/python/env.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"

#define RETURN_IF_ARROW_ERROR(expr)    \
    do {                               \
        auto status = to_status(expr); \
        if (!status.ok()) {            \
            return status;             \
        }                              \
    } while (0)

#define ARROW_ASSIGN_OR_RETURN(lhs, rhs) ARROW_ASSIGN_OR_RETURN_IMPL(VARNAME_LINENUM(value_or_err), lhs, rhs)

#define ARROW_ASSIGN_OR_RETURN_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                            \
    RETURN_IF_ARROW_ERROR(varname.status());           \
    lhs = std::move(varname).ValueOrDie();

namespace starrocks {

using ArrowFlightClient = arrow::flight::FlightClient;

class ArrowFlightWithRW {
public:
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;

    Status init(const std::string& uri_string, const PyFunctionDescriptor& func_desc,
                std::shared_ptr<PyWorker> process);
    StatusOr<std::shared_ptr<arrow::RecordBatch>> rpc(arrow::RecordBatch& batch);

    void close();

private:
    bool _begin = false;
    std::unique_ptr<ArrowFlightClient> _arrow_client;
    std::unique_ptr<FlightStreamWriter> _writer;
    std::unique_ptr<FlightStreamReader> _reader;
    std::shared_ptr<PyWorker> _process;
};

Status ArrowFlightWithRW::init(const std::string& uri_string, const PyFunctionDescriptor& func_desc,
                               std::shared_ptr<PyWorker> process) {
    using namespace arrow::flight;
    ARROW_ASSIGN_OR_RETURN(auto location, Location::Parse(uri_string));
    ARROW_ASSIGN_OR_RETURN(_arrow_client, ArrowFlightClient::Connect(location));
    ASSIGN_OR_RETURN(auto command, func_desc.to_json_string());
    FlightDescriptor descriptor = FlightDescriptor::Command(command);
    ARROW_ASSIGN_OR_RETURN(auto result, _arrow_client->DoExchange(descriptor));
    _reader = std::move(result.reader);
    _writer = std::move(result.writer);
    _process = std::move(process);
    return Status::OK();
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> ArrowFlightWithRW::rpc(arrow::RecordBatch& batch) {
    auto do_rpc = [this](arrow::RecordBatch& batch) -> StatusOr<std::shared_ptr<arrow::RecordBatch>> {
        if (!_begin) {
            RETURN_IF_ARROW_ERROR(_writer->Begin(batch.schema()));
            _begin = true;
        }
        RETURN_IF_ARROW_ERROR(_writer->WriteRecordBatch(batch));
        arrow::flight::FlightStreamChunk stream_chunk;
        ARROW_ASSIGN_OR_RETURN(stream_chunk, _reader->Next());
        return stream_chunk.data;
    };
    auto result_with_st = do_rpc(batch);

    if (!result_with_st.status().ok()) {
        _process->mark_dead();
    }
    return result_with_st;
}

void ArrowFlightWithRW::close() {
    if (_writer != nullptr) {
        WARN_IF_ERROR(to_status(_writer->Close()), "arrow flight rpc close error:");
    }
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> ArrowFlightFuncCallStub::do_evaluate(RecordBatch&& batch) {
    size_t num_rows = batch.num_rows();
    ASSIGN_OR_RETURN(auto result_batch, _client->rpc(batch));
    if (result_batch->num_rows() != num_rows) {
        return Status::InternalError(
                fmt::format("unexpected result batch rows from UDF:{} expect:{}", result_batch->num_rows(), num_rows));
    }
    return result_batch;
}

auto PyWorkerManager::get_client(const PyFunctionDescriptor& func_desc) -> StatusOr<WorkerClientPtr> {
    std::shared_ptr<PyWorker> handle;
    std::string url;
    ASSIGN_OR_RETURN(handle, _acquire_worker(func_desc.driver_id, config::python_worker_reuse, &url));
    auto arrow_client = std::make_unique<ArrowFlightWithRW>();
    RETURN_IF_ERROR(arrow_client->init(url, func_desc, std::move(handle)));
    return arrow_client;
}

StatusOr<std::shared_ptr<arrow::Schema>> convert_type_to_schema(const TypeDescriptor& typedesc) {
    // conver to field
    arrow::SchemaBuilder schema_builder;
    std::shared_ptr<arrow::Field> field;
    RETURN_IF_ERROR(convert_to_arrow_field(typedesc, "result", true, &field));
    RETURN_IF_ARROW_ERROR(schema_builder.AddField(field));
    std::shared_ptr<arrow::Schema> schema;
    auto result_schema = schema_builder.Finish();
    RETURN_IF_ARROW_ERROR(std::move(result_schema).Value(&schema));
    return schema;
}

StatusOr<std::string> PyFunctionDescriptor::to_json_string() const {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    // Adding basic string properties
    doc.AddMember("symbol", rapidjson::Value().SetString(symbol.c_str(), allocator), allocator);
    doc.AddMember("location", rapidjson::Value().SetString(location.c_str(), allocator), allocator);
    doc.AddMember("input_type", rapidjson::Value().SetString(input_type.c_str(), allocator), allocator);
    doc.AddMember("content", rapidjson::Value().SetString(content.c_str(), content.size(), allocator), allocator);

    {
        // serialize return type schema
        ASSIGN_OR_RETURN(auto schema, convert_type_to_schema(return_type));
        auto serialized_schema_result = arrow::ipc::SerializeSchema(*schema);
        std::shared_ptr<arrow::Buffer> serialized_schema;
        RETURN_IF_ARROW_ERROR(std::move(serialized_schema_result).Value(&serialized_schema));
        const uint8_t* data = serialized_schema->data();
        size_t serialized_size = serialized_schema->size();
        int base64_length = (size_t)(4.0 * ceil((double)serialized_size / 3.0)) + 1;
        char p[base64_length];
        int len = base64_encode2((unsigned char*)data, serialized_size, (unsigned char*)p);
        doc.AddMember("return_type", rapidjson::Value().SetString(p, len, allocator), allocator);
    }

    // Convert document to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    auto value = std::string(buffer.GetString(), buffer.GetSize());

    return value;
}

std::unique_ptr<UDFCallStub> build_py_call_stub(FunctionContext* context, const PyFunctionDescriptor& func_desc) {
    auto& instance = PyWorkerManager::getInstance();
    auto worker_with_st = instance.get_client(func_desc);
    if (!worker_with_st.ok()) {
        return create_error_call_stub(worker_with_st.status());
    }
    auto worker = worker_with_st.value();
    return std::make_unique<ArrowFlightFuncCallStub>(context, worker);
}
} // namespace starrocks
