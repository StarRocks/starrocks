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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/data_sink.cpp

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

#include "exec/data_sink.h"

#include <algorithm>
#include <map>
#include <memory>

#include "common/logging.h"
#include "exec/exec_node.h"
#include "exec/file_builder.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/data_stream_sender.h"
#include "runtime/export_sink.h"
#include "runtime/iceberg_table_sink.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/multi_cast_data_stream_sink.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/result_sink.h"
#include "runtime/runtime_state.h"
#include "runtime/schema_table_sink.h"

namespace starrocks {

static std::unique_ptr<DataStreamSender> create_data_stream_sink(
        RuntimeState* state, const TDataStreamSink& data_stream_sink, const RowDescriptor& row_desc,
        const TPlanFragmentExecParams& params, int32_t sender_id,
        const std::vector<TPlanFragmentDestination>& destinations) {
    bool send_query_statistics_with_every_batch =
            params.__isset.send_query_statistics_with_every_batch && params.send_query_statistics_with_every_batch;
    bool enable_exchange_pass_through =
            params.__isset.enable_exchange_pass_through && params.enable_exchange_pass_through;
    bool enable_exchange_perf = params.__isset.enable_exchange_perf && params.enable_exchange_perf;

    // TODO: figure out good buffer size based on size of output row
    auto ret = std::make_unique<DataStreamSender>(state, sender_id, row_desc, data_stream_sink, destinations, 16 * 1024,
                                                  send_query_statistics_with_every_batch, enable_exchange_pass_through,
                                                  enable_exchange_perf);
    return ret;
}

Status DataSink::create_data_sink(RuntimeState* state, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs, const TPlanFragmentExecParams& params,
                                  int32_t sender_id, const RowDescriptor& row_desc, std::unique_ptr<DataSink>* sink) {
    DCHECK(sink != nullptr);
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        *sink = create_data_stream_sink(state, thrift_sink.stream_sink, row_desc, params, sender_id,
                                        params.destinations);
        break;
    }
    case TDataSinkType::RESULT_SINK:
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        *sink = std::make_unique<ResultSink>(row_desc, output_exprs, thrift_sink.result_sink, 1024);
        break;
    case TDataSinkType::MEMORY_SCRATCH_SINK:
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        *sink = std::make_unique<MemoryScratchSink>(row_desc, output_exprs, thrift_sink.memory_scratch_sink);
        break;
    case TDataSinkType::MYSQL_TABLE_SINK: {
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        // TODO: figure out good buffer size based on size of output row
        *sink = std::make_unique<MysqlTableSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        if (!thrift_sink.__isset.export_sink) {
            return Status::InternalError("Missing export sink sink.");
        }
        *sink = std::make_unique<ExportSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status;
        DCHECK(thrift_sink.__isset.olap_table_sink);
        *sink = std::make_unique<stream_load::OlapTableSink>(state->obj_pool(), output_exprs, &status, state);
        RETURN_IF_ERROR(status);
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink || thrift_sink.multi_cast_stream_sink.sinks.size() == 0)
                << "Missing mcast stream sink.";
        auto mcast_data_stream_sink = std::make_unique<MultiCastDataStreamSink>(state);
        const auto& thrift_mcast_stream_sink = thrift_sink.multi_cast_stream_sink;

        for (size_t i = 0; i < thrift_mcast_stream_sink.sinks.size(); i++) {
            const auto& sink = thrift_mcast_stream_sink.sinks[i];
            const auto& destinations = thrift_mcast_stream_sink.destinations[i];
            auto ret = create_data_stream_sink(state, sink, row_desc, params, sender_id, destinations);
            mcast_data_stream_sink->add_data_stream_sink(std::move(ret));
        }
        *sink = std::move(mcast_data_stream_sink);
        break;
    }
    case TDataSinkType::SCHEMA_TABLE_SINK: {
        if (!thrift_sink.__isset.schema_table_sink) {
            return Status::InternalError("Missing schema table sink.");
        }
        *sink = std::make_unique<SchemaTableSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        if (!thrift_sink.__isset.iceberg_table_sink) {
            return Status::InternalError("Missing iceberg table sink");
        }
        *sink = std::make_unique<IcebergTableSink>(state->obj_pool(), output_exprs);
        break;
    }

    default:
        std::stringstream error_msg;
        auto i = _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
        const char* str = "Unknown data sink type ";

        if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented.";
        return Status::InternalError(error_msg.str());
    }

    if (*sink != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink, state));
    }

    return Status::OK();
}

Status DataSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    return Status::OK();
}

Status DataSink::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

Status DataSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::NotSupported("Don't support vector query engine");
}

} // namespace starrocks
