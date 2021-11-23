// This file is made available under Elastic License 2.0.
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
#include "runtime/memory_scratch_sink.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/result_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks {

Status DataSink::create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs, const TPlanFragmentExecParams& params,
                                  const RowDescriptor& row_desc, std::unique_ptr<DataSink>* sink) {
    DCHECK(sink != nullptr);
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        bool send_query_statistics_with_every_batch = params.__isset.send_query_statistics_with_every_batch
                                                              ? params.send_query_statistics_with_every_batch
                                                              : false;
        // TODO: figure out good buffer size based on size of output row
        *sink = std::make_unique<DataStreamSender>(pool, params.use_vectorized, params.sender_id, row_desc,
                                                   thrift_sink.stream_sink, params.destinations, 16 * 1024,
                                                   send_query_statistics_with_every_batch);
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
        *sink = std::make_unique<MysqlTableSink>(pool, row_desc, output_exprs);
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        if (!thrift_sink.__isset.export_sink) {
            return Status::InternalError("Missing export sink sink.");
        }
        *sink = std::make_unique<ExportSink>(pool, row_desc, output_exprs);
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status;
        DCHECK(thrift_sink.__isset.olap_table_sink);
        *sink = std::make_unique<stream_load::OlapTableSink>(pool, row_desc, output_exprs, &status,
                                                             params.use_vectorized);
        RETURN_IF_ERROR(status);
        break;
    }

    default:
        std::stringstream error_msg;
        std::map<int, const char*>::const_iterator i = _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
        const char* str = "Unknown data sink type ";

        if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented.";
        return Status::InternalError(error_msg.str());
    }

    if (*sink != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink));
    }

    return Status::OK();
}

Status DataSink::init(const TDataSink& thrift_sink) {
    return Status::OK();
}

Status DataSink::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

Status DataSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    return Status::NotSupported("Don't support vector query engine");
}

} // namespace starrocks
