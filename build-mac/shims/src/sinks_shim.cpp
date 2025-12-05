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

// macOS shim: provide minimal sink implementations to avoid Parquet/HDFS deps
#include "runtime/hive_table_sink.h"
#include "runtime/table_function_table_sink.h"

namespace starrocks {

HiveTableSink::HiveTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}
HiveTableSink::~HiveTableSink() = default;
Status HiveTableSink::init(const TDataSink&, RuntimeState*) { return Status::NotSupported("hive sink not supported on macOS"); }
Status HiveTableSink::prepare(RuntimeState*) { return Status::NotSupported("hive sink not supported on macOS"); }
Status HiveTableSink::open(RuntimeState*) { return Status::NotSupported("hive sink not supported on macOS"); }
Status HiveTableSink::send_chunk(RuntimeState*, Chunk*) { return Status::NotSupported("hive sink not supported on macOS"); }
Status HiveTableSink::close(RuntimeState*, Status) { return Status::OK(); }
Status HiveTableSink::decompose_to_pipeline(pipeline::OpFactories, const TDataSink&, pipeline::PipelineBuilderContext*) const {
    return Status::NotSupported("hive sink not supported on macOS");
}

TableFunctionTableSink::TableFunctionTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}
TableFunctionTableSink::~TableFunctionTableSink() = default;
Status TableFunctionTableSink::init(const TDataSink&, RuntimeState*) { return Status::NotSupported("table function sink not supported on macOS"); }
Status TableFunctionTableSink::prepare(RuntimeState*) { return Status::NotSupported("table function sink not supported on macOS"); }
Status TableFunctionTableSink::open(RuntimeState*) { return Status::NotSupported("table function sink not supported on macOS"); }
Status TableFunctionTableSink::send_chunk(RuntimeState*, Chunk*) { return Status::NotSupported("table function sink not supported on macOS"); }
Status TableFunctionTableSink::close(RuntimeState*, Status) { return Status::OK(); }
Status TableFunctionTableSink::decompose_to_pipeline(pipeline::OpFactories, const TDataSink&, pipeline::PipelineBuilderContext*) const {
    return Status::NotSupported("table function sink not supported on macOS");
}

} // namespace starrocks

