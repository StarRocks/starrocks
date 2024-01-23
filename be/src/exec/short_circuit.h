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

#pragma once

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "gen_cpp/ShortCircuit_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "storage/table_reader.h"
#include "util/stopwatch.hpp"

namespace starrocks {

class ShortCircuitExecutor {
public:
    ShortCircuitExecutor(ExecEnv* exec_env);
    ~ShortCircuitExecutor();

    Status prepare(TExecShortCircuitParams& common_request);

    Status execute();

    Status fetch_data(brpc::Controller* cntl, PExecShortCircuitResult& response);

    RuntimeState* runtime_state();

private:
    void close();
    Status build_source_exec_helper(starrocks::ObjectPool* pool, std::vector<TPlanNode>& tnodes, int* index,
                                    DescriptorTbl& descs, const TScanRange& scan_range, starrocks::ExecNode** node);
    Status build_source_exec_node(starrocks::ObjectPool* pool, TPlanNode& t_node, DescriptorTbl& descs,
                                  const TScanRange& scan_range, starrocks::ExecNode** node);

    // for scan node
    TExecShortCircuitParams* _common_request = nullptr;

    // used for identity and fetch data
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    bool _closed = false;
    bool _finish = false;

    // env
    ExecEnv* _exec_env;
    std::shared_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile;
    bool _enable_profile;

    // exec
    ExecNode* _source = nullptr;
    TDescriptorTable* _t_desc_tbl;
    std::unique_ptr<DataSink> _sink;
    std::vector<std::unique_ptr<TFetchDataResult>> _results;
};

class MysqlResultMemorySink;
} // namespace starrocks
