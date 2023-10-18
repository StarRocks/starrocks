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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/data_sink.h

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

#include <utility>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/query_statistics.h"

namespace starrocks {

class ObjectPool;
class RuntimeProfile;
class RuntimeState;
class TPlanFragmentExecParams;
class RowDescriptor;

// Superclass of all data sinks.
class DataSink {
public:
    DataSink() = default;
    virtual ~DataSink() = default;

    [[nodiscard]] virtual Status init(const TDataSink& thrift_sink, RuntimeState* state);

    // Setup. Call before send(), Open(), or Close().
    // Subclasses must call DataSink::Prepare().
    [[nodiscard]] virtual Status prepare(RuntimeState* state);

    // Setup. Call before send() or close().
    [[nodiscard]] virtual Status open(RuntimeState* state) = 0;

    [[nodiscard]] virtual Status send_chunk(RuntimeState* state, Chunk* chunk);

    // Releases all resources that were allocated in prepare()/send().
    // Further send() calls are illegal after calling close().
    // It must be okay to call this multiple times. Subsequent calls should
    // be ignored.
    [[nodiscard]] virtual Status close(RuntimeState* state, Status exec_status) {
        _closed = true;
        return Status::OK();
    }

    // Creates a new data sink from thrift_sink. A pointer to the
    // new sink is written to *sink, and is owned by the caller.
    [[nodiscard]] static Status create_data_sink(RuntimeState* state, const TDataSink& thrift_sink,
                                                 const std::vector<TExpr>& output_exprs,
                                                 const TPlanFragmentExecParams& params, int32_t sender_id,
                                                 const RowDescriptor& row_desc, std::unique_ptr<DataSink>* sink);

    // Returns the runtime profile for the sink.
    virtual RuntimeProfile* profile() = 0;

    virtual void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
        _query_statistics = std::move(statistics);
    }

protected:
    RuntimeState* _runtime_state = nullptr;

    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed{false};

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;
};

} // namespace starrocks
