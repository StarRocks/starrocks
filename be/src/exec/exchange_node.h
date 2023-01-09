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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/exchange_node.h

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

#include "exec/exec_node.h"
#include "exec/sort_exec_exprs.h"
#include "runtime/data_stream_recvr.h"

namespace starrocks {

class RuntimeProfile;

// Receiver node for data streams. The data stream receiver is created in Prepare()
// and closed in Close().
// is_merging is set to indicate that rows from different senders must be merged
// according to the sort parameters in _sort_exec_exprs. (It is assumed that the rows
// received from the senders themselves are sorted.)
// If _is_merging is true, the exchange node creates a DataStreamRecvr with the
// _is_merging flag and retrieves retrieves rows from the receiver via calls to
// DataStreamRecvr::GetNext(). It also prepares, opens and closes the ordering exprs in
// its SortExecExprs member that are used to compare rows.
// If _is_merging is false, the exchange node directly retrieves batches from the row
// batch queue of the DataStreamRecvr via calls to DataStreamRecvr::GetBatch().

class ExchangeNode final : public ExecNode {
public:
    ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~ExchangeNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    // Blocks until the first batch is available for consumption via GetNext().
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status collect_query_statistics(QueryStatistics* statistics) override;
    Status close(RuntimeState* state) override;

    // the number of senders needs to be set after the c'tor, because it's not
    // recorded in TPlanNode, and before calling prepare()
    void set_num_senders(int num_senders) { _num_senders = num_senders; }

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

protected:
    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Implements GetNext() for the case where _is_merging is true. Delegates the GetNext()
    // call to the underlying DataStreamRecvr.
    Status get_next_merging(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    const TExchangeNode& _texchange_node;

    int _num_senders; // needed for _stream_recvr construction

    // created in prepare() and owned by the RuntimeState
    std::shared_ptr<DataStreamRecvr> _stream_recvr;

    // our input rows are a prefix of the rows we produce
    RowDescriptor _input_row_desc;

    std::unique_ptr<Chunk> _input_chunk;
    bool _is_finished = false;

    // True if this is a merging exchange node. If true, GetNext() is delegated to the
    // underlying _stream_recvr, and _input_batch is not used/valid.
    bool _is_merging;

    // Sort expressions and parameters passed to the merging receiver..
    SortExecExprs _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    // Offset specifying number of rows to skip.
    int64_t _offset;

    // Number of rows skipped so far.
    int64_t _num_rows_skipped;

    // Sub plan query statistics receiver. It is shared with DataStreamRecvr and will be
    // called in two different threads. When ExchangeNode is destructed, this may be accessed
    // by recvr thread in DataStreamMgr's transmit_data.
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;
};

}; // namespace starrocks
