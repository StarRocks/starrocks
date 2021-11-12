// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_result_writer.h

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

#include "common/statusor.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class TupleRow;
class RowBatch;
class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;
using TFetchDataResultPtr = std::unique_ptr<TFetchDataResult>;
// convert the row batch to mysql protocol row
class MysqlResultWriter final : public ResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                      RuntimeProfile* parent_profile);

    ~MysqlResultWriter() override;

    Status init(RuntimeState* state) override;

    // convert one row batch to mysql result and
    // append this batch to the result sink
    Status append_row_batch(const RowBatch* batch) override;

    Status append_chunk(vectorized::Chunk* chunk) override;

    Status close() override;

    // decompose append_chunk into two functions: process_chunk and try_add_batch,
    // the former transform input chunk into TFetchDataResult, the latter add TFetchDataResult
    // to queue whose consumers are rpc threads that invoke fetch_data rpc.
    StatusOr<TFetchDataResultPtr> process_chunk(vectorized::Chunk* chunk);

    // try to add result into _sinker if ResultQueue is not full and this operation is
    // non-blocking. return true on success, false in case of that ResultQueue is full.
    StatusOr<bool> try_add_batch(TFetchDataResultPtr result);

private:
    void _init_profile();
    // convert one tuple row
    Status _add_one_row(TupleRow* row);

private:
    BufferControlBlock* _sinker;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    MysqlRowBuffer* _row_buffer;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append batch opertion
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
};

} // namespace starrocks
