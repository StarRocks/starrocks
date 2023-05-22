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

#pragma once

#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;

class StatisticResultWriter final : public ResultWriter {
public:
    StatisticResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                          RuntimeProfile* parent_profile);

    ~StatisticResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    Status close() override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

    StatusOr<bool> try_add_batch(TFetchDataResultPtrs& results) override;

private:
    void _init_profile();

    StatusOr<TFetchDataResultPtr> _process_chunk(Chunk* chunk);

    Status _fill_statistic_data_v1(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);
    Status _fill_dict_statistic_data(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_statistic_histogram(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_table_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                      TFetchDataResult* result);

    Status _fill_full_statistic_data_v4(int version, const Columns& columns, const Chunk* chunk,
                                        TFetchDataResult* result);

    Status _fill_full_statistic_data_external(int version, const Columns& columns, const Chunk* chunk,
                                              TFetchDataResult* result);

private:
    BufferControlBlock* _sinker;
    const std::vector<ExprContext*>& _output_expr_ctxs;

    // parent profile from result sink. not owned
    RuntimeProfile* _parent_profile;
    // total time
    RuntimeProfile::Counter* _total_timer = nullptr;
    // serialize time
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
};

} // namespace starrocks
