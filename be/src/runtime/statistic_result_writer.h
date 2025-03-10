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

#include "runtime/buffer_control_result_writer.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;

class StatisticResultWriter final : public BufferControlResultWriter {
public:
    StatisticResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                          RuntimeProfile* parent_profile);

    ~StatisticResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

private:
    void _init_profile() override;

    StatusOr<TFetchDataResultPtr> _process_chunk(Chunk* chunk);

    Status _fill_statistic_data_v1(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_statistic_data_v2(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_dict_statistic_data(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_statistic_histogram(int version, const Columns& columns, const Chunk* chunk, TFetchDataResult* result);

    Status _fill_table_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                      TFetchDataResult* result);
    Status _fill_partition_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                          TFetchDataResult* result);

    Status _fill_full_statistic_data_v4(int version, const Columns& columns, const Chunk* chunk,
                                        TFetchDataResult* result);

    Status _fill_full_statistic_data_v5(int version, const Columns& columns, const Chunk* chunk,
                                        TFetchDataResult* result);

    Status _fill_full_statistic_data_external(int version, const Columns& columns, const Chunk* chunk,
                                              TFetchDataResult* result);

    Status _fill_full_statistic_query_external(int version, const Columns& columns, const Chunk* chunk,
                                               TFetchDataResult* result);

    Status _fill_full_statistic_query_external_v2(int version, const Columns& columns, const Chunk* chunk,
                                                  TFetchDataResult* result);

    Status _fill_statistic_histogram_external(int version, const Columns& columns, const Chunk* chunk,
                                              TFetchDataResult* result);

    Status _fill_multi_columns_statistics_data(int version, const Columns& columns, const Chunk* chunk,
                                               TFetchDataResult* result);

    Status _fill_multi_columns_statistics_data_for_query(int version, const Columns& columns, const Chunk* chunk,
                                                         TFetchDataResult* result);

private:
    const std::vector<ExprContext*>& _output_expr_ctxs;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
};

} // namespace starrocks
