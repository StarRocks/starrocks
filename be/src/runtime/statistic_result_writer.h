// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;

namespace vectorized {

class StatisticResultWriter final : public ResultWriter {
public:
    StatisticResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                          RuntimeProfile* parent_profile);

    ~StatisticResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(vectorized::Chunk* chunk) override;

    Status close() override;

    StatusOr<TFetchDataResultPtrs> process_chunk(vectorized::Chunk* chunk) override;

    StatusOr<bool> try_add_batch(TFetchDataResultPtrs& results) override;

private:
    void _init_profile();

    StatusOr<TFetchDataResultPtr> _process_chunk(vectorized::Chunk* chunk);

    Status _fill_statistic_data_v1(int version, const vectorized::Columns& columns, const vectorized::Chunk* chunk,
                                   TFetchDataResult* result);
    Status _fill_dict_statistic_data(int version, const vectorized::Columns& columns, const vectorized::Chunk* chunk,
                                     TFetchDataResult* result);

    Status _fill_statistic_histogram(int version, const vectorized::Columns& columns, const vectorized::Chunk* chunk,
                                     TFetchDataResult* result);

    Status _fill_full_statistic_data_v4(int version, const Columns& columns, const Chunk* chunk,
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

} // namespace vectorized
} // namespace starrocks
