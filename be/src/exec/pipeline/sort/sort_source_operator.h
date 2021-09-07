// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "exec/sort_exec_exprs.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/mysql_result_writer.h"
#include "util/stack_util.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
class SortSourceOperator final : public SourceOperator {
public:
    SortSourceOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<vectorized::ChunksSorter>&& chunks_sorter)
            : SourceOperator(id, "sort_source", plan_node_id), _chunks_sorter(chunks_sorter) {}

    ~SortSourceOperator() override = default;

    // Result sink will send result chunk to BufferControlBlock directly,
    // Then FE will pull result from BufferControlBlock
    bool has_output() override;

    bool need_input() override;

    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) {}

    virtual void finish(RuntimeState* state) override;

private:
    std::shared_ptr<vectorized::ChunksSorter> _chunks_sorter;

    bool _is_finished = false;
    vectorized::ChunkPtr _full_chunk = nullptr;

    std::atomic<bool> _is_source_complete = false;
};

class SortSourceOperatorFactory final : public OperatorFactory {
public:
    SortSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                              std::shared_ptr<vectorized::ChunksSorter>&& chunks_sorter)
            : OperatorFactory(id, plan_node_id), _chunks_sorter(chunks_sorter) {}

    ~SortSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        auto ope = std::make_shared<SortSourceOperator>(_id, _plan_node_id, std::move(_chunks_sorter));
        return ope;
    }

private:
    std::shared_ptr<vectorized::ChunksSorter> _chunks_sorter;
};

} // namespace pipeline
} // namespace starrocks
