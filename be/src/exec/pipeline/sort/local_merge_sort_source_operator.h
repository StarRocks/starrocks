// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/sort_exec_exprs.h"

namespace starrocks {
namespace pipeline {
class SortContext;

/*
 * LocalMergeSortSourceOperator is used to merge multiple sorted datas from partion sort sink operator.
 * It is one instance and Execute in single threaded mode,  
 * It completely depends on SortContext with a heap to Dynamically filter out the smallest or largest data.
 */
class LocalMergeSortSourceOperator final : public SourceOperator {
public:
    LocalMergeSortSourceOperator(int32_t id, int32_t plan_node_id, SortContext* sort_context)
            : SourceOperator(id, "local_merge_sort_source", plan_node_id), _sort_context(sort_context) {
        _sort_context->ref();
    }

    ~LocalMergeSortSourceOperator() override = default;

    Status close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) {}

    void set_finishing(RuntimeState* state) override;
    void set_finished(RuntimeState* state) override;

private:
    bool _is_finished = false;
    SortContext* _sort_context;
};

class LocalMergeSortSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalMergeSortSourceOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<SortContext>&& sort_context)
            : SourceOperatorFactory(id, "local_merge_sort_source", plan_node_id),
              _sort_context(std::move(sort_context)) {}

    ~LocalMergeSortSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LocalMergeSortSourceOperator>(_id, _plan_node_id, _sort_context.get());
    }

private:
    // share data with multiple partition sort sink opeartor through _sort_context.
    std::shared_ptr<SortContext> _sort_context;
};

} // namespace pipeline
} // namespace starrocks
