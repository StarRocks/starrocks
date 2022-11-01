// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

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
    LocalMergeSortSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                 SortContext* sort_context)
            : SourceOperator(factory, id, "local_merge_source", plan_node_id, driver_sequence),
              _sort_context(sort_context) {
        _sort_context->ref();
    }

    ~LocalMergeSortSourceOperator() override = default;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;
    Status set_finished(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    SortContext* _sort_context;
};

class LocalMergeSortSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalMergeSortSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                        std::shared_ptr<SortContextFactory> sort_context_factory)
            : SourceOperatorFactory(id, "local_merge_source", plan_node_id),
              _sort_context_factory(std::move(sort_context_factory)) {}

    ~LocalMergeSortSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    // share data with multiple partition sort sink opeartor through _sort_context.
    std::shared_ptr<SortContextFactory> _sort_context_factory;
};

} // namespace pipeline
} // namespace starrocks
