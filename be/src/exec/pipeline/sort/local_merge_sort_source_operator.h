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

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/sort_exec_exprs.h"

namespace starrocks::pipeline {
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
              _sort_context(sort_context) {}

    ~LocalMergeSortSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) {}

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

private:
    bool _is_finished = false;
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

} // namespace starrocks::pipeline
