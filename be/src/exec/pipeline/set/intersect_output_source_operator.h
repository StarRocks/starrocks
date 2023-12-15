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

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/intersect_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// IntersectOutputSourceOperator traverses the hast set and picks up entries hit by all
// IntersectProbeSinkOperators after probe phase is finished.
// For more detail information, see the comments of class IntersectBuildSinkOperator.
class IntersectOutputSourceOperator final : public SourceOperator {
public:
    IntersectOutputSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                  std::shared_ptr<IntersectContext> intersect_ctx, const int32_t dependency_index)
            : SourceOperator(factory, id, "intersect_output_source", plan_node_id, driver_sequence),
              _intersect_ctx(std::move(intersect_ctx)),
              _dependency_index(dependency_index) {
        _intersect_ctx->ref();
    }

    bool has_output() const override {
        return _intersect_ctx->is_dependency_finished(_dependency_index) && !_intersect_ctx->is_output_finished();
    }

    bool is_finished() const override {
        return _intersect_ctx->is_dependency_finished(_dependency_index) && _intersect_ctx->is_output_finished();
    }

    Status set_finished(RuntimeState* state) override {
        RETURN_IF_ERROR(_intersect_ctx->set_finished());
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::shared_ptr<IntersectContext> _intersect_ctx;
    const int32_t _dependency_index;
};

class IntersectOutputSourceOperatorFactory final : public SourceOperatorFactory {
public:
    IntersectOutputSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                         IntersectPartitionContextFactoryPtr intersect_partition_ctx_factory,
                                         const int32_t dependency_index)
            : SourceOperatorFactory(id, "intersect_output_source", plan_node_id),
              _intersect_partition_ctx_factory(std::move(intersect_partition_ctx_factory)),
              _dependency_index(dependency_index) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<IntersectOutputSourceOperator>(
                this, _id, _plan_node_id, driver_sequence,
                _intersect_partition_ctx_factory->get_or_create(driver_sequence), _dependency_index);
    }

    void close(RuntimeState* state) override;

private:
    IntersectPartitionContextFactoryPtr _intersect_partition_ctx_factory;
    const int32_t _dependency_index;
};

} // namespace starrocks::pipeline
