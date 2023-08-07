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

#include "exec/hash_joiner.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

using HashJoiner = starrocks::HashJoiner;

class HashJoinProbeOperator : public OperatorWithDependency {
public:
    HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          int32_t driver_sequence, HashJoinerPtr join_prober, HashJoinerPtr join_builder);
    ~HashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    bool is_ready() const override;
    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _join_prober.get());
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

protected:
    /// Reference the read-only hash table from builder in the first pull_chunk.
    Status _reference_builder_hash_table_once();

protected:
    const HashJoinerPtr _join_prober;
    // For non-broadcast join, _join_builder is identical to _join_prober.
    // For broadcast join, _join_prober references the hash table owned by _join_builder,
    // so increase the reference number of _join_builder to prevent it closing early.
    const HashJoinerPtr _join_builder;
};

class HashJoinProbeOperatorFactory : public OperatorFactory {
public:
    HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner);

    ~HashJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

protected:
    HashJoinerFactoryPtr _hash_joiner_factory;
};

} // namespace starrocks::pipeline
