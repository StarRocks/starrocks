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

#include "exec/pipeline/nljoin/nljoin_build_operator.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller_factory.h"

namespace starrocks::pipeline {

class SpillableNLJoinBuildOperator final : public NLJoinBuildOperator {
public:
    template <class... Args>
    SpillableNLJoinBuildOperator(Args&&... args) : NLJoinBuildOperator(std::forward<Args>(args)...) {}

    ~SpillableNLJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    void set_channel(SpillProcessChannelPtr channel) { _spill_channel = std::move(channel); }

private:
    bool _is_finished = false;
    SpillProcessChannelPtr _spill_channel;
    spill::SpillStrategy _strategy;
};

class SpillableNLJoinBuildOperatorFactory final : public OperatorFactory {
public:
    SpillableNLJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id,
                                        std::shared_ptr<NLJoinContext> cross_join_context)
            : OperatorFactory(id, "spillable_nljoin_build", plan_node_id),
              _cross_join_context(std::move(cross_join_context)) {}

    ~SpillableNLJoinBuildOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
    std::shared_ptr<NLJoinContext> _cross_join_context;
};
} // namespace starrocks::pipeline