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

#include "base/phmap/phmap.h"
#include "common/global_types.h"
#include "exec/pipeline_node.h"
#include "runtime/descriptors.h"

namespace starrocks {
class LookUpDispatcher;

class LookUpNode final : public PipelineNode {
public:
    LookUpNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~LookUpNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status open(RuntimeState* state) override { return Status::OK(); }

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override { return Status::OK(); }

    Status collect_query_statistics(QueryStatistics* statistics) override { return Status::OK(); }

    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    void set_num_fetchers(int32_t num_fetchers) { _num_peer_fetchers = num_fetchers; }

private:
    size_t _num_peer_fetchers = 0;
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
};
} // namespace starrocks