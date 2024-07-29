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

#include "exec/exec_node.h"
#include "exec/scan_node.h"

namespace starrocks {
class CaptureVersionNode final : public ExecNode {
public:
    CaptureVersionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    ~CaptureVersionNode() override = default;

    StatusOr<pipeline::MorselQueueFactoryPtr> scan_range_to_morsel_queue_factory(
            const std::vector<TScanRangeParams>& global_scan_ranges);

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
};
} // namespace starrocks