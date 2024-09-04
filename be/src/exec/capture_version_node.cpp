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

#include "exec/capture_version_node.h"

#include "exec/pipeline/capture_version_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

pipeline::OpFactories CaptureVersionNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    return OpFactories{std::make_shared<pipeline::CaptureVersionOpFactory>(context->next_operator_id(), id())};
}

StatusOr<pipeline::MorselQueueFactoryPtr> CaptureVersionNode::scan_range_to_morsel_queue_factory(
        const std::vector<TScanRangeParams>& scan_ranges) {
    pipeline::Morsels morsels;
    [[maybe_unused]] bool has_more_morsel = false;
    pipeline::ScanMorsel::build_scan_morsels(id(), scan_ranges, true, &morsels, &has_more_morsel);
    DCHECK(has_more_morsel == false);
    auto morsel_queue = std::make_unique<pipeline::FixedMorselQueue>(std::move(morsels));
    return std::make_unique<pipeline::SharedMorselQueueFactory>(std::move(morsel_queue), 1);
}

} // namespace starrocks