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

#include "exec/pipeline/source_operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

// CaptureVersion returns an empty result set.
class CaptureVersionOperator final : public SourceOperator {
public:
    CaptureVersionOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "capture_version", plan_node_id, false, driver_sequence) {}

    ~CaptureVersionOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return !_is_finished; }

    bool is_finished() const override { return _is_finished; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    MultiRowsetReleaseGuard _rowset_release_guard;
    RuntimeProfile::Counter* _capture_tablet_rowsets_timer = nullptr;
    bool _is_finished{};
};

class CaptureVersionOpFactory final : public SourceOperatorFactory {
public:
    CaptureVersionOpFactory(int32_t id, int32_t plan_node_id)
            : SourceOperatorFactory(id, "capture_version", plan_node_id) {}

    ~CaptureVersionOpFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<CaptureVersionOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    bool with_morsels() const override { return true; }

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }
};

} // namespace starrocks::pipeline
