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

#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/scan/morsel.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

// Liveness tracker shared between the MorselQueueFactory stand-in and the
// Pipeline stand-in so the pipeline teardown can observe factory liveness without
// dereferencing freed memory itself.
struct MorselFactoryLiveness {
    bool factory_alive = true;
    bool factory_destroyed_before_pipeline = false;
};

// Stands in for the MorselQueueFactory owned by FragmentContext::_morsel_queue_factories.
// In production a scan operator's ConnectorChunkSource::close() calls into this
// object during fragment teardown.
class TrackingMorselQueueFactory final : public MorselQueueFactory {
public:
    explicit TrackingMorselQueueFactory(std::shared_ptr<MorselFactoryLiveness> live) : _live(std::move(live)) {}
    ~TrackingMorselQueueFactory() override { _live->factory_alive = false; }

    MorselQueue* create(int /*driver_sequence*/) override { return nullptr; }
    size_t size() const override { return 0; }
    size_t num_original_morsels() const override { return 0; }
    bool is_shared() const override { return false; }
    bool could_local_shuffle() const override { return false; }

private:
    std::shared_ptr<MorselFactoryLiveness> _live;
};

// Stands in for the operator subtree owned by FragmentContext::_pipelines. The
// real teardown chain is ~PipelineDriver -> ~ScanOperator ->
// ConnectorChunkSource::close() -> MorselQueueFactory::mark_split_source_morsel_finished();
// this destructor observes whether the factory is still alive at that point.
class TrackingPipeline final : public Pipeline {
public:
    explicit TrackingPipeline(std::shared_ptr<MorselFactoryLiveness> live)
            : Pipeline(0, OpFactories{}, nullptr), _live(std::move(live)) {}
    // Pipeline has a non-virtual destructor on this branch, so this cannot be
    // marked `override`. ~TrackingPipeline is still invoked correctly because the
    // Pipelines vector holds a std::shared_ptr constructed as TrackingPipeline,
    // whose type-erased deleter destroys the concrete type.
    ~TrackingPipeline() {
        if (!_live->factory_alive) {
            _live->factory_destroyed_before_pipeline = true;
        }
    }

private:
    std::shared_ptr<MorselFactoryLiveness> _live;
};

std::shared_ptr<FragmentContext> make_fragment_context(const TUniqueId& query_id, const TUniqueId& fragment_id) {
    auto fragment_ctx = std::make_shared<FragmentContext>();
    fragment_ctx->set_query_id(query_id);
    fragment_ctx->set_fragment_instance_id(fragment_id);
    fragment_ctx->set_runtime_state(std::make_shared<RuntimeState>());
    return fragment_ctx;
}

// Regression test for the scan-teardown use-after-free that crashed CN nodes:
// ConnectorChunkSource::close() runs from ~ScanOperator while FragmentContext's
// _pipelines are being destroyed, and calls back into a MorselQueueFactory owned
// by _morsel_queue_factories. The factory must therefore be destroyed AFTER the
// pipelines, i.e. declared before them. This pins that ordering so a future
// reshuffle of the member declarations cannot silently reintroduce the UAF.
TEST(FragmentContextMorselUafTest, MorselQueueFactoryOutlivesPipelines) {
    TUniqueId query_id;
    query_id.hi = 10;
    query_id.lo = 20;
    TUniqueId fragment_id;
    fragment_id.hi = 30;
    fragment_id.lo = 40;

    auto live = std::make_shared<MorselFactoryLiveness>();
    {
        auto fragment_ctx = make_fragment_context(query_id, fragment_id);
        fragment_ctx->morsel_queue_factories().emplace(0, std::make_unique<TrackingMorselQueueFactory>(live));

        Pipelines pipelines;
        pipelines.emplace_back(std::make_shared<TrackingPipeline>(live));
        fragment_ctx->set_pipelines(ExecutionGroups{}, std::move(pipelines));
    } // ~FragmentContext destroys members in reverse declaration order here.

    EXPECT_FALSE(live->factory_destroyed_before_pipeline)
            << "MorselQueueFactory was destroyed before the pipelines; scan teardown would use-after-free";
}

} // namespace
} // namespace starrocks::pipeline
