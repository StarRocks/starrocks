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

#include "base/testutil/assert.h"
#include "base/testutil/scoped_updater.h"
#include "common/config_connector_sink_fwd.h"
#include "connector_primitive/connector.h"
#include "connector_primitive/data_source_provider.h"
#include "connector_primitive/sink_memory_manager.h"
#include "runtime/mem_tracker.h"

namespace starrocks::connector {

namespace {

class TestConnector final : public Connector {
public:
    ConnectorType connector_type() const override { return ConnectorType::BENCHMARK; }
};

class TestDataSourceProvider final : public DataSourceProvider {
public:
    DataSourcePtr create_data_source(const TScanRange&) override { return nullptr; }
    const TupleDescriptor* tuple_descriptor(RuntimeState*) const override { return nullptr; }
};

class TestConnectorSink final : public ConnectorSink {
public:
    Status init(formats::AsyncFlushStreamPoller*, RuntimeProfile*, SinkMemoryManager*) override { return Status::OK(); }
    Status add(const ChunkPtr&) override { return Status::OK(); }
    Status finish() override { return Status::OK(); }
    void rollback() override { rollback_calls++; }
    bool is_finished() override { return finished; }
    Status status() override { return status_value; }
    SinkOperatorMemoryManager* op_mem_mgr() const override { return op_mem_mgr_value; }
    void register_memory_candidates(SinkOperatorMemoryManager* op_mem_mgr) override { registered_mem_mgr = op_mem_mgr; }

    bool finished = true;
    int rollback_calls = 0;
    Status status_value = Status::OK();
    SinkOperatorMemoryManager* op_mem_mgr_value = nullptr;
    SinkOperatorMemoryManager* registered_mem_mgr = nullptr;
};

struct TestConnectorSinkContext final : public ConnectorSinkContext {};

class TestConnectorSinkProvider final : public ConnectorSinkProvider {
public:
    StatusOr<std::unique_ptr<ConnectorSink>> create_sink(int32_t) override {
        return std::make_unique<TestConnectorSink>();
    }
};

class TestSinkOperatorMemoryManager final : public SinkOperatorMemoryManager {
public:
    bool kill_victim() override {
        ++kill_victim_calls;
        if (!kill_victim_result) {
            return false;
        }
        kill_victim_result = false;
        writer_occupied_memory_value = 0;
        return true;
    }

    int64_t update_releasable_memory() override {
        ++update_releasable_memory_calls;
        return releasable_memory_value;
    }

    int64_t update_writer_occupied_memory() override {
        ++update_writer_occupied_memory_calls;
        return writer_occupied_memory_value;
    }

    int64_t releasable_memory() const override { return releasable_memory_value; }

    int64_t writer_occupied_memory() const override { return writer_occupied_memory_value; }

    bool kill_victim_result = true;
    int64_t releasable_memory_value = 0;
    int64_t writer_occupied_memory_value = 0;
    int kill_victim_calls = 0;
    int update_releasable_memory_calls = 0;
    int update_writer_occupied_memory_calls = 0;
};

} // namespace

TEST(ConnectorPrimitiveTest, ConnectorKeepsReadSideTypeContract) {
    TestConnector connector;

    ASSERT_EQ(ConnectorType::BENCHMARK, connector.connector_type());
    ASSERT_EQ("benchmark", Connector::BENCHMARK);
}

TEST(ConnectorPrimitiveTest, DefaultScanRangeConversionBuildsDynamicMorselQueue) {
    TestDataSourceProvider provider;
    TScanRangeParams scan_range;

    auto builder_or = provider.convert_scan_range_to_morsel_queue_builder(
            {scan_range}, 10, 2, false, TTabletInternalParallelMode::type::AUTO, 1, 4);
    ASSERT_TRUE(builder_or.ok()) << builder_or.status().to_string();
    auto builder = std::move(builder_or).value();

    ASSERT_EQ(1, builder->num_original_morsels());
    ASSERT_EQ(4, builder->max_degree_of_parallelism());
    ASSERT_TRUE(builder->can_uniform_distribute());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(pipeline::MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(1, queue->num_original_morsels());
}

TEST(ConnectorPrimitiveTest, DataSourceProviderStoresScanNodeHints) {
    TestDataSourceProvider provider;

    ASSERT_EQ(0, provider.estimated_scan_row_bytes());
    ASSERT_FALSE(provider.is_filtered_above_iterator());

    provider.set_estimated_scan_row_bytes(128);
    provider.set_filtered_above_iterator(true);

    ASSERT_EQ(128, provider.estimated_scan_row_bytes());
    ASSERT_TRUE(provider.is_filtered_above_iterator());
}

TEST(ConnectorPrimitiveTest, ConnectorSinkContractLivesInPrimitiveLayer) {
    TestConnectorSinkProvider provider;

    auto sink_or = provider.create_sink(7);
    ASSERT_TRUE(sink_or.ok()) << sink_or.status().to_string();
    auto sink = std::move(sink_or).value();

    ASSERT_OK(sink->init(nullptr, nullptr, nullptr));
    ASSERT_OK(sink->add(nullptr));
    ASSERT_OK(sink->finish());
    ASSERT_TRUE(sink->is_finished());
    ASSERT_OK(sink->status());
    ASSERT_EQ(nullptr, sink->op_mem_mgr());
}

TEST(ConnectorPrimitiveTest, SinkMemoryManagerRegistersAbstractOperatorManagers) {
    SinkMemoryManager manager(nullptr, nullptr);

    auto child = std::make_unique<TestSinkOperatorMemoryManager>();
    auto* child_ptr = child.get();

    ASSERT_EQ(child_ptr, manager.register_child_manager(std::move(child)));
}

TEST(ConnectorPrimitiveTest, SinkMemoryManagerUsesAbstractChildrenForBackpressure) {
    SCOPED_UPDATE(double, config::connector_sink_mem_low_watermark_ratio, 0.1);
    SCOPED_UPDATE(double, config::connector_sink_mem_urgent_space_ratio, 0.05);
    MemTracker query_pool_tracker(MemTrackerType::QUERY_POOL, 1000, "connector_primitive_query_pool");
    query_pool_tracker.consume(950);
    SinkMemoryManager manager(&query_pool_tracker, nullptr);

    auto child = std::make_unique<TestSinkOperatorMemoryManager>();
    auto* child_ptr = child.get();
    child_ptr->writer_occupied_memory_value = 10;
    ASSERT_EQ(child_ptr, manager.register_child_manager(std::move(child)));

    auto sibling = std::make_unique<TestSinkOperatorMemoryManager>();
    auto* sibling_ptr = sibling.get();
    sibling_ptr->writer_occupied_memory_value = 100;
    ASSERT_EQ(sibling_ptr, manager.register_child_manager(std::move(sibling)));

    EXPECT_TRUE(manager.can_accept_more_input(child_ptr));
    EXPECT_GE(child_ptr->kill_victim_calls, 1);
    EXPECT_GE(child_ptr->update_writer_occupied_memory_calls, 1);
    EXPECT_EQ(0, sibling_ptr->update_writer_occupied_memory_calls);
}

TEST(ConnectorPrimitiveTest, SinkMemoryManagerRejectsInputWhenReleasableMemoryRemains) {
    SCOPED_UPDATE(double, config::connector_sink_mem_low_watermark_ratio, 0.1);
    SCOPED_UPDATE(double, config::connector_sink_mem_urgent_space_ratio, 0.05);
    MemTracker query_pool_tracker(MemTrackerType::QUERY_POOL, 1000, "connector_primitive_releasable_pool");
    query_pool_tracker.consume(950);
    SinkMemoryManager manager(&query_pool_tracker, nullptr);

    auto child = std::make_unique<TestSinkOperatorMemoryManager>();
    auto* child_ptr = child.get();
    child_ptr->releasable_memory_value = 64;
    ASSERT_EQ(child_ptr, manager.register_child_manager(std::move(child)));

    EXPECT_FALSE(manager.can_accept_more_input(child_ptr));
    EXPECT_GE(child_ptr->update_releasable_memory_calls, 1);
    EXPECT_EQ(0, child_ptr->kill_victim_calls);
}

} // namespace starrocks::connector
