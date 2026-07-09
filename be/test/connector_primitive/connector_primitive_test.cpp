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

#include "base/testutil/scoped_updater.h"
#include "common/config_connector_sink_fwd.h"
#include "connector_primitive/connector.h"
#include "connector_primitive/connector_sink_commit.h"
#include "connector_primitive/data_source_provider.h"
#include "connector_primitive/sink_memory_manager.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
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

TEST(ConnectorPrimitiveTest, CommitResultWrapsFileResultWithConnectorMetadata) {
    CommitResult result{
            .file_result =
                    {
                            .io_status = Status::OK(),
                            .format = formats::PARQUET,
                            .file_statistics =
                                    {
                                            .record_count = 10,
                                            .file_size = 128,
                                    },
                            .location = "s3://bucket/table/data.parquet",
                    },
    };

    auto& fingerprint_ref = result.set_partition_null_fingerprint("01");
    auto& referenced_file_ref = result.set_referenced_data_file("s3://bucket/table/original.parquet");

    ASSERT_EQ(&result, &fingerprint_ref);
    ASSERT_EQ(&result, &referenced_file_ref);
    ASSERT_TRUE(result.file_result.io_status.ok());
    ASSERT_EQ(formats::PARQUET, result.file_result.format);
    ASSERT_EQ(10, result.file_result.file_statistics.record_count);
    ASSERT_EQ(128, result.file_result.file_statistics.file_size);
    ASSERT_EQ("01", result.partition_null_fingerprint);
    ASSERT_EQ("s3://bucket/table/original.parquet", result.referenced_data_file);
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
