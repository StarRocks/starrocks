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

#include "connector/connector.h"
#include "connector/data_source_provider.h"

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

} // namespace starrocks::connector
