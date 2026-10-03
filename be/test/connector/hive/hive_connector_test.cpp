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

#include "connector/hive/hive_connector.h"

#include <gtest/gtest.h>

#include "runtime/runtime_state.h"

namespace starrocks::connector {

class HiveConnectorTest : public ::testing::Test {};

// Test HiveConnector type
TEST_F(HiveConnectorTest, test_connector_type) {
    HiveConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::HIVE);
}

// Test HiveDataSourceProvider creates data source
TEST_F(HiveConnectorTest, test_create_data_source) {
    THdfsScanNode hdfs_scan_node;
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(THdfsScanRange());

    auto data_source = provider.create_data_source(scan_range);
    EXPECT_NE(data_source, nullptr);
    EXPECT_EQ(data_source->name(), "HiveDataSource");
}

// Test open with no data (file_length = 0) - covers early return path
TEST_F(HiveConnectorTest, test_open_no_data) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.file_length = 0; // Triggers early return before _check_all_slots_nullable

    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    auto runtime_state = std::make_shared<RuntimeState>();

    auto status = data_source->open(runtime_state.get());
    EXPECT_TRUE(status.ok());
}

// Test bucket properties constructor
TEST_F(HiveConnectorTest, test_bucket_properties) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__isset.bucket_properties = true;

    HiveDataSourceProvider provider(0, hdfs_scan_node);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(THdfsScanRange());

    auto data_source = provider.create_data_source(scan_range);
    EXPECT_NE(data_source, nullptr);
}

// Test extended column index
TEST_F(HiveConnectorTest, test_extended_column_index) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__isset.extended_slot_ids = true;
    hdfs_scan_node.extended_slot_ids = {10, 20, 30};

    HiveDataSourceProvider provider(0, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    EXPECT_EQ(data_source->extended_column_index(10), 0);
    EXPECT_EQ(data_source->extended_column_index(20), 1);
    EXPECT_EQ(data_source->extended_column_index(30), 2);
    EXPECT_EQ(data_source->extended_column_index(99), -1); // Not found
}

// Test scan_range_indicate_const_column_index
TEST_F(HiveConnectorTest, test_scan_range_indicate_const_column_index) {
    THdfsScanNode hdfs_scan_node;

    HiveDataSourceProvider provider(0, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.__isset.identity_partition_slot_ids = true;
    hdfs_scan_range.identity_partition_slot_ids = {5, 10, 15};

    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(5), 0);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(10), 1);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(15), 2);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(99), -1); // Not found
}

// A mid-query refreshed cloud configuration overrides the plan-time one, including when delivered
// through the DataSourceProvider base pointer (the incremental scan-range RPC path).
TEST_F(HiveConnectorTest, test_refreshed_cloud_configuration) {
    TCloudConfiguration holder;

    // No plan-time config and no refresh: nullptr.
    THdfsScanNode bare_scan_node;
    HiveDataSourceProvider bare_provider(0, bare_scan_node);
    EXPECT_EQ(bare_provider.effective_cloud_configuration(&holder), nullptr);

    // Plan-time config only: returned directly.
    TCloudConfiguration plan_time;
    plan_time.__set_cloud_properties({{"fs.gs.temporary.access.token", "tok1"}});
    THdfsScanNode scan_node;
    scan_node.__set_cloud_configuration(plan_time);
    HiveDataSourceProvider provider(0, scan_node);
    const TCloudConfiguration* effective = provider.effective_cloud_configuration(&holder);
    ASSERT_NE(effective, nullptr);
    EXPECT_EQ(effective->cloud_properties.at("fs.gs.temporary.access.token"), "tok1");

    // A refresh delivered through the base interface wins over the plan-time config and is
    // copied into the caller's holder.
    TCloudConfiguration refreshed;
    refreshed.__set_cloud_properties({{"fs.gs.temporary.access.token", "tok2"}});
    static_cast<DataSourceProvider*>(&provider)->set_refreshed_cloud_configuration(refreshed);
    effective = provider.effective_cloud_configuration(&holder);
    ASSERT_EQ(effective, &holder);
    EXPECT_EQ(effective->cloud_properties.at("fs.gs.temporary.access.token"), "tok2");

    // A refresh also applies when the plan carried no cloud configuration at all.
    bare_provider.set_refreshed_cloud_configuration(refreshed);
    EXPECT_EQ(bare_provider.effective_cloud_configuration(&holder), &holder);
}

} // namespace starrocks::connector
