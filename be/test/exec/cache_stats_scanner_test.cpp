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

#ifndef BE_TEST
#define BE_TEST
#endif

#include "exec/cache_stats_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config_exec_fwd.h"
#include "common/status.h"
#include "connector/cache_stats_connector.h"
#include "fs/fs_util.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks {

class CacheStatsScannerTest : public ::testing::Test {
public:
    CacheStatsScannerTest() : _tablet_id(next_id()) {
        _location_provider = std::make_shared<lake::FixedLocationProvider>(kRootLocation);
        _tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);

        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName))
                      .ok());
        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName))
                      .ok());
        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName))
                      .ok());

        TUniqueId query_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        TUniqueId fragment_id;
        _state = _pool.add(
                new RuntimeState(query_id, fragment_id, query_options, query_globals, ExecEnv::GetInstance()));
        _state->init_mem_trackers(query_id);
    }

    ~CacheStatsScannerTest() override {
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kRootLocation);
    }

protected:
    void build_desc_tbl(bool with_unknown_slot = false) {
        TDescriptorTableBuilder table_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;

        auto slot0 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("tablet_id")
                             .column_pos(0)
                             .nullable(false)
                             .build();
        auto slot1 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("cached_bytes")
                             .column_pos(1)
                             .nullable(false)
                             .build();
        auto slot2 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("total_bytes")
                             .column_pos(2)
                             .nullable(false)
                             .build();

        tuple_desc_builder.add_slot(slot0);
        tuple_desc_builder.add_slot(slot1);
        tuple_desc_builder.add_slot(slot2);
        if (with_unknown_slot) {
            auto slot3 = TSlotDescriptorBuilder()
                                 .type(LogicalType::TYPE_BIGINT)
                                 .column_name("unknown_col")
                                 .column_pos(3)
                                 .nullable(true)
                                 .build();
            tuple_desc_builder.add_slot(slot3);
        }
        tuple_desc_builder.build(&table_desc_builder);

        CHECK(DescriptorTbl::create(_state, &_pool, table_desc_builder.desc_tbl(), &_desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        _state->set_desc_tbl(_desc_tbl);
    }

    const TupleDescriptor* tuple_desc() { return _desc_tbl->get_tuple_descriptor(0); }

    constexpr static const char* const kRootLocation = "./CacheStatsScannerTest";
    lake::TabletManager* _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    int64_t _tablet_id;

    ObjectPool _pool;
    RuntimeState* _state = nullptr;
    DescriptorTbl* _desc_tbl = nullptr;
};

TEST_F(CacheStatsScannerTest, test_init_invalid_version) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "abc";

    auto st = scanner.init(nullptr, scan_range);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("Invalid") != std::string::npos) << st.message();
}

TEST_F(CacheStatsScannerTest, test_get_chunk_not_supported) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "99";

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    auto st = scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk, nullptr);
}

TEST_F(CacheStatsScannerTest, test_get_chunk_not_supported_with_unknown_slot) {
    build_desc_tbl(true);
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "2";

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    auto st = scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk, nullptr);

    scanner.close(nullptr);
}

TEST_F(CacheStatsScannerTest, test_connector_data_source) {
    const int64_t version = 4;
    build_desc_tbl();

    connector::CacheStatsConnector connector;
    ASSERT_EQ(connector.connector_type(), connector::ConnectorType::CACHE_STATS);

    TCacheStatsScanNode scan_node;
    scan_node.__set_tuple_id(0);

    TPlanNode plan_node;
    plan_node.__set_cache_stats_scan_node(scan_node);

    auto provider = connector.create_data_source_provider(nullptr, plan_node);
    ASSERT_NE(provider, nullptr);
    ASSERT_FALSE(provider->insert_local_exchange_operator());
    ASSERT_FALSE(provider->accept_empty_scan_ranges());
    ASSERT_EQ(provider->tuple_descriptor(_state), _state->desc_tbl().get_tuple_descriptor(0));

    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = _tablet_id;
    internal_scan_range.version = std::to_string(version);

    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    auto data_source = provider->create_data_source(scan_range);
    ASSERT_NE(data_source, nullptr);
    ASSERT_EQ(data_source->name(), "CacheStatsDataSource");
    ASSERT_TRUE(data_source->open(_state).ok());

    ChunkPtr chunk;
    auto st = data_source->get_next(_state, &chunk);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
    ASSERT_EQ(chunk, nullptr);
    ASSERT_EQ(data_source->raw_rows_read(), 0);
    ASSERT_EQ(data_source->num_rows_read(), 0);
    ASSERT_EQ(data_source->num_bytes_read(), 0);
    ASSERT_EQ(data_source->cpu_time_spent(), 0);

    data_source->close(_state);
}

} // namespace starrocks
