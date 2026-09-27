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

#include "connector/cache_stats/cache_stats_connector.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/config_exec_fwd.h"
#include "connector/cache_stats/cache_stats_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class CacheStatsConnectorTest : public ::testing::Test {
public:
    void SetUp() override {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<const QueryExecutionServices*>(nullptr), nullptr);
        TUniqueId query_id;
        _runtime_state->init_mem_trackers(query_id);
        _pool = _runtime_state->obj_pool();
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

        auto status = DescriptorTbl::create(_runtime_state.get(), _pool, table_desc_builder.desc_tbl(), &_desc_tbl,
                                            config::vector_chunk_size);
        ASSERT_TRUE(status.ok()) << status.to_string();
        _runtime_state->set_desc_tbl(_desc_tbl);
    }

    const TupleDescriptor* tuple_desc() { return _desc_tbl->get_tuple_descriptor(0); }

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ObjectPool* _pool = nullptr;
    DescriptorTbl* _desc_tbl = nullptr;
    int64_t _tablet_id = 12345;
};

TEST_F(CacheStatsConnectorTest, InitInvalidVersion) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "abc";

    auto status = scanner.init(nullptr, scan_range);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(status.message().find("Invalid"), std::string::npos) << status.message();
}

TEST_F(CacheStatsConnectorTest, GetChunkNotSupported) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "99";

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    auto status = scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_TRUE(status.is_not_supported()) << status.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk, nullptr);
}

TEST_F(CacheStatsConnectorTest, GetChunkNotSupportedWithUnknownSlot) {
    build_desc_tbl(true);
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "2";

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    auto status = scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_TRUE(status.is_not_supported()) << status.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk, nullptr);

    scanner.close(nullptr);
}

TEST_F(CacheStatsConnectorTest, ConnectorDataSource) {
    const int64_t version = 4;
    build_desc_tbl();

    CacheStatsConnector connector;
    ASSERT_EQ(connector.connector_type(), ConnectorType::CACHE_STATS);

    TCacheStatsScanNode scan_node;
    scan_node.__set_tuple_id(0);

    TPlanNode plan_node;
    plan_node.__set_cache_stats_scan_node(scan_node);

    auto provider = connector.create_data_source_provider(nullptr, plan_node);
    ASSERT_NE(provider, nullptr);
    ASSERT_FALSE(provider->insert_local_exchange_operator());
    ASSERT_FALSE(provider->accept_empty_scan_ranges());
    ASSERT_EQ(provider->tuple_descriptor(_runtime_state.get()), _runtime_state->desc_tbl().get_tuple_descriptor(0));

    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = _tablet_id;
    internal_scan_range.version = std::to_string(version);

    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    auto data_source = provider->create_data_source(scan_range);
    ASSERT_NE(data_source, nullptr);
    ASSERT_EQ(data_source->name(), "CacheStatsDataSource");
    ASSERT_TRUE(data_source->open(_runtime_state.get()).ok());

    ChunkPtr chunk;
    auto status = data_source->get_next(_runtime_state.get(), &chunk);
    ASSERT_TRUE(status.is_not_supported()) << status.to_string();
    ASSERT_EQ(chunk, nullptr);
    ASSERT_EQ(data_source->raw_rows_read(), 0);
    ASSERT_EQ(data_source->num_rows_read(), 0);
    ASSERT_EQ(data_source->num_bytes_read(), 0);
    ASSERT_EQ(data_source->cpu_time_spent(), 0);

    data_source->close(_runtime_state.get());
}

} // namespace starrocks::connector
