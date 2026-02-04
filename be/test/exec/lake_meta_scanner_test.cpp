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

#include "exec/lake_meta_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/status.h"
#include "exec/lake_meta_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "fs/fs_util.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake_meta_reader.h"
#include "storage/meta_reader.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class LakeMetaScannerTest : public ::testing::Test {
public:
    LakeMetaScannerTest() : _tablet_id(next_id()) {
        // setup TabletManager
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

        {
            // create the tablet with its schema prepared (no rowsets)
            auto metadata = std::make_shared<TabletMetadata>();
            metadata->set_id(_tablet_id);
            metadata->set_version(2);

            auto schema = metadata->mutable_schema();
            schema->set_id(10);
            schema->set_schema_version(1);
            schema->set_num_short_key_columns(1);
            schema->set_keys_type(DUP_KEYS);
            schema->set_num_rows_per_row_block(65535);
            auto c0 = schema->add_column();
            c0->set_unique_id(0);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
            auto st = _tablet_mgr->put_tablet_metadata(*metadata);
            CHECK(st.ok()) << st;

            auto tablet_or = _tablet_mgr->get_tablet(_tablet_id, 2);
            EXPECT_TRUE(tablet_or.ok());
        }

        TUniqueId query_id;
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _state = _pool.add(
                new RuntimeState(query_id, fragment_id, query_options, query_globals, ExecEnv::GetInstance()));
        _state->init_mem_trackers(query_id);

        // Setup FragmentContext with fe_addr for schema RPC
        _fragment_ctx = std::make_unique<pipeline::FragmentContext>();
        TNetworkAddress fe;
        fe.hostname = "127.0.0.1";
        fe.port = 9020;
        _fragment_ctx->set_fe_addr(fe);
        _state->set_fragment_ctx(_fragment_ctx.get());

        std::vector<::starrocks::TTupleId> tuple_ids{0};
        _tnode = std::make_unique<TPlanNode>();
        _tnode->__set_node_id(1);
        _tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
        _tnode->__set_row_tuples(tuple_ids);
        _tnode->__set_limit(-1);

        // Setup id_to_names for meta scan: slot_id -> "field:column_name"
        _tnode->meta_scan_node.id_to_names[0] = "rows:c0";

        TDescriptorTableBuilder table_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        auto slot =
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("col1").column_pos(0).nullable(true).build();
        TTupleDescriptorBuilder tuple_desc_builder;
        tuple_desc_builder.add_slot(slot);
        tuple_desc_builder.build(&table_desc_builder);

        CHECK(DescriptorTbl::create(_state, &_pool, table_desc_builder.desc_tbl(), &_tbl, config::vector_chunk_size)
                      .ok());

        _parent = std::make_unique<LakeMetaScanNode>(&_pool, *_tnode, *_tbl);
    }

    ~LakeMetaScannerTest() override {
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kRootLocation);
    }

public:
    constexpr static const char* const kRootLocation = "./LakeMetaScannerTest";
    lake::TabletManager* _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    int64_t _tablet_id;

    ObjectPool _pool;
    RuntimeState* _state = nullptr;
    std::unique_ptr<TPlanNode> _tnode;
    DescriptorTbl* _tbl;
    std::unique_ptr<LakeMetaScanNode> _parent;
    std::unique_ptr<pipeline::FragmentContext> _fragment_ctx;
};

TEST_F(LakeMetaScannerTest, test_init_lazy_and_real) {
    auto range = _pool.add(new TInternalScanRange());
    range->tablet_id = _tablet_id;
    range->version = "2";
    MetaScannerParams params{.scan_range = range};

    LakeMetaScanner scanner(_parent.get());
    DeferOp defer([&]() { scanner.close(_state); });
    auto st = scanner.init(_state, params);
    // after init() called, reader is not created at all
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(nullptr, scanner.TEST_reader());

    auto st2 = scanner.open(_state);
    // after open() called, reader is created and initialized
    ASSERT_TRUE(st2.ok()) << st2;
    ASSERT_NE(nullptr, scanner.TEST_reader());
}

TEST_F(LakeMetaScannerTest, test_read_schema) {
    auto range = _pool.add(new TInternalScanRange());
    range->tablet_id = _tablet_id;
    range->version = "2";
    MetaScannerParams params{.scan_range = range};

    DeferOp defer_hook([&]() {
        SyncPoint::GetInstance()->ClearCallBack("TableSchemaService::_fetch_schema_via_rpc::test_hook");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->EnableProcessing();

    // Test case 1: With schema_key (fast schema evolution v2 path)
    {
        // Arrange: Set schema_key in TMetaScanNode
        TTableSchemaKey t_schema_key;
        t_schema_key.__set_schema_id(200);
        t_schema_key.__set_db_id(1);
        t_schema_key.__set_table_id(1);
        _tnode->meta_scan_node.__set_schema_key(t_schema_key);

        // Setup test hook to mock RPC response with schema_id=200, version=5, columns: c0(INT, key), c1(INT, non-key)
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_fetch_schema_via_rpc::test_hook", [](void* arg) {
            auto* arr = static_cast<std::array<void*, 4>*>(arg);
            auto* request = static_cast<const TGetTableSchemaRequest*>((*arr)[0]);
            auto* response_batch = static_cast<TBatchGetTableSchemaResponse*>((*arr)[1]);
            auto* status = static_cast<Status*>((*arr)[2]);
            auto* mock_thrift_rpc = static_cast<bool*>((*arr)[3]);

            // Verify request
            ASSERT_EQ(request->schema_key.schema_id, 200);
            ASSERT_EQ(request->schema_key.db_id, 1);
            ASSERT_EQ(request->schema_key.table_id, 1);

            // Mock RPC: skip actual RPC call
            *mock_thrift_rpc = true;
            *status = Status::OK();

            // Set batch response status
            response_batch->status.__set_status_code(TStatusCode::OK);
            response_batch->__set_responses(std::vector<TGetTableSchemaResponse>{});

            // Create response with schema_id=200, version=5
            auto& resp = response_batch->responses.emplace_back();
            resp.status.__set_status_code(TStatusCode::OK);

            // Create TTabletSchema with schema_id=200, version=5, columns: c0(INT, key), c1(INT, non-key)
            TTabletSchema thrift_schema;
            thrift_schema.__set_id(200);
            thrift_schema.__set_schema_version(5);
            thrift_schema.__set_short_key_column_count(1);
            thrift_schema.__set_keys_type(TKeysType::DUP_KEYS);

            // Column c0: INT, key
            TColumn c0;
            c0.__set_column_name("c0");
            c0.column_type.__set_type(TPrimitiveType::INT);
            c0.__set_is_key(true);
            c0.__set_is_allow_null(false);
            thrift_schema.columns.push_back(c0);

            // Column c1: INT, non-key
            TColumn c1;
            c1.__set_column_name("c1");
            c1.column_type.__set_type(TPrimitiveType::INT);
            c1.__set_is_key(false);
            c1.__set_is_allow_null(false);
            thrift_schema.columns.push_back(c1);

            resp.__set_schema(thrift_schema);
        });

        // Recreate parent with updated tnode
        _parent = std::make_unique<LakeMetaScanNode>(&_pool, *_tnode, *_tbl);

        LakeMetaScanner scanner(_parent.get());
        auto st = scanner.init(_state, params);
        DeferOp defer([&]() { scanner.close(_state); });
        ASSERT_TRUE(st.ok()) << st;

        auto st2 = scanner.open(_state);
        ASSERT_TRUE(st2.ok()) << st2;

        // Verify the schema was fetched and used (schema_id=200, version=5, with c0 and c1 columns)
        auto* reader = scanner.TEST_reader();
        ASSERT_NE(reader, nullptr);
        // Access TEST_tablet_schema - LakeMetaReader inherits from MetaReader
        const auto& schema = reader->TEST_tablet_schema();
        ASSERT_NE(schema, nullptr);

        // Verify schema_id=200, version=5
        ASSERT_EQ(schema->id(), 200);
        ASSERT_EQ(schema->schema_version(), 5);

        // Verify schema has 2 columns: c0 (INT, key) and c1 (INT, non-key)
        ASSERT_EQ(schema->num_columns(), 2);

        // Verify column c0: INT, key
        const auto& col0 = schema->column(0);
        ASSERT_EQ(col0.name(), "c0");
        ASSERT_EQ(col0.type(), LogicalType::TYPE_INT);
        ASSERT_TRUE(col0.is_key());

        // Verify column c1: INT, non-key
        const auto& col1 = schema->column(1);
        ASSERT_EQ(col1.name(), "c1");
        ASSERT_EQ(col1.type(), LogicalType::TYPE_INT);
        ASSERT_FALSE(col1.is_key());
    }

    // Test case 2: Without schema_key (legacy path)
    {
        // Arrange: Do NOT set schema_key in TMetaScanNode (legacy path)
        _tnode->meta_scan_node.__isset.schema_key = false;

        // Recreate parent with updated tnode
        _parent = std::make_unique<LakeMetaScanNode>(&_pool, *_tnode, *_tbl);

        LakeMetaScanner scanner(_parent.get());
        DeferOp defer([&]() { scanner.close(_state); });
        auto st = scanner.init(_state, params);
        ASSERT_TRUE(st.ok()) << st;

        auto st2 = scanner.open(_state);
        ASSERT_TRUE(st2.ok()) << st2;

        // Verify the schema was from tablet metadata (schema_id=10, version=1, with c0 column)
        auto* reader = scanner.TEST_reader();
        ASSERT_NE(reader, nullptr);
        // Access TEST_tablet_schema - LakeMetaReader inherits from MetaReader
        const auto& schema = reader->TEST_tablet_schema();
        ASSERT_NE(schema, nullptr);

        // Verify schema_id=10, version=1 (from tablet metadata)
        ASSERT_EQ(schema->id(), 10);
        ASSERT_EQ(schema->schema_version(), 1);

        // Verify schema has 1 column: c0 (INT, key)
        ASSERT_EQ(schema->num_columns(), 1);

        // Verify column c0: INT, key
        const auto& col0 = schema->column(0);
        ASSERT_EQ(col0.name(), "c0");
        ASSERT_EQ(col0.type(), LogicalType::TYPE_INT);
        ASSERT_TRUE(col0.is_key());
    }
}

} // namespace starrocks
