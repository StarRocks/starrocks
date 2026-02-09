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

#include <array>
#include <atomic>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "connector/lake_connector.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/scan/morsel.h"
#include "fs/fs_util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/base_rowset.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeDataSourceTest : public ::testing::Test {
public:
    LakeDataSourceTest()
            : _tablet_mgr(ExecEnv::GetInstance()->lake_tablet_manager()),
              _location_provider(std::make_shared<FixedLocationProvider>(kRootLocation)) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        CHECK_OK(fs::remove_all(kRootLocation));
        if (_backup_location_provider != nullptr) {
            (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        }
    }

protected:
    void create_rowsets_for_testing(TabletMetadata* tablet_metadata, int64_t version) {
        std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}; // 23 rows
        std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

        std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41}; // 12 rows
        std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto c3 = Int32Column::create();
        c0->append_numbers(k0.data(), k0.size() * sizeof(int));
        c1->append_numbers(v0.data(), v0.size() * sizeof(int));
        c2->append_numbers(k1.data(), k1.size() * sizeof(int));
        c3->append_numbers(v1.data(), v1.size() * sizeof(int));

        Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
        Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_metadata->id()));

        {
            int64_t txn_id = next_id();
            // write rowset 1 with 2 segments
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
            ASSERT_OK(writer->open());

            // write rowset data
            // segment #1
            ASSERT_OK(writer->write(chunk0));
            ASSERT_OK(writer->write(chunk1));
            ASSERT_OK(writer->finish());

            // segment #2
            ASSERT_OK(writer->write(chunk0));
            ASSERT_OK(writer->write(chunk1));
            ASSERT_OK(writer->finish());

            const auto& files = writer->segments();
            ASSERT_EQ(2, files.size());

            // add rowset metadata
            auto* rowset = tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_num_rows(k0.size() + k1.size());
            auto* segs = rowset->mutable_segments();
            for (const auto& file : writer->segments()) {
                segs->Add()->assign(file.path);
            }

            writer->close();
        }

        // write tablet metadata
        tablet_metadata->set_version(version);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));
    }

    static void set_status(TStatus* status, TStatusCode::type code, std::string msg = "") {
        status->__set_status_code(code);
        if (!msg.empty()) {
            status->__set_error_msgs(std::vector<std::string>{std::move(msg)});
        }
    }

    constexpr static const char* const kRootLocation = "test_lake_data_source_test";

    TabletManager* _tablet_mgr = nullptr;
    std::shared_ptr<LocationProvider> _location_provider;
    std::shared_ptr<LocationProvider> _backup_location_provider;

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDataSourceTest, test_convert_scan_range_to_morsel_queue) {
    create_rowsets_for_testing(_tablet_metadata.get(), 2);

    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_cloud();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));

    bool enable_tablet_internal_parallel = true;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::FORCE_SPLIT;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    auto data_source_provider = dynamic_cast<connector::LakeDataSourceProvider*>(scan_node->data_source_provider());
    data_source_provider->set_lake_tablet_manager(_tablet_mgr);

    ASSERT_TRUE(data_source_provider->always_shared_scan());

    config::tablet_internal_parallel_max_splitted_scan_bytes = 32;
    config::tablet_internal_parallel_min_splitted_scan_rows = 4;

    int pipeline_dop = 2;
    config::tablet_internal_parallel_min_scan_dop = 4;

    auto tablet_metas = std::vector<TabletMetadata*>();
    tablet_metas.emplace_back(_tablet_metadata.get());
    auto scan_ranges = create_scan_ranges_cloud(tablet_metas);

    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(data_source_provider->could_split());
    ASSERT_TRUE(data_source_provider->could_split_physically());

    ASSERT_TRUE(morsel_queue_factory->is_shared());
    auto morsel_queue = morsel_queue_factory->create(1);
    ASSERT_TRUE(morsel_queue->max_degree_of_parallelism() > 1);
}

TEST_F(LakeDataSourceTest, get_tablet_schema) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    create_rowsets_for_testing(_tablet_metadata.get(), 2);

    // 1) Build a TLakeScanNode with schema_key.
    int64_t schema_id = next_id(); // Ensure schema_id misses local schema and triggers RPC fetch.
    if (schema_id == _tablet_metadata->schema().id()) {
        schema_id = next_id();
    }
    const int64_t db_id = 100;
    const int64_t table_id = 101;

    TLakeScanNode lake_scan_node;
    lake_scan_node.__set_tuple_id(0);
    TTableSchemaKey schema_key;
    schema_key.__set_schema_id(schema_id);
    schema_key.__set_db_id(db_id);
    schema_key.__set_table_id(table_id);
    lake_scan_node.__set_schema_key(schema_key);

    TPlanNode plan_node;
    plan_node.__set_node_id(0);
    plan_node.__set_lake_scan_node(lake_scan_node);

    // 2) Prepare runtime state: descriptor table + fragment ctx (fe_addr for schema RPC).
    auto runtime_state = create_runtime_state();
    pipeline::FragmentContext fragment_ctx;
    TNetworkAddress fe;
    fe.hostname = "127.0.0.1";
    fe.port = 9020;
    fragment_ctx.set_fe_addr(fe);
    runtime_state->set_fragment_ctx(&fragment_ctx);

    // Build a minimal descriptor table with required column names.
    TDescriptorTableBuilder desc_tbl_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot0 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c0").column_pos(0).nullable(true).build();
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(1).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot0);
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* desc_tbl = nullptr;
    CHECK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), desc_tbl_builder.desc_tbl(), &desc_tbl,
                                config::vector_chunk_size)
                  .ok());
    runtime_state->set_desc_tbl(desc_tbl);

    // Connector scan open path needs tuple->table_desc() to be non-null for profile strings.
    TTableDescriptor tdesc;
    tdesc.__set_id(0);
    tdesc.__set_tableType(TTableType::OLAP_TABLE);
    tdesc.__set_tableName("test_table");
    tdesc.__set_dbName("test_db");
    auto* table_desc = runtime_state->obj_pool()->add(new OlapTableDescriptor(tdesc));
    desc_tbl->get_tuple_descriptor(0)->set_table_desc(table_desc);

    // 4) Install schema RPC hook: validate request fields and return a schema.
    std::atomic<bool> invoked{false};
    SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_fetch_schema_via_rpc::test_hook", [&](void* arg) {
        invoked.store(true);
        auto* arr = static_cast<std::array<void*, 4>*>(arg);
        auto* request_ptr = static_cast<const TGetTableSchemaRequest*>((*arr)[0]);
        auto* response_batch = static_cast<TBatchGetTableSchemaResponse*>((*arr)[1]);
        auto* status = static_cast<Status*>((*arr)[2]);
        auto* mock_thrift_rpc = static_cast<bool*>((*arr)[3]);

        ASSERT_EQ(request_ptr->source, TTableSchemaRequestSource::SCAN);
        ASSERT_EQ(request_ptr->tablet_id, _tablet_metadata->id());
        ASSERT_EQ(request_ptr->query_id, runtime_state->query_id());
        ASSERT_EQ(request_ptr->schema_key.schema_id, schema_id);
        ASSERT_EQ(request_ptr->schema_key.db_id, db_id);
        ASSERT_EQ(request_ptr->schema_key.table_id, table_id);

        *mock_thrift_rpc = true;
        *status = Status::OK();
        set_status(&response_batch->status, TStatusCode::OK);
        response_batch->__set_responses(std::vector<TGetTableSchemaResponse>{});
        auto& resp = response_batch->responses.emplace_back();
        set_status(&resp.status, TStatusCode::OK);

        // Minimal schema payload; LakeDataSource::get_tablet only needs schema fetch to succeed.
        TTabletSchema t_schema;
        t_schema.__set_id(schema_id);
        t_schema.__set_keys_type(TKeysType::DUP_KEYS);
        t_schema.__set_short_key_column_count(1);

        TColumn c0;
        c0.__set_column_name("c0");
        c0.column_type.__set_type(TPrimitiveType::INT);
        c0.__set_is_key(true);
        c0.__set_is_allow_null(false);
        t_schema.columns.push_back(c0);

        TColumn c1;
        c1.__set_column_name("c1");
        c1.column_type.__set_type(TPrimitiveType::INT);
        c1.__set_is_key(false);
        c1.__set_is_allow_null(false);
        t_schema.columns.push_back(c1);

        resp.__set_schema(t_schema);
    });

    // 5) Explicitly construct a LakeDataSource and call open().
    starrocks::connector::LakeDataSourceProvider provider(/*scan_node=*/nullptr, plan_node);

    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));

    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    // Prepare morsel with rowsets so LakeDataSource can initialize safely.
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id(), _tablet_metadata->version()));
    auto lake_rowsets = tablet.get_rowsets();
    std::vector<BaseRowsetSharedPtr> base_rowsets;
    base_rowsets.reserve(lake_rowsets.size());
    for (auto& rs : lake_rowsets) {
        base_rowsets.emplace_back(rs);
    }

    pipeline::ScanMorsel morsel(plan_node.node_id, scan_range);
    morsel.set_rowsets(base_rowsets);

    starrocks::connector::LakeDataSource ds(&provider, scan_range);
    RuntimeProfile parent_profile("LakeDataSourceTest");
    ds.set_runtime_profile(&parent_profile);
    ds.set_morsel(&morsel);
    DeferOp close_guard([&] { ds.close(runtime_state.get()); });
    ASSERT_OK(ds.open(runtime_state.get()));
    ASSERT_TRUE(invoked.load());

    auto tablet_schema = ds.TEST_tablet_schema();
    ASSERT_TRUE(tablet_schema != nullptr);
    ASSERT_EQ(schema_id, tablet_schema->id());
}

} // namespace starrocks::lake
