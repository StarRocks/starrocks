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
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/lake/lake_connector.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/vector_search_option.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeDataSourceTest : public ::testing::Test {
public:
    LakeDataSourceTest()
            : _location_provider(std::make_shared<FixedLocationProvider>(kRootLocation)),
              _parent_tracker(std::make_unique<MemTracker>(-1)),
              _update_mem_tracker(
                      std::make_unique<MemTracker>(10 * 1024 * 1024, "lake_data_source", _parent_tracker.get())),
              _update_manager(std::make_unique<UpdateManager>(_location_provider, _update_mem_tracker.get())),
              _owned_tablet_mgr(
                      std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024)),
              _tablet_mgr(_owned_tablet_mgr.get()) {
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
        (void)fs::remove_all(kRootLocation);
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { CHECK_OK(fs::remove_all(kRootLocation)); }

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
            for (const auto& file : writer->segments()) {
                rowset->add_segment_metas()->set_filename(file.path);
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

    std::shared_ptr<RuntimeState> create_runtime_state_for_test() {
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(query_globals);
        auto* fragment_dict_state = runtime_state->obj_pool()->add(new FragmentDictState());
        runtime_state->set_fragment_dict_state(fragment_dict_state);
        runtime_state->set_fragment_runtime_state(&_fragment_runtime_state);
        TUniqueId query_id;
        runtime_state->init_mem_trackers(query_id, _parent_tracker.get());
        return runtime_state;
    }

    static TPlanNode create_lake_plan_node() {
        TLakeScanNode lake_scan_node;
        lake_scan_node.__set_tuple_id(0);

        TPlanNode plan_node;
        plan_node.__set_node_id(1);
        plan_node.__set_lake_scan_node(lake_scan_node);
        return plan_node;
    }

    static std::vector<TScanRangeParams> create_scan_ranges(const std::vector<TabletMetadata*>& tablet_metas) {
        std::vector<TScanRangeParams> scan_ranges;
        scan_ranges.reserve(tablet_metas.size());
        for (const auto* tablet_meta : tablet_metas) {
            TInternalScanRange internal_scan_range;
            internal_scan_range.__set_tablet_id(tablet_meta->id());
            internal_scan_range.__set_version(std::to_string(tablet_meta->version()));

            TScanRange scan_range;
            scan_range.__set_internal_scan_range(internal_scan_range);

            TScanRangeParams params;
            params.__set_scan_range(scan_range);
            scan_ranges.emplace_back(std::move(params));
        }
        return scan_ranges;
    }

    constexpr static const char* const kRootLocation = "test_lake_data_source_test";

    std::shared_ptr<LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _parent_tracker;
    std::unique_ptr<MemTracker> _update_mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _owned_tablet_mgr;
    TabletManager* _tablet_mgr = nullptr;

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    pipeline::FragmentRuntimeState _fragment_runtime_state;
};

TEST_F(LakeDataSourceTest, test_convert_scan_range_to_morsel_queue) {
    create_rowsets_for_testing(_tablet_metadata.get(), 2);

    bool enable_tablet_internal_parallel = true;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::FORCE_SPLIT;

    auto plan_node = create_lake_plan_node();
    connector::LakeDataSourceProvider data_source_provider(plan_node);
    data_source_provider.set_lake_tablet_manager(_tablet_mgr);

    ASSERT_FALSE(data_source_provider.always_shared_scan());

    config::tablet_internal_parallel_max_splitted_scan_bytes = 32;
    config::tablet_internal_parallel_min_splitted_scan_rows = 4;

    int pipeline_dop = 2;
    config::tablet_internal_parallel_min_scan_dop = 4;

    auto scan_ranges = create_scan_ranges({_tablet_metadata.get()});

    data_source_provider.set_estimated_scan_row_bytes(0);
    auto missing_estimate = data_source_provider.convert_scan_range_to_morsel_queue_builder(
            scan_ranges, plan_node.node_id, pipeline_dop, enable_tablet_internal_parallel,
            tablet_internal_parallel_mode, scan_ranges.size());
    ASSERT_ERROR(missing_estimate.status());
    EXPECT_NE(std::string::npos, missing_estimate.status().to_string().find("estimated scan row bytes"));
    data_source_provider.set_estimated_scan_row_bytes(sizeof(int32_t));

    ASSIGN_OR_ABORT(auto builder, data_source_provider.convert_scan_range_to_morsel_queue_builder(
                                          scan_ranges, plan_node.node_id, pipeline_dop, enable_tablet_internal_parallel,
                                          tablet_internal_parallel_mode, scan_ranges.size()));
    ASSERT_TRUE(data_source_provider.could_split());
    ASSERT_TRUE(data_source_provider.could_split_physically());
    ASSERT_TRUE(builder->has_more_from_split());
    ASSERT_GT(builder->max_degree_of_parallelism(), 0);
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
    auto runtime_state = create_runtime_state_for_test();
    TNetworkAddress fe;
    fe.hostname = "127.0.0.1";
    fe.port = 9020;
    _fragment_runtime_state.set_fe_addr(fe);

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
    starrocks::connector::LakeDataSourceProvider provider(plan_node);
    provider.set_lake_tablet_manager(_tablet_mgr);

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

// Exercise the vector-search branches of LakeDataSource::open() that were added by
// the shared-data vector index read PR:
//   * open() propagation of TVectorSearchOptions from thrift into _use_vector_index etc.
//   * init_scanner_columns: the slot-id != _vector_slot_id (regular column lookup) branch.
//   * init_reader_params: the `if (_use_vector_index)` block that copies query_vector,
//     vector_range, ann_params, etc. onto the reader params.
//
// We do not need a real .vi file — the test is over once init_reader_params +
// init_scanner_columns have filled _params, so open() is allowed to return any
// status. We do assert that _params.use_vector_index landed as true via the
// TEST_params() accessor.
TEST_F(LakeDataSourceTest, open_with_vector_search_options) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    create_rowsets_for_testing(_tablet_metadata.get(), 2);

    // 1) Build TLakeScanNode with vector_search_options set. vector_slot_id is set to
    //    a value that does not match any tuple slot (see below), so init_scanner_columns
    //    exercises the regular-column-lookup branch for every slot.
    int64_t schema_id = next_id();
    if (schema_id == _tablet_metadata->schema().id()) {
        schema_id = next_id();
    }
    TLakeScanNode lake_scan_node;
    lake_scan_node.__set_tuple_id(0);
    TTableSchemaKey schema_key;
    schema_key.__set_schema_id(schema_id);
    schema_key.__set_db_id(200);
    schema_key.__set_table_id(201);
    lake_scan_node.__set_schema_key(schema_key);

    TVectorSearchOptions vec_opts;
    vec_opts.__set_enable_use_ann(true);
    vec_opts.__set_refine_distance(false);
    vec_opts.__set_vector_distance_column_name("vec_distance");
    // Use a slot id that does NOT match any tuple slot, so init_scanner_columns
    // exercises the else branch for every slot (regular column lookup) without
    // synthesizing a virtual column index that the downstream schema conversion
    // wouldn't know how to handle in this minimal test setup.
    vec_opts.__set_vector_slot_id(999);
    vec_opts.__set_vector_limit_k(10);
    vec_opts.__set_query_vector(std::vector<std::string>{"0.1", "0.2", "0.3"});
    vec_opts.__set_vector_range(0.5);
    vec_opts.__set_result_order(0);
    vec_opts.__set_pq_refine_factor(1.0);
    vec_opts.__set_k_factor(1.0);
    lake_scan_node.__set_vector_search_options(vec_opts);

    TPlanNode plan_node;
    plan_node.__set_node_id(0);
    plan_node.__set_lake_scan_node(lake_scan_node);

    // 2) Runtime state with fragment_ctx (fe_addr required by schema RPC path).
    auto runtime_state = create_runtime_state_for_test();
    TNetworkAddress fe;
    fe.hostname = "127.0.0.1";
    fe.port = 9020;
    _fragment_runtime_state.set_fe_addr(fe);

    // 3) Desc table with two INT slots matching the schema we mock below.
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

    TTableDescriptor tdesc;
    tdesc.__set_id(0);
    tdesc.__set_tableType(TTableType::OLAP_TABLE);
    tdesc.__set_tableName("test_table");
    tdesc.__set_dbName("test_db");
    auto* table_desc = runtime_state->obj_pool()->add(new OlapTableDescriptor(tdesc));
    desc_tbl->get_tuple_descriptor(0)->set_table_desc(table_desc);

    // 4) Schema RPC hook returns the same two-column schema as get_tablet_schema so the
    //    non-vector slot still maps to a real tablet column (avoids -1 from field_index).
    SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_fetch_schema_via_rpc::test_hook", [&](void* arg) {
        auto* arr = static_cast<std::array<void*, 4>*>(arg);
        auto* response_batch = static_cast<TBatchGetTableSchemaResponse*>((*arr)[1]);
        auto* status = static_cast<Status*>((*arr)[2]);
        auto* mock_thrift_rpc = static_cast<bool*>((*arr)[3]);

        *mock_thrift_rpc = true;
        *status = Status::OK();
        set_status(&response_batch->status, TStatusCode::OK);
        response_batch->__set_responses(std::vector<TGetTableSchemaResponse>{});
        auto& resp = response_batch->responses.emplace_back();
        set_status(&resp.status, TStatusCode::OK);

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

    starrocks::connector::LakeDataSourceProvider provider(plan_node);
    provider.set_lake_tablet_manager(_tablet_mgr);
    provider.set_filtered_above_iterator(true);

    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

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

    // open() may or may not succeed end-to-end — we only care that the vector-search
    // branches fired on the way through.
    (void)ds.open(runtime_state.get());

    const auto& params = ds.TEST_params();
    EXPECT_TRUE(params.use_vector_index);
    EXPECT_TRUE(params.has_predicate_above_iterator);
    ASSERT_NE(params.vector_search_option, nullptr);
    EXPECT_EQ(params.vector_search_option->k, 10);
    EXPECT_EQ(params.vector_search_option->vector_distance_column_name, "vec_distance");
    ASSERT_EQ(params.vector_search_option->query_vector.size(), 3);
    EXPECT_FLOAT_EQ(params.vector_search_option->query_vector[0], 0.1f);
}

TEST_F(LakeDataSourceTest, test_has_all_pk_columns_selected) {
    // Build a PK tablet schema: c0 (key), c1 (key), c2 (value)
    TabletSchemaPB pk_schema_pb;
    pk_schema_pb.set_id(next_id());
    pk_schema_pb.set_num_short_key_columns(2);
    pk_schema_pb.set_keys_type(PRIMARY_KEYS);
    pk_schema_pb.set_num_rows_per_row_block(65535);
    {
        auto* c = pk_schema_pb.add_column();
        c->set_unique_id(next_id());
        c->set_name("c0");
        c->set_type("INT");
        c->set_is_key(true);
        c->set_is_nullable(false);
    }
    {
        auto* c = pk_schema_pb.add_column();
        c->set_unique_id(next_id());
        c->set_name("c1");
        c->set_type("INT");
        c->set_is_key(true);
        c->set_is_nullable(false);
    }
    {
        auto* c = pk_schema_pb.add_column();
        c->set_unique_id(next_id());
        c->set_name("c2");
        c->set_type("INT");
        c->set_is_key(false);
        c->set_is_nullable(false);
    }
    auto pk_tablet_schema = TabletSchema::create(pk_schema_pb);

    auto runtime_state = create_runtime_state_for_test();
    TSlotDescriptorBuilder slot_desc_builder;

    // Case 1: Select all PK columns (c0, c1) → true
    {
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c0").column_pos(0).nullable(false).build());
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(1).nullable(false).build());
        tuple_builder.build(&builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), builder.desc_tbl(), &desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        ASSERT_TRUE(connector::has_all_pk_columns_selected(pk_tablet_schema.get(), tuple_desc->slots()));
    }

    // Case 2: Select only one PK column (c0) → false (c1 missing)
    {
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c0").column_pos(0).nullable(false).build());
        tuple_builder.build(&builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), builder.desc_tbl(), &desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        ASSERT_FALSE(connector::has_all_pk_columns_selected(pk_tablet_schema.get(), tuple_desc->slots()));
    }

    // Case 3: Select only value column (c2) → false
    {
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c2").column_pos(2).nullable(false).build());
        tuple_builder.build(&builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), builder.desc_tbl(), &desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        ASSERT_FALSE(connector::has_all_pk_columns_selected(pk_tablet_schema.get(), tuple_desc->slots()));
    }

    // Case 4: Select all columns (c0, c1, c2) → true (superset of PK)
    {
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c0").column_pos(0).nullable(false).build());
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(1).nullable(false).build());
        tuple_builder.add_slot(
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c2").column_pos(2).nullable(false).build());
        tuple_builder.build(&builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), builder.desc_tbl(), &desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        ASSERT_TRUE(connector::has_all_pk_columns_selected(pk_tablet_schema.get(), tuple_desc->slots()));
    }

    // Case 5: DUP_KEYS table → false
    ASSERT_FALSE(connector::has_all_pk_columns_selected(_tablet_schema.get(), {}));

    // Case 6: nullptr schema → false
    ASSERT_FALSE(connector::has_all_pk_columns_selected(nullptr, {}));
}

// clang-format-10 (the version CI enforces) mis-parses the #ifdef USE_STAROS block in this
// pre-existing test once the file grows and reflows it into invalid indentation; keep the
// hand-formatting so `clang-format-changed` stays clean.
// clang-format off
TEST_F(LakeDataSourceTest, test_warmup_pk_index_sst_files) {
    // Case 1: Non-PK table → skip warmup
    { ASSERT_OK(connector::warmup_pk_index_sst_files(_tablet_metadata.get(), _tablet_mgr)); }

    // Case 2: nullptr metadata → skip warmup
    { ASSERT_OK(connector::warmup_pk_index_sst_files(nullptr, _tablet_mgr)); }

    // Case 3: PK table with cloud-native persistent index but no SST files → skip
    {
        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        ASSERT_OK(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr));
    }

    // Case 4: PK table with LOCAL persistent index → skip
    {
        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::LOCAL);
        auto* sst_meta = pk_metadata.mutable_sstable_meta();
        auto* sst = sst_meta->add_sstables();
        sst->set_filename("dummy.sst");
        sst->set_filesize(100);
        ASSERT_OK(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr));
    }

#ifdef USE_STAROS
    // Case 5: PK table with cloud-native persistent index and SST file that exists
    {
        // Create a fake SST file on disk
        std::string sst_filename = "test_warmup.sst";
        std::string sst_path = _tablet_mgr->sst_location(_tablet_metadata->id(), sst_filename);
        {
            ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(sst_path));
            std::string sst_content(4096, 'x');
            ASSERT_OK(wf->append(sst_content));
            ASSERT_OK(wf->close());
        }

        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* sst_meta = pk_metadata.mutable_sstable_meta();
        auto* sst = sst_meta->add_sstables();
        sst->set_filename(sst_filename);
        sst->set_filesize(4096);

        ASSERT_OK(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr));
    }

    // Case 6: SST file exists but filesize not set in pb → fallback to get_size()
    {
        std::string sst_filename = "test_warmup_nosize.sst";
        std::string sst_path = _tablet_mgr->sst_location(_tablet_metadata->id(), sst_filename);
        {
            ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(sst_path));
            std::string sst_content(2048, 'y');
            ASSERT_OK(wf->append(sst_content));
            ASSERT_OK(wf->close());
        }

        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* sst_meta = pk_metadata.mutable_sstable_meta();
        auto* sst = sst_meta->add_sstables();
        sst->set_filename(sst_filename);
        // filesize not set (defaults to 0), should fallback to get_size()

        ASSERT_OK(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr));
    }

    // Case 7: SST file does not exist → should return error
    {
        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* sst_meta = pk_metadata.mutable_sstable_meta();
        auto* sst = sst_meta->add_sstables();
        sst->set_filename("nonexistent.sst");
        sst->set_filesize(100);

        ASSERT_FALSE(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr).ok());
    }

    // Case 8: SST with invalid encryption_meta → should return error
    {
        TabletMetadata pk_metadata;
        pk_metadata.set_id(_tablet_metadata->id());
        pk_metadata.set_version(1);
        pk_metadata.set_enable_persistent_index(true);
        pk_metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* sst_meta = pk_metadata.mutable_sstable_meta();
        auto* sst = sst_meta->add_sstables();
        sst->set_filename("dummy_enc.sst");
        sst->set_filesize(1024);
        sst->set_encryption_meta("invalid_encryption_meta");

        ASSERT_FALSE(connector::warmup_pk_index_sst_files(&pk_metadata, _tablet_mgr).ok());
    }
#endif
}
// clang-format on

// ---------------------------------------------------------------------------
// Unit tests for the late-runtime-filter reinit decision (P3b connector wiring).
//
// These exercise LakeDataSource::needs_late_runtime_filter_reinit in isolation:
// pure logic over the observed-vs-current RuntimeFilterSnapshots, no open(), no
// reader, no tablet IO. connector_lake_test is compiled with -fno-access-control
// (be/test/connector/CMakeLists.txt), so the private nested RuntimeFilterSnapshot(s)
// types and the _observed_runtime_filter_snapshots member are directly accessible.
//
// A late reinit is needed when a non-stream-build runtime filter has meaningfully
// changed since the seed prune was captured (newly arrived, version increased,
// appeared, disappeared, or a descriptor/stream-build attribute flipped). Stream
// -build filters never trigger a reinit.
// ---------------------------------------------------------------------------
TEST_F(LakeDataSourceTest, needs_late_runtime_filter_reinit_matrix) {
    // The ctor merely copies scan_range.internal_scan_range and stores the provider
    // pointer, so a minimal provider + default scan range is safe here.
    TPlanNode plan_node;
    plan_node.__set_node_id(0);
    TLakeScanNode lake_scan_node;
    lake_scan_node.__set_tuple_id(0);
    plan_node.__set_lake_scan_node(lake_scan_node);
    connector::LakeDataSourceProvider provider(plan_node);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    using Snapshot = connector::LakeDataSource::RuntimeFilterSnapshot;
    using Snapshots = connector::LakeDataSource::RuntimeFilterSnapshots;
    auto snap = [](bool has_descriptor, bool stream_build, bool arrived, size_t version) {
        Snapshot s;
        s.has_descriptor = has_descriptor;
        s.stream_build = stream_build;
        s.arrived = arrived;
        s.version = version;
        return s;
    };

    struct Case {
        std::string name;
        Snapshots observed;
        Snapshots current;
        bool expected;
    };

    const std::vector<Case> cases = {
            // current empty: reinit iff observed has any non-stream-build filter.
            {"A_empty_current_nonstream_observed", {{1, snap(true, false, true, 3)}}, {}, true},
            {"A_empty_current_empty_observed", {}, {}, false},
            {"A_empty_current_all_stream_observed", {{1, snap(true, true, true, 3)}}, {}, false},
            // current entry without a descriptor always forces a reinit.
            {"B_current_missing_descriptor", {}, {{1, snap(false, false, false, 0)}}, true},
            // a filter present in current but not observed: stream-build is ignored, others trigger.
            {"C_new_stream_filter_ignored", {}, {{2, snap(true, true, false, 0)}}, false},
            {"C_new_nonstream_filter", {}, {{2, snap(true, false, false, 0)}}, true},
            // descriptor / stream-build attribute flips trigger.
            {"D_has_descriptor_flip", {{1, snap(false, false, false, 0)}}, {{1, snap(true, false, false, 0)}}, true},
            {"D_stream_build_flip", {{1, snap(true, true, false, 0)}}, {{1, snap(true, false, false, 0)}}, true},
            // matched stream-build filter: arrival/version changes are ignored.
            {"E_stream_match_arrival_and_version_ignored",
             {{1, snap(true, true, false, 0)}},
             {{1, snap(true, true, true, 5)}},
             false},
            // matched non-stream filter: arrival flip or version increase triggers.
            {"F_arrived_flip", {{1, snap(true, false, false, 0)}}, {{1, snap(true, false, true, 0)}}, true},
            {"F_version_increase", {{1, snap(true, false, true, 3)}}, {{1, snap(true, false, true, 5)}}, true},
            // strict '>' comparison: equal version does not trigger.
            {"F_version_equal_no_reinit", {{1, snap(true, false, true, 5)}}, {{1, snap(true, false, true, 5)}}, false},
            {"F_not_arrived_match_no_reinit",
             {{1, snap(true, false, false, 0)}},
             {{1, snap(true, false, false, 0)}},
             false},
            // a non-stream filter that disappeared from current triggers; a stream one does not.
            {"G_nonstream_observed_dropped",
             {{1, snap(true, false, true, 3)}, {2, snap(true, false, true, 1)}},
             {{1, snap(true, false, true, 3)}},
             true},
            {"G_stream_observed_dropped_ignored",
             {{1, snap(true, false, true, 3)}, {2, snap(true, true, true, 1)}},
             {{1, snap(true, false, true, 3)}},
             false},
    };

    for (const auto& c : cases) {
        ds._observed_runtime_filter_snapshots = c.observed;
        EXPECT_EQ(ds.needs_late_runtime_filter_reinit(c.current), c.expected) << "case: " << c.name;
    }
}

TEST_F(LakeDataSourceTest, stream_build_rf_never_triggers_late_reinit) {
    TPlanNode plan_node;
    plan_node.__set_node_id(0);
    TLakeScanNode lake_scan_node;
    lake_scan_node.__set_tuple_id(0);
    plan_node.__set_lake_scan_node(lake_scan_node);
    connector::LakeDataSourceProvider provider(plan_node);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    using Snapshot = connector::LakeDataSource::RuntimeFilterSnapshot;
    using Snapshots = connector::LakeDataSource::RuntimeFilterSnapshots;
    auto snap = [](bool has_descriptor, bool stream_build, bool arrived, size_t version) {
        Snapshot s;
        s.has_descriptor = has_descriptor;
        s.stream_build = stream_build;
        s.arrived = arrived;
        s.version = version;
        return s;
    };

    // A stream-build RF that arrives (false -> true) and bumps its version (0 -> 5)
    // must NOT trigger a late reinit.
    ds._observed_runtime_filter_snapshots = {{1, snap(true, true, false, 0)}};
    EXPECT_FALSE(ds.needs_late_runtime_filter_reinit(Snapshots{{1, snap(true, true, true, 5)}}));

    // The same evolution on a non-stream-build RF DOES trigger, as a control.
    ds._observed_runtime_filter_snapshots = {{1, snap(true, false, false, 0)}};
    EXPECT_TRUE(ds.needs_late_runtime_filter_reinit(Snapshots{{1, snap(true, false, true, 5)}}));

    // current empty + observed entirely stream-build: nothing to reinit.
    ds._observed_runtime_filter_snapshots = {{1, snap(true, true, true, 3)}};
    EXPECT_FALSE(ds.needs_late_runtime_filter_reinit(Snapshots{}));
}

// ---------------------------------------------------------------------------
// Unit tests for the split-context dispatch (apply_child_split_context) and the
// reuse gating (reusable_child_context). Pure logic over a stack LakeSplitContext
// + prepared read state; no open(), no reader. connector_lake_test is compiled
// with -fno-access-control, so the private methods / members / TEST_params() are
// directly reachable.
// ---------------------------------------------------------------------------
namespace {
using RRS = pipeline::LakeSplitContext::RowidRangeSource;

connector::LakeDataSourceProvider make_split_provider(bool could_split_physically) {
    TPlanNode plan_node;
    plan_node.__set_node_id(0);
    TLakeScanNode lake_scan_node;
    lake_scan_node.__set_tuple_id(0);
    plan_node.__set_lake_scan_node(lake_scan_node);
    connector::LakeDataSourceProvider provider(plan_node);
    provider._could_split_physically = could_split_physically; // protected, -fno-access-control
    return provider;
}

// Build a LakeSplitContext with a non-null rowid_range and (optionally) prepared
// tablet/segment read state. publish_bounds makes has_rowid_bounds_cache() true.
pipeline::LakeSplitContext make_split_context(RRS source, bool with_prepared, bool publish_bounds) {
    pipeline::LakeSplitContext ctx;
    ctx.rowid_range_source = source;
    ctx.rowid_range = std::make_shared<RowidRangeOption>();
    ctx.rowset_index = 0;
    ctx.segment_index = 0;
    if (with_prepared) {
        ctx.prepared_tablet_read_state = std::make_shared<lake::PreparedTabletReadState>();
        ctx.prepared_segment_read_state = std::make_shared<lake::PreparedSegmentReadState>();
        if (publish_bounds) {
            ctx.prepared_segment_read_state->publish_rowid_bounds_cache({}, std::nullopt);
        }
    }
    return ctx;
}
} // namespace

TEST_F(LakeDataSourceTest, apply_child_split_context_physical_matrix) {
    auto provider = make_split_provider(/*could_split_physically=*/true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    // REGULAR + use_prepared_state=true + prepared present: no morsel counter, prepared state
    // preserved (can reuse: has context and not a pre-refinement coarse split), not a refine task.
    {
        auto ctx = make_split_context(RRS::REGULAR, /*with_prepared=*/true, /*publish_bounds=*/false);
        ds.apply_child_split_context(ctx, /*use_prepared_state=*/true);
        EXPECT_EQ(ds._lake_prerefinement_coarse_splits, 0);
        EXPECT_EQ(ds._lake_initial_coarse_splits, 0);
        EXPECT_EQ(ds._lake_refined_splits, 0);
        EXPECT_EQ(ds.TEST_params().prepared_tablet_read_state, ctx.prepared_tablet_read_state);
        EXPECT_EQ(ds.TEST_params().prepared_segment_read_state, ctx.prepared_segment_read_state);
        EXPECT_FALSE(ds.TEST_params().refine_initial_coarse_split_and_append_refined_tasks);
        EXPECT_EQ(ds.TEST_params().rowid_range_option, ctx.rowid_range); // REGULAR: not trimmed
    }
    // INITIAL_COARSE + reusable prepared: counter++, prepared preserved, refine task requested.
    {
        connector::LakeDataSource ds2(&provider, scan_range);
        auto ctx = make_split_context(RRS::INITIAL_COARSE, true, false);
        ds2.apply_child_split_context(ctx, true);
        EXPECT_EQ(ds2._lake_initial_coarse_splits, 1);
        EXPECT_TRUE(ds2.TEST_params().refine_initial_coarse_split_and_append_refined_tasks);
        EXPECT_EQ(ds2.TEST_params().prepared_tablet_read_state, ctx.prepared_tablet_read_state);
    }
    // REFINED + prepared: refined counter++, prepared preserved, no refine task.
    {
        connector::LakeDataSource ds3(&provider, scan_range);
        auto ctx = make_split_context(RRS::REFINED, true, false);
        ds3.apply_child_split_context(ctx, true);
        EXPECT_EQ(ds3._lake_refined_splits, 1);
        EXPECT_FALSE(ds3.TEST_params().refine_initial_coarse_split_and_append_refined_tasks);
        EXPECT_EQ(ds3.TEST_params().prepared_tablet_read_state, ctx.prepared_tablet_read_state);
    }
    // PRE_REFINEMENT_COARSE + rowid-bounds cache ready: reusable, prerefinement counter++.
    {
        connector::LakeDataSource ds4(&provider, scan_range);
        auto ctx = make_split_context(RRS::PRE_REFINEMENT_COARSE, true, /*publish_bounds=*/true);
        ds4.apply_child_split_context(ctx, true);
        EXPECT_EQ(ds4._lake_prerefinement_coarse_splits, 1);
        EXPECT_EQ(ds4.TEST_params().prepared_tablet_read_state, ctx.prepared_tablet_read_state);
    }
    // PRE_REFINEMENT_COARSE + rowid-bounds cache NOT ready: not reusable -> prepared state cleared.
    {
        connector::LakeDataSource ds5(&provider, scan_range);
        auto ctx = make_split_context(RRS::PRE_REFINEMENT_COARSE, true, /*publish_bounds=*/false);
        ds5.apply_child_split_context(ctx, true);
        EXPECT_EQ(ds5._lake_prerefinement_coarse_splits, 1);
        EXPECT_EQ(ds5.TEST_params().prepared_tablet_read_state, nullptr);
        EXPECT_EQ(ds5.TEST_params().prepared_segment_read_state, nullptr);
        EXPECT_FALSE(ds5.TEST_params().refine_initial_coarse_split_and_append_refined_tasks);
    }
    // use_prepared_state=false: prepared state always cleared, rowid range left untrimmed.
    {
        connector::LakeDataSource ds6(&provider, scan_range);
        auto ctx = make_split_context(RRS::REGULAR, true, false);
        ds6.apply_child_split_context(ctx, /*use_prepared_state=*/false);
        EXPECT_EQ(ds6.TEST_params().prepared_tablet_read_state, nullptr);
        EXPECT_EQ(ds6.TEST_params().prepared_segment_read_state, nullptr);
        EXPECT_EQ(ds6.TEST_params().rowid_range_option, ctx.rowid_range);
    }
}

TEST_F(LakeDataSourceTest, apply_child_split_context_logical_branch) {
    auto provider = make_split_provider(/*could_split_physically=*/false);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    auto ctx = make_split_context(RRS::REGULAR, /*with_prepared=*/true, false);
    ctx.short_key_range =
            std::make_shared<ShortKeyRangesOption>(std::vector<ShortKeyRangeOptionPtr>{}, /*is_first_split=*/false);
    ds.apply_child_split_context(ctx, /*use_prepared_state=*/true);

    // Logical split branch: rowid range cleared, short-key range forwarded, prepared state cleared.
    EXPECT_EQ(ds.TEST_params().rowid_range_option, nullptr);
    EXPECT_EQ(ds.TEST_params().short_key_ranges_option, ctx.short_key_range);
    EXPECT_EQ(ds.TEST_params().prepared_tablet_read_state, nullptr);
    EXPECT_EQ(ds.TEST_params().prepared_segment_read_state, nullptr);
    EXPECT_FALSE(ds.TEST_params().refine_initial_coarse_split_and_append_refined_tasks);
}

TEST_F(LakeDataSourceTest, reusable_child_context_matrix) {
    auto provider = make_split_provider(true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    auto build_morsel = [&](RRS source, bool with_prepared, int64_t from_version) {
        auto ctx = std::make_unique<pipeline::LakeSplitContext>(make_split_context(source, with_prepared, false));
        auto morsel = std::make_unique<pipeline::ScanMorsel>(0, scan_range);
        morsel->set_split_context(std::move(ctx));
        morsel->set_from_version(from_version);
        return morsel;
    };

    // Happy path: REFINED + prepared physical split + from_version == 0 -> returns the split context.
    {
        auto morsel = build_morsel(RRS::REFINED, /*with_prepared=*/true, /*from_version=*/0);
        EXPECT_NE(ds.reusable_child_context(*morsel), nullptr);
    }
    // from_version != 0 -> not reusable.
    {
        auto morsel = build_morsel(RRS::REFINED, true, /*from_version=*/5);
        EXPECT_EQ(ds.reusable_child_context(*morsel), nullptr);
    }
    // Non-REFINED source -> not reusable.
    {
        auto morsel = build_morsel(RRS::INITIAL_COARSE, true, 0);
        EXPECT_EQ(ds.reusable_child_context(*morsel), nullptr);
    }
    // Missing prepared state (not a prepared physical split) -> not reusable.
    {
        auto morsel = build_morsel(RRS::REFINED, /*with_prepared=*/false, 0);
        EXPECT_EQ(ds.reusable_child_context(*morsel), nullptr);
    }
    // No split context at all -> not reusable.
    {
        auto morsel = std::make_unique<pipeline::ScanMorsel>(0, scan_range);
        EXPECT_EQ(ds.reusable_child_context(*morsel), nullptr);
    }
}

// can_reuse_with gates an existing reader's reuse on: a reusable reader key is present, the
// incoming morsel is itself a reusable child (REFINED + from_version==0 + prepared physical
// split), and its prepared tablet read state matches the one the reader was opened against.
TEST_F(LakeDataSourceTest, can_reuse_with_matrix) {
    auto provider = make_split_provider(true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    auto make_refined_morsel = [&](lake::PreparedTabletReadStatePtr tablet_state, int64_t from_version) {
        auto ctx = std::make_unique<pipeline::LakeSplitContext>(
                make_split_context(RRS::REFINED, /*with_prepared=*/true, false));
        ctx->prepared_tablet_read_state = std::move(tablet_state); // pin the state we compare against
        auto morsel = std::make_unique<pipeline::ScanMorsel>(0, scan_range);
        morsel->set_split_context(std::move(ctx));
        morsel->set_from_version(from_version);
        return morsel;
    };

    auto state_a = std::make_shared<lake::PreparedTabletReadState>();
    auto state_b = std::make_shared<lake::PreparedTabletReadState>();

    // No reusable reader key yet -> nothing can be reused.
    EXPECT_FALSE(ds.can_reuse_with(*make_refined_morsel(state_a, 0)));

    ds._reusable_reader_key.prepared_tablet_read_state = state_a;
    // Matching prepared tablet read state + reusable child -> reusable.
    EXPECT_TRUE(ds.can_reuse_with(*make_refined_morsel(state_a, 0)));
    // A different prepared tablet read state -> not reusable (would read the wrong snapshot).
    EXPECT_FALSE(ds.can_reuse_with(*make_refined_morsel(state_b, 0)));
    // Matching state but not a reusable child (from_version != 0) -> not reusable.
    EXPECT_FALSE(ds.can_reuse_with(*make_refined_morsel(state_a, 7)));
}

// has_reusable_state requires BOTH an initialized reader and a reusable reader key.
TEST_F(LakeDataSourceTest, has_reusable_state_requires_reader_and_key) {
    auto provider = make_split_provider(true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    // Fresh data source: no reader, no key.
    EXPECT_FALSE(ds.has_reusable_state());

    // Key without a reader is still not reusable.
    ds._reusable_reader_key.prepared_tablet_read_state = std::make_shared<lake::PreparedTabletReadState>();
    EXPECT_FALSE(ds.has_reusable_state());

    // A sentinel non-null reader (has_reusable_state only checks the pointer, never derefs it)
    // plus the key -> reusable.
    ds._reader =
            std::shared_ptr<lake::TabletReader>(reinterpret_cast<lake::TabletReader*>(0x1), [](lake::TabletReader*) {});
    EXPECT_TRUE(ds.has_reusable_state());

    // Reader without a key -> not reusable.
    ds._reusable_reader_key.prepared_tablet_read_state = nullptr;
    EXPECT_FALSE(ds.has_reusable_state());

    ds._reader = nullptr; // drop the sentinel before teardown
}

// A PRE_REFINEMENT_COARSE child, when applied with use_prepared_state, must have its coarse
// rowid range trimmed down to the seed's pruned scan range (the coarse range is a superset;
// the pruned range subtracts the pages the seed already ruled out). Bare Rowset/Segment are
// enough: trim only does pointer bookkeeping + a SparseRange intersection, no disk read.
TEST_F(LakeDataSourceTest, apply_child_split_context_trims_coarse_by_pruned_range) {
    auto provider = make_split_provider(/*could_split_physically=*/true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    auto* col = schema_pb.add_column();
    col->set_unique_id(0);
    col->set_name("c0");
    col->set_type("INT");
    col->set_is_key(true);
    col->set_is_nullable(false);
    col->set_length(4);
    col->set_index_length(4);
    auto tablet_schema = TabletSchema::create(schema_pb);
    RowsetMetadataPB rowset_meta; // must outlive the bare Rowset below
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString("/tmp/p3b_trim"));
    auto rowset = std::make_shared<lake::Rowset>(/*tablet_mgr=*/nullptr, /*tablet_id=*/10001, &rowset_meta,
                                                 /*index=*/0, tablet_schema);
    auto segment = std::make_shared<Segment>(fs, FileInfo{"/tmp/p3b_trim/0.dat"}, /*seg_id=*/0, tablet_schema,
                                             /*tablet_manager=*/nullptr);

    auto tablet_state = std::make_shared<lake::PreparedTabletReadState>();
    tablet_state->rowsets = {rowset};
    tablet_state->rowset_segments = {{segment}};
    auto segment_state = std::make_shared<lake::PreparedSegmentReadState>();
    // Seed published a pruned scan range of [0, 50) for this segment.
    auto pruned = std::make_shared<SparseRange<>>();
    pruned->add(Range<>(0, 50));
    segment_state->publish_pruned_scan_range(pruned);

    // The coarse split covers [0, 100) of the segment.
    auto rowid_range = std::make_shared<RowidRangeOption>();
    auto coarse = std::make_shared<SparseRange<>>();
    coarse->add(Range<>(0, 100));
    rowid_range->add(rowset.get(), segment.get(), coarse, /*is_first_split_of_segment=*/true);

    pipeline::LakeSplitContext ctx;
    ctx.rowid_range_source = RRS::PRE_REFINEMENT_COARSE;
    ctx.rowid_range = rowid_range;
    ctx.prepared_tablet_read_state = tablet_state;
    ctx.prepared_segment_read_state = segment_state;
    ctx.rowset_index = 0;
    ctx.segment_index = 0;

    ds.apply_child_split_context(ctx, /*use_prepared_state=*/true);

    // [0,100) trimmed by [0,50) -> [0,50), span 50.
    auto trimmed = ds.TEST_params().rowid_range_option;
    ASSERT_NE(trimmed, nullptr);
    auto split = trimmed->get_segment_rowid_range(rowset.get(), segment.get());
    ASSERT_NE(split.row_id_range, nullptr);
    EXPECT_EQ(split.row_id_range->span_size(), 50u);
}

// ---------------------------------------------------------------------------
// Batch 3: init_counter registration, reopen_reader guard, capture_runtime_filter_snapshots.
// Pure logic; connector_lake_test is -fno-access-control so private methods /
// members / the private RuntimeFilterSnapshot fields are reachable.
// ---------------------------------------------------------------------------
TEST_F(LakeDataSourceTest, init_counter_registers_prepared_split_counters) {
    auto provider = make_split_provider(/*could_split_physically=*/false);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);
    RuntimeProfile parent("LakeDataSourceTest");
    ds.set_runtime_profile(&parent);
    auto rs = create_runtime_state_for_test();
    ds.init_counter(rs.get());

    RuntimeProfile* profile = ds._runtime_profile;
    ASSERT_NE(profile, nullptr);
    EXPECT_NE(profile->get_counter("LakePreparedSplit"), nullptr);
    for (const auto* name : {"PreparedRowsets", "PreparedSegments", "PreparedScanRows", "PreparedScanRanges",
                             "PreRefinementCoarseMorsels", "InitialCoarseMorsels", "RefinedMorsels",
                             "ReusableSegmentIterCreated", "ReusableSegmentIterReused", "LateRuntimeFilterReinit"}) {
        auto* c = profile->get_counter(name);
        EXPECT_NE(c, nullptr) << name;
        if (c != nullptr) {
            EXPECT_EQ(c->value(), 0) << name;
        }
    }
}

TEST_F(LakeDataSourceTest, reopen_reader_requires_initialized_reader) {
    auto provider = make_split_provider(/*could_split_physically=*/true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);
    auto rs = create_runtime_state_for_test();

    // _reader / _prj_iter are null on a fresh data source: reopen must fail early
    // rather than dereferencing them.
    auto st = ds.reopen_reader(rs.get());
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("reader is initialized"), std::string::npos) << st.to_string();
}

TEST_F(LakeDataSourceTest, capture_runtime_filter_snapshots_fields) {
    auto provider = make_split_provider(/*could_split_physically=*/false);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);

    ObjectPool pool;
    auto* coll = pool.add(new RuntimeFilterProbeCollector());

    // d1: non-stream, runtime filter arrived, version 7.
    auto* d1 = pool.add(new RuntimeFilterProbeDescriptor());
    d1->_filter_id = 1;
    d1->_is_stream_build_filter = false;
    auto* rf1 = MinMaxRuntimeFilter<TYPE_INT>::create_with_range<true>(&pool, 10, /*is_close_interval=*/true);
    rf1->_rf_version = 7;
    d1->set_runtime_filter(rf1);
    coll->add_descriptor(d1);

    // d2: stream-build, not yet arrived.
    auto* d2 = pool.add(new RuntimeFilterProbeDescriptor());
    d2->_filter_id = 2;
    d2->_is_stream_build_filter = true;
    coll->add_descriptor(d2);

    // d3: a null descriptor entry exercises the has_descriptor == false branch.
    coll->descriptors().emplace(3, nullptr);

    ds.set_runtime_filters(coll);
    auto snaps = ds.capture_runtime_filter_snapshots();

    ASSERT_EQ(snaps.size(), 3u);
    EXPECT_TRUE(snaps[1].has_descriptor);
    EXPECT_FALSE(snaps[1].stream_build);
    EXPECT_TRUE(snaps[1].arrived);
    EXPECT_EQ(snaps[1].version, 7u);
    EXPECT_TRUE(snaps[2].has_descriptor);
    EXPECT_TRUE(snaps[2].stream_build);
    EXPECT_FALSE(snaps[2].arrived);
    EXPECT_FALSE(snaps[3].has_descriptor);

    // No runtime filters attached -> empty snapshot map.
    connector::LakeDataSource ds_empty(&provider, scan_range);
    EXPECT_TRUE(ds_empty.capture_runtime_filter_snapshots().empty());
}

// The FE flag (session var -> OlapScanNode -> thrift use_prepared_physical_split_scan)
// only takes effect after LakeDataSourceProvider::init reads it. Verify the isset/value
// plumbing so a missing __isset or a false value both leave the feature disabled.
TEST_F(LakeDataSourceTest, provider_reads_prepared_split_flag) {
    ObjectPool pool;
    auto rs = create_runtime_state_for_test();

    auto make_provider = [&](std::optional<bool> flag) {
        TPlanNode plan_node;
        plan_node.__set_node_id(0);
        TLakeScanNode lake_scan_node;
        lake_scan_node.__set_tuple_id(0);
        if (flag.has_value()) {
            lake_scan_node.__set_use_prepared_physical_split_scan(*flag);
        }
        plan_node.__set_lake_scan_node(lake_scan_node);
        auto provider = std::make_unique<connector::LakeDataSourceProvider>(plan_node);
        provider->set_lake_tablet_manager(_tablet_mgr);
        CHECK_OK(provider->init(&pool, rs.get()));
        return provider;
    };

    // Flag set to true -> feature enabled.
    EXPECT_TRUE(make_provider(true)->_enable_lake_prepared_physical_split_scan);
    // Flag explicitly false -> disabled.
    EXPECT_FALSE(make_provider(false)->_enable_lake_prepared_physical_split_scan);
    // Flag never set (__isset false) -> disabled, matching an old FE that does not send it.
    EXPECT_FALSE(make_provider(std::nullopt)->_enable_lake_prepared_physical_split_scan);
}

// get_split_tasks forwards to the tablet reader, but the reader is null until the data
// source is opened. A fresh data source must treat the call as a no-op rather than
// dereferencing the null reader (the split-source morsel path may query it early).
TEST_F(LakeDataSourceTest, get_split_tasks_null_reader_is_safe) {
    auto provider = make_split_provider(/*could_split_physically=*/true);
    TScanRange scan_range;
    connector::LakeDataSource ds(&provider, scan_range);
    ASSERT_EQ(ds._reader, nullptr);

    std::vector<pipeline::ScanSplitContextPtr> split_tasks;
    ds.get_split_tasks(&split_tasks); // must not crash on the null reader
    EXPECT_TRUE(split_tasks.empty());
}

// disable_runtime_filter_dependent_cache must be flag-only: a late runtime filter can disable the
// shared prepared cache while sibling split readers are still borrowing it (shared_ptr copy + a raw
// pointer into the rowid-bounds vector) during iterator init without a lock. Disabling must flip the
// cache to "not available" (so future readers re-prune) WITHOUT resetting/clearing the underlying
// data, so those in-flight borrows stay valid instead of racing / dangling.
TEST_F(LakeDataSourceTest, disable_runtime_filter_dependent_cache_is_flag_only) {
    lake::PreparedSegmentReadState state;
    auto pruned = std::make_shared<SparseRange<>>();
    pruned->add(Range<>(0, 50));
    state.publish_pruned_scan_range(pruned);
    std::vector<std::optional<Range<rowid_t>>> bounds;
    bounds.emplace_back(Range<rowid_t>(0, 50));
    state.publish_rowid_bounds_cache(std::move(bounds), std::nullopt);
    ASSERT_TRUE(state.has_pruned_scan_range());
    ASSERT_TRUE(state.has_rowid_bounds_cache());

    // Mirror how make_segment_read_state_cache borrows the published data on the read path.
    auto borrowed_range = state.pruned_scan_range;                 // shared_ptr copy
    const auto* borrowed_bounds = &state.seek_ranges_rowid_bounds; // raw pointer into the vector

    state.disable_runtime_filter_dependent_cache();

    // Future readers see the caches as unavailable and will re-prune with the late filter.
    EXPECT_FALSE(state.has_pruned_scan_range());
    EXPECT_FALSE(state.has_rowid_bounds_cache());
    // But the underlying data is NOT reset/cleared, so the in-flight borrow is still valid.
    EXPECT_EQ(state.pruned_scan_range, borrowed_range);
    ASSERT_NE(borrowed_range, nullptr);
    EXPECT_EQ(borrowed_range->span_size(), 50u);
    EXPECT_FALSE(state.seek_ranges_rowid_bounds.empty());
    EXPECT_EQ(borrowed_bounds->size(), 1u);
}

} // namespace starrocks::lake
