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

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config_exec_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/object_pool.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/lake/lake_global_late_materialization_context.h"
#include "exec/pipeline/lookup/tablet_adaptor.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/options.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/storage_env.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks {

namespace {

class DummyGLMContext final : public GlobalLateMaterilizationContext {
public:
    ~DummyGLMContext() override = default;
};

TabletSchemaCSPtr make_test_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_id(100);
    schema_pb.set_keys_type(DUP_KEYS);
    return std::make_shared<const TabletSchema>(schema_pb);
}

RowsetSharedPtr make_test_rowset(const TabletSchemaCSPtr& schema, std::string rowset_id, int64_t num_segments) {
    auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
    rs_meta_pb->set_rowset_id(std::move(rowset_id));
    rs_meta_pb->set_start_version(0);
    rs_meta_pb->set_end_version(0);
    rs_meta_pb->set_num_segments(num_segments);
    TabletSchemaPB tablet_schema_pb;
    schema->to_schema_pb(&tablet_schema_pb);
    rs_meta_pb->mutable_tablet_schema()->CopyFrom(tablet_schema_pb);

    auto rowset_meta = std::make_shared<RowsetMeta>(rs_meta_pb);
    return std::make_shared<Rowset>(schema, "", rowset_meta, nullptr);
}

TabletSharedPtr create_test_tablet(int64_t tablet_id, int32_t schema_hash) {
    TCreateTabletReq request;
    request.tablet_id = tablet_id;
    request.__set_version(1);
    request.tablet_schema.schema_hash = schema_hash;
    request.tablet_schema.short_key_column_count = 1;
    request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
    request.tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn pk1;
    pk1.column_name = "pk1_bigint";
    pk1.__set_is_key(true);
    pk1.column_type.type = TPrimitiveType::BIGINT;
    request.tablet_schema.columns.push_back(pk1);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::INT;
    request.tablet_schema.columns.push_back(v1);

    auto st = StorageEngine::instance()->create_tablet(request);
    if (!st.ok()) {
        return nullptr;
    }
    return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
}

std::vector<SlotDescriptor*> create_slots(RuntimeState* state, ObjectPool* pool,
                                          const std::vector<std::string>& names) {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    TSlotDescriptorBuilder slot_builder;
    int32_t pos = 0;
    for (const auto& name : names) {
        tuple_builder.add_slot(slot_builder.type(LogicalType::TYPE_INT).column_name(name).column_pos(pos++).build());
    }
    tuple_builder.build(&dtb);
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(state, pool, dtb.desc_tbl(), &desc_tbl, config::vector_chunk_size);
    if (!st.ok()) {
        return {};
    }
    return desc_tbl->get_tuple_descriptor(0)->slots();
}

} // namespace

TEST(GlobalLateMaterilizationContextMgrTest, GetOrCreateCtxOnce) {
    GlobalLateMaterilizationContextMgr mgr;
    int create_count = 0;

    auto* ctx1 = mgr.get_or_create_ctx(7, [&]() {
        ++create_count;
        return new DummyGLMContext();
    });
    auto* ctx2 = mgr.get_or_create_ctx(7, [&]() {
        ++create_count;
        return new DummyGLMContext();
    });

    ASSERT_EQ(1, create_count);
    ASSERT_EQ(ctx1, ctx2);
    ASSERT_EQ(ctx1, mgr.get_ctx(7));

    delete ctx1;
}

TEST(OlapScanLazyMaterializationContextTest, CaptureAndLookupRowsets) {
    OlapScanLazyMaterializationContext ctx;
    auto schema = make_test_schema();
    auto rs1 = make_test_rowset(schema, "10001", 2);
    auto rs2 = make_test_rowset(schema, "10002", 1);

    ctx.capture_rowsets(10, 99, {rs1, rs2});

    int32_t segment_idx = -1;
    EXPECT_EQ(rs1, ctx.get_rowset(10, 0, &segment_idx));
    EXPECT_EQ(0, segment_idx);

    EXPECT_EQ(rs1, ctx.get_rowset(10, 1, &segment_idx));
    EXPECT_EQ(1, segment_idx);

    EXPECT_EQ(nullptr, ctx.get_rowset(10, 2, &segment_idx));

    EXPECT_EQ(rs2, ctx.get_rowset(10, 3, &segment_idx));
    EXPECT_EQ(0, segment_idx);

    EXPECT_EQ(nullptr, ctx.get_rowset(12345, 0, &segment_idx));

    auto rowset_id_map = ctx.get_rowset_id_to_drssid(10);
    ASSERT_EQ(2, rowset_id_map.size());
    EXPECT_EQ(0, rowset_id_map[rs1->rowset_id()]);
    EXPECT_EQ(3, rowset_id_map[rs2->rowset_id()]);

    EXPECT_EQ(99, ctx.get_rowsets_version(10));
}

TEST(OlapScanLazyMaterializationContextTest, ScanNodeSetOnlyOnce) {
    OlapScanLazyMaterializationContext ctx;
    TOlapScanNode first;
    first.__set_schema_id(111);
    TOlapScanNode second;
    second.__set_schema_id(222);

    ctx.set_scan_node(first);
    ctx.set_scan_node(second);

    EXPECT_EQ(111, ctx.scan_node().schema_id);
}

TEST(LookUpTabletAdaptorFactoryTest, CreateByType) {
    auto olap_adaptor = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::OLAP_SCAN);
    ASSERT_TRUE(olap_adaptor.ok());
    ASSERT_NE(nullptr, olap_adaptor.value().get());

    auto lake_adaptor = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::LAKE_SCAN);
    ASSERT_TRUE(lake_adaptor.ok());
    ASSERT_NE(nullptr, lake_adaptor.value().get());
}

TEST(LookUpTabletAdaptorFactoryTest, UnsupportedType) {
    auto adaptor = create_look_up_tablet_adaptor(static_cast<RowPositionDescriptor::Type>(-1));
    ASSERT_FALSE(adaptor.ok());
    EXPECT_TRUE(adaptor.status().is_not_supported());
}

class OlapScanTabletAdaptorTest : public testing::Test {
public:
    void SetUp() override {
        _tablet_id = rand();
        _schema_hash = rand();
        _tablet = create_test_tablet(_tablet_id, _schema_hash);
        ASSERT_NE(nullptr, _tablet.get());
    }

    void TearDown() override {}

protected:
    int64_t _tablet_id = 0;
    int32_t _schema_hash = 0;
    TabletSharedPtr _tablet;
};

TEST_F(OlapScanTabletAdaptorTest, InitReadColumnsInvalidField) {
    OlapScanLazyMaterializationContext ctx;
    TOlapScanNode scan_node;
    ctx.set_scan_node(scan_node);

    auto adaptor_or = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::OLAP_SCAN);
    ASSERT_TRUE(adaptor_or.ok());
    auto adaptor = std::move(adaptor_or.value());
    ASSERT_TRUE(adaptor->capture(&ctx).ok());
    ASSERT_TRUE(adaptor->init(_tablet_id).ok());
    ASSERT_TRUE(adaptor->init_schema(nullptr).ok());

    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    ObjectPool pool;
    auto slots = create_slots(&state, &pool, {"missing_col"});
    ASSERT_FALSE(slots.empty());
    auto status = adaptor->init_read_columns(slots);
    ASSERT_TRUE(status.is_internal_error());
}

TEST_F(OlapScanTabletAdaptorTest, InitGlobalDictsInvalidField) {
    OlapScanLazyMaterializationContext ctx;
    TOlapScanNode scan_node;
    ctx.set_scan_node(scan_node);

    auto adaptor_or = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::OLAP_SCAN);
    ASSERT_TRUE(adaptor_or.ok());
    auto adaptor = std::move(adaptor_or.value());
    ASSERT_TRUE(adaptor->capture(&ctx).ok());
    ASSERT_TRUE(adaptor->init(_tablet_id).ok());
    ASSERT_TRUE(adaptor->init_schema(nullptr).ok());

    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    FragmentDictState dict_state;
    state.set_fragment_dict_state(&dict_state);
    ObjectPool pool;
    auto slots = create_slots(&state, &pool, {"missing_col"});
    ASSERT_FALSE(slots.empty());
    auto status = adaptor->init_global_dicts(&state, &pool, slots);
    ASSERT_TRUE(status.is_internal_error());
}

TEST_F(OlapScanTabletAdaptorTest, GetIteratorMissingRowset) {
    OlapScanLazyMaterializationContext ctx;
    TOlapScanNode scan_node;
    ctx.set_scan_node(scan_node);

    auto adaptor_or = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::OLAP_SCAN);
    ASSERT_TRUE(adaptor_or.ok());
    auto adaptor = std::move(adaptor_or.value());
    ASSERT_TRUE(adaptor->capture(&ctx).ok());
    ASSERT_TRUE(adaptor->init(_tablet_id).ok());

    SparseRange<rowid_t> rowids;
    auto iter_or = adaptor->get_iterator(0, std::move(rowids));
    ASSERT_FALSE(iter_or.ok());
    ASSERT_TRUE(iter_or.status().is_internal_error());
}

TEST(PositionDesctiporTest, test) {
    auto pool = new ObjectPool();
    {
        TRowPositionDescriptor tdesc;
        tdesc.row_position_type = TRowPositionType::LAKE_ROW_POSITION;
        RowPositionDescriptor::from_thrift(tdesc, pool);
    }
}

TEST(LakeScanTabletAdaptorTest, InvalidRssid) {
    LakeScanLazyMaterializationContext ctx;
    TLakeScanNode scan_node;
    ctx.set_scan_node(scan_node);

    auto adaptor_or = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::LAKE_SCAN);
    ASSERT_TRUE(adaptor_or.ok());
    auto adaptor = std::move(adaptor_or.value());
    ASSERT_TRUE(adaptor->capture(&ctx).ok());
    ASSERT_TRUE(adaptor->init(123).ok());

    SparseRange<rowid_t> rowids;
    auto iter_or = adaptor->get_iterator(-1, std::move(rowids));
    ASSERT_FALSE(iter_or.ok());
    ASSERT_TRUE(iter_or.status().is_internal_error());
}

// The GLM lookup re-reads columns of a tablet a Lake scan already scanned, so it must apply the cache
// policy that scan resolved for that tablet -- both when loading segments and when reading rowsets.
// Before the fix the adaptor only kept the query-level page-cache setting and loaded segments with a
// hardcoded `segments(true)`, so fill_data_cache / skip_page_cache / skip_disk_cache from the scan
// range were silently ignored and the lookup could populate caches the query explicitly disabled.
//
// The assertions below read the SegmentReadOptions that the lookup's RowsetReadOptions produce, via the
// `Rowset::read::seg_options` sync point at the end of the chain
// (LakeScanCacheOptions -> LakeIOOptions -> RowsetReadOptions -> SegmentReadOptions).
class LakeScanTabletAdaptorCacheTest : public testing::Test {
public:
    void SetUp() override {
        _tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
        ASSERT_NE(nullptr, _tablet_mgr);
        _location_provider = std::make_shared<lake::FixedLocationProvider>(kRootLocation);
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        for (const std::string dir :
             {lake::kSegmentDirectoryName, lake::kMetadataDirectoryName, lake::kTxnLogDirectoryName}) {
            ASSERT_OK(FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, dir)));
        }

        _tablet_id = next_id();
        TabletMetadata metadata;
        metadata.set_id(_tablet_id);
        metadata.set_version(1);
        auto* schema = metadata.mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto* c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = schema->add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(metadata));

        // One rowset with one real segment, so get_iterator() actually loads a footer and reads.
        auto tablet_schema = TabletSchema::create(*schema);
        auto chunk_schema = std::make_shared<starrocks::Schema>(ChunkHelper::convert_schema(tablet_schema));
        std::vector<int> keys{1, 2, 3, 4, 5};
        std::vector<int> values{10, 20, 30, 40, 50};
        auto key_column = Int32Column::create();
        auto value_column = Int32Column::create();
        key_column->append_numbers(keys.data(), keys.size() * sizeof(int));
        value_column->append_numbers(values.data(), values.size() * sizeof(int));
        Chunk chunk({std::move(key_column), std::move(value_column)}, chunk_schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(lake::kHorizontal, next_id()));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
        ASSERT_EQ(1, static_cast<int>(writer->segments().size()));
        auto* rowset = metadata.add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(kRowsetId);
        rowset->set_num_rows(static_cast<int64_t>(keys.size()));
        for (const auto& file : writer->segments()) {
            rowset->add_segment_metas()->set_filename(file.path);
        }
        writer->close();

        metadata.set_version(kVersion);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(metadata));
    }

    void TearDown() override {
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kRootLocation);
    }

protected:
    struct ObservedOptions {
        bool seen = false;
        bool use_page_cache = false;
        LakeIOOptions lake_io_opts;
    };

    // Drive the lookup adaptor over the single captured segment and report the read options it built.
    ObservedOptions run_lookup(const LakeScanCacheOptions& cache_options, bool query_use_page_cache) {
        // starrocks_test shares one process: pin the global so a sibling test cannot flip the expectation.
        const bool saved_disable_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = false;
        DeferOp restore_config([&]() { config::disable_storage_page_cache = saved_disable_page_cache; });

        LakeScanLazyMaterializationContext ctx;
        TLakeScanNode scan_node;
        ctx.set_scan_node(scan_node);

        auto tablet = _tablet_mgr->get_tablet(_tablet_id, kVersion);
        CHECK(tablet.ok()) << tablet.status();
        std::vector<BaseRowsetSharedPtr> rowsets;
        for (auto& rs : tablet->get_rowsets()) {
            rowsets.emplace_back(rs);
        }
        CHECK_EQ(1, static_cast<int>(rowsets.size()));
        ctx.capture_rowsets(static_cast<int32_t>(_tablet_id), kVersion, rowsets, cache_options);

        TQueryOptions query_options;
        query_options.__set_use_page_cache(query_use_page_cache);
        RuntimeState state(TUniqueId(), query_options, TQueryGlobals(), nullptr);
        FragmentDictState dict_state;
        state.set_fragment_dict_state(&dict_state);
        ObjectPool pool;
        auto slots = create_slots(&state, &pool, {"c0"});
        CHECK(!slots.empty());

        auto adaptor_or = create_look_up_tablet_adaptor(RowPositionDescriptor::Type::LAKE_SCAN);
        CHECK(adaptor_or.ok());
        auto adaptor = std::move(adaptor_or.value());
        CHECK_OK(adaptor->capture(&ctx));
        CHECK_OK(adaptor->init(_tablet_id));
        CHECK_OK(adaptor->init_schema(&state));
        CHECK_OK(adaptor->init_access_path(&state, &pool));
        CHECK_OK(adaptor->init_global_dicts(&state, &pool, slots));
        CHECK_OK(adaptor->init_read_columns(slots));

        ObservedOptions observed;
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp sync_point_guard([]() {
            SyncPoint::GetInstance()->ClearAllCallBacks();
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("Rowset::read::seg_options", [&](void* arg) {
            const auto* seg_options = static_cast<const SegmentReadOptions*>(arg);
            observed.seen = true;
            observed.use_page_cache = seg_options->use_page_cache;
            observed.lake_io_opts = seg_options->lake_io_opts;
        });

        SparseRange<rowid_t> row_id_range;
        row_id_range.add(Range<rowid_t>(0, 1));
        // rssid == rowset id + segment_idx; the single segment has segment_idx 0.
        auto iter_or = adaptor->get_iterator(kRowsetId, std::move(row_id_range));
        CHECK(iter_or.ok()) << iter_or.status();
        CHECK(iter_or.value() != nullptr);
        iter_or.value()->close();
        return observed;
    }

    constexpr static const char* const kRootLocation = "./lake_scan_tablet_adaptor_cache_test";
    constexpr static int64_t kVersion = 2;
    constexpr static uint32_t kRowsetId = 1;

    lake::TabletManager* _tablet_mgr = nullptr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    int64_t _tablet_id = 0;
};

// A permissive scan range: everything the scan enabled must reach the lookup's reads.
TEST_F(LakeScanTabletAdaptorCacheTest, PermissivePolicyIsHonored) {
    auto observed = run_lookup(
            {.use_page_cache = true, .fill_data_cache = true, .fill_metadata_cache = true, .skip_disk_cache = false},
            /*query_use_page_cache=*/true);
    ASSERT_TRUE(observed.seen);
    EXPECT_TRUE(observed.use_page_cache);
    EXPECT_TRUE(observed.lake_io_opts.fill_data_cache);
    EXPECT_TRUE(observed.lake_io_opts.fill_metadata_cache);
    EXPECT_FALSE(observed.lake_io_opts.skip_disk_cache);
}

// The scan range said "skip the page cache" (and don't fill either cache): the lookup must obey even
// though the query-level page-cache setting is still on. Pre-fix this read came back with
// use_page_cache=true and fill_data_cache=true (from the hardcoded `segments(true)`).
TEST_F(LakeScanTabletAdaptorCacheTest, RestrictiveScanRangePolicyIsHonored) {
    auto observed = run_lookup(
            {.use_page_cache = false, .fill_data_cache = false, .fill_metadata_cache = false, .skip_disk_cache = true},
            /*query_use_page_cache=*/true);
    ASSERT_TRUE(observed.seen);
    EXPECT_FALSE(observed.use_page_cache);
    EXPECT_FALSE(observed.lake_io_opts.fill_data_cache);
    EXPECT_FALSE(observed.lake_io_opts.fill_metadata_cache);
    EXPECT_TRUE(observed.lake_io_opts.skip_disk_cache);
}

// The two page-cache inputs are ANDed: a query that turned the page cache off keeps it off even when
// the captured scan-range policy allows it. Data/disk cache flags come from the scan range alone.
TEST_F(LakeScanTabletAdaptorCacheTest, QueryLevelPageCacheOffWins) {
    auto observed = run_lookup(
            {.use_page_cache = true, .fill_data_cache = true, .fill_metadata_cache = true, .skip_disk_cache = false},
            /*query_use_page_cache=*/false);
    ASSERT_TRUE(observed.seen);
    EXPECT_FALSE(observed.use_page_cache);
    EXPECT_TRUE(observed.lake_io_opts.fill_data_cache);
}

} // namespace starrocks
