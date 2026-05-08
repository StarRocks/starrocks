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

#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "exec/pipeline/lookup/tablet_adaptor.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "runtime/runtime_state.h"
#include "storage/lake/rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
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

TEST(LakeScanLazyMaterializationContextTest, ScanNodeSetOnlyOnce) {
    LakeScanLazyMaterializationContext ctx;
    TLakeScanNode first;
    first.__set_next_uniq_id(7);
    TLakeScanNode second;
    second.__set_next_uniq_id(9);

    ctx.set_scan_node(first);
    ctx.set_scan_node(second);

    EXPECT_EQ(7, ctx.scan_node().next_uniq_id);
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

// Build a lake::RowsetPtr with a given number of segments.
// Mirrors make_test_rowset() but uses the lake::Rowset / BaseRowset hierarchy.
static lake::RowsetPtr make_lake_rowset(int num_segments, ObjectPool* pool) {
    static int64_t next_id = 10000;
    auto meta = pool->add(new RowsetMetadataPB());
    meta->set_num_rows(100);
    for (int i = 0; i < num_segments; ++i) {
        meta->add_segments("seg_" + std::to_string(i) + ".dat");
    }
    return std::make_shared<lake::Rowset>(nullptr, next_id++, std::move(meta), 0, nullptr);
}

static std::vector<BaseRowsetSharedPtr> as_base(const std::vector<lake::RowsetPtr>& rs) {
    std::vector<BaseRowsetSharedPtr> res;
    for (const auto& r : rs) {
        res.emplace_back(std::static_pointer_cast<BaseRowset>(r));
    }
    return res;
}

TEST(LakeScanLazyMaterializationContextTest, SetScanNode) {
    LakeScanLazyMaterializationContext ctx;
    TLakeScanNode scan_node;
    scan_node.__set_next_uniq_id(123);
    ctx.set_scan_node(scan_node);
    EXPECT_EQ(123, ctx.scan_node().next_uniq_id);
}

// Basic capture + version query.
TEST(LakeScanLazyMaterializationContextTest, CaptureRowsetsStoresVersion) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    auto rs = make_lake_rowset(2, &pool);
    ctx.capture_rowsets(100, /*version=*/42, as_base({rs}));
    EXPECT_EQ(42, ctx.get_rowsets_version(100));
}

// Multiple tablets stored independently.
TEST(LakeScanLazyMaterializationContextTest, VersionsIsolatedPerTablet) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    ctx.capture_rowsets(1, 10, as_base({make_lake_rowset(1, &pool)}));
    ctx.capture_rowsets(2, 20, as_base({make_lake_rowset(1, &pool)}));
    ctx.capture_rowsets(3, 30, as_base({make_lake_rowset(1, &pool)}));

    EXPECT_EQ(10, ctx.get_rowsets_version(1));
    EXPECT_EQ(20, ctx.get_rowsets_version(2));
    EXPECT_EQ(30, ctx.get_rowsets_version(3));
}

// drssid 0 → first rowset, segment 0.
TEST(LakeScanLazyMaterializationContextTest, GetRowsetFirstSegment) {
    LakeScanLazyMaterializationContext ctx;
    ObjectPool pool;
    auto rs0 = make_lake_rowset(2, &pool);
    ctx.capture_rowsets(100, 1, as_base({rs0}));

    int32_t seg_idx = -1;
    auto got = ctx.get_rowset(100, /*drssid=*/0, &seg_idx);
    ASSERT_NE(nullptr, got);
    EXPECT_EQ(rs0.get(), got.get());
    EXPECT_EQ(0, seg_idx);
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

} // namespace starrocks
