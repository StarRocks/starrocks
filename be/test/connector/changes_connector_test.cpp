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

#include "connector/changes_connector.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::connector {

namespace {

constexpr const char* kRootLocation = "test_changes_connector";
constexpr const char* kChangeTypeColumnName = "__CHANGE_TYPE__";
constexpr const char* kRowVersionColumnName = "__ROW_VERSION__";

// End-to-end test for the public ChangesConnector surface
// (create_data_source -> open -> get_next -> close). Backed by a real
// lake::TabletManager over an on-disk FixedLocationProvider; each test
// publishes only the TabletMetadata its scenario needs.
class ChangesConnectorTest : public ::testing::Test {
public:
    ChangesConnectorTest()
            : _tablet_mgr(StorageEnv::GetInstance()->lake_tablet_manager()),
              _location_provider(std::make_shared<lake::FixedLocationProvider>(kRootLocation)) {}

    void SetUp() override {
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));
        _runtime_state = lake::create_runtime_state();
        _fragment_ctx = _runtime_state->obj_pool()->add(new pipeline::FragmentContext());
        _runtime_state->set_fragment_ctx(_fragment_ctx, &_fragment_ctx->fragment_runtime_state());
        _runtime_state->set_fragment_dict_state(_fragment_ctx->dict_state());
    }

    void TearDown() override {
        (void)fs::remove_all(kRootLocation);
        if (_backup_location_provider != nullptr) {
            (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        }
    }

protected:
    // -------------------------------------------------------------------
    // TupleDescriptor builder
    // -------------------------------------------------------------------

    enum class TupleShape {
        DATA_ONLY,
        CHANGE_TYPE_ONLY,
        ROW_VERSION_ONLY,
        BOTH_NON_NULLABLE,
        BOTH_NULLABLE,
    };

    // Replace the runtime state's DescriptorTbl with a fresh table holding a
    // single tuple (id = 0). When `include_data` is true the tuple contains an
    // INT data column "c0" plus optional CHANGES metadata slots controlled by
    // `shape`; when false, the data column is skipped so the metadata slots
    // sit on their own (mirroring queries that project only metadata columns).
    // Returns 0 (the tuple id).
    TTupleId install_tuple_descriptor(TupleShape shape, bool include_data = true) {
        TDescriptorTableBuilder tbl_builder;
        TTupleDescriptorBuilder tup;
        int col_pos = 0;
        if (include_data) {
            tup.add_slot(TSlotDescriptorBuilder()
                                 .type(TYPE_INT)
                                 .column_name("c0")
                                 .column_pos(col_pos++)
                                 .nullable(false)
                                 .build());
        }
        const bool include_ct = (shape == TupleShape::CHANGE_TYPE_ONLY || shape == TupleShape::BOTH_NON_NULLABLE ||
                                 shape == TupleShape::BOTH_NULLABLE);
        const bool include_rv = (shape == TupleShape::ROW_VERSION_ONLY || shape == TupleShape::BOTH_NON_NULLABLE ||
                                 shape == TupleShape::BOTH_NULLABLE);
        const bool meta_nullable = (shape == TupleShape::BOTH_NULLABLE);
        if (include_ct) {
            tup.add_slot(TSlotDescriptorBuilder()
                                 .type(TYPE_TINYINT)
                                 .column_name(kChangeTypeColumnName)
                                 .column_pos(col_pos++)
                                 .nullable(meta_nullable)
                                 .build());
        }
        if (include_rv) {
            tup.add_slot(TSlotDescriptorBuilder()
                                 .type(TYPE_BIGINT)
                                 .column_name(kRowVersionColumnName)
                                 .column_pos(col_pos++)
                                 .nullable(meta_nullable)
                                 .build());
        }
        tup.build(&tbl_builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK_OK(DescriptorTbl::create(_runtime_state.get(), _runtime_state->obj_pool(), tbl_builder.desc_tbl(),
                                       &desc_tbl, config::vector_chunk_size));
        _runtime_state->set_desc_tbl(desc_tbl);
        return 0;
    }

    // Locate a slot id in the currently-installed tuple descriptor by
    // column name. Returns -1 when no such slot exists.
    SlotId slot_id_of(TTupleId tuple_id, const std::string& col_name) const {
        const auto* td = _runtime_state->desc_tbl().get_tuple_descriptor(tuple_id);
        if (td == nullptr) return -1;
        for (auto* slot : td->slots()) {
            if (slot->col_name() == col_name) return slot->id();
        }
        return -1;
    }

    // -------------------------------------------------------------------
    // TPlanNode / TScanRange / Provider builders
    // -------------------------------------------------------------------

    // Construct one TChangesMetaDescriptor with the supplied kind / name /
    // nullability. `type` is derived from kind purely to keep the wire
    // payload self-describing; BE stamping itself looks only at kind.
    static TChangesMetaDescriptor make_meta_descriptor(TChangesMetaKind::type kind, const std::string& name,
                                                       bool is_nullable) {
        TChangesMetaDescriptor d;
        d.__set_kind(kind);
        d.__set_name(name);
        d.__set_is_nullable(is_nullable);
        TTypeDesc type;
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar;
        scalar.__set_type(kind == TChangesMetaKind::CHANGE_TYPE ? TPrimitiveType::TINYINT : TPrimitiveType::BIGINT);
        node.__set_scalar_type(scalar);
        type.types.push_back(node);
        d.__set_type(type);
        return d;
    }

    // Build a TPlanNode carrying both metadata kinds with their default names,
    // matching the production shape for a relation with no name conflicts.
    // Tests that need conflict scenarios call make_plan_node_with_descriptors
    // directly with custom names.
    TPlanNode make_plan_node(TTupleId tuple_id, int64_t schema_id) {
        std::vector<TChangesMetaDescriptor> descriptors = {
                make_meta_descriptor(TChangesMetaKind::CHANGE_TYPE, kChangeTypeColumnName, true),
                make_meta_descriptor(TChangesMetaKind::ROW_VERSION, kRowVersionColumnName, true)};
        return make_plan_node_with_descriptors(tuple_id, schema_id, descriptors);
    }

    TPlanNode make_plan_node_with_descriptors(TTupleId tuple_id, int64_t schema_id,
                                              const std::vector<TChangesMetaDescriptor>& meta_descriptors) {
        TPlanNode tn;
        TChangesScanNode csn;
        csn.__set_tuple_id(tuple_id);
        TTableSchemaKey key;
        key.__set_db_id(1);
        key.__set_table_id(2);
        key.__set_schema_id(schema_id);
        csn.__set_schema_key(key);
        if (!meta_descriptors.empty()) {
            csn.__set_meta_descriptors(meta_descriptors);
        }
        tn.__set_changes_scan_node(csn);
        return tn;
    }

    TScanRange make_scan_range(int64_t tablet_id, int64_t base_version, int64_t head_version) {
        TChangesScanRange r;
        r.__set_tablet_id(tablet_id);
        r.__set_base_version(base_version);
        r.__set_head_version(head_version);
        TScanRange sr;
        sr.__set_changes_scan_range(r);
        return sr;
    }

    std::unique_ptr<ChangesDataSourceProvider> make_provider(TTupleId tuple_id, int64_t schema_id) {
        return std::make_unique<ChangesDataSourceProvider>(/*scan_node=*/nullptr, make_plan_node(tuple_id, schema_id));
    }

    // -------------------------------------------------------------------
    // Tablet bootstrap + metadata publishing
    // -------------------------------------------------------------------

    // Description of a rowset to attach to a TabletMetadata snapshot. When
    // `num_rows > 0` and `segment_path` is empty, publish_metadata() writes
    // a real segment on disk and fills in segment_path so downstream
    // snapshots can reuse the path (modeling cross-version shared segments).
    struct RowsetSpec {
        int64_t version = 0;
        uint32_t id = 0;
        int64_t num_rows = 0;
        std::string segment_path;
        bool delete_predicate = false;
        bool max_compact_input = false;
    };

    // Bootstrap an empty TabletMetadata at v=1 so the tablet is registered
    // with the TabletManager and subsequent rowset writes can find it.
    void initialize_tablet(int64_t tablet_id, int64_t schema_id) {
        auto meta = std::make_shared<TabletMetadata>();
        meta->set_id(tablet_id);
        meta->set_version(1);
        set_default_schema(meta.get(), schema_id);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*meta));
    }

    // Install a single INT data column "c0" plus the supplied schema id so
    // TableSchemaService::_get_local_schema resolves the schema via the
    // tablet metadata fast path (no FE RPC required).
    void set_default_schema(TabletMetadata* meta, int64_t schema_id) {
        auto* schema = meta->mutable_schema();
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto* c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }

    // Write a single-segment rowset on disk for `tablet_id`. Stores the
    // resulting segment path through `out_path`.
    void write_segment(int64_t tablet_id, const std::shared_ptr<TabletSchema>& tablet_schema, int64_t num_rows,
                       std::string* out_path) {
        auto data_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto c0 = Int32Column::create();
        std::vector<int32_t> values;
        values.reserve(static_cast<size_t>(num_rows));
        for (int64_t i = 0; i < num_rows; i++) {
            values.push_back(static_cast<int32_t>(i));
        }
        c0->append_numbers(values.data(), values.size() * sizeof(int32_t));
        Chunk chunk({std::move(c0)}, data_schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(lake::kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
        const auto& files = writer->segments();
        ASSERT_EQ(1u, files.size());
        *out_path = files.front().path;
        writer->close();
    }

    // Publish a TabletMetadata snapshot at the given version. Mutates
    // *rowsets so callers can chain segment_path values across versions.
    void publish_metadata(int64_t tablet_id, int64_t version, int64_t schema_id, const std::vector<int64_t>& ancestors,
                          std::vector<RowsetSpec>* rowsets) {
        auto meta = std::make_shared<TabletMetadata>();
        meta->set_id(tablet_id);
        meta->set_version(version);
        for (int64_t a : ancestors) {
            meta->add_metadata_ancestors(a);
        }
        set_default_schema(meta.get(), schema_id);
        auto tablet_schema = TabletSchema::create(meta->schema());

        if (rowsets != nullptr) {
            for (auto& spec : *rowsets) {
                if (spec.num_rows > 0 && spec.segment_path.empty()) {
                    write_segment(tablet_id, tablet_schema, spec.num_rows, &spec.segment_path);
                }
                auto* rmeta = meta->add_rowsets();
                rmeta->set_id(spec.id);
                rmeta->set_version(spec.version);
                rmeta->set_overlapped(false);
                rmeta->set_num_rows(spec.num_rows);
                if (spec.delete_predicate) {
                    rmeta->mutable_delete_predicate()->set_version(static_cast<int32_t>(spec.version));
                }
                if (spec.max_compact_input) {
                    rmeta->set_max_compact_input_rowset_id(spec.id);
                }
                if (!spec.segment_path.empty()) {
                    rmeta->add_segment_metas()->set_filename(spec.segment_path);
                }
            }
        }
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*meta));
    }

    // -------------------------------------------------------------------
    // get_next() pumping
    // -------------------------------------------------------------------

    // Drain `ds` to EOF. Returns total rows surfaced. When `chunks_out` is
    // non-null, surfaced chunks are appended so callers can inspect
    // stamping columns.
    int64_t drain(DataSource* ds, std::vector<ChunkPtr>* chunks_out = nullptr) {
        int64_t total = 0;
        while (true) {
            ChunkPtr chunk;
            Status st = ds->get_next(_runtime_state.get(), &chunk);
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            if (chunk == nullptr) break;
            total += chunk->num_rows();
            if (chunks_out != nullptr) {
                chunks_out->push_back(std::move(chunk));
            }
        }
        return total;
    }

protected:
    lake::TabletManager* _tablet_mgr = nullptr;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    std::shared_ptr<RuntimeState> _runtime_state;
    pipeline::FragmentContext* _fragment_ctx = nullptr;
};

} // namespace

// ============================================================================
// Test 1 — Connector + DataSourceProvider + small DataSource accessor shells.
// ============================================================================

TEST_F(ChangesConnectorTest, test_provider_and_connector_shells) {
    ChangesConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::CHANGES);

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    TPlanNode plan_node = make_plan_node(tuple_id, schema_id);

    auto provider_ptr = connector.create_data_source_provider(/*scan_node=*/nullptr, plan_node);
    ASSERT_NE(nullptr, provider_ptr);
    EXPECT_FALSE(provider_ptr->insert_local_exchange_operator());
    EXPECT_TRUE(provider_ptr->accept_empty_scan_ranges());

    const auto* tuple_desc = provider_ptr->tuple_descriptor(_runtime_state.get());
    ASSERT_NE(nullptr, tuple_desc);
    EXPECT_EQ(tuple_id, tuple_desc->id());

    int64_t tablet_id = next_id();
    auto ds_ptr = provider_ptr->create_data_source(make_scan_range(tablet_id, 0, 0));
    ASSERT_NE(nullptr, ds_ptr);
    EXPECT_EQ("ChangesDataSource", ds_ptr->name());

    EXPECT_OK(ds_ptr->parse_runtime_filters(_runtime_state.get()));

    EXPECT_EQ(0, ds_ptr->raw_rows_read());
    EXPECT_EQ(0, ds_ptr->num_rows_read());
    EXPECT_EQ(0, ds_ptr->num_bytes_read());
    EXPECT_EQ(0, ds_ptr->cpu_time_spent());

    // close() before open() and close() called twice must both be safe.
    ds_ptr->close(_runtime_state.get());
    ds_ptr->close(_runtime_state.get());
}

// ============================================================================
// Test 2 — Open()-time error paths. Each sub-case provisions the real input
// that surfaces that error in production.
// ============================================================================

TEST_F(ChangesConnectorTest, test_open_error_paths) {
    // Sub-case A: requested tuple_id missing from the descriptor table.
    {
        install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        constexpr TTupleId kMissingTupleId = 999;
        auto provider = make_provider(kMissingTupleId, schema_id);

        int64_t tablet_id = next_id();
        auto ds = provider->create_data_source(make_scan_range(tablet_id, 0, 0));
        Status st = ds->open(_runtime_state.get());
        ASSERT_FALSE(st.ok());
        EXPECT_TRUE(st.is_internal_error());
        EXPECT_NE(std::string::npos, std::string(st.message()).find("tuple descriptor"));
        ds->close(_runtime_state.get());
    }

    // Sub-case B: base > head; open() must surface InvalidArgument.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, /*rowsets=*/nullptr);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/5, /*head=*/3));
        Status st = ds->open(_runtime_state.get());
        ASSERT_FALSE(st.ok());
        EXPECT_TRUE(st.is_invalid_argument());
        EXPECT_NE(std::string::npos, std::string(st.message()).find("CHANGES version range invalid"));
        ds->close(_runtime_state.get());
    }

    // Sub-case C: in-range rowset with delete_predicate; open() surfaces
    // NotSupported. The rowset carries no segments because open()
    // short-circuits before any read.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> rowsets = {{.version = 2, .id = 100, .num_rows = 0, .delete_predicate = true}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &rowsets);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        Status st = ds->open(_runtime_state.get());
        ASSERT_FALSE(st.ok());
        EXPECT_TRUE(st.is_not_supported());
        EXPECT_NE(std::string::npos, std::string(st.message()).find("DELETE_PREDICATE_FOUND"));
        ds->close(_runtime_state.get());
    }

    // Sub-case D: tuple descriptor advertises a data slot whose column name
    // is absent from the tablet schema. _init_read_schema must fail fast
    // with InternalError("invalid field name: ...") so the slot id -> chunk
    // column index map is never built over a partial projection.
    {
        TDescriptorTableBuilder tbl_builder;
        TTupleDescriptorBuilder tup;
        tup.add_slot(TSlotDescriptorBuilder()
                             .type(TYPE_INT)
                             .column_name("missing_col")
                             .column_pos(0)
                             .nullable(false)
                             .build());
        tup.build(&tbl_builder);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK_OK(DescriptorTbl::create(_runtime_state.get(), _runtime_state->obj_pool(), tbl_builder.desc_tbl(),
                                       &desc_tbl, config::vector_chunk_size));
        _runtime_state->set_desc_tbl(desc_tbl);

        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, /*rowsets=*/nullptr);

        auto provider = make_provider(/*tuple_id=*/0, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        Status st = ds->open(_runtime_state.get());
        ASSERT_FALSE(st.ok());
        EXPECT_TRUE(st.is_internal_error());
        EXPECT_NE(std::string::npos, std::string(st.message()).find("invalid field name"));
        EXPECT_NE(std::string::npos, std::string(st.message()).find("missing_col"));
        ds->close(_runtime_state.get());
    }
}

// ============================================================================
// Test 3 — Metadata traversal: each sub-case crafts a TabletMetadata chain
// whose surfaced row count witnesses which rowsets the traversal admitted
// for reading.
// ============================================================================

TEST_F(ChangesConnectorTest, test_metadata_traversal_scenarios) {
    auto open_and_drain = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id, int64_t base,
                              int64_t head) -> int64_t {
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, base, head));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        return total;
    };

    // Sub-case A: base == head triggers the early return in
    // _do_metadata_traversal; no rowsets surface for reading.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, /*rowsets=*/nullptr);
        EXPECT_EQ(0, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/3));
    }

    // Sub-case B: head with no metadata_ancestors and a single in-range
    // rowset. Loop terminates via metadata_ancestors_size() == 0.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);
        EXPECT_EQ(5, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/2));
    }

    // Sub-case C: ancestors present but all <= base; loop exits via
    // versions_to_read.empty() and only head rowsets surface.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 200, .num_rows = 7}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);
        std::vector<RowsetSpec> r4 = {{.version = 2, .id = 200, .num_rows = 7, .segment_path = r2[0].segment_path},
                                      {.version = 4, .id = 201, .num_rows = 3}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{2}, &r4);
        // base=3 filters id=200 (v=2); only id=201 (v=4) qualifies = 3 rows.
        EXPECT_EQ(3, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/4));
    }

    // Sub-case D: multi-level ancestor walk (v=5 -> v=4 -> v=3, base=2).
    // Every rowset surfaced through v=5's snapshot.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 300, .num_rows = 3}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, &r3);
        std::vector<RowsetSpec> r4 = {{.version = 3, .id = 300, .num_rows = 3, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 301, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);
        std::vector<RowsetSpec> r5 = {{.version = 3, .id = 300, .num_rows = 3, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 301, .num_rows = 4, .segment_path = r4[1].segment_path},
                                      {.version = 5, .id = 302, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/5, schema_id, /*ancestors=*/{4}, &r5);
        EXPECT_EQ(3 + 4 + 5, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/5));
    }

    // Sub-case E: shared rowset id across head and ancestor; seen_rowset_ids
    // dedup keeps only one copy in _changes_rowsets so the row counter does
    // not double-count.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 100, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, &r3);
        std::vector<RowsetSpec> r4 = {{.version = 3, .id = 100, .num_rows = 5, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 101, .num_rows = 2}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);
        // Without dedup, id=100 would be added twice and total would be 5+5+2=12.
        EXPECT_EQ(5 + 2, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/4));
    }

    // Sub-case F: rowset with version <= base is filtered out by
    // _scan_metadata_for_changes_rowsets.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r5 = {{.version = 2, .id = 500, .num_rows = 8}, // version <= base, filtered
                                      {.version = 5, .id = 501, .num_rows = 6}};
        publish_metadata(tablet_id, /*version=*/5, schema_id, /*ancestors=*/{}, &r5);
        EXPECT_EQ(6, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/5));
    }

    // Sub-case G: rowset with max_compact_input_rowset_id is filtered.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 700, .num_rows = 9, .max_compact_input = true},
                                      {.version = 3, .id = 701, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, &r3);
        EXPECT_EQ(4, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/3));
    }
}

// ============================================================================
// Test 4 — ChangesMetaAppendingIterator under each TupleShape: chunk shape (column
// count), column types, slot-id resolution, and stamping values.
// ============================================================================

TEST_F(ChangesConnectorTest, test_chunk_stamping_with_slot_variants) {
    constexpr int64_t kRowsetVersion = 7;
    constexpr int64_t kNumRows = 4;

    auto open_and_collect = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id,
                                std::vector<ChunkPtr>* chunks_out) -> int64_t {
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> rowsets = {{.version = kRowsetVersion, .id = 1, .num_rows = kNumRows}};
        publish_metadata(tablet_id, /*version=*/kRowsetVersion, schema_id, /*ancestors=*/{}, &rowsets);
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(
                make_scan_range(tablet_id, /*base=*/kRowsetVersion - 1, /*head=*/kRowsetVersion));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get(), chunks_out);
        ds->close(_runtime_state.get());
        return total;
    };

    // Sub-case A: both meta slots, non-nullable. Stamping appends an
    // Int8Column (CHANGE_TYPE=0) and an Int64Column (ROW_VERSION=v).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        ASSERT_NE(-1, ct_id);
        ASSERT_NE(-1, rv_id);

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        const auto& ct_col = chunks.front()->get_column_by_slot_id(ct_id);
        const auto& rv_col = chunks.front()->get_column_by_slot_id(rv_id);
        ASSERT_NE(nullptr, ct_col);
        ASSERT_NE(nullptr, rv_col);
        EXPECT_FALSE(ct_col->is_nullable());
        EXPECT_FALSE(rv_col->is_nullable());
        const auto* ct_data = down_cast<const Int8Column*>(ct_col.get());
        const auto* rv_data = down_cast<const Int64Column*>(rv_col.get());
        for (size_t i = 0; i < ct_data->size(); i++) {
            EXPECT_EQ(0, ct_data->get_data()[i]);
        }
        for (size_t i = 0; i < rv_data->size(); i++) {
            EXPECT_EQ(kRowsetVersion, rv_data->get_data()[i]);
        }
    }

    // Sub-case B: both meta slots, nullable. Stamping wraps columns in
    // NullableColumn with an all-zero null mask.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NULLABLE);
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        ASSERT_NE(-1, ct_id);
        ASSERT_NE(-1, rv_id);

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        const auto& ct_col = chunks.front()->get_column_by_slot_id(ct_id);
        const auto& rv_col = chunks.front()->get_column_by_slot_id(rv_id);
        ASSERT_TRUE(ct_col->is_nullable());
        ASSERT_TRUE(rv_col->is_nullable());
        const auto* ct_nullable = down_cast<const NullableColumn*>(ct_col.get());
        const auto* rv_nullable = down_cast<const NullableColumn*>(rv_col.get());
        EXPECT_EQ(0u, ct_nullable->null_count());
        EXPECT_EQ(0u, rv_nullable->null_count());
        const auto* ct_data = down_cast<const Int8Column*>(ct_nullable->data_column().get());
        const auto* rv_data = down_cast<const Int64Column*>(rv_nullable->data_column().get());
        for (size_t i = 0; i < ct_data->size(); i++) {
            EXPECT_EQ(0, ct_data->get_data()[i]);
        }
        for (size_t i = 0; i < rv_data->size(); i++) {
            EXPECT_EQ(kRowsetVersion, rv_data->get_data()[i]);
        }
    }

    // Sub-case C: __CHANGE_TYPE__ only; ROW_VERSION slot is absent.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::CHANGE_TYPE_ONLY);
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        ASSERT_NE(-1, ct_id);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kRowVersionColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        EXPECT_TRUE(chunks.front()->is_slot_exist(ct_id));
        const auto& ct_col = chunks.front()->get_column_by_slot_id(ct_id);
        const auto* ct_data = down_cast<const Int8Column*>(ct_col.get());
        for (size_t i = 0; i < ct_data->size(); i++) {
            EXPECT_EQ(0, ct_data->get_data()[i]);
        }
    }

    // Sub-case D: __ROW_VERSION__ only; CHANGE_TYPE slot is absent.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::ROW_VERSION_ONLY);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        ASSERT_NE(-1, rv_id);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kChangeTypeColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        EXPECT_TRUE(chunks.front()->is_slot_exist(rv_id));
        const auto& rv_col = chunks.front()->get_column_by_slot_id(rv_id);
        const auto* rv_data = down_cast<const Int64Column*>(rv_col.get());
        for (size_t i = 0; i < rv_data->size(); i++) {
            EXPECT_EQ(kRowsetVersion, rv_data->get_data()[i]);
        }
    }

    // Sub-case E: data column only. ChangesMetaAppendingIterator stays a passthrough.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::DATA_ONLY);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kChangeTypeColumnName));
        ASSERT_EQ(-1, slot_id_of(tuple_id, kRowVersionColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        // Only the data column "c0" is present; no metadata columns appended.
        EXPECT_EQ(1u, chunks.front()->num_columns());
    }

    // Sub-case F: only __ROW_VERSION__ in the tuple, no data slot (mirrors
    // SELECT __ROW_VERSION__ FROM t [_CHANGES_b_h_]). _init_read_schema must
    // force-include a tablet column to drive segment-iterator row count, else
    // every chunk would surface with num_rows() == 0 and the data source would
    // drop the whole rowset before stamping metadata.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::ROW_VERSION_ONLY, /*include_data=*/false);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        ASSERT_NE(-1, rv_id);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kChangeTypeColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        EXPECT_TRUE(chunks.front()->is_slot_exist(rv_id));
        const auto& rv_col = chunks.front()->get_column_by_slot_id(rv_id);
        const auto* rv_data = down_cast<const Int64Column*>(rv_col.get());
        for (size_t i = 0; i < rv_data->size(); i++) {
            EXPECT_EQ(kRowsetVersion, rv_data->get_data()[i]);
        }
    }

    // Sub-case G: only __CHANGE_TYPE__ in the tuple, no data slot. Same
    // row-count-driver concern as Sub-case F.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::CHANGE_TYPE_ONLY, /*include_data=*/false);
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        ASSERT_NE(-1, ct_id);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kRowVersionColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        EXPECT_TRUE(chunks.front()->is_slot_exist(ct_id));
        const auto& ct_col = chunks.front()->get_column_by_slot_id(ct_id);
        const auto* ct_data = down_cast<const Int8Column*>(ct_col.get());
        for (size_t i = 0; i < ct_data->size(); i++) {
            EXPECT_EQ(0, ct_data->get_data()[i]);
        }
    }
}

// ============================================================================
// Test 5 — Pump get_next() until EOF across multiple rowsets, verify final
// EndOfFile and the public counters.
// ============================================================================

TEST_F(ChangesConnectorTest, test_get_next_drains_to_eof) {
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 800, .num_rows = 6}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);
    std::vector<RowsetSpec> r3 = {{.version = 2, .id = 800, .num_rows = 6, .segment_path = r2[0].segment_path},
                                  {.version = 3, .id = 801, .num_rows = 4}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));

    int64_t total_rows = 0;
    int chunk_count = 0;
    int64_t total_bytes_seen = 0;
    while (true) {
        ChunkPtr chunk;
        Status st = ds->get_next(_runtime_state.get(), &chunk);
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        ASSERT_NE(nullptr, chunk);
        EXPECT_GT(chunk->num_rows(), 0u);
        total_rows += chunk->num_rows();
        total_bytes_seen += chunk->bytes_usage();
        chunk_count++;
    }
    EXPECT_EQ(6 + 4, total_rows);
    EXPECT_GT(chunk_count, 0);

    EXPECT_EQ(total_rows, ds->raw_rows_read());
    EXPECT_EQ(total_rows, ds->num_rows_read());
    EXPECT_EQ(total_bytes_seen, ds->num_bytes_read());
    EXPECT_EQ(0, ds->cpu_time_spent());

    // Subsequent get_next continues to surface EndOfFile.
    {
        ChunkPtr chunk;
        Status st = ds->get_next(_runtime_state.get(), &chunk);
        EXPECT_TRUE(st.is_end_of_file());
    }

    ds->close(_runtime_state.get());
}

// ============================================================================
// Test 6 — Post-read conjunct evaluation. ChangesDataSource::_read_next_chunk
// runs `_conjunct_ctxs` against the stamped chunk as a correctness backstop;
// each sub-case witnesses one branch (predicate keeps all rows / filters all
// rows).
// ============================================================================

TEST_F(ChangesConnectorTest, test_post_read_conjunct_filtering) {
    auto open_with_predicate_and_drain = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id, int64_t base,
                                             int64_t head, int32_t gt_value) -> int64_t {
        SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
        EXPECT_NE(-1, c0_slot_id);

        std::vector<TExpr> texprs;
        // Builds `c0 > gt_value`; matches the data column populated by write_segment.
        texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, gt_value));
        std::vector<ExprContext*> conjunct_ctxs;
        CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(),
                                                                &texprs, &conjunct_ctxs));

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, base, head));
        ds->set_predicates(conjunct_ctxs);
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
        return total;
    };

    // Sub-case A: predicate keeps every row (c0 > -1 on [0..3]).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 1, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);
        EXPECT_EQ(4, open_with_predicate_and_drain(tuple_id, schema_id, tablet_id,
                                                   /*base=*/1, /*head=*/2, /*gt_value=*/-1));
    }

    // Sub-case B: predicate filters every row (c0 > 999 on [0..3]).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 1, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);
        EXPECT_EQ(0, open_with_predicate_and_drain(tuple_id, schema_id, tablet_id,
                                                   /*base=*/1, /*head=*/2, /*gt_value=*/999));
    }
}

} // namespace starrocks::connector
