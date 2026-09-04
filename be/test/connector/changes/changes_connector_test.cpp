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

#include "connector/changes/changes_connector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "base/hash/crc32c.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/configbase.h"
#include "common/system/cpu_info.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/casts.h"
#include "platform/store_path.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/rowset/segment_writer.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::connector {

namespace {

constexpr const char* kRootLocation = "test_changes_connector";
constexpr const char* kChangeTypeColumnName = "__CHANGE_TYPE__";
constexpr const char* kRowVersionColumnName = "__ROW_VERSION__";

void expect_change_not_trackable(const Status& status, std::string_view expected_detail) {
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is_internal_error());
    const std::string message(status.message());
    EXPECT_EQ(0, message.find("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): "));
    EXPECT_NE(std::string::npos, message.find(expected_detail));
}

// End-to-end test for the public ChangesConnector surface
// (create_data_source -> open -> get_next -> close). Backed by a real
// lake::TabletManager over an on-disk FixedLocationProvider; each test
// publishes only the TabletMetadata its scenario needs.
class ChangesConnectorTest : public ::testing::Test {
public:
    ChangesConnectorTest()
            : _location_provider(std::make_shared<lake::FixedLocationProvider>(kRootLocation)),
              _update_mem_tracker(-1, "changes_connector_test") {}

    static void SetUpTestSuite() {
        ASSERT_TRUE(config::init(nullptr));
        CpuInfo::init();
    }

    void SetUp() override {
        _old_compact_threads = config::compact_threads;
        if (config::compact_threads <= 0) {
            config::compact_threads = 1;
        }

        CHECK_OK(_store_path_registry.init({StorePath(kRootLocation)}));
        StorageEnvOptions storage_env_options;
        storage_env_options.lake_location_provider_mode = LakeLocationProviderMode::kFixed;
        storage_env_options.store_path_registry = &_store_path_registry;
        storage_env_options.update_mem_tracker = &_update_mem_tracker;
        storage_env_options.lake_metadata_cache_limit = 1024 * 1024;
        CHECK_OK(StorageEnv::GetInstance()->init(storage_env_options));

        _tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
        CHECK(_tablet_mgr != nullptr);
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        (void)FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));

        reset_runtime_state();
    }

    void TearDown() override {
        if (_backup_location_provider != nullptr) {
            (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        }
        _runtime_state.reset();
        StorageEnv::GetInstance()->stop();
        StorageEnv::GetInstance()->stop_lake_tablet_manager();
        StorageEnv::GetInstance()->destroy();
        config::compact_threads = _old_compact_threads;
        (void)fs::remove_all(kRootLocation);
    }

protected:
    void reset_runtime_state(const TQueryOptions& query_options = TQueryOptions()) {
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<const QueryExecutionServices*>(nullptr), nullptr);
        auto* fragment_dict_state = _runtime_state->obj_pool()->add(new FragmentDictState());
        _runtime_state->set_fragment_dict_state(fragment_dict_state);
        TUniqueId query_id;
        _runtime_state->init_mem_trackers(query_id);
        _runtime_state->set_fragment_runtime_state(&_fragment_runtime_state);
    }

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
    TTupleId install_tuple_descriptor(TupleShape shape, bool include_data = true, bool c1_is_output = true,
                                      bool c0_is_output = true, bool change_type_is_output = true) {
        TDescriptorTableBuilder tbl_builder;
        TTupleDescriptorBuilder tup;
        int col_pos = 0;
        if (include_data) {
            tup.add_slot(TSlotDescriptorBuilder()
                                 .type(TYPE_INT)
                                 .column_name("c0")
                                 .column_pos(col_pos++)
                                 .nullable(false)
                                 .is_output_column(c0_is_output)
                                 .build());
            // Column-update tests project the value column c1 alongside the key
            // c0, so they can assert a DELETE row carries the before c1 value and
            // its paired INSERT row the new one. _with_c1 is opt-in: the default
            // single-key schema leaves it off, keeping the existing tests' tuple
            // shape unchanged.
            if (_with_c1) {
                tup.add_slot(TSlotDescriptorBuilder()
                                     .type(TYPE_INT)
                                     .column_name("c1")
                                     .column_pos(col_pos++)
                                     .nullable(false)
                                     .is_output_column(c1_is_output)
                                     .build());
            }
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
                                 .is_output_column(change_type_is_output)
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
    // payload self-describing; the BE looks only at kind when appending the column.
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
        TChangeScanSpec spec;
        spec.__set_derivation_mode(TChangeDerivationMode::VERSION_CHAIN_DIFF);
        spec.__set_base_version(base_version);
        spec.__set_head_version(head_version);
        TChangesScanRange r;
        r.__set_tablet_id(tablet_id);
        r.__set_scan_spec(spec);
        TScanRange sr;
        sr.__set_changes_scan_range(r);
        return sr;
    }

    // FULL_SCAN: read every row visible at head_version as an insert (no base, no ancestor walk).
    TScanRange make_full_scan_range(int64_t tablet_id, int64_t head_version) {
        TChangeScanSpec spec;
        spec.__set_derivation_mode(TChangeDerivationMode::FULL_SCAN);
        spec.__set_head_version(head_version);
        TChangesScanRange r;
        r.__set_tablet_id(tablet_id);
        r.__set_scan_spec(spec);
        TScanRange sr;
        sr.__set_changes_scan_range(r);
        return sr;
    }

    std::unique_ptr<ChangesDataSourceProvider> make_provider(TTupleId tuple_id, int64_t schema_id) {
        return std::make_unique<ChangesDataSourceProvider>(make_plan_node(tuple_id, schema_id));
    }

    // -------------------------------------------------------------------
    // Tablet bootstrap + metadata publishing
    // -------------------------------------------------------------------

    // Description of a rowset to attach to a TabletMetadata. When
    // `num_rows > 0` and `segment_path` is empty, publish_metadata() writes
    // a real segment on disk and fills in segment_path so downstream
    // metadata versions can reuse the path (modeling cross-version shared segments).
    struct RowsetSpec {
        int64_t version = 0;
        uint32_t id = 0;
        int64_t num_rows = 0;
        std::string segment_path;
        bool delete_predicate = false;
        bool max_compact_input = false;
        // c0 value of the first row in this rowset's single segment; row i holds
        // start_value + i. Distinct ranges per rowset let a test tell which
        // segment a surfaced row came from (e.g. before value vs after value).
        int32_t start_value = 0;
        // Rowids deleted in this rowset's own publish (PRIMARY KEYS only).
        // publish_metadata() writes a delete vector for the single segment and
        // records it under the metadata's version, so a CHANGES read at that
        // version drops these rows.
        std::vector<uint32_t> deleted_rows;
        // Explicit c1 (value column) values for the segment's rows, used only
        // when the fixture runs with _with_c1. Must have num_rows entries when
        // non-empty; when empty the writer fills c1 = start_value + rowid like c0.
        std::vector<int32_t> c1_values;
        // Explicit cX (middle value column) values, used only when the fixture
        // runs with _with_cx. Same length rule as c1_values.
        std::vector<int32_t> cX_values;
        // When non-empty, this rowset holds several segments with these per-segment
        // row counts (instead of the single segment of num_rows). Segment s holds c0 =
        // start_value + 1000*s + rowid so the segments are distinguishable. Lets a
        // DUP/AGG whole-rowset read exercise the path that must surface every segment;
        // delete vectors and delta column groups stay single-segment.
        std::vector<int64_t> segment_rows;
    };

    // KEYS type the helpers publish; switch to PRIMARY_KEYS for tests that
    // exercise the delete-vector-applying read path.
    KeysType _keys_type = DUP_KEYS;

    // When true, the published schema, segment writer, and tuple descriptor
    // carry a non-key value column c1 in addition to the key c0. The
    // column-update (delta column group) tests overlay c1 and assert its
    // before/after values, which a key-only schema cannot express.
    bool _with_c1 = false;

    // When true, the published schema and segment writer carry a second non-key
    // value column cX (unique id 5) positioned BEFORE c1, so a rowset written
    // under this schema places c1 at a later ordinal than a head schema that
    // omits cX. Lets a test model a light DROP COLUMN across a CHANGES range and
    // verify the read resolves columns against the scan schema rather than the
    // rowset's own historical schema.
    bool _with_cx = false;

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
        schema->set_keys_type(_keys_type);
        schema->set_num_rows_per_row_block(65535);
        auto* c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        if (_with_cx) {
            // Added before c1 so c1 sits at a later ordinal than in a head schema
            // that omits cX (models a light DROP COLUMN shifting c1's ordinal).
            auto* cx = schema->add_column();
            cx->set_unique_id(5);
            cx->set_name("cX");
            cx->set_type("INT");
            cx->set_is_key(false);
            cx->set_is_nullable(false);
            cx->set_aggregation(_keys_type == PRIMARY_KEYS ? "REPLACE" : "NONE");
        }
        if (_with_c1) {
            auto* c1 = schema->add_column();
            c1->set_unique_id(1);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            // A non-key column needs an aggregation method in the storage schema,
            // matching the production column shape per keys type: DUPLICATE KEYS uses
            // NONE, PRIMARY and AGGREGATE KEYS use REPLACE. The aggregation also gates
            // predicate pushdown on the column (OlapPredicateParser::can_pushdown pushes
            // a non-PK column's predicate only when its aggregation is NONE), so under
            // AGGREGATE KEYS a c1 predicate is not pushable and lands in the residual tree.
            c1->set_aggregation(_keys_type == DUP_KEYS ? "NONE" : "REPLACE");
        }
    }

    // Write delete vectors for several segments of one metadata version into a single
    // delvec file (one entry in version_to_file), each page at its own offset,
    // and record a per-segment page under *meta's version — matching
    // MetaFileBuilder's layout when one publish records delete bits for several
    // segments at once. A CHANGES read of any of these segments at *meta's
    // version then drops the listed rows.
    void attach_delvecs(TabletMetadata* meta, const std::vector<std::pair<uint32_t, std::vector<uint32_t>>>& entries) {
        if (entries.empty()) {
            return;
        }
        // Serialize every segment's delvec, concatenated into one buffer.
        std::string file_buf;
        struct PageLoc {
            uint32_t segment_id;
            uint64_t offset;
            size_t size;
            uint32_t crc32c;
        };
        std::vector<PageLoc> locs;
        for (const auto& [segment_id, deleted_rows] : entries) {
            DelVector delvec;
            std::vector<uint32_t> sorted = deleted_rows;
            std::sort(sorted.begin(), sorted.end());
            delvec.init(meta->version(), sorted.data(), sorted.size());
            std::string buf = delvec.save();
            uint64_t offset = file_buf.size();
            locs.push_back({segment_id, offset, buf.size(), crc32c::Mask(crc32c::Value(buf.data(), buf.size()))});
            file_buf.append(buf);
        }

        std::string name = lake::gen_delvec_filename(next_id());
        auto path = _tablet_mgr->delvec_location(meta->id(), name);
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(opts, path));
        ASSERT_OK(wf->append(Slice(file_buf.data(), file_buf.size())));
        ASSERT_OK(wf->close());

        auto* delvec_meta = meta->mutable_delvec_meta();
        auto& file = (*delvec_meta->mutable_version_to_file())[meta->version()];
        file.set_name(name);
        file.set_size(file_buf.size());
        for (const auto& loc : locs) {
            auto& page = (*delvec_meta->mutable_delvecs())[loc.segment_id];
            page.set_version(meta->version());
            page.set_offset(loc.offset);
            page.set_size(loc.size);
            page.set_crc32c(loc.crc32c);
            page.set_crc32c_gen_version(meta->version());
        }
    }

    // Which CDC capture map a page is recorded into. COLUMN_OVERLAY writes
    // column_overlay_vecs: a per-segment bitmap of the rowids this publish
    // column-updated, stored exactly like a delvec page.
    enum class CdcCaptureMap { COMPACTION_INPUT, COMPACTION_OUTPUT, COLUMN_OVERLAY };

    // Record per-rssid delete-vector pages into one of the three CDC capture
    // maps: compaction_input_delvecs (a segment merged away by this publish's
    // compaction), compaction_output_delvecs (a compaction output's own delete
    // bits at compaction time), or column_overlay_vecs (the rowids a column
    // update overlaid). The pages' bytes go into a fresh delvec file recorded
    // under a distinct version in version_to_file, matching production where the
    // page-based loader resolves a capture page's file by its own version().
    void attach_cdc_delvecs(TabletMetadata* meta, CdcCaptureMap which,
                            const std::vector<std::pair<uint32_t, std::vector<uint32_t>>>& entries) {
        if (entries.empty()) {
            return;
        }
        // A version key for these pages that does not collide with the
        // metadata's own delvecs file (recorded under meta->version()).
        int64_t page_version = meta->version() * 1000 + static_cast<int64_t>(which) + 1;

        std::string file_buf;
        struct PageLoc {
            uint32_t segment_id;
            uint64_t offset;
            size_t size;
            uint32_t crc32c;
        };
        std::vector<PageLoc> locs;
        for (const auto& [segment_id, deleted_rows] : entries) {
            DelVector delvec;
            std::vector<uint32_t> sorted = deleted_rows;
            std::sort(sorted.begin(), sorted.end());
            delvec.init(page_version, sorted.data(), sorted.size());
            std::string buf = delvec.save();
            uint64_t offset = file_buf.size();
            locs.push_back({segment_id, offset, buf.size(), crc32c::Mask(crc32c::Value(buf.data(), buf.size()))});
            file_buf.append(buf);
        }

        std::string name = lake::gen_delvec_filename(next_id());
        auto path = _tablet_mgr->delvec_location(meta->id(), name);
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(opts, path));
        ASSERT_OK(wf->append(Slice(file_buf.data(), file_buf.size())));
        ASSERT_OK(wf->close());

        auto* delvec_meta = meta->mutable_delvec_meta();
        auto& file = (*delvec_meta->mutable_version_to_file())[page_version];
        file.set_name(name);
        file.set_size(file_buf.size());
        auto* change_locator = meta->mutable_cdc_metadata()->mutable_pk_change_locator();
        for (const auto& loc : locs) {
            DelvecPagePB* page_ptr = nullptr;
            switch (which) {
            case CdcCaptureMap::COMPACTION_INPUT:
                page_ptr = &(*change_locator->mutable_compaction_input_delvecs())[loc.segment_id];
                break;
            case CdcCaptureMap::COMPACTION_OUTPUT:
                page_ptr = &(*change_locator->mutable_compaction_output_delvecs())[loc.segment_id];
                break;
            case CdcCaptureMap::COLUMN_OVERLAY:
                page_ptr = &(*change_locator->mutable_column_overlay_vecs())[loc.segment_id];
                break;
            }
            auto& page = *page_ptr;
            page.set_version(page_version);
            page.set_offset(loc.offset);
            page.set_size(loc.size);
            page.set_crc32c(loc.crc32c);
            page.set_crc32c_gen_version(page_version);
        }
    }

    // Write a real `.cols` delta column group file that overlays the value
    // column c1 for segment `segment_rssid`, and record it in *meta's dcg_meta.
    // `overlaid_c1` holds the post-overlay c1 value for EVERY rowid of the base
    // segment (after value at updated rowids, a copy of the base value elsewhere),
    // matching how a column partial update writes the whole column back. The
    // `.cols` segment carries only c1 (keyed by its unique id) and lives in the
    // segment directory beside the base segment, so the read path resolves it by
    // the base segment's parent directory. Mirrors the production writer in
    // ColumnModePartialUpdateHandler. Requires the fixture to run with _with_c1.
    void attach_dcg(TabletMetadata* meta, uint32_t segment_rssid, const std::vector<int32_t>& overlaid_c1) {
        ASSERT_TRUE(_with_c1);
        constexpr ColumnUID kC1Uid = 1;
        auto full_schema = TabletSchema::create(meta->schema());
        // The `.cols` segment's schema is the partial schema of just c1, keyed by
        // unique id — the same shape new_dcg_segment opens it under on read.
        auto cols_schema = TabletSchema::create_with_uid(full_schema, {kC1Uid});
        auto cols_data_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(cols_schema));

        auto c1 = Int32Column::create();
        c1->append_numbers(overlaid_c1.data(), overlaid_c1.size() * sizeof(int32_t));
        Chunk chunk({std::move(c1)}, cols_data_schema);

        std::string cols_name = lake::gen_cols_filename(next_id());
        auto cols_path = _tablet_mgr->segment_location(meta->id(), cols_name);
        WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(wopts, cols_path));
        SegmentWriterOptions writer_opts;
        SegmentWriter writer(std::move(wf), 0, cols_schema, writer_opts);
        ASSERT_OK(writer.init(false));
        ASSERT_OK(writer.append_chunk(chunk));
        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        ASSERT_OK(writer.finalize(&segment_file_size, &index_size, &footer_position));

        auto* dcgs = meta->mutable_dcg_meta()->mutable_dcgs();
        auto& dcg_ver = (*dcgs)[segment_rssid];
        dcg_ver.add_unique_column_ids()->add_column_ids(kC1Uid);
        dcg_ver.add_column_files(cols_name);
        dcg_ver.add_versions(meta->version());
        dcg_ver.add_encryption_metas("");
        dcg_ver.add_column_file_sizes(static_cast<int64_t>(segment_file_size));
    }

    // Write a single-segment rowset on disk for `tablet_id`. Stores the
    // resulting segment path through `out_path`. When the fixture runs with
    // _with_c1 the segment also carries the value column c1, taken from
    // `c1_values` if supplied, else c1 = start_value + rowid like c0.
    void write_segment(int64_t tablet_id, const std::shared_ptr<TabletSchema>& tablet_schema, int64_t num_rows,
                       std::string* out_path, int32_t start_value = 0, const std::vector<int32_t>& c1_values = {},
                       const std::vector<int32_t>& cX_values = {}) {
        auto data_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto c0 = Int32Column::create();
        std::vector<int32_t> values;
        values.reserve(static_cast<size_t>(num_rows));
        for (int64_t i = 0; i < num_rows; i++) {
            values.push_back(start_value + static_cast<int32_t>(i));
        }
        c0->append_numbers(values.data(), values.size() * sizeof(int32_t));
        Columns columns;
        columns.push_back(std::move(c0));
        if (_with_cx) {
            auto cx = Int32Column::create();
            std::vector<int32_t> vx;
            vx.reserve(static_cast<size_t>(num_rows));
            for (int64_t i = 0; i < num_rows; i++) {
                vx.push_back(cX_values.empty() ? start_value + static_cast<int32_t>(i) : cX_values[i]);
            }
            cx->append_numbers(vx.data(), vx.size() * sizeof(int32_t));
            columns.push_back(std::move(cx));
        }
        if (_with_c1) {
            auto c1 = Int32Column::create();
            std::vector<int32_t> v1;
            v1.reserve(static_cast<size_t>(num_rows));
            for (int64_t i = 0; i < num_rows; i++) {
                v1.push_back(c1_values.empty() ? start_value + static_cast<int32_t>(i) : c1_values[i]);
            }
            c1->append_numbers(v1.data(), v1.size() * sizeof(int32_t));
            columns.push_back(std::move(c1));
        }
        Chunk chunk(std::move(columns), data_schema);

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

    // Write a multi-segment rowset (one writer, one finish() per segment) and return each segment's
    // path. Segment s holds segment_rows[s] rows with c0 = start_value + 1000*s + rowid, so a reader
    // can tell which segment a surfaced row came from.
    void write_segments_for_rowset(int64_t tablet_id, const std::shared_ptr<TabletSchema>& tablet_schema,
                                   const std::vector<int64_t>& segment_rows, int32_t start_value,
                                   std::vector<std::string>* out_paths) {
        auto data_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(lake::kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        for (size_t s = 0; s < segment_rows.size(); s++) {
            int32_t base = start_value + static_cast<int32_t>(1000 * s);
            auto c0 = Int32Column::create();
            std::vector<int32_t> values;
            values.reserve(static_cast<size_t>(segment_rows[s]));
            for (int64_t i = 0; i < segment_rows[s]; i++) {
                values.push_back(base + static_cast<int32_t>(i));
            }
            c0->append_numbers(values.data(), values.size() * sizeof(int32_t));
            Columns columns;
            columns.push_back(std::move(c0));
            if (_with_c1) {
                auto c1 = Int32Column::create();
                c1->append_numbers(values.data(), values.size() * sizeof(int32_t));
                columns.push_back(std::move(c1));
            }
            Chunk chunk(std::move(columns), data_schema);
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish()); // seals segment s
        }
        const auto& files = writer->segments();
        ASSERT_EQ(segment_rows.size(), files.size());
        out_paths->clear();
        for (const auto& f : files) {
            out_paths->push_back(f.path);
        }
        writer->close();
    }

    // Publish a TabletMetadata at the given version. Mutates
    // *rowsets so callers can chain segment_path values across versions.
    // When `cdc_status` is non-OK it is stored under cdc_metadata, marking the
    // publish as one whose changes cannot be reconstructed. `mutate`, when set,
    // runs after rowsets and delete vectors are attached and before the metadata
    // is written, so a test can add CHANGES CDC capture maps to the final shape.
    void publish_metadata(int64_t tablet_id, int64_t version, int64_t schema_id, const std::vector<int64_t>& ancestors,
                          std::vector<RowsetSpec>* rowsets, const Status& cdc_status = Status::OK(),
                          const std::function<void(TabletMetadata*)>& mutate = nullptr) {
        auto meta = std::make_shared<TabletMetadata>();
        meta->set_id(tablet_id);
        meta->set_version(version);
        for (int64_t a : ancestors) {
            meta->add_metadata_ancestors(a);
        }
        set_default_schema(meta.get(), schema_id);
        // A primary-key CHANGES read is gated on the per-table CDC switch: the connector
        // rejects any in-range version whose metadata does not record enable_cdc. Every
        // primary-key scenario here reads the pk_change_locator capture maps that only
        // exist once CDC is enabled, so mark each published version accordingly.
        // Duplicate/aggregate keys are not gated and need no flag.
        if (_keys_type == PRIMARY_KEYS) {
            meta->mutable_cdc_metadata()->set_enable_cdc(true);
        }
        if (!cdc_status.ok()) {
            cdc_status.to_protobuf(meta->mutable_cdc_metadata()->mutable_capture_status());
        }
        auto tablet_schema = TabletSchema::create(meta->schema());

        if (rowsets != nullptr) {
            // The single segment's rss id is rowset_id + segment_idx(=0).
            std::vector<std::pair<uint32_t, std::vector<uint32_t>>> delvec_entries;
            for (auto& spec : *rowsets) {
                if (!spec.segment_rows.empty()) {
                    std::vector<std::string> seg_paths;
                    write_segments_for_rowset(tablet_id, tablet_schema, spec.segment_rows, spec.start_value,
                                              &seg_paths);
                    auto* rmeta = meta->add_rowsets();
                    rmeta->set_id(spec.id);
                    rmeta->set_version(spec.version);
                    rmeta->set_overlapped(false);
                    int64_t total = 0;
                    for (size_t s = 0; s < spec.segment_rows.size(); s++) {
                        auto* smeta = rmeta->add_segment_metas();
                        smeta->set_filename(seg_paths[s]);
                        smeta->set_num_rows(spec.segment_rows[s]);
                        total += spec.segment_rows[s];
                    }
                    rmeta->set_num_rows(total);
                    continue;
                }
                if (spec.num_rows > 0 && spec.segment_path.empty()) {
                    write_segment(tablet_id, tablet_schema, spec.num_rows, &spec.segment_path, spec.start_value,
                                  spec.c1_values, spec.cX_values);
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
                    // Real writers always set the per-segment row count (SegmentFileInfo::to_proto);
                    // each spec is a single-segment rowset, so the segment's count is spec.num_rows.
                    auto* smeta = rmeta->add_segment_metas();
                    smeta->set_filename(spec.segment_path);
                    smeta->set_num_rows(spec.num_rows);
                }
                if (!spec.deleted_rows.empty()) {
                    delvec_entries.emplace_back(spec.id, spec.deleted_rows);
                }
            }
            attach_delvecs(meta.get(), delvec_entries);
        }
        if (mutate != nullptr) {
            mutate(meta.get());
        }
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*meta));
    }

    // -------------------------------------------------------------------
    // get_next() pumping
    // -------------------------------------------------------------------

    // Drain `ds` under a `c0 > 50` predicate already installed via
    // set_predicates, asserting every surfaced row satisfies the predicate and
    // that the storage layer itself filtered rows (rows_vec_cond_filtered > 0
    // on the insert side), proving the predicate was pushed down rather than
    // left for a post-read backstop. Returns the total surfaced row count so
    // callers can additionally check the exact surviving-row count. Shared by
    // the PK and DUP/AGG pushdown-stats tests, which each build their own
    // fixture (PK vs. DUP tablet, one wide segment) and predicate before
    // calling this.
    int64_t drain_and_expect_pushdown_filtered(DataSource* ds, SlotId c0_slot_id) {
        std::vector<ChunkPtr> chunks;
        int64_t total = drain(ds, &chunks);
        for (const auto& ch : chunks) {
            const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
            for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 50);
        }
        auto* cds = down_cast<connector::ChangesDataSource*>(ds);
        EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0);
        return total;
    }

    // Drain `ds` to EOF. Returns total rows surfaced. When `chunks_out` is
    // non-null, surfaced chunks are appended so callers can inspect
    // the appended metadata columns.
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

    // Pump get_next() until it returns a non-OK, non-EOF status (the lazy traversal
    // surfaces degradation / unreachable-base / delete-predicate when the cursor
    // reaches that publish), or OK if the source drains cleanly to EOF.
    Status drain_until_error(DataSource* ds) {
        while (true) {
            ChunkPtr chunk;
            Status st = ds->get_next(_runtime_state.get(), &chunk);
            if (st.is_end_of_file()) return Status::OK();
            if (!st.ok()) return st;
            if (chunk == nullptr) return Status::OK();
        }
    }

    // One surfaced change row, decoded for assertion: the data key (c0), the
    // __CHANGE_TYPE__ (0=INSERT after value, 1=DELETE before value), and the
    // __ROW_VERSION__ (the publish version).
    struct ChangeRow {
        int32_t c0;
        int8_t change_type;
        int64_t row_version;
        bool operator<(const ChangeRow& o) const {
            return std::tie(row_version, change_type, c0) < std::tie(o.row_version, o.change_type, o.c0);
        }
        bool operator==(const ChangeRow& o) const {
            return c0 == o.c0 && change_type == o.change_type && row_version == o.row_version;
        }
    };

    static const Int8Column* as_int8(const ColumnPtr& col) {
        if (col->is_nullable()) {
            return down_cast<const Int8Column*>(down_cast<const NullableColumn*>(col.get())->data_column().get());
        }
        return down_cast<const Int8Column*>(col.get());
    }
    static const Int64Column* as_int64(const ColumnPtr& col) {
        if (col->is_nullable()) {
            return down_cast<const Int64Column*>(down_cast<const NullableColumn*>(col.get())->data_column().get());
        }
        return down_cast<const Int64Column*>(col.get());
    }

    // One surfaced change row decoded with its value column c1, for the
    // column-update tests: c0 is the key, c1 the overlaid value, plus the change
    // type and version. A DELETE row carries the before c1, its paired INSERT the new.
    struct ChangeRowC1 {
        int32_t c0;
        int32_t c1;
        int8_t change_type;
        int64_t row_version;
        bool operator<(const ChangeRowC1& o) const {
            return std::tie(row_version, change_type, c0, c1) < std::tie(o.row_version, o.change_type, o.c0, o.c1);
        }
        bool operator==(const ChangeRowC1& o) const {
            return c0 == o.c0 && c1 == o.c1 && change_type == o.change_type && row_version == o.row_version;
        }
    };

    // Open `ds`, drain it, and decode every surfaced row into a sorted ChangeRow
    // vector (sorted by row_version, then change_type, then c0). Requires a tuple
    // shape carrying c0 + both metadata columns.
    std::vector<ChangeRow> collect_change_rows(DataSource* ds, TTupleId tuple_id) {
        std::vector<ChunkPtr> chunks;
        drain(ds, &chunks);
        SlotId c0_id = slot_id_of(tuple_id, "c0");
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        std::vector<ChangeRow> rows;
        for (const auto& chunk : chunks) {
            const auto* c0 = down_cast<const Int32Column*>(chunk->get_column_by_slot_id(c0_id).get());
            const auto* ct = as_int8(chunk->get_column_by_slot_id(ct_id));
            const auto* rv = as_int64(chunk->get_column_by_slot_id(rv_id));
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                rows.push_back({c0->get_data()[i], ct->get_data()[i], rv->get_data()[i]});
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    // Like collect_change_rows but also decodes the value column c1, for the
    // column-update tests. Requires a tuple shape carrying c0 + c1 + both
    // metadata columns (install with _with_c1 set).
    std::vector<ChangeRowC1> collect_change_rows_with_c1(DataSource* ds, TTupleId tuple_id) {
        std::vector<ChunkPtr> chunks;
        drain(ds, &chunks);
        SlotId c0_id = slot_id_of(tuple_id, "c0");
        SlotId c1_id = slot_id_of(tuple_id, "c1");
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        std::vector<ChangeRowC1> rows;
        for (const auto& chunk : chunks) {
            const auto* c0 = down_cast<const Int32Column*>(chunk->get_column_by_slot_id(c0_id).get());
            const auto* c1 = down_cast<const Int32Column*>(chunk->get_column_by_slot_id(c1_id).get());
            const auto* ct = as_int8(chunk->get_column_by_slot_id(ct_id));
            const auto* rv = as_int64(chunk->get_column_by_slot_id(rv_id));
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                rows.push_back({c0->get_data()[i], c1->get_data()[i], ct->get_data()[i], rv->get_data()[i]});
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

protected:
    lake::TabletManager* _tablet_mgr = nullptr;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    std::shared_ptr<RuntimeState> _runtime_state;
    pipeline::FragmentRuntimeState _fragment_runtime_state;
    StorePathRegistry _store_path_registry;
    MemTracker _update_mem_tracker;
    int32_t _old_compact_threads = 0;
};

} // namespace

TEST(ChangesErrorTest, FormatsCodeSymbolAndMessage) {
    Status status = make_cdc_error(TCdcErrorCode::CHANGE_NOT_TRACKABLE, "tablet 42 history is unavailable");

    EXPECT_TRUE(status.is_internal_error());
    EXPECT_EQ("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): tablet 42 history is unavailable", std::string(status.message()));
}

TEST(ChangesErrorTest, FormatsUnknownInvalidAndEmptyInputs) {
    Status unknown = make_cdc_error(TCdcErrorCode::UNKNOWN, "detail");
    Status invalid = make_cdc_error(static_cast<TCdcErrorCode::type>(9999), "detail");
    Status empty = make_cdc_error(TCdcErrorCode::CHANGE_NOT_TRACKABLE, "");

    EXPECT_TRUE(unknown.is_internal_error());
    EXPECT_TRUE(invalid.is_internal_error());
    EXPECT_TRUE(empty.is_internal_error());
    EXPECT_EQ("CDC-ERROR-0 (UNKNOWN): detail", std::string(unknown.message()));
    EXPECT_EQ("CDC-ERROR-9999 (): detail", std::string(invalid.message()));
    EXPECT_EQ("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): ", std::string(empty.message()));
}

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

    // Framework sets the profile before open(); closing a never-opened scan must not deref null counters.
    RuntimeProfile parent_profile("ChangesScanTest");
    ds_ptr->set_runtime_profile(&parent_profile);

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
        EXPECT_EQ(std::string::npos, std::string(st.message()).find("CDC-ERROR-"));
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
        EXPECT_EQ(std::string::npos, std::string(st.message()).find("CDC-ERROR-"));
        ds->close(_runtime_state.get());
    }

    // Sub-case C: in-range rowset with delete_predicate. The lazy traversal plans the
    // publish only when the cursor reaches it, so open() succeeds and the read
    // surfaces NotSupported.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> rowsets = {{.version = 2, .id = 100, .num_rows = 0, .delete_predicate = true}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &rowsets);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "CDC for DUP_KEYS does not support delete");
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
        EXPECT_EQ(std::string::npos, std::string(st.message()).find("CDC-ERROR-"));
        ds->close(_runtime_state.get());
    }
}

// ============================================================================
// Test 3 — Metadata traversal: each sub-case crafts a TabletMetadata chain
// whose surfaced row count witnesses which rowsets the traversal admitted
// for reading.
// ============================================================================

// A new DUP rowset with several segments must surface every segment's rows as INSERTs. The
// whole-rowset (all_rows) read keeps the full rowset rather than a single segment-range view, so a
// row in any segment past the first is not silently dropped.
TEST_F(ChangesConnectorTest, test_dup_multi_segment_new_rowset_surfaces_every_segment) {
    _keys_type = DUP_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // One rowset new at v=2 with three segments (5 + 7 + 4 rows).
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .segment_rows = {5, 7, 4}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    CHECK_OK(ds->open(_runtime_state.get()));
    int64_t total = drain(ds.get());
    ds->close(_runtime_state.get());
    EXPECT_EQ(5 + 7 + 4, total);
}

// changes_scan_cache_mode reaches every cache the scan touches, including the tablet metadata
// reads that do not go through LakeIOOptions. Asserting on the metadata cache covers the path
// that is easiest to leave behind when the mode is threaded through the read options alone.
TEST_F(ChangesConnectorTest, test_cache_mode_gates_metadata_cache_population) {
    _keys_type = DUP_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .segment_rows = {4}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    const std::string head_meta_key = _tablet_mgr->tablet_metadata_location(tablet_id, 2);
    auto provider = make_provider(tuple_id, schema_id);

    auto scan_with_mode = [&](TChangesScanCacheMode::type mode) {
        auto sr = make_scan_range(tablet_id, /*base=*/1, /*head=*/2);
        sr.changes_scan_range.__set_cache_mode(mode);
        auto ds = provider->create_data_source(sr);
        CHECK_OK(ds->open(_runtime_state.get()));
        EXPECT_EQ(4, drain(ds.get()));
        ds->close(_runtime_state.get());
    };

    // NEVER reads head metadata without leaving it behind.
    _tablet_mgr->prune_metacache();
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));
    scan_with_mode(TChangesScanCacheMode::NEVER);
    EXPECT_EQ(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));

    // ALWAYS keeps it for the next reader.
    _tablet_mgr->prune_metacache();
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));
    scan_with_mode(TChangesScanCacheMode::ALWAYS);
    EXPECT_NE(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));

    // A value this BE does not recognize behaves as ALWAYS. During a rolling upgrade a newer FE
    // may send a mode added after this BE was built, and the fallback must not be the one that
    // silently stops caching.
    _tablet_mgr->prune_metacache();
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));
    scan_with_mode(static_cast<TChangesScanCacheMode::type>(99));
    EXPECT_NE(nullptr, _tablet_mgr->metacache()->lookup_tablet_metadata(head_meta_key));
}

// FULL_SCAN surfaces every row visible at head as an insert -- including a compaction-output
// rowset, which the VERSION_CHAIN_DIFF path deliberately skips because its bulk rows pre-existed.
// This guards reading a newly-added / empty-base partition whose sub-head history vacuum may have
// reclaimed: FULL_SCAN reads only head and must not inherit VERSION_CHAIN_DIFF's compaction-output skip.
TEST_F(ChangesConnectorTest, test_full_scan_surfaces_all_head_rows_including_compaction_output) {
    _keys_type = DUP_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v2: a normal rowset (5 rows). v3: a compaction output (8 rows) that merged v2 away.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 5}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    std::vector<RowsetSpec> r3 = {{.version = 3, .id = 200, .num_rows = 8, .max_compact_input = true}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

    auto provider = make_provider(tuple_id, schema_id);

    // FULL_SCAN at head=3 surfaces the compaction output's 8 live rows.
    {
        auto ds = provider->create_data_source(make_full_scan_range(tablet_id, /*head=*/3));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        EXPECT_EQ(8, total);
    }

    // Contrast: VERSION_CHAIN_DIFF (2,3] surfaces nothing -- the compaction output's rows pre-existed,
    // so the version-chain-diff path skips it. FULL_SCAN must NOT inherit this skip.
    {
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        EXPECT_EQ(0, total);
    }
}

// FULL_SCAN must reject a delete-predicate rowset the same way VERSION_CHAIN_DIFF does. A full scan
// reads segments raw and cannot apply a delete predicate, so surfacing its rows would emit rows the
// DELETE removed. A DUP partition added after the base bookmark that gets inserts and then a
// DELETE ... WHERE before head carries such a rowset; the read must fail, not return stale rows.
TEST_F(ChangesConnectorTest, test_full_scan_rejects_delete_predicate) {
    _keys_type = DUP_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // head=2: a data rowset (5 rows) plus a delete-predicate rowset (a DELETE ... WHERE).
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 5},
                                  {.version = 2, .id = 101, .num_rows = 0, .delete_predicate = true}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_full_scan_range(tablet_id, /*head=*/2));
    ASSERT_OK(ds->open(_runtime_state.get()));
    Status st = drain_until_error(ds.get());
    expect_change_not_trackable(st, "CDC for DUP_KEYS does not support delete");
    ds->close(_runtime_state.get());
}

// The primary-key read path reaches segments through get_each_segment_iterator_no_delvec, a
// different helper than the DUP/AGG path uses. Assert the mode reaches it too: a mode that only
// took effect for DUP/AGG would leave the main CHANGES workload -- incremental MV refresh on a
// primary-key table -- reading every segment footer from remote storage on every scan.
TEST_F(ChangesConnectorTest, test_cache_mode_reaches_primary_key_segment_loading) {
    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 4, .start_value = 100}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    const std::string segment_key = _tablet_mgr->segment_location(tablet_id, r2[0].segment_path);
    auto provider = make_provider(tuple_id, schema_id);

    auto scan_with_mode = [&](TChangesScanCacheMode::type mode) {
        auto sr = make_full_scan_range(tablet_id, /*head=*/2);
        sr.changes_scan_range.__set_cache_mode(mode);
        auto ds = provider->create_data_source(sr);
        ASSERT_OK(ds->open(_runtime_state.get()));
        EXPECT_EQ(4, drain(ds.get()));
        ds->close(_runtime_state.get());
    };

    _tablet_mgr->prune_metacache();
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_segment(segment_key));
    scan_with_mode(TChangesScanCacheMode::NEVER);
    EXPECT_EQ(nullptr, _tablet_mgr->metacache()->lookup_segment(segment_key));

    _tablet_mgr->prune_metacache();
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_segment(segment_key));
    scan_with_mode(TChangesScanCacheMode::ALWAYS);
    EXPECT_NE(nullptr, _tablet_mgr->metacache()->lookup_segment(segment_key));
}

TEST_F(ChangesConnectorTest, test_full_scan_pk_applies_delvec_and_dcg) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v2: one segment (rssid=10), 4 rows: c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    // v3 (head): the same segment survives, but this publish both deletes rowid 0 (c0=100) via a
    // live delete vector and column-updates rowid 1 (c0=101), overlaying c1 11 -> 100 via a delta
    // column group.
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_delvecs(meta, {{/*rssid=*/10, {0}}});
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 100, 12, 13});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    // FULL_SCAN at head=3 reads the head's live rows: rowid 0 is dropped by the delete vector, and the
    // read applies the dcg overlay so rowid 1 surfaces its updated c1=100 rather than the stored 11.
    auto ds = provider->create_data_source(make_full_scan_range(tablet_id, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    std::vector<ChangeRowC1> expected = {
            {/*c0=*/101, /*c1=*/100, /*INSERT*/ 0, /*ROW_VERSION=*/3},
            {/*c0=*/102, /*c1=*/12, /*INSERT*/ 0, /*ROW_VERSION=*/3},
            {/*c0=*/103, /*c1=*/13, /*INSERT*/ 0, /*ROW_VERSION=*/3},
    };
    EXPECT_EQ(expected, rows);
}

TEST_F(ChangesConnectorTest, test_metadata_traversal_edge_cases) {
    auto open_and_drain = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id, int64_t base,
                              int64_t head) -> int64_t {
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, base, head));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        return total;
    };

    // Sub-case A: base == head; the lazy traversal reaches base on the first advance,
    // so no rowsets surface for reading.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{}, /*rowsets=*/nullptr);
        EXPECT_EQ(0, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/3));
    }

    // Sub-case B: head whose recorded ancestor is exactly base; one in-range
    // rowset surfaces and the traversal stops at base.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        EXPECT_EQ(5, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/2));
    }

    // Sub-case C: head carries one rowset already present at base and one new
    // at head; the recorded ancestor base ends the traversal.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 200, .num_rows = 7}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        std::vector<RowsetSpec> r4 = {{.version = 3, .id = 200, .num_rows = 7, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 201, .num_rows = 3}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);
        // id=200 already exists at base (it is in meta(3)), so the set difference
        // meta(4) \ meta(3) excludes it; only id=201 (new at v=4) surfaces = 3 rows.
        EXPECT_EQ(3, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/4));
    }

    // Sub-case D: multi-level ancestor traversal (v=5 -> v=4 -> v=3, base=2). v=3's
    // recorded ancestor is exactly base, ending the traversal after every rowset
    // surfaced through v=5's metadata.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 300, .num_rows = 3}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        std::vector<RowsetSpec> r4 = {{.version = 3, .id = 300, .num_rows = 3, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 301, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);
        std::vector<RowsetSpec> r5 = {{.version = 3, .id = 300, .num_rows = 3, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 301, .num_rows = 4, .segment_path = r4[1].segment_path},
                                      {.version = 5, .id = 302, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/5, schema_id, /*ancestors=*/{4}, &r5);
        EXPECT_EQ(3 + 4 + 5, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/5));
    }

    // Sub-case E: a rowset id present in two adjacent metadata versions is not "new" at
    // the later one; the per-publish set difference counts it once.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 100, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        std::vector<RowsetSpec> r4 = {{.version = 3, .id = 100, .num_rows = 5, .segment_path = r3[0].segment_path},
                                      {.version = 4, .id = 101, .num_rows = 2}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);
        // id=100 is new at v=3 (meta(3) \ meta(2)) and persists into v=4, so it is
        // in both meta(3) and meta(4); meta(4) \ meta(3) excludes it. The dedup is
        // intrinsic to the set difference, not a separately tracked id set: total =
        // 5 (id=100 new at v=3) + 2 (id=101 new at v=4) = 7, never 5+5+2.
        EXPECT_EQ(5 + 2, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/4));
    }

    // Sub-case F: a rowset already present at base is excluded by the set
    // difference regardless of its version.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r3 = {{.version = 2, .id = 500, .num_rows = 8}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        std::vector<RowsetSpec> r5 = {{.version = 2, .id = 500, .num_rows = 8, .segment_path = r3[0].segment_path},
                                      {.version = 5, .id = 501, .num_rows = 6}};
        publish_metadata(tablet_id, /*version=*/5, schema_id, /*ancestors=*/{3}, &r5);
        // id=500 already exists at base (it is in meta(3)), so meta(5) \ meta(3)
        // excludes it; its version is irrelevant to the detection. Only
        // id=501 (new at v=5) surfaces = 6 rows.
        EXPECT_EQ(6, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/3, /*head=*/5));
    }

    // Sub-case G: rowset with max_compact_input_rowset_id is filtered.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr);
        std::vector<RowsetSpec> r3 = {{.version = 3, .id = 700, .num_rows = 9, .max_compact_input = true},
                                      {.version = 3, .id = 701, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        EXPECT_EQ(4, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/2, /*head=*/3));
    }
}

// ============================================================================
// Test 4 — metadata-column append under each TupleShape: chunk shape (column
// count), column types, slot-id resolution, and the appended metadata values.
// ============================================================================

TEST_F(ChangesConnectorTest, test_append_metadata_columns_slot_variants) {
    constexpr int64_t kRowsetVersion = 7;
    constexpr int64_t kNumRows = 4;

    auto open_and_collect = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id,
                                std::vector<ChunkPtr>* chunks_out) -> int64_t {
        initialize_tablet(tablet_id, schema_id);
        // Publish an empty base metadata so the set difference meta(head) \ meta(base)
        // has a real meta_before; the lone rowset is then new at head = kNumRows rows.
        publish_metadata(tablet_id, /*version=*/kRowsetVersion - 1, schema_id,
                         /*ancestors=*/{kRowsetVersion - 2}, /*rowsets=*/nullptr);
        std::vector<RowsetSpec> rowsets = {{.version = kRowsetVersion, .id = 1, .num_rows = kNumRows}};
        publish_metadata(tablet_id, /*version=*/kRowsetVersion, schema_id, /*ancestors=*/{kRowsetVersion - 1},
                         &rowsets);
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(
                make_scan_range(tablet_id, /*base=*/kRowsetVersion - 1, /*head=*/kRowsetVersion));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get(), chunks_out);
        ds->close(_runtime_state.get());
        return total;
    };

    // Sub-case A: both meta slots, non-nullable. The metadata-column append adds
    // an Int8Column (CHANGE_TYPE=0) and an Int64Column (ROW_VERSION=v).
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

    // Sub-case B: both meta slots, nullable. The metadata-column append wraps
    // columns in NullableColumn with an all-zero null mask.
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

    // Sub-case E: data column only. No metadata columns are appended.
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
    // drop the whole rowset before appending the metadata columns.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::ROW_VERSION_ONLY, /*include_data=*/false);
        SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
        ASSERT_NE(-1, rv_id);
        ASSERT_EQ(-1, slot_id_of(tuple_id, kChangeTypeColumnName));

        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(kNumRows, open_and_collect(tuple_id, next_id(), next_id(), &chunks));
        ASSERT_FALSE(chunks.empty());
        EXPECT_TRUE(chunks.front()->is_slot_exist(rv_id));
        // The forced row-count filler column carries no slot and must be stripped before surfacing,
        // leaving __ROW_VERSION__ as the only column.
        EXPECT_EQ(1u, chunks.front()->num_columns());
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
        // The forced row-count filler column carries no slot and must be stripped before surfacing,
        // leaving __CHANGE_TYPE__ as the only column.
        EXPECT_EQ(1u, chunks.front()->num_columns());
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
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
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
// runs `_conjunct_ctxs` against the chunk after the metadata columns are
// appended, as a correctness backstop;
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
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
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
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        EXPECT_EQ(0, open_with_predicate_and_drain(tuple_id, schema_id, tablet_id,
                                                   /*base=*/1, /*head=*/2, /*gt_value=*/999));
    }
}

// ============================================================================
// Test 7 — PRIMARY KEYS after-value read. A new rowset is read with the delete
// vector recorded at its own publish version, so only the rows still alive at
// that publish surface; rows deleted within their own publish (born-and-died)
// do not. DUP/AGG read every rowset whole and ignore delete vectors.
// ============================================================================

TEST_F(ChangesConnectorTest, test_primary_keys_surviving_rows) {
    _keys_type = PRIMARY_KEYS;

    auto open_and_drain = [&](TTupleId tuple_id, int64_t schema_id, int64_t tablet_id, int64_t base,
                              int64_t head) -> int64_t {
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, base, head));
        CHECK_OK(ds->open(_runtime_state.get()));
        int64_t total = drain(ds.get());
        ds->close(_runtime_state.get());
        return total;
    };

    // Sub-case A: a new rowset with no delete vector surfaces every row, same
    // as DUP/AGG — the PRIMARY KEYS read path applies an (absent) delete
    // vector and changes nothing.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 6}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        EXPECT_EQ(6, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/2));
    }

    // Sub-case B: born-and-died. The new rowset's own publish (v=2) records a
    // delete vector dropping rows 1 and 4 of its 6 rows, so only 4 surface.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 20, .num_rows = 6, .deleted_rows = {1, 4}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        EXPECT_EQ(6 - 2, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/2));
    }

    // Sub-case C: every row of the new rowset is deleted in its own publish;
    // the segment iterator returns end-of-file and zero rows surface.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 30, .num_rows = 3, .deleted_rows = {0, 1, 2}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        EXPECT_EQ(0, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/2));
    }

    // Sub-case D: across two publishes, each new rowset surfaces its own
    // surviving rows. v=2 inserts 5 (1 born-and-died), v=3 inserts 4 (none
    // dropped): 4 + 4 = 8.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 40, .num_rows = 5, .deleted_rows = {2}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        std::vector<RowsetSpec> r3 = {{.version = 2, .id = 40, .num_rows = 5, .segment_path = r2[0].segment_path},
                                      {.version = 3, .id = 41, .num_rows = 4}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2, 1}, &r3);
        EXPECT_EQ((5 - 1) + 4, open_and_drain(tuple_id, schema_id, tablet_id, /*base=*/1, /*head=*/3));
    }

    // Sub-case E: the CHANGE_TYPE column stays 0 (INSERT) for every surfaced
    // row — this chunk emits after values only.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
        ASSERT_NE(-1, ct_id);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 50, .num_rows = 5, .deleted_rows = {0}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        ASSERT_OK(ds->open(_runtime_state.get()));
        std::vector<ChunkPtr> chunks;
        EXPECT_EQ(4, drain(ds.get(), &chunks));
        ds->close(_runtime_state.get());
        ASSERT_FALSE(chunks.empty());
        const auto& ct_col = chunks.front()->get_column_by_slot_id(ct_id);
        const auto* ct_data = down_cast<const Int8Column*>(ct_col.get());
        for (size_t i = 0; i < ct_data->size(); i++) {
            EXPECT_EQ(0, ct_data->get_data()[i]);
        }
    }
}

// ============================================================================
// Test 8 — Degradation read. A publish marked non-OK under cdc_metadata, a version
// without CDC enabled, or an ancestor chain that cannot reach base surfaces a CDC
// error envelope when the lazy traversal reaches that publish during reading,
// rather than partial changes. A failed persisted capture retains its original
// diagnostic inside the envelope. An empty (base == head) interval stays OK and
// surfaces zero rows.
// ============================================================================

TEST_F(ChangesConnectorTest, test_degradation_read) {
    // Sub-case A: an in-range node carries a non-OK cdc_metadata.capture_status;
    // the lazy traversal wraps it in the CDC envelope while retaining the original
    // persisted capture diagnostic.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 60, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2,
                         Status::NotSupported("changes degraded by recover"));

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "changes degraded by recover");
        // This and the capture-off rejection share one error code; the frontend tells them apart by the
        // other one's wording. Mention capture being enabled here and this starts reporting as that.
        EXPECT_NE(std::string::npos, std::string(st.message()).find("whose changes were not captured"));
        ds->close(_runtime_state.get());
    }

    // Sub-case B: a non-OK status on an ancestor (not the head) is still seen
    // by the traversal and surfaces.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 70, .num_rows = 3}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2,
                         Status::NotSupported("degraded ancestor"));
        std::vector<RowsetSpec> r3 = {{.version = 2, .id = 70, .num_rows = 3, .segment_path = r2[0].segment_path},
                                      {.version = 3, .id = 71, .num_rows = 4}};
        // v=3's only recorded ancestor is v=2, so the traversal must visit (and
        // check) v=2 to continue toward base.
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "degraded ancestor");
        ds->close(_runtime_state.get());
    }

    // Sub-case C: head has no ancestor leading down to base; the chain cannot
    // span the interval, so the traversal surfaces NotSupported on reach.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 80, .num_rows = 5}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{}, &r2);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "cannot reach base");
        ds->close(_runtime_state.get());
    }

    // Sub-case D: a primary-key version without CDC enabled cannot be tracked.
    {
        _keys_type = PRIMARY_KEYS;
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr, Status::OK(),
                         [](TabletMetadata* meta) { meta->mutable_cdc_metadata()->set_enable_cdc(false); });

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "change data capture was not enabled at that version");
        ds->close(_runtime_state.get());
        _keys_type = DUP_KEYS;
    }

    // Sub-case E: an empty interval (base == head) returns OK with zero rows
    // even though that head node carries a non-OK status — the traversal reaches base
    // immediately, so no node is ever checked.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, /*rowsets=*/nullptr,
                         Status::NotSupported("degraded but interval empty"));

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/3, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        EXPECT_EQ(0, drain(ds.get()));
        ds->close(_runtime_state.get());
    }
}

TEST_F(ChangesConnectorTest, test_parent_metadata_read_failure_stays_unclassified) {
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // The head declares v2 as its direct parent, but no v2 metadata object is published.
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, /*rowsets=*/nullptr);

    auto raw_parent_read = _tablet_mgr->get_tablet_metadata(tablet_id, /*version=*/2);
    ASSERT_FALSE(raw_parent_read.ok());
    ASSERT_TRUE(raw_parent_read.status().is_not_found());

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    Status st = drain_until_error(ds.get());
    ds->close(_runtime_state.get());

    ASSERT_FALSE(st.ok());
    EXPECT_EQ(raw_parent_read.status().code(), st.code());
    EXPECT_EQ(std::string(raw_parent_read.status().message()), std::string(st.message()));
    EXPECT_EQ(std::string::npos, std::string(st.message()).find("CDC-ERROR-"));
}

// ============================================================================
// Test 9 — PRIMARY KEYS before-value (DELETE) side, the surviving-segment
// source. A publish meta_before -> meta_after sets new delete bits on a segment
// that survives the publish; reading those rowids at meta_before's version
// surfaces the rows as DELETEs carrying their before values, tagged
// __CHANGE_TYPE__=1 and __ROW_VERSION__=meta_after.version.
// ============================================================================

TEST_F(ChangesConnectorTest, test_primary_keys_before_values_surviving_segment) {
    _keys_type = PRIMARY_KEYS;

    // Insert a base segment S (rowset id=10) at v=2 holding c0 in [100..104].
    // Helpers below chain off this segment as the surviving old segment.
    auto insert_base_segment = [&](int64_t schema_id, int64_t tablet_id, std::string* seg_path) {
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 5, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
        *seg_path = r2[0].segment_path;
    };

    // Sub-case A: integer update. v=3 deletes rowid 2 of surviving S (before value
    // c0=102) and writes a new full-row rowset id=11 (after value c0=200). For
    // (base=2, head=3): DELETE(102) + INSERT(200), same version 3.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {
                {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {2}},
                {.version = 3, .id = 11, .num_rows = 1, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/200, /*INSERT*/ 0, /*v=*/3}, {/*c0=*/102, /*DELETE*/ 1, /*v=*/3}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case B: pure delete. v=3 deletes rowid 0 of surviving S (before value
    // c0=100) and writes no new rowset. For (base=2, head=3): a lone DELETE(100)
    // with no paired INSERT.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {
                {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {0}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/100, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }

    // Sub-case C: insert only. v=3 writes a new rowset id=11 (c0=200) and sets
    // no new delete bit on S. For (base=2, head=3): a lone INSERT(200), no
    // DELETE — the surviving segment is untouched so the surviving-segment source yields nothing.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {{.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path},
                                      {.version = 3, .id = 11, .num_rows = 1, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/200, /*INSERT*/ 0, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }

    // Sub-case D: one publish mixing update k1 + delete k2 + insert k3. v=3
    // deletes rowids {1,3} of surviving S (before values c0=101 update-loser and
    // c0=103 delete) and writes a new rowset id=11 with two rows (c0=200 the
    // update winner, c0=201 the insert). For (base=2, head=3):
    //   DELETE(101) + INSERT(200)  -> the update pair (same key in production)
    //   DELETE(103)                -> the delete
    //   INSERT(201)                -> the insert
    // all at version 3. The connector emits these as a set; pairing is the
    // reader's job, so it asserts the multiset.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {
                {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {1, 3}},
                {.version = 3, .id = 11, .num_rows = 2, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/200, /*INSERT*/ 0, /*v=*/3},
                                           {/*c0=*/201, /*INSERT*/ 0, /*v=*/3},
                                           {/*c0=*/101, /*DELETE*/ 1, /*v=*/3},
                                           {/*c0=*/103, /*DELETE*/ 1, /*v=*/3}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case E: cross-version ordering. Two consecutive update publishes
    // (v=3 deletes rowid 0 of S -> before c0=100, inserts c0=200; v=4 deletes
    // rowid 1 of S -> before c0=101, inserts c0=300). For (base=2, head=4) the
    // output spans both versions; collect_change_rows sorts by row_version then
    // change_type, witnessing the read order (version asc, DELETE before
    // INSERT within a version).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {
                {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {0}},
                {.version = 3, .id = 11, .num_rows = 1, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);
        std::vector<RowsetSpec> r4 = {
                {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {0, 1}},
                {.version = 3, .id = 11, .num_rows = 1, .segment_path = r3[1].segment_path},
                {.version = 4, .id = 12, .num_rows = 1, .start_value = 300}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/4));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // v=3: DELETE(100)+INSERT(200); v=4: new_dels = {0,1}-{0} = {1} ->
        // DELETE(101)+INSERT(300).
        std::vector<ChangeRow> expected = {
                {/*c0=*/200, 0, 3}, {/*c0=*/100, 1, 3}, {/*c0=*/300, 0, 4}, {/*c0=*/101, 1, 4}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
        // The sorted vector itself is the read order: every v=3 row precedes
        // every v=4 row, and within each version DELETE (1) precedes INSERT (0)
        // only if sorting change_type descending — collect_change_rows sorts
        // change_type ascending, so assert the version partition explicitly.
        ASSERT_EQ(4u, rows.size());
        EXPECT_EQ(3, rows[0].row_version);
        EXPECT_EQ(3, rows[1].row_version);
        EXPECT_EQ(4, rows[2].row_version);
        EXPECT_EQ(4, rows[3].row_version);
    }

    // Sub-case F: a born-and-died delete bit on a NEW rowset is not a before value. v=3 writes
    // a new rowset id=11 with 3 rows and deletes rowid 1 of *that same new
    // rowset* in its own publish; S is untouched. For (base=2, head=3): only
    // the 2 surviving new rows surface as INSERTs, no DELETE — the new rowset's
    // delete bit is born-and-died, never the surviving-segment source.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        std::string seg_path;
        insert_base_segment(schema_id, tablet_id, &seg_path);

        std::vector<RowsetSpec> r3 = {{.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path},
                                      {.version = 3, .id = 11, .num_rows = 3, .start_value = 200, .deleted_rows = {1}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // New rowset rows c0 200,201,202; rowid 1 (c0=201) deleted in its own
        // publish -> INSERT(200), INSERT(202); no DELETE.
        std::vector<ChangeRow> expected = {{/*c0=*/200, 0, 3}, {/*c0=*/202, 0, 3}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }
}

// ============================================================================
// Test 10 — PRIMARY KEYS before-value (DELETE) side, the compaction-input
// source: an input segment merged away by this publish's compaction. The
// deleted rows are gone from meta_after's rowsets; their pre-removal delvec sits
// in compaction_input_delvecs by rssid, and the rows are read from meta_before,
// which still references the input segment.
// ============================================================================

TEST_F(ChangesConnectorTest, test_primary_keys_before_values_compaction_input) {
    _keys_type = PRIMARY_KEYS;

    // Sub-case A: one batch deletes k1, then compaction merges
    // away the segment holding k1. v=2 inserts input segment S (id=10) with c0
    // in [100..104]. v=3 merges S into compaction output O (id=20) and records
    // S's pre-removal delvec {2} (c0=102 deleted) in compaction_input_delvecs. S is
    // gone from v=3's rowsets. For (base=2, head=3): one DELETE(102) read from
    // meta_before's S; O is a compaction output so the after-value side skips it.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 5, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        // v=3: S (id=10) merged away; only the compaction output O (id=20) remains.
        std::vector<RowsetSpec> r3 = {
                {.version = 3, .id = 20, .num_rows = 4, .max_compact_input = true, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                         [&](TabletMetadata* meta) {
                             // S's pre-removal delvec: rowid 2 (c0=102) deleted by the batch.
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_INPUT, {{/*rssid=*/10, {2}}});
                         });

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/102, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }

    // Sub-case B: compaction_input_delvecs minus DelVec_before. The input segment S
    // already carried an old delete bit at meta_before (rowid 0, c0=100, from a
    // prior publish), and the batch adds a new one (rowid 2, c0=102) before S is
    // merged away. compaction_input_delvecs[S] = {0,2} holds both; subtracting
    // DelVec_before(S) = {0} leaves only the new delete -> one DELETE(102).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        // v=2 inserts S (id=10) with c0 in [100..104] and already deletes rowid 0.
        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 5, .start_value = 100, .deleted_rows = {0}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {
                {.version = 3, .id = 20, .num_rows = 3, .max_compact_input = true, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                         [&](TabletMetadata* meta) {
                             // Pre-removal delvec of S holds the old bit {0} plus the batch's new {2}.
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_INPUT, {{/*rssid=*/10, {0, 2}}});
                         });

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // {0,2} - {0} = {2} (c0=102); rowid 0 (c0=100) was an old delete, not re-emitted.
        std::vector<ChangeRow> expected = {{/*c0=*/102, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }
}

// ============================================================================
// Test 11 — PRIMARY KEYS before-value (DELETE) side for compaction-output
// segments. An output BORN this publish is read from itself (the
// compaction-output source) with no delete vector applied, after excluding the
// conflict-resolution bits recorded in compaction_output_delvecs (whose before
// values come from the input side via the compaction-input source). An output
// that SURVIVED from an earlier publish is, on a later edge, an ordinary
// surviving segment handled by the surviving-segment source -- its before value
// is read from meta_before WITH any dcg overlay. Covers the pure-compaction
// (0 rows) case, and the later-delete and overlay-then-delete paths for survivors.
// ============================================================================

TEST_F(ChangesConnectorTest, test_primary_keys_before_values_compaction_output) {
    _keys_type = PRIMARY_KEYS;

    // Sub-case A: pure compaction publish. v=3 merges S0 (id=10) and S1 (id=11)
    // into output O (id=20) with no in-batch DML — O has no delete bits, the
    // input delvecs are empty. A pure repackaging produces no changes: 0 rows.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 3, .start_value = 100},
                                      {.version = 2, .id = 11, .num_rows = 2, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {
                {.version = 3, .id = 20, .num_rows = 5, .max_compact_input = true, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        EXPECT_TRUE(rows.empty());
    }

    // Sub-case B: the compaction-output read yields exactly DelVec_after(O) -
    // conflict(O). The output O (id=20) holds c0 in [100..104]; its delvec marks
    // rowids {1,3} deleted, of which {1} is a conflict-resolution bit (recorded
    // in compaction_output_delvecs) and {3} is a later DML's real delete. The
    // compaction-output read takes only rowid 3 (c0=103) from O -> one DELETE(103).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 5, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        // O (id=20) carries delvec {1,3}; conflict bit {1} is excluded.
        std::vector<RowsetSpec> r3 = {{.version = 3,
                                       .id = 20,
                                       .num_rows = 5,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {1, 3}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                         [&](TabletMetadata* meta) {
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_OUTPUT, {{/*rssid=*/20, {1}}});
                         });

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // {1,3} - {1} = {3} (c0=103); the conflict bit (rowid 1, c0=101) is not emitted from O.
        std::vector<ChangeRow> expected = {{/*c0=*/103, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }

    // Sub-case C: the first key updated / a compaction merges the holding
    // segments / a second key updated, all within one batch. The full publish
    // exercises all three before-value sources at once. The segment helper writes
    // consecutive c0 = start_value + rowid, so the narrative keys below land at:
    // K_a (before value 100) and K_b (before value 101) in S0; the
    // conflict-resolution bit is the first key, the compaction-output delete is
    // the second.
    //   meta_before (v=2): S0 (id=10) c0=[100,101] (rowids 0,1), S1 (id=11) c0=[102].
    //   meta_after (v=3): S0,S1 merged into output O (id=20) c0=[100,101,102]
    //     (rowids 0,1,2). New DML rowsets: S3 (id=30) c0=200 (first update's after
    //     value), S4 (id=31) c0=201 (second update's after value).
    //     - the compaction-input source: compaction_input_delvecs[S0=10] = {0} (the first key, superseded
    //       before S0 was merged away) -> DELETE(100) read from meta_before's S0.
    //     - the compaction-output source: O's delvec {0,1}: rowid 0 is the conflict bit (recorded in
    //       compaction_output_delvecs[O]), rowid 1 is the second update's real
    //       delete -> the compaction-output read takes only rowid 1 (c0=101) -> DELETE(101).
    //     - after-value side: S3 -> INSERT(200), S4 -> INSERT(201).
    // The first key is emitted once (DELETE from the input side, INSERT from
    // S3), not re-emitted from O; the second key is not missed (DELETE from O,
    // INSERT from S4).
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 2, .start_value = 100},
                                      {.version = 2, .id = 11, .num_rows = 1, .start_value = 102}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {{.version = 3,
                                       .id = 20,
                                       .num_rows = 3,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {0, 1}},
                                      {.version = 3, .id = 30, .num_rows = 1, .start_value = 200},
                                      {.version = 3, .id = 31, .num_rows = 1, .start_value = 201}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                         [&](TabletMetadata* meta) {
                             // S0's pre-removal delvec: rowid 0 (c0=100), the first key superseded.
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_INPUT, {{/*rssid=*/10, {0}}});
                             // O's conflict bit: rowid 0 of O (c0=100) resolved against the first update.
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_OUTPUT, {{/*rssid=*/20, {0}}});
                         });

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/200, /*INSERT*/ 0, /*v=*/3},
                                           {/*c0=*/201, /*INSERT*/ 0, /*v=*/3},
                                           {/*first key old=*/100, /*DELETE*/ 1, /*v=*/3},
                                           {/*second key old=*/101, /*DELETE*/ 1, /*v=*/3}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case D: a SURVIVING compaction output that receives a later DML delete. On the later edge O
    // is an ordinary surviving segment, so the surviving-segment source subtracts meta_before's delete vector for O.
    // The conflict capture mattered only on the publish that produced O (the next publish records its own
    // capture, not O's); subtracting it here would re-emit O's earlier-deleted rows as fresh deletes
    // at the later version.
    //   v=2 (compaction): output O (id=20) c0=[100..109]; conflict resolution
    //     deleted rowid 5 (c0=105), recorded in BOTH O's delvec and
    //     compaction_output_delvecs[O].
    //   v=3 (normal DML): O survives and a delete removes rowid 9 (c0=109); O's
    //     delvec accumulates to {5,9}; the conflict map is gone (not re-attached).
    // CHANGES(base=2, head=3] must emit only DELETE(109): rowid 5 was deleted at
    // the boundary base v=2 and must not reappear at v=3.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 10,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {5}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2, Status::OK(),
                         [&](TabletMetadata* meta) {
                             attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_OUTPUT, {{/*rssid=*/20, {5}}});
                         });

        std::vector<RowsetSpec> r3 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 10,
                                       .segment_path = r2[0].segment_path,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {5, 9}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/109, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }

    // Sub-case E: a surviving compaction output deleted across TWO later publishes,
    // with no conflict bits at all. Each edge must emit only that edge's own new
    // delete, proving meta_before's delete vector (which the surviving-segment source subtracts for a
    // surviving output) drives every post-compaction edge.
    //   v=2 (compaction): output O (id=20) c0=[200..209], no deletes.
    //   v=3: delete rowid 3 (c0=203). v=4: delete rowid 7 (c0=207).
    // CHANGES(base=2, head=4] traverses edges (3,4) then (2,3): DELETE(203)@v3 and
    // DELETE(207)@v4, each emitted once.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {
                {.version = 2, .id = 20, .num_rows = 10, .max_compact_input = true, .start_value = 200}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 10,
                                       .segment_path = r2[0].segment_path,
                                       .max_compact_input = true,
                                       .start_value = 200,
                                       .deleted_rows = {3}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        std::vector<RowsetSpec> r4 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 10,
                                       .segment_path = r3[0].segment_path,
                                       .max_compact_input = true,
                                       .start_value = 200,
                                       .deleted_rows = {3, 7}}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/4));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRow> expected = {{/*c0=*/203, /*DELETE*/ 1, /*v=*/3}, {/*c0=*/207, /*DELETE*/ 1, /*v=*/4}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case F: a SURVIVING compaction output whose row is column-updated (dcg overlay) in one
    // publish and then plain-DELETEd in a later publish. The DELETE's before value must be the overlaid
    // value the row held just before deletion, not the pre-overlay base value. The overlay lives in
    // dcg_meta().dcgs() (a `.cols` file), which the per-publish capture never prunes, so it persists
    // into the delete edge's meta_before; the surviving output is handled by the surviving-segment
    // source, whose from_before=true read applies that overlay. (A raw meta_after read would surface the
    // stale base value.)
    //   v=2 (compaction): output O (id=20) c0=[100..103], base c1=[10,11,12,13].
    //   v=3: O survives; a column update overlays c1 of rowid 1 (c0=101) to 99 -> the column-update
    //        route emits DELETE(101, before c1=11)@3 + INSERT(101, after c1=99)@3.
    //   v=4: O survives; rowid 1 (c0=101) is deleted -> the surviving-segment source emits DELETE(101, before c1=99)@4,
    //        reading meta_before(v=3) WITH the overlay.
    {
        _with_c1 = true;
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 4,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .c1_values = {10, 11, 12, 13}}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        // v=3: surviving O (base segment unchanged) + a column update overlaying c1 of rowid 1 to 99,
        // recorded in O's dcg and column_overlay_vecs[O].
        std::vector<RowsetSpec> r3 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 4,
                                       .segment_path = r2[0].segment_path,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .c1_values = {10, 11, 12, 13}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                         [&](TabletMetadata* meta) {
                             attach_dcg(meta, /*segment_rssid=*/20, /*overlaid_c1=*/{10, 99, 12, 13});
                             attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/20, {1}}});
                         });

        // v=4: rowid 1 (c0=101) is deleted. The v=3 overlay lives in this edge's meta_before (v=3);
        // the surviving-segment source reads the before value there WITH the overlay. No column_overlay_vecs
        // at v=4 (the column update was v=3), so the column-update route does not re-fire here.
        std::vector<RowsetSpec> r4 = {{.version = 2,
                                       .id = 20,
                                       .num_rows = 4,
                                       .segment_path = r3[0].segment_path,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {1},
                                       .c1_values = {10, 11, 12, 13}}};
        publish_metadata(tablet_id, /*version=*/4, schema_id, /*ancestors=*/{3}, &r4);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/4));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*before c1=*/11, /*DELETE*/ 1, /*v=*/3},
                                             {/*c0=*/101, /*after c1=*/99, /*INSERT*/ 0, /*v=*/3},
                                             {/*c0=*/101, /*overlaid before c1=*/99, /*DELETE*/ 1, /*v=*/4}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case G (an update with NO conflict on the output): a compaction output BORN this publish that
    // merged cleanly -- no concurrent write was superseded while it published, so
    // compaction_output_delvecs has no entry for it -- and which a load in the SAME batch then
    // updates. The update sets an ordinary delete bit on the output (the old position) and writes the
    // after value into a fresh DML rowset. The compaction-output source takes the
    // compaction_output_delvecs.find(O) == end() branch:
    // the baseline is empty, so the output's whole delete vector is emitted. Sub-cases B and C always
    // carried a conflict bit on the output and only exercised the non-empty-baseline branch; this pins
    // the clean-output path.
    //   meta_before (v=2): S0 (id=10) c0=[100,101,102] (rowids 0,1,2).
    //   meta_after (v=3): S0 merged into output O (id=20) c0=[100,101,102]; the load updates the middle
    //     key, deleting rowid 1 (c0=101) on O and writing the after value into S3 (id=30) c0=201. No
    //     conflict-resolution capture is attached, so compaction_output_delvecs is empty.
    //     - the compaction-output source: O's delvec {1}, empty baseline -> DELETE(101) read raw from O.
    //     - after-value side: S3 -> INSERT(201). O's untouched rows (100,102) are a pure repackaging.
    {
        _with_c1 = false; // sub-case F left it true; this sub-case asserts on c0 only.
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 3, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {{.version = 3,
                                       .id = 20,
                                       .num_rows = 3,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {1}},
                                      {.version = 3, .id = 30, .num_rows = 1, .start_value = 201}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // O's whole delete vector {1} is emitted (empty conflict baseline) -> DELETE(101); the new
        // value comes from the DML rowset -> INSERT(201).
        std::vector<ChangeRow> expected = {{/*c0=*/101, /*DELETE*/ 1, /*v=*/3}, {/*c0=*/201, /*INSERT*/ 0, /*v=*/3}};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, rows);
    }

    // Sub-case H (a pure delete, no conflict on the output): the same clean compaction output as G, but
    // the load only DELETEs the key -- it writes no replacement row. The compaction-output source still
    // emits the output's whole delete vector (empty conflict baseline), and with no after-value rowset
    // the change stands
    // alone as a single DELETE: the delete side does not depend on a paired insert.
    //   meta_before (v=2): S0 (id=10) c0=[100,101,102] (rowids 0,1,2).
    //   meta_after (v=3): S0 merged into output O (id=20) c0=[100,101,102]; the load deletes rowid 1
    //     (c0=101) on O. No new rowset, no conflict-resolution capture.
    //     - the compaction-output source: O's delvec {1}, empty baseline -> DELETE(101). Nothing on
    //       the after-value side.
    {
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);

        std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 3, .start_value = 100}};
        publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

        std::vector<RowsetSpec> r3 = {{.version = 3,
                                       .id = 20,
                                       .num_rows = 3,
                                       .max_compact_input = true,
                                       .start_value = 100,
                                       .deleted_rows = {1}}};
        publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3);

        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
        ASSERT_OK(ds->open(_runtime_state.get()));
        auto rows = collect_change_rows(ds.get(), tuple_id);
        ds->close(_runtime_state.get());

        // Only the delete is emitted; there is no paired insert.
        std::vector<ChangeRow> expected = {{/*c0=*/101, /*DELETE*/ 1, /*v=*/3}};
        EXPECT_EQ(expected, rows);
    }
}

// ============================================================================
// Test 12 — PRIMARY KEYS column-update (delta column group) route. A publish
// overlays the value column c1 of a segment via a `.cols` file and records the
// overlaid rowids in column_overlay_vecs; that overlay sets no delete bit, so
// the row is invisible to the three delete-bit sources. The route emits the
// before value (read from meta_before, pre-overlay) paired with the after value
// (read from meta_after, post-overlay) for each overlaid row that the same
// publish did not also whole-row-delete or freshly insert.
// ============================================================================

// Surviving-segment overlay: dcg on a segment that existed before the publish. meta_before has S
// (rowset id=10, rssid=10) with c1=[10,11,12,13]; meta_after references the same
// S and overlays c1=[10,21,22,13] on rowids {1,2}. CHANGES(base,head] emits
// DELETE(before c1)+INSERT(after c1) for k1 and k2 only, both at the publish version.
TEST_F(ChangesConnectorTest, test_primary_keys_column_update_surviving_segment) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: base segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: same S, overlay c1 on rowids {1,2}: c1 = [10,21,22,13].
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 21, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1, 2}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*before c1=*/11, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/101, /*after c1=*/21, /*INSERT*/ 0, /*v=*/3},
                                         {/*c0=*/102, /*before c1=*/12, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/102, /*after c1=*/22, /*INSERT*/ 0, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// New-rowset overlay: dcg on a brand-new full-row rowset added this publish. The after-value
// side already emits the rows once (with the overlaid value, since the after-value
// read of N at head applies head's dcg); the column-update route emits nothing
// extra. No DELETE for N's keys.
TEST_F(ChangesConnectorTest, test_primary_keys_column_update_new_rowset_skipped) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: an unrelated base segment so the chain has a non-empty meta_before.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 2, .start_value = 100, .c1_values = {10, 11}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: NEW rowset N (id=20, rssid=20), c0 in [200..201], with a dcg + row-vec
    // on N itself. N is absent from meta_before, so the column-update route's
    // route-3 branch skips it. The after-value read of N at v=3 applies v=3's dcg,
    // surfacing the overlaid c1 = [30,31].
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 2, .segment_path = r2[0].segment_path, .c1_values = {10, 11}},
            {.version = 3, .id = 20, .num_rows = 2, .start_value = 200, .c1_values = {300, 301}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/20, /*overlaid_c1=*/{30, 31});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/20, {0, 1}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // Only the two new rows of N surface, as INSERTs carrying the overlaid c1;
    // no DELETE for k200/k201.
    std::vector<ChangeRowC1> expected = {{/*c0=*/200, /*c1=*/30, /*INSERT*/ 0, /*v=*/3},
                                         {/*c0=*/201, /*c1=*/31, /*INSERT*/ 0, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// Survival filter: a row the publish both column-updated and whole-row-deleted.
// Its before value is emitted by the surviving-segment source (its new delete bit);
// the column-update route must emit neither a second DELETE nor an INSERT for it.
TEST_F(ChangesConnectorTest, test_primary_keys_column_update_survival_filter) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: base segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: rowid 2 is in BOTH column_overlay_vecs[S] AND DelVec_after(S). The
    // overlay file still carries a value at rowid 2, but the survival filter
    // drops it from the column-update route. The surviving-segment source emits the single DELETE
    // (before c1=12) from the surviving segment's new delete bit.
    std::vector<RowsetSpec> r3 = {{.version = 2,
                                   .id = 10,
                                   .num_rows = 4,
                                   .segment_path = r2[0].segment_path,
                                   .deleted_rows = {2},
                                   .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 11, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {2}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // Exactly one DELETE(k2, before c1=12) from the surviving-segment source; nothing from the
    // column-update route for rowid 2.
    std::vector<ChangeRowC1> expected = {{/*c0=*/102, /*before c1=*/12, /*DELETE*/ 1, /*v=*/3}};
    EXPECT_EQ(expected, rows);
}

// Compaction-output overlay: dcg on this publish's compaction output O. O is absent from
// meta_before, so its pre-overlay value is the raw base value read without the
// overlay (read raw from after_meta), and its after value is the post-overlay read from
// meta_after. Emits DELETE(base c1)+INSERT(after c1).
TEST_F(ChangesConnectorTest, test_primary_keys_column_update_compaction_output) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: input segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: compaction output O (id=20, rssid=20) repackaging S's rows with
    // base c1 = [10,11,12,13]. The same publish column-updates rowids {1,2} of O,
    // overlaying c1 = [10,21,22,13], recorded in O's dcg + column_overlay_vecs[O].
    // O is a compaction output absent from meta_before -> the compaction-output overlay case.
    std::vector<RowsetSpec> r3 = {{.version = 3,
                                   .id = 20,
                                   .num_rows = 4,
                                   .max_compact_input = true,
                                   .start_value = 100,
                                   .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/20, /*overlaid_c1=*/{10, 21, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/20, {1, 2}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*base c1=*/11, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/101, /*after c1=*/21, /*INSERT*/ 0, /*v=*/3},
                                         {/*c0=*/102, /*base c1=*/12, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/102, /*after c1=*/22, /*INSERT*/ 0, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// ============================================================================
// Test 13 — Multi-publish range must not collapse. A segment is added in one
// in-range publish and a row of it deleted in a later in-range publish, while
// the head node records base directly in its ancestor window. The traversal
// must still advance one publish at a time so the delete surfaces; collapsing the
// whole range into a single base->head diff would treat the segment as newly
// added against base and drop the delete as born-and-died.
// ============================================================================

TEST_F(ChangesConnectorTest, test_primary_keys_multi_publish_delete_not_collapsed) {
    _keys_type = PRIMARY_KEYS;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: insert segment S (id=80) with c0 in [100..102]. Created after base.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 80, .num_rows = 3, .start_value = 100}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: the same S, now with rowid 1 (key 101) deleted. The head records both
    // its direct parent (2) and base (1), so the old reaches-base short-circuit
    // would collapse (1,3] into one diff and drop this delete.
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 80, .num_rows = 3, .segment_path = r2[0].segment_path, .deleted_rows = {1}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2, 1}, &r3);

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // The three rows surface as INSERTs at their creation publish (v=2); key 101
    // additionally surfaces as a DELETE at the publish that removed it (v=3).
    std::vector<ChangeRow> expected = {{/*c0=*/100, /*INSERT*/ 0, /*v=*/2},
                                       {/*c0=*/101, /*INSERT*/ 0, /*v=*/2},
                                       {/*c0=*/102, /*INSERT*/ 0, /*v=*/2},
                                       {/*c0=*/101, /*DELETE*/ 1, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// ============================================================================
// Test 14 — Cross-publish insert + column-update. A row inserted in
// publish v2 and column-updated in a LATER publish v3 surfaces its v2 INSERT
// with the insert-time value (c1=10), not v3's overlay. The after-value read for
// each publish is built from that publish's own meta_after, so the v2 INSERT
// reads v2's dcg — which carries no overlay yet — and sees the insert-time
// value. The v3 column-update then surfaces as a DELETE/INSERT pair: before c1 from
// meta_before=v2 (pre-overlay, 10), after c1 from v3 (post-overlay, 20).
// ============================================================================
TEST_F(ChangesConnectorTest, test_primary_keys_column_update_cross_publish_insert) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: insert segment S (id=10, rssid=10): one row c0=100, c1=10.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 1, .start_value = 100, .c1_values = {10}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // v=3: same surviving S (keeps birth version=2), column-updated: overlay c1=20
    // on rowid 0, recorded in dcg + column_overlay_vecs[10].
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 1, .segment_path = r2[0].segment_path, .c1_values = {10}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{20});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {0}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // POST-REFACTOR: the v2 INSERT now reads from the publish's own meta_after (v2),
    // whose dcg has no overlay yet, so it carries the insert-time value (c1=10).
    std::vector<ChangeRowC1> expected = {{/*c0=*/100, /*c1=*/10, /*INSERT*/ 0, /*v=*/2},
                                         {/*c0=*/100, /*c1=*/10, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/100, /*c1=*/20, /*INSERT*/ 0, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// ============================================================================
// Pin: a row inserted in one publish and column-updated in a LATER
// publish must surface its INSERT with the insert-time value, not the later
// overlay. v2 inserts (c0=1, c1=20) as a surviving segment; v3 carries it
// forward and column-updates c1 := 99. The v2 INSERT must read c1=20.
// ============================================================================
TEST_F(ChangesConnectorTest, test_cross_publish_insert_then_column_update_keeps_insert_time_value) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v2: insert (c0=1, c1=20) as rowset rssid=100.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 1, .start_value = 1, .c1_values = {20}}};
    publish_metadata(tablet_id, 2, schema_id, /*ancestors=*/{1}, &r2);

    // v3 carries rowset 100 forward (reuse the now-filled v2 spec) and column-updates c1 := 99 on it.
    std::vector<RowsetSpec> r3 = {r2[0]}; // same id/version/filled segment_path -> surviving segment
    publish_metadata(tablet_id, 3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     /*mutate=*/[&](TabletMetadata* m) {
                         attach_dcg(m, /*segment_rssid=*/100u, /*overlaid_c1=*/{99});
                         attach_cdc_delvecs(m, CdcCaptureMap::COLUMN_OVERLAY, {{100u, {0u}}});
                     });

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/3));
    CHECK_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // Expected surfaced rows: INSERT@v2 (c1=20), DELETE@v3 (c1=20), INSERT@v3 (c1=99).
    // PIN: the v2 INSERT carries its insert-time c1=20, NOT the v3 overlay 99.
    bool seen_insert_v2_20 = false;
    for (const auto& r : rows) {
        if (r.row_version == 2 && r.change_type == 0 /*INSERT*/ && r.c0 == 1) {
            EXPECT_EQ(20, r.c1);
            seen_insert_v2_20 = true;
        }
    }
    EXPECT_TRUE(seen_insert_v2_20);
}

// ============================================================================
// ChangesReadPlanner — classify + locate the change reads for one publish edge.
// ============================================================================

TEST_F(ChangesConnectorTest, test_planner_load_insert_alive_rows) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v2: a new LOAD rowset id=100, 3 rows, rowid 1 deleted within the same publish -> alive {0,2}.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 100, .num_rows = 3, .start_value = 0, .deleted_rows = {1}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.insert_changes.size());
    const auto& s = plan.insert_changes[0];
    EXPECT_FALSE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(2u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(0));
    EXPECT_TRUE(s.rowids->contains(2));
    EXPECT_TRUE(plan.delete_changes.empty());
}

TEST_F(ChangesConnectorTest, test_planner_compaction_output_column_update_insert) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v2: a compaction output rowset id=200 (its bulk rows pre-existed, so not logical inserts).
    // This same publish column-updates rowids {3,4} and deletes rowid 4 (delvec_after(200)={4}).
    // The only after value to surface is the still-alive column-updated row 3.
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 200, .num_rows = 5, .max_compact_input = true, .deleted_rows = {4}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2, Status::OK(),
                     [this](TabletMetadata* meta) {
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/200u, {3u, 4u}}});
                     });

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.insert_changes.size());
    const auto& s = plan.insert_changes[0];
    EXPECT_FALSE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(1u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(3));
    // Intentionally no assertion on delete_changes: the before-value step adds a DELETE for this segment.
}

TEST_F(ChangesConnectorTest, test_planner_surviving_segment_column_update_insert) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v1->v2 carries the SAME rowset 100 forward (empty segment_path on v1 lets publish_metadata
    // write the segment and fill the path in place; reusing the filled spec at its original
    // version=1 makes version-comparison classify it as carried, not added).
    std::vector<RowsetSpec> r1 = {{.version = 1, .id = 100, .num_rows = 3, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/1, schema_id, /*ancestors=*/{}, &r1);
    // v2 carries 100 forward unchanged on disk, but column-updates rowid 2 (no new delvec on 100).
    std::vector<RowsetSpec> r2 = {{.version = 1, .id = 100, .num_rows = 3, .segment_path = r1[0].segment_path}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2, Status::OK(),
                     [this](TabletMetadata* meta) {
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/100u, {2u}}});
                     });

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.insert_changes.size());
    const auto& s = plan.insert_changes[0];
    EXPECT_FALSE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(1u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(2));
    // Intentionally no assertion on delete_changes: the before-value step adds a DELETE for this segment.
}

// A carried LOAD segment whose delete vector grew this publish surfaces the newly-deleted rows as a
// whole-row DELETE, read from before_meta with the dcg overlay (the before value).
TEST_F(ChangesConnectorTest, test_planner_whole_row_delete) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v1: a LOAD rowset id=100, 3 rows, no delvec.
    std::vector<RowsetSpec> r1 = {{.version = 1, .id = 100, .num_rows = 3, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/1, schema_id, /*ancestors=*/{}, &r1);
    // v2 carries 100 forward (still version=1 -> classified as carried, not added) and records a NEW
    // delvec {1} on it, so the publish deletes rowid 1.
    std::vector<RowsetSpec> r2 = {
            {.version = 1, .id = 100, .num_rows = 3, .segment_path = r1[0].segment_path, .deleted_rows = {1}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.delete_changes.size());
    const auto& s = plan.delete_changes[0];
    EXPECT_TRUE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(1u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(1));
}

// A LOAD segment merged away by this publish's compaction (gone from after_meta) surfaces the rows it
// deleted as a whole-row DELETE, read from before_meta. The after-side delvec is taken from the
// compaction_input_delvecs entry the merge recorded under the input's rssid.
TEST_F(ChangesConnectorTest, test_planner_compaction_input_delete) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v1: a LOAD rowset id=100, 3 rows.
    std::vector<RowsetSpec> r1 = {{.version = 1, .id = 100, .num_rows = 3, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/1, schema_id, /*ancestors=*/{}, &r1);
    // v2: 100 is compacted away (published WITHOUT it); the merge recorded its input delete vector {2}.
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr, Status::OK(),
                     [this](TabletMetadata* meta) {
                         attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_INPUT, {{/*rssid=*/100u, {2u}}});
                     });

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.delete_changes.size());
    const auto& s = plan.delete_changes[0];
    EXPECT_TRUE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(1u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(2));
}

// A born compaction output's deleted rows surface as a compaction-output DELETE: read raw from after_meta (no
// dcg, no delvec). The rows are delvec_after minus the compaction conflict-resolution baseline.
TEST_F(ChangesConnectorTest, test_planner_compaction_output_delete) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v1: empty.
    publish_metadata(tablet_id, /*version=*/1, schema_id, /*ancestors=*/{}, /*rowsets=*/nullptr);
    // v2: a born compaction output rowset id=200, 2 rows, delvec_after {0,1}; the conflict-resolution
    // baseline (already-deleted while compacting) is {0}. Net compaction-output delete = {0,1} - {0} = {1}.
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 200, .num_rows = 2, .max_compact_input = true, .deleted_rows = {0, 1}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2, Status::OK(),
                     [this](TabletMetadata* meta) {
                         attach_cdc_delvecs(meta, CdcCaptureMap::COMPACTION_OUTPUT, {{/*rssid=*/200u, {0u}}});
                     });

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.delete_changes.size());
    const auto& s = plan.delete_changes[0];
    EXPECT_FALSE(s.from_before_meta);
    EXPECT_FALSE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(1u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(1));
    // No column update on this output -> no insert-side change.
    EXPECT_TRUE(plan.insert_changes.empty());
}

// One carried segment hit by BOTH a whole-row delete and a column update this publish: the whole-row
// delete before values and the column-update before values are both from_before_meta + read_with_dcg over the
// same before position, so they merge into a SINGLE delete_changes entry whose rowids is the union.
TEST_F(ChangesConnectorTest, test_planner_same_segment_delete_and_column_update_union) {
    _keys_type = PRIMARY_KEYS;
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v1: a LOAD rowset id=100, 4 rows, no delvec.
    std::vector<RowsetSpec> r1 = {{.version = 1, .id = 100, .num_rows = 4, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/1, schema_id, /*ancestors=*/{}, &r1);
    // v2 carries 100 forward, deletes rowid 3 (delvec_after(100)={3}) AND column-updates rowid 1.
    std::vector<RowsetSpec> r2 = {
            {.version = 1, .id = 100, .num_rows = 4, .segment_path = r1[0].segment_path, .deleted_rows = {3}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2, Status::OK(),
                     [this](TabletMetadata* meta) {
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/100u, {1u}}});
                     });

    ASSIGN_OR_ABORT(auto before, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
    ASSIGN_OR_ABORT(auto after, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ChangesReadPlanner planner(_tablet_mgr, /*is_primary_keys=*/true, LakeIOOptions{});
    ASSIGN_OR_ABORT(auto plan, planner.plan_version_diff(before, after));

    ASSERT_EQ(1u, plan.delete_changes.size());
    const auto& s = plan.delete_changes[0];
    EXPECT_TRUE(s.from_before_meta);
    EXPECT_TRUE(s.read_with_dcg);
    EXPECT_EQ(0, s.rowset_pos);
    EXPECT_EQ(0, s.segment_pos);
    ASSERT_TRUE(s.rowids.has_value());
    EXPECT_EQ(2u, s.rowids->cardinality());
    EXPECT_TRUE(s.rowids->contains(1));
    EXPECT_TRUE(s.rowids->contains(3));
}

// ============================================================================
// Test 13 — PRIMARY KEYS storage-layer predicate pushdown. A data-column
// predicate is pushed into the PK segment read (opts.pred_tree), so rows are
// filtered inside the segment iterator rather than only post-read. Metadata-
// column predicates and mixed conjuncts must stay post-read (feeding them to
// the parser would build a ColumnPredicate on a column not in the tablet
// schema and crash the read).
// ============================================================================

// Append an integer `<slot> <opcode> <value>` binary-pred subtree (pred node,
// slot ref, literal) to `nodes` in prefix order. `prim_type` is the column's
// logical type, needed because __CHANGE_TYPE__ is TINYINT and __ROW_VERSION__
// BIGINT — types ExprsTestHelper's typed helpers do not all cover.
static void append_int_binary_pred(std::vector<TExprNode>* nodes, SlotId slot_id, TPrimitiveType::type prim_type,
                                   TExprOpcode::type opcode, int64_t value) {
    TTypeDesc ttype = gen_type_desc(prim_type);
    TExprNode pred;
    pred.node_type = TExprNodeType::BINARY_PRED;
    pred.num_children = 2;
    pred.__set_opcode(opcode);
    pred.__set_child_type(prim_type);
    pred.type = gen_type_desc(TPrimitiveType::BOOLEAN);

    TExprNode slot_ref;
    slot_ref.node_type = TExprNodeType::SLOT_REF;
    slot_ref.type = ttype;
    slot_ref.num_children = 0;
    slot_ref.__isset.slot_ref = true;
    slot_ref.slot_ref.slot_id = slot_id;
    slot_ref.slot_ref.tuple_id = 0;
    slot_ref.__set_is_nullable(true);

    TExprNode literal;
    literal.num_children = 0;
    literal.is_nullable = false;
    literal.type = ttype;
    literal.node_type = TExprNodeType::INT_LITERAL;
    TIntLiteral int_literal;
    int_literal.value = value;
    literal.__set_int_literal(int_literal);

    nodes->emplace_back(pred);
    nodes->emplace_back(slot_ref);
    nodes->emplace_back(literal);
}

// Build a single `<slot> <opcode> <value>` conjunct on one integer column.
static TExpr make_int_binary_pred_texpr(SlotId slot_id, TPrimitiveType::type prim_type, TExprOpcode::type opcode,
                                        int64_t value) {
    TExpr texpr;
    append_int_binary_pred(&texpr.nodes, slot_id, prim_type, opcode, value);
    return texpr;
}

// `left <opcode> right` over two slots. Two distinct slot ids mean it never reduces to a
// single-column ColumnPredicate (normalize needs a constant RHS; build_column_expr_predicates
// needs exactly one slot id), so it lands in get_not_push_down_conjuncts -> _residual_conjunct_ctxs
// and is evaluated post-read. Nothing reaches storage, so rows_vec_cond_filtered stays 0.
static TExpr make_two_slot_int_cmp_texpr(SlotId left_slot, SlotId right_slot, TExprOpcode::type opcode) {
    TTypeDesc int_type = gen_type_desc(TPrimitiveType::INT);
    TExprNode pred;
    pred.node_type = TExprNodeType::BINARY_PRED;
    pred.num_children = 2;
    pred.__set_opcode(opcode);
    pred.__set_child_type(TPrimitiveType::INT);
    pred.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    TExprNode lhs;
    lhs.node_type = TExprNodeType::SLOT_REF;
    lhs.type = int_type;
    lhs.num_children = 0;
    lhs.__isset.slot_ref = true;
    lhs.slot_ref.slot_id = left_slot;
    lhs.slot_ref.tuple_id = 0;
    lhs.__set_is_nullable(true);
    TExprNode rhs = lhs;
    rhs.slot_ref.slot_id = right_slot;
    TExpr texpr;
    texpr.nodes.emplace_back(pred); // prefix order: pred, lhs, rhs
    texpr.nodes.emplace_back(lhs);
    texpr.nodes.emplace_back(rhs);
    return texpr;
}

// Build one compound `(<data_slot> > <gt_value>) AND (<meta_slot> = <eq_value>)`
// conjunct: a single expression referencing both a data column and a metadata
// column, so the whole conjunct must stay post-read.
static TExpr make_and_data_gt_meta_eq_texpr(SlotId data_slot, int32_t gt_value, SlotId meta_slot,
                                            TPrimitiveType::type meta_type, int64_t eq_value) {
    TExprNode compound;
    compound.__set_node_type(TExprNodeType::COMPOUND_PRED);
    compound.__set_num_children(2);
    compound.__set_opcode(TExprOpcode::COMPOUND_AND);
    compound.__set_child_type(TPrimitiveType::BOOLEAN);
    compound.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    compound.__set_is_nullable(true);

    TExpr texpr;
    texpr.nodes.emplace_back(compound);
    append_int_binary_pred(&texpr.nodes, data_slot, TPrimitiveType::INT, TExprOpcode::GT, gt_value);
    append_int_binary_pred(&texpr.nodes, meta_slot, meta_type, TExprOpcode::EQ, eq_value);
    return texpr;
}

// A PRIMARY KEYS tablet with one 100-row segment (c0 = 0..99) inserted at v=2.
// Spanning a single wide segment lets a c0 > 50 predicate filter rows inside
// the segment iterator, so rows_vec_cond_filtered becomes > 0 only when the
// predicate is pushed down.
TEST_F(ChangesConnectorTest, test_pk_predicate_pushdown_filters_and_reports_stats) {
    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    CHECK_OK(ds->open(_runtime_state.get()));

    // c0 = 51..99 survive (49 rows), every surfaced row has c0 > 50, and the
    // segment iterator itself filtered rows (proves pushdown, not post-read).
    EXPECT_EQ(49, drain_and_expect_pushdown_filtered(ds.get(), c0_slot_id));
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Same PK multi-row fixture and c0 > 50 predicate as the test above, but with
// join-runtime-filter pushdown turned on and a real (empty) RuntimeFilterProbeCollector
// wired into the DataSource. ChunkPredicateBuilder::_get_column_predicates
// (be/src/exec/olap_scan_prepare.cpp) unconditionally iterates
// _opts.runtime_filters->descriptors() once enable_join_runtime_filter_pushdown() and
// is_olap_scan are both true, regardless of whether any predicate actually references a
// runtime filter slot. Before the fix, _build_pushdown_predicates() left
// ScanConjunctsManagerOptions::runtime_filters null, so this configuration dereferenced a
// null pointer and crashed every predicated CHANGES scan in production (the flag defaults
// to on). This test's runtime state carries the flag, unlike the rest of the suite, so it
// is the one that reaches that branch.
TEST_F(ChangesConnectorTest, test_pk_pushdown_with_join_runtime_filter_enabled) {
    TQueryOptions query_options;
    query_options.__set_enable_join_runtime_filter_pushdown(true);
    reset_runtime_state(query_options);
    ASSERT_TRUE(_runtime_state->enable_join_runtime_filter_pushdown());

    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    RuntimeFilterProbeCollector runtime_filters;
    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ds->set_runtime_filters(&runtime_filters);
    CHECK_OK(ds->open(_runtime_state.get()));

    // Same surviving rows as the flag-off test: c0 = 51..99 (49 rows). Reaching this
    // point without crashing is itself the regression check for the null dereference;
    // the row count and per-row predicate confirm the pushdown still filters correctly.
    EXPECT_EQ(49, drain_and_expect_pushdown_filtered(ds.get(), c0_slot_id));
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Same PK multi-row fixture and c0 > 50 predicate as the pushdown-stats test
// above, but asserts the pushdown-filtered row count is surfaced through the
// query profile rather than through the insert_read_stats() accessor. A
// RuntimeProfile is attached via set_runtime_profile() before open(), mirroring
// how the scan operator wires a DataSource in production; the base class hangs
// a "DataSource" child profile off it, and the pushdown counters registered by
// ChangesDataSource live on that child.
TEST_F(ChangesConnectorTest, test_predicate_pushdown_surfaces_profile_counters) {
    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    RuntimeProfile profile("test");
    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ds->set_runtime_profile(&profile);
    CHECK_OK(ds->open(_runtime_state.get()));
    drain(ds.get());
    ds->close(_runtime_state.get());

    auto* child = profile.get_child("DataSource");
    ASSERT_NE(nullptr, child);
    auto* pred_filter_counter = child->get_counter("PredFilterRows");
    ASSERT_NE(nullptr, pred_filter_counter);
    EXPECT_GT(pred_filter_counter->value(), 0);
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Constant-false/null conjunct (WHERE false / 1=0): open() fetches the published head metadata and
// initializes the tablet schema, then _init_pushdown_predicates detects the const-false conjunct and
// short-circuits to EndOfFile; close() must stay safe. The attached RuntimeProfile makes close()'s
// _update_counter() actually run, which guards that _init_counter() is called before the early return
// (otherwise it would touch null counters).
TEST_F(ChangesConnectorTest, test_const_false_predicate_short_circuit) {
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, /*rowsets=*/nullptr);

    // Constant boolean-false conjunct, modeling `WHERE false` / `1=0`.
    TExpr false_texpr;
    {
        TScalarType scalar_type;
        scalar_type.type = TPrimitiveType::BOOLEAN;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        type_node.__set_scalar_type(scalar_type);
        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);

        TBoolLiteral bool_literal;
        bool_literal.value = false;

        TExprNode node;
        node.node_type = TExprNodeType::BOOL_LITERAL;
        node.num_children = 0;
        node.type = type_desc;
        node.is_nullable = false;
        node.__set_bool_literal(bool_literal);

        false_texpr.nodes.push_back(node);
    }
    std::vector<TExpr> texprs{false_texpr};
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    RuntimeProfile profile("test");
    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ds->set_runtime_profile(&profile);

    // (a) open() short-circuits to EndOfFile on the constant-false predicate (evaluated while building
    //     the pushdown predicates).
    Status st = ds->open(_runtime_state.get());
    EXPECT_TRUE(st.is_end_of_file()) << st.to_string();

    // (b) _init_counter() ran before the early return: the counter close() touches is registered.
    auto* child = profile.get_child("DataSource");
    ASSERT_NE(nullptr, child);
    EXPECT_NE(nullptr, child->get_counter("RawRowsRead"));

    // (c) close() after the short-circuit is safe (non-null counters, no reader).
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A DUP_KEYS tablet with one 100-row segment (c0 = 0..99) inserted at v=2 —
// same shape as the PK stats test above, minus PRIMARY_KEYS. Before this
// change, _build_pushdown_predicates() short-circuited for non-PK tables and
// _build_segment_iterator's DUP/AGG branch never set opts.pred_tree, so
// rows_vec_cond_filtered stayed 0 and c0 > 50 was enforced only by the
// post-read conjunct backstop. Now the DUP/AGG branch consumes
// _pushdown_pred_tree the same way the PK branch does.
TEST_F(ChangesConnectorTest, test_dup_predicate_pushdown_filters_and_reports_stats) {
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    CHECK_OK(ds->open(_runtime_state.get()));

    // c0 = 51..99 survive (49 rows), same as the PK fixture above, now via the
    // DUP/AGG read path.
    EXPECT_EQ(49, drain_and_expect_pushdown_filtered(ds.get(), c0_slot_id));
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// An AGGREGATE-key tablet with one 100-row segment (c0 = 0..99, c1 = 0..99) inserted at v=2.
// c1 is a value column with a REPLACE aggregation, so OlapPredicateParser::can_pushdown rejects
// its predicate (a non-PK column is pushable only when its aggregation is NONE). The predicate
// still normalizes into a single-column ColumnPredicate, so it lands in the non-pushdown residual
// tree (_residual_pred_tree) rather than in _residual_conjunct_ctxs — the branch the other residual
// tests (two-slot / metadata-column conjuncts) never reach. The connector enforces it after the
// storage read: the rows are filtered correctly while rows_vec_cond_filtered stays 0 because the
// segment iterator saw no predicate.
TEST_F(ChangesConnectorTest, test_agg_non_pushable_column_predicate_filtered_by_residual_tree) {
    _keys_type = AGG_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c1_slot_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c1_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    CHECK_OK(ds->open(_runtime_state.get()));

    // c1 = 51..99 survive (49 rows); every surfaced row satisfies c1 > 50.
    std::vector<ChunkPtr> chunks;
    EXPECT_EQ(49, drain(ds.get(), &chunks));
    for (const auto& ch : chunks) {
        const auto* c1 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c1_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c1->get_data()[i], 50);
    }
    // The residual tree, not storage, filtered the rows: the segment iterator saw no predicate.
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_EQ(0, cds->insert_read_stats().rows_vec_cond_filtered);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Builds a RuntimeFilterProbeDescriptor carrying a hand-built bloom runtime filter on the given slot,
// typed to the slot's own logical type LT. Built per RF column's real logical type: a wrong type
// would abort in ScanConjunctsManager's type-dispatched RF builder (down_cast to the wrong
// MinMaxRuntimeFilter<Type>) before the filter ever reaches evaluation. The membership (bloom) bitset
// must be sized via init() before insert(), otherwise it stays empty and matches every probe (only the
// min/max component would filter) -> the row-level RF prunes nothing.
template <LogicalType LT>
RuntimeFilterProbeDescriptor* make_bloom_desc(ObjectPool* pool, RuntimeState* state, int32_t filter_id, SlotId slot,
                                              const std::vector<RunTimeCppType<LT>>& vals) {
    TRuntimeFilterDescription t;
    t.__set_filter_id(filter_id);
    t.__set_has_remote_targets(false);
    t.__set_build_plan_node_id(1);
    t.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    t.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<LT>(slot, true);
    t.__isset.plan_node_id_to_target_expr = true;
    t.plan_node_id_to_target_expr.emplace(1, col_ref);
    auto* desc = pool->add(new RuntimeFilterProbeDescriptor());
    CHECK_OK(desc->init(pool, t, /*node_id=*/1, state));
    auto* rf = pool->add(new ComposedRuntimeBloomFilter<LT>());
    rf->membership_filter().init(vals.size());
    for (const auto& v : vals) rf->insert(v);
    desc->set_runtime_filter(rf);
    return desc;
}

// Same DUP fixture and c0 > 50 predicate as test_dup_predicate_pushdown_filters_and_reports_stats
// above, but with join-runtime-filter pushdown turned on and two runtime filters wired in: one bloom
// filter on the data column c0 (values {60, 70}, both inside the WHERE-surviving range so the RF's
// own pruning is visible apart from the WHERE clause) and one bloom filter on the CHANGES metadata
// column __ROW_VERSION__. Rowset::read's DUP/AGG path now forwards runtime_filter_preds to the
// segment iterator, which evaluates them at read('SegmentIterator::_filter_by_non_expr_predicates');
// the c0 filter must reach that evaluation and prune rows, while the __ROW_VERSION__ filter must never
// reach OlapPredicateParser — __ROW_VERSION__ is appended after the segment read and is absent from
// _tablet_schema, so OlapPredicateParser::can_pushdown CHECK-fails if a probe slot ref resolves to it.
TEST_F(ChangesConnectorTest, test_dup_runtime_filter_prunes_and_excludes_metadata_column) {
    TQueryOptions query_options;
    query_options.__set_enable_join_runtime_filter_pushdown(true);
    reset_runtime_state(query_options);
    ASSERT_TRUE(_runtime_state->enable_join_runtime_filter_pushdown());

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    SlotId meta_slot_id = slot_id_of(tuple_id, kRowVersionColumnName);
    ASSERT_NE(-1, c0_slot_id);
    ASSERT_NE(-1, meta_slot_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    ObjectPool* rf_pool = _runtime_state->obj_pool();
    RuntimeFilterProbeCollector collector;
    collector.add_descriptor(make_bloom_desc<TYPE_INT>(rf_pool, _runtime_state.get(), 1, c0_slot_id, {60, 70}));
    // metadata-column RF (BIGINT __ROW_VERSION__): must be excluded from the storage path.
    collector.add_descriptor(make_bloom_desc<TYPE_BIGINT>(rf_pool, _runtime_state.get(), 2, meta_slot_id, {2}));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ds->set_runtime_filters(&collector);
    CHECK_OK(ds->open(_runtime_state.get())); // must not crash despite the metadata-column RF

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);
    EXPECT_EQ(2, total); // c0 > 50 -> 51..99, then RF{60,70} -> {60, 70}
    for (const auto& ch : chunks) {
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_TRUE(c0->get_data()[i] == 60 || c0->get_data()[i] == 70);
    }

    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    const auto& s = cds->insert_read_stats();
    // The data-column RF actually reached the storage layer and pruned rows there, not just at the
    // connector's post-read residual step (the metadata-column RF never contributes to these counters).
    ASSERT_GT(s.rf_cond_input_rows, 0);
    ASSERT_LT(s.rf_cond_output_rows, s.rf_cond_input_rows);

    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Same construction as test_dup_runtime_filter_prunes_and_excludes_metadata_column above, but on a
// PRIMARY_KEYS tablet, so the RF is asserted against the PK no-delvec read path
// (_build_segment_iterator's is_primary_key branch -> Rowset::get_each_segment_iterator_no_delvec).
// Before this change, get_each_segment_iterator_no_delvec dropped runtime_filter_preds and
// runtime_range_pruner when building SegmentReadOptions, so a PK CHANGES read never evaluated any
// runtime filter — rf_cond_input_rows stayed 0 regardless of what was wired into the collector.
TEST_F(ChangesConnectorTest, test_pk_runtime_filter_prunes) {
    TQueryOptions query_options;
    query_options.__set_enable_join_runtime_filter_pushdown(true);
    reset_runtime_state(query_options);
    ASSERT_TRUE(_runtime_state->enable_join_runtime_filter_pushdown());

    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);

    // A pushdown WHERE predicate is required so the read reaches
    // SegmentIterator::_filter_by_non_expr_predicates at all (empty non_expr_pred_tree short-circuits
    // that whole step on a no-delvec PK read, and the RF conditions are evaluated inside it). c0 > 5
    // keeps 94 of the 100 rows so RF{10, 20} narrows further and its own contribution to
    // rf_cond_input_rows/rf_cond_output_rows is visible apart from the WHERE clause.
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/5));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    ObjectPool* rf_pool = _runtime_state->obj_pool();
    RuntimeFilterProbeCollector collector;
    collector.add_descriptor(make_bloom_desc<TYPE_INT>(rf_pool, _runtime_state.get(), 1, c0_slot_id, {10, 20}));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ds->set_runtime_filters(&collector);
    CHECK_OK(ds->open(_runtime_state.get())); // must not crash

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);
    EXPECT_EQ(2, total); // c0 > 5 -> 6..99, then RF{10, 20} -> {10, 20}
    for (const auto& ch : chunks) {
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_TRUE(c0->get_data()[i] == 10 || c0->get_data()[i] == 20);
    }

    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    const auto& s = cds->insert_read_stats();
    // The RF actually reached the storage layer on the PK no-delvec read path and pruned rows there,
    // not just at the connector's post-read residual step.
    ASSERT_GT(s.rf_cond_input_rows, 0);
    ASSERT_LT(s.rf_cond_output_rows, s.rf_cond_input_rows);

    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Same DUP fixture and bloom filter on c0 as test_dup_runtime_filter_prunes_and_excludes_metadata_column
// above, but with NO WHERE conjunct at all (set_predicates is never called). Before this change,
// _init_pushdown_predicates::_conjunct_ctxs.empty() early-returned before the RF storage objects were
// ever built, so a pure join / ORDER BY ... LIMIT CHANGES query with a runtime filter but no data-column
// WHERE clause got no storage-level RF skip (rf_cond_input_rows stayed 0). This asserts the RF machinery
// now builds whenever there is a pushable runtime filter, independent of WHERE conjuncts.
TEST_F(ChangesConnectorTest, test_runtime_filter_prunes_without_where_conjunct) {
    TQueryOptions query_options;
    query_options.__set_enable_join_runtime_filter_pushdown(true);
    reset_runtime_state(query_options);

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id); // DUP
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);

    RuntimeFilterProbeCollector collector;
    collector.add_descriptor(
            make_bloom_desc<TYPE_INT>(_runtime_state->obj_pool(), _runtime_state.get(), 1, c0_slot_id, {60, 70}));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    // Deliberately NO set_predicates(): this is the pure-RF, no-WHERE case that the old
    // _conjunct_ctxs.empty() early-return skipped.
    ds->set_runtime_filters(&collector);
    CHECK_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    drain(ds.get(), &chunks);

    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    const auto& s = cds->insert_read_stats();
    ASSERT_GT(s.rf_cond_input_rows, 0); // storage RF fired WITHOUT a WHERE conjunct
    ASSERT_LT(s.rf_cond_output_rows, s.rf_cond_input_rows);
    ds->close(_runtime_state.get());
}

// c1 is referenced only by a PUSHED-DOWN predicate (c1 > 50) and is not projected
// (isOutputColumn=false). It must be read and filtered at storage, but dropped from
// the output chunk by init_output_schema — the surfaced chunk holds exactly c0.
TEST_F(ChangesConnectorTest, test_pk_predicate_only_column_dropped_from_output) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::DATA_ONLY, /*include_data=*/true,
                                             /*c1_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}}; // c1==c0==0..99
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_id = slot_id_of(tuple_id, "c0");
    SlotId c1_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c0_id);
    ASSERT_NE(-1, c1_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    EXPECT_EQ(49, total); // c1 > 50 -> 51..99
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_TRUE(ch->is_slot_exist(c0_id));
        EXPECT_FALSE(ch->is_slot_exist(c1_id));
        EXPECT_EQ(1u, ch->num_columns());
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 50);
    }
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0); // c1 read + pushdown-filtered
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Same as test_pk_predicate_only_column_dropped_from_output, but on a DUP_KEYS
// tablet so the drop is exercised on the Rowset::read path rather than the PK
// no-delvec path.
TEST_F(ChangesConnectorTest, test_dup_predicate_only_column_dropped_from_output) {
    _keys_type = DUP_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::DATA_ONLY, /*include_data=*/true,
                                             /*c1_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}}; // c1==c0==0..99
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_id = slot_id_of(tuple_id, "c0");
    SlotId c1_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c0_id);
    ASSERT_NE(-1, c1_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    EXPECT_EQ(49, total); // c1 > 50 -> 51..99
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_TRUE(ch->is_slot_exist(c0_id));
        EXPECT_FALSE(ch->is_slot_exist(c1_id));
        EXPECT_EQ(1u, ch->num_columns());
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 50);
    }
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0); // c1 read + pushdown-filtered
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A metadata-only projection (SELECT __ROW_VERSION__) whose sole data column c0 is
// referenced only by a pushed-down predicate (c0 > 50, isOutputColumn=false).
// Dropping every read-schema column would leave the segment read with an empty
// output schema, whose chunks report zero rows and would silently drop all changes.
// One column must stay materialized to carry the row count, so the 49 matching
// changes still surface with their __ROW_VERSION__ metadata.
TEST_F(ChangesConnectorTest, test_metadata_only_projection_with_all_data_columns_pushed_down) {
    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::ROW_VERSION_ONLY, /*include_data=*/true,
                                             /*c1_is_output=*/true, /*c0_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_id = slot_id_of(tuple_id, "c0");
    SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
    ASSERT_NE(-1, c0_id);
    ASSERT_NE(-1, rv_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    EXPECT_EQ(49, total); // c0 > 50 -> 51..99, no silent row loss
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_TRUE(ch->is_slot_exist(rv_id));
        const auto* rv = as_int64(ch->get_column_by_slot_id(rv_id));
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_EQ(2, rv->get_data()[i]);
    }
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0); // c0 read + pushdown-filtered
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A DUP_KEYS tablet whose sort key c0 spans several short-key blocks (segment
// writer indexes one short-key entry per 100 rows under BE_TEST), with a range
// predicate that excludes most of them. The connector derives c0 as the sort-key
// column, ScanConjunctsManager builds a c0 > 400 scan-key range, and parse_seek_range
// turns it into a SeekRange fed to the DUP/AGG read — so the short-key seek skips
// whole blocks before any column is read, surfacing as rows_key_range_filtered > 0.
// PK reads exact rowids and derives no ranges, so this narrowing is DUP/AGG-only.
//
// enable_short_key_for_one_column_filter must be on for a single-key-column filter to
// build a scan key at all (build_scan_keys otherwise skips one-column filters). It is a
// production toggle, not a test contrivance: a one-column sort-key CHANGES scan relies on
// it exactly as an operator would; the wiring under test (derive names -> get_key_ranges ->
// parse_seek_range -> opts.ranges) is identical regardless of how many key columns exist.
TEST_F(ChangesConnectorTest, test_dup_short_key_range_narrows_read) {
    bool saved = config::enable_short_key_for_one_column_filter;
    config::enable_short_key_for_one_column_filter = true;
    DeferOp restore([&] { config::enable_short_key_for_one_column_filter = saved; });

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // One 500-row segment, c0 = 0..499 ascending. Short-key entries sit at rowids
    // 0,100,...,400, so c0 > 400 lets the seek drop the first four 100-row blocks.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 500, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/400));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    CHECK_OK(ds->open(_runtime_state.get()));
    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    // c0 = 401..499 survive (99 rows); every surfaced row satisfies c0 > 400.
    EXPECT_EQ(99, total);
    for (const auto& ch : chunks) {
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 400);
    }
    // The short-key seek skipped the excluded blocks before reading columns.
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_key_range_filtered, 0);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// The PK analogue of test_dup_short_key_range_narrows_read: short-key range
// narrowing now runs for PRIMARY KEYS too. A bulk-insert window fills a whole
// new segment, so its changed rowids are the entire segment (0..499) and the
// per-segment rowid range is not sparse. The connector derives c0 as the
// sort-key column, builds a c0 > 400 scan-key range, and feeds it as opts.ranges
// to the PK read; the segment iterator intersects that short-key window with the
// whole-segment rowid range, so the seek skips the first four 100-row blocks
// before reading columns, surfacing as rows_key_range_filtered > 0 — exactly the
// value the DUP read gets on the same shape.
//
// enable_short_key_for_one_column_filter is on for the same reason as the DUP
// test: c0 is the only sort-key column, and build_scan_keys otherwise skips a
// one-column filter, so no scan key (and no seek range) would be built at all.
TEST_F(ChangesConnectorTest, test_pk_short_key_range_narrows_read) {
    _keys_type = PRIMARY_KEYS;
    bool saved = config::enable_short_key_for_one_column_filter;
    config::enable_short_key_for_one_column_filter = true;
    DeferOp restore([&] { config::enable_short_key_for_one_column_filter = saved; });

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // One 500-row segment inserted at v=2, c0 = 0..499 ascending, no delete vector.
    // The PK insert-change read selects the whole segment (rowids 0..499); short-key
    // entries sit at rowids 0,100,...,400, so c0 > 400 lets the seek drop the first
    // four 100-row blocks.
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 500, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    ASSERT_NE(-1, c0_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c0_slot_id, /*gt=*/400));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    CHECK_OK(ds->open(_runtime_state.get()));
    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    // c0 = 401..499 survive (99 rows); every surfaced row satisfies c0 > 400.
    EXPECT_EQ(99, total);
    for (const auto& ch : chunks) {
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 400);
    }
    // The short-key seek skipped the excluded blocks before reading columns.
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_key_range_filtered, 0);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A predicate on a CHANGES metadata column (__CHANGE_TYPE__) stays post-read:
// the column is appended after the read, not in the tablet schema, so it must
// never reach the parser. The result is filtered correctly and no rows are
// filtered at the storage layer (rows_vec_cond_filtered == 0), proving the
// conjunct did not slip into the pushdown path (which would crash).
TEST_F(ChangesConnectorTest, test_pk_metadata_predicate_stays_post_read) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: base segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    // v=3: overlay c1 on rowids {1,2} -> DELETE(before)+INSERT(after) for k101, k102.
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 21, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1, 2}}});
                     });

    // WHERE __CHANGE_TYPE__ = 1 (DELETE). __CHANGE_TYPE__ is TINYINT.
    SlotId ct_slot_id = slot_id_of(tuple_id, kChangeTypeColumnName);
    ASSERT_NE(-1, ct_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(make_int_binary_pred_texpr(ct_slot_id, TPrimitiveType::TINYINT, TExprOpcode::EQ, /*=*/1));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_EQ(0, cds->insert_read_stats().rows_vec_cond_filtered);
    EXPECT_EQ(0, cds->delete_read_stats().rows_vec_cond_filtered);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());

    // Only the two DELETE (before) rows survive the post-read filter.
    std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*before c1=*/11, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/102, /*before c1=*/12, /*DELETE*/ 1, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// A single compound conjunct `c0 > 50 AND __ROW_VERSION__ = v` references both a
// data and a metadata column, so the whole conjunct stays post-read (the split
// is per-conjunct, not per-leaf). Result is filtered correctly; nothing pushed
// to storage.
TEST_F(ChangesConnectorTest, test_pk_mixed_predicate_post_read) {
    _keys_type = PRIMARY_KEYS;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    SlotId rv_slot_id = slot_id_of(tuple_id, kRowVersionColumnName);
    ASSERT_NE(-1, c0_slot_id);
    ASSERT_NE(-1, rv_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(make_and_data_gt_meta_eq_texpr(c0_slot_id, /*gt=*/50, rv_slot_id, TPrimitiveType::BIGINT,
                                                       /*__ROW_VERSION__=*/2));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));
    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    // c0 in [0..99] at v=2: c0 > 50 keeps 51..99 (49), __ROW_VERSION__ = 2 keeps all.
    EXPECT_EQ(49, total);
    for (const auto& ch : chunks) {
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_slot_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_GT(c0->get_data()[i], 50);
    }
    // The whole conjunct stayed post-read (references a metadata slot); nothing filtered at storage.
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_EQ(0, cds->insert_read_stats().rows_vec_cond_filtered);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A column-updated row: the pushdown predicate on the value column c1 sees the
// overlaid value on the INSERT (after) side and the pre-overlay value on the
// DELETE (before) side, because the pred_tree is applied over each side's own
// segment read (after-side reads the dcg overlay, before-side reads raw). A
// c1 > 50 predicate matches only the after value.
TEST_F(ChangesConnectorTest, test_pk_dcg_column_predicate_uses_overlaid_value) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: base segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    // v=3: overlay c1 on rowid {1} raising 11 -> 100. DELETE(before c1=11)+INSERT(after c1=100).
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 100, 12, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1}}});
                     });

    // WHERE c1 > 50 — a pushdown-eligible data-column predicate.
    SlotId c1_slot_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c1_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());

    // Only the INSERT (after) row for k101 has c1 = 100 > 50; the DELETE (before)
    // row carried c1 = 11 and is filtered out by the before-side read's predicate.
    std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*after c1=*/100, /*INSERT*/ 0, /*v=*/3}};
    EXPECT_EQ(expected, rows);
}

// A predicate-only value column c1 (isOutputColumn=false) whose base segment
// values are overlaid by a later column update. init_output_schema drops c1 from
// the segment read's OUTPUT projection, but the dcg overlay and the pushed-down
// c1 > 50 filter both run inside the segment iterator, ahead of that projection —
// so the dropped c1 is still overlaid, then filtered on its overlaid value.
// Rowids {1,2} have base c1 {11,12} (< 50) raised to {100,99} (> 50): their
// INSERT (after) rows survive ONLY because the overlay was applied during the
// read. Rowid 3 is overlaid to 20 (< 50) and filtered at storage on the after
// side, so the insert read reports a vectorized filter. c1 must not surface.
TEST_F(ChangesConnectorTest, test_pk_pushdown_only_column_with_dcg_overlay_dropped) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE, /*include_data=*/true,
                                             /*c1_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // v=2: base segment S (id=10), c0 in [100..103], c1 = [10,11,12,13].
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    // v=3: overlay c1 on rowids {1,2,3}, raising 11->100 and 12->99 (both cross the
    // c1 > 50 threshold) and 13->20 (stays below). Each updated rowid yields a
    // DELETE(before, raw c1) + INSERT(after, overlaid c1).
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 100, 99, 20});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1, 2, 3}}});
                     });

    SlotId c0_id = slot_id_of(tuple_id, "c0");
    SlotId c1_id = slot_id_of(tuple_id, "c1");
    SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
    SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
    ASSERT_NE(-1, c0_id);
    ASSERT_NE(-1, c1_id);
    ASSERT_NE(-1, ct_id);
    ASSERT_NE(-1, rv_id);

    // WHERE c1 > 50 — pushdown-eligible on the value column, referenced only here.
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    // Only the two overlaid INSERT (after) rows pass c1 > 50 (overlaid 100, 99);
    // all three DELETE (before) rows carry the base c1 (11, 12, 13) and are
    // filtered out, and the third INSERT (overlaid 20) is filtered too.
    EXPECT_EQ(2, total);
    ASSERT_FALSE(chunks.empty());
    std::vector<ChangeRow> rows;
    for (const auto& ch : chunks) {
        EXPECT_TRUE(ch->is_slot_exist(c0_id));
        EXPECT_FALSE(ch->is_slot_exist(c1_id)); // dropped by init_output_schema
        EXPECT_TRUE(ch->is_slot_exist(ct_id));
        EXPECT_TRUE(ch->is_slot_exist(rv_id));
        EXPECT_EQ(3u, ch->num_columns()); // exactly c0 + __CHANGE_TYPE__ + __ROW_VERSION__
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_id).get());
        const auto* ct = as_int8(ch->get_column_by_slot_id(ct_id));
        const auto* rv = as_int64(ch->get_column_by_slot_id(rv_id));
        for (size_t i = 0; i < ch->num_rows(); i++) {
            rows.push_back({c0->get_data()[i], ct->get_data()[i], rv->get_data()[i]});
        }
    }
    std::sort(rows.begin(), rows.end());
    // Survivors are exactly the two overlaid INSERT rows (k101, k102). Their base
    // c1 (11, 12) is < 50: were the overlay not applied during the pushdown read,
    // the after side would filter on the base values and surface zero rows.
    std::vector<ChangeRow> expected = {{/*c0=*/101, /*INSERT*/ 0, /*v=*/3}, {/*c0=*/102, /*INSERT*/ 0, /*v=*/3}};
    EXPECT_EQ(expected, rows);

    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0); // c1 read + pushdown-filtered at storage
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// A pushdown predicate on the value column c1 (not the key c0) filters at the
// storage layer: c1 is a scan slot in the read schema, read, and filtered inside
// the segment iterator. The surfaced key c0 reflects c1's filter — only rows
// whose c1 passed survive.
TEST_F(ChangesConnectorTest, test_pk_predicate_only_column) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    // v=2: 100 rows, c0 = 0..99, c1 = 0..99 (write_segment fills c1 = start_value + rowid).
    std::vector<RowsetSpec> r2 = {{.version = 2, .id = 10, .num_rows = 100, .start_value = 0}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    // WHERE c1 > 50, where c1 is the non-key value column, distinct from the key c0.
    SlotId c0_slot_id = slot_id_of(tuple_id, "c0");
    SlotId c1_slot_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c0_slot_id);
    ASSERT_NE(-1, c1_slot_id);
    std::vector<TExpr> texprs;
    texprs.emplace_back(ExprsTestHelper::create_binary_pred_texpr<TYPE_INT, int32_t>(c1_slot_id, /*gt=*/50));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);

    // c1 = 51..99 survive (49 rows); each row's c1 (== c0) is > 50.
    EXPECT_EQ(49u, rows.size());
    for (const auto& r : rows) {
        EXPECT_GT(r.c1, 50);
        EXPECT_EQ(r.c0, r.c1); // write_segment set c1 = c0 for these rows
    }
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_GT(cds->insert_read_stats().rows_vec_cond_filtered, 0);
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// c0 < c1 stays residual (two-slot comparison, never pushed down). c1 is read and used for
// filtering but not projected (isOutputColumn=false); init_output_schema does NOT drop it (it is
// residual), so the connector's post-eval narrowing must, leaving exactly c0.
TEST_F(ChangesConnectorTest, test_pk_residual_only_data_column_dropped) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::DATA_ONLY, /*include_data=*/true,
                                             /*c1_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2,
                                   .id = 10,
                                   .num_rows = 10,
                                   .start_value = 0,
                                   .c1_values = std::vector<int32_t>(10, 5)}}; // c0=0..9, c1=5
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);

    SlotId c0_id = slot_id_of(tuple_id, "c0");
    SlotId c1_id = slot_id_of(tuple_id, "c1");
    ASSERT_NE(-1, c0_id);
    ASSERT_NE(-1, c1_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(make_two_slot_int_cmp_texpr(c0_id, c1_id, TExprOpcode::LT)); // c0 < c1
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/1, /*head=*/2));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    EXPECT_EQ(5, total); // c0 < 5 keeps c0 in {0,1,2,3,4}
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_FALSE(ch->is_slot_exist(c1_id));
        EXPECT_EQ(1u, ch->num_columns());
        const auto* c0 = down_cast<const Int32Column*>(ch->get_column_by_slot_id(c0_id).get());
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_LT(c0->get_data()[i], 5);
    }
    auto* cds = down_cast<connector::ChangesDataSource*>(ds.get());
    EXPECT_EQ(0, cds->insert_read_stats().rows_vec_cond_filtered); // residual, not storage-filtered
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// WHERE __CHANGE_TYPE__ = 1 is a metadata-touching residual: __CHANGE_TYPE__ is appended after the
// read so the residual can be evaluated, but it is non-output and must not be surfaced; the
// projected __ROW_VERSION__ stays.
TEST_F(ChangesConnectorTest, test_predicate_only_meta_column_dropped) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE, /*include_data=*/true,
                                             /*c1_is_output=*/true, /*c0_is_output=*/true,
                                             /*change_type_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 21, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1, 2}}});
                     });

    SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
    SlotId rv_id = slot_id_of(tuple_id, kRowVersionColumnName);
    ASSERT_NE(-1, ct_id);
    ASSERT_NE(-1, rv_id);

    std::vector<TExpr> texprs;
    texprs.emplace_back(make_int_binary_pred_texpr(ct_id, TPrimitiveType::TINYINT, TExprOpcode::EQ, /*=*/1));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    EXPECT_EQ(2, total); // two DELETE (before) rows survive __CHANGE_TYPE__ = 1
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_FALSE(ch->is_slot_exist(ct_id));
        EXPECT_TRUE(ch->is_slot_exist(rv_id));
        const auto* rv = as_int64(ch->get_column_by_slot_id(rv_id));
        for (size_t i = 0; i < ch->num_rows(); i++) EXPECT_EQ(3, rv->get_data()[i]);
    }
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// Every surfaced slot is predicate-only (the sole slot __CHANGE_TYPE__ is non-output and referenced
// only by WHERE __CHANGE_TYPE__ = 1). Narrowing to an empty output chunk would report zero rows and
// silently drop every change; the connector must instead surface the post-eval chunk with its row
// count intact, exactly as a non-CHANGES scan leaves its predicate-only columns in place. Models the
// FE plan `SELECT <const> ... WHERE <predicate-only column>`.
TEST_F(ChangesConnectorTest, test_all_columns_non_output_preserves_row_count) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;
    auto tuple_id = install_tuple_descriptor(TupleShape::CHANGE_TYPE_ONLY, /*include_data=*/false,
                                             /*c1_is_output=*/true, /*c0_is_output=*/true,
                                             /*change_type_is_output=*/false);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);
    std::vector<RowsetSpec> r2 = {
            {.version = 2, .id = 10, .num_rows = 4, .start_value = 100, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/2, schema_id, /*ancestors=*/{1}, &r2);
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 4, .segment_path = r2[0].segment_path, .c1_values = {10, 11, 12, 13}}};
    publish_metadata(tablet_id, /*version=*/3, schema_id, /*ancestors=*/{2}, &r3, Status::OK(),
                     [&](TabletMetadata* meta) {
                         attach_dcg(meta, /*segment_rssid=*/10, /*overlaid_c1=*/{10, 21, 22, 13});
                         attach_cdc_delvecs(meta, CdcCaptureMap::COLUMN_OVERLAY, {{/*rssid=*/10, {1, 2}}});
                     });

    SlotId ct_id = slot_id_of(tuple_id, kChangeTypeColumnName);
    ASSERT_NE(-1, ct_id);
    ASSERT_EQ(-1, slot_id_of(tuple_id, kRowVersionColumnName));

    std::vector<TExpr> texprs;
    texprs.emplace_back(make_int_binary_pred_texpr(ct_id, TPrimitiveType::TINYINT, TExprOpcode::EQ, /*=*/1));
    std::vector<ExprContext*> conjunct_ctxs;
    CHECK_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(_runtime_state->obj_pool(), _runtime_state.get(), &texprs,
                                                            &conjunct_ctxs));

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ds->set_predicates(conjunct_ctxs);
    ASSERT_OK(ds->open(_runtime_state.get()));

    std::vector<ChunkPtr> chunks;
    int64_t total = drain(ds.get(), &chunks);

    // The two DELETE rows survive __CHANGE_TYPE__ = 1 and are not silently dropped despite no output column.
    EXPECT_EQ(2, total);
    ASSERT_FALSE(chunks.empty());
    for (const auto& ch : chunks) {
        EXPECT_GT(ch->num_rows(), 0u);
    }
    ds->close(_runtime_state.get());
    ExprExecutor::close(conjunct_ctxs, _runtime_state.get());
}

// ============================================================================
// A CHANGES range that spans a light DROP COLUMN must read each rowset against
// the scan (head) schema, not the rowset's own historical schema. The base
// rowset is written under S1 = [c0, cX, c1] (c1 at ordinal 2); the head drops cX
// so the scan schema is S2 = [c0, c1] (c1 at ordinal 1). A primary-key
// before-value (DELETE) read of that base rowset goes through the no-delvec path,
// which must forward the scan tablet_schema down to the segment iterator —
// otherwise column ids resolve by ordinal against S1 and the c1 output reads cX's
// data. Regression test for that path dropping opts.tablet_schema.
// ============================================================================
TEST_F(ChangesConnectorTest, test_pk_before_value_read_uses_scan_schema_across_dropped_column) {
    _keys_type = PRIMARY_KEYS;
    _with_c1 = true;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t base_schema_id = next_id();
    int64_t head_schema_id = next_id();
    int64_t tablet_id = next_id();

    // Base v2 under S1 = [c0, cX, c1]. cX and c1 hold disjoint value ranges so the
    // surfaced value reveals which physical column the read actually resolved.
    _with_cx = true;
    initialize_tablet(tablet_id, base_schema_id);
    std::vector<RowsetSpec> r2 = {{.version = 2,
                                   .id = 10,
                                   .num_rows = 5,
                                   .start_value = 100,
                                   .c1_values = {500, 501, 502, 503, 504},
                                   .cX_values = {900, 901, 902, 903, 904}}};
    publish_metadata(tablet_id, /*version=*/2, base_schema_id, /*ancestors=*/{1}, &r2);
    std::string seg_path = r2[0].segment_path;

    // Head v3 under S2 = [c0, c1] (cX dropped) deletes rowids {1, 3} of the base
    // segment, so (base=2, head=3) surfaces their before values as DELETEs.
    _with_cx = false;
    std::vector<RowsetSpec> r3 = {
            {.version = 2, .id = 10, .num_rows = 5, .segment_path = seg_path, .deleted_rows = {1, 3}}};
    publish_metadata(tablet_id, /*version=*/3, head_schema_id, /*ancestors=*/{2}, &r3);

    auto provider = make_provider(tuple_id, head_schema_id);
    auto ds = provider->create_data_source(make_scan_range(tablet_id, /*base=*/2, /*head=*/3));
    ASSERT_OK(ds->open(_runtime_state.get()));
    auto rows = collect_change_rows_with_c1(ds.get(), tuple_id);
    ds->close(_runtime_state.get());

    // With the scan schema forwarded, the DELETE before-values carry c1 (501, 503).
    // Resolving against the base rowset's own S1 would surface cX (901, 903) at the
    // same ordinal instead.
    std::vector<ChangeRowC1> expected = {{/*c0=*/101, /*c1=*/501, /*DELETE*/ 1, /*v=*/3},
                                         {/*c0=*/103, /*c1=*/503, /*DELETE*/ 1, /*v=*/3}};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, rows);
}

// ============================================================================
// Reshard boundary. A tablet id created by a split / merge at reshard version S
// has no metadata below S, so its metadata at S records an EMPTY ancestor chain.
// The two tests below pin the connector behavior a per-generation CDC dispatch
// relies on: a scan whose base is exactly S terminates at S without reading
// below it, and a scan misdispatched with base < S fails CLASSIFIED so the
// frontend treats it as a planning error rather than a transient read failure.
// ============================================================================

TEST_F(ChangesConnectorTest, test_reshard_new_tablet_base_at_reshard_version_stops_clean) {
    constexpr int64_t kReshardVersion = 5;
    constexpr int64_t kInheritedRows = 6;
    constexpr int64_t kNewRows = 3;

    auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    initialize_tablet(tablet_id, schema_id);

    // The reshard-written metadata at S: rowsets inherited from the pre-reshard
    // source (stamped at their original versions) and no ancestor chain.
    std::vector<RowsetSpec> at_reshard = {{.version = 3, .id = 100, .num_rows = kInheritedRows}};
    publish_metadata(tablet_id, /*version=*/kReshardVersion, schema_id, /*ancestors=*/{}, &at_reshard);
    // The first normal publish on the new id, whose direct parent is S.
    std::vector<RowsetSpec> at_head = {
            {.version = 3, .id = 100, .num_rows = kInheritedRows, .segment_path = at_reshard[0].segment_path},
            {.version = kReshardVersion + 1, .id = 101, .num_rows = kNewRows}};
    publish_metadata(tablet_id, /*version=*/kReshardVersion + 1, schema_id, /*ancestors=*/{kReshardVersion}, &at_head);

    auto provider = make_provider(tuple_id, schema_id);
    auto ds = provider->create_data_source(
            make_scan_range(tablet_id, /*base=*/kReshardVersion, /*head=*/kReshardVersion + 1));
    ASSERT_OK(ds->open(_runtime_state.get()));
    // Exactly the S+1 changes: the walk stops at S, so the inherited rowset -- which
    // is already present at base -- never surfaces, and the empty chain at S is never
    // consulted.
    EXPECT_EQ(kNewRows, drain(ds.get()));
    ds->close(_runtime_state.get());
}

TEST_F(ChangesConnectorTest, test_reshard_new_tablet_base_below_reshard_version_is_classified) {
    constexpr int64_t kReshardVersion = 5;
    constexpr int64_t kInheritedRows = 6;
    constexpr int64_t kNewRows = 3;

    // Same fixture as the clean-stop case above, published for both key types: the
    // classification must not depend on the primary-key CDC gate.
    auto run = [&](KeysType keys_type) {
        _keys_type = keys_type;
        auto tuple_id = install_tuple_descriptor(TupleShape::BOTH_NON_NULLABLE);
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        initialize_tablet(tablet_id, schema_id);
        std::vector<RowsetSpec> at_reshard = {{.version = 3, .id = 100, .num_rows = kInheritedRows}};
        publish_metadata(tablet_id, /*version=*/kReshardVersion, schema_id, /*ancestors=*/{}, &at_reshard);
        std::vector<RowsetSpec> at_head = {
                {.version = 3, .id = 100, .num_rows = kInheritedRows, .segment_path = at_reshard[0].segment_path},
                {.version = kReshardVersion + 1, .id = 101, .num_rows = kNewRows}};
        publish_metadata(tablet_id, /*version=*/kReshardVersion + 1, schema_id, /*ancestors=*/{kReshardVersion},
                         &at_head);

        // Misdispatch: base sits one version BELOW the reshard, so the walk reaches S
        // and finds no way down. Contrast test_parent_metadata_read_failure_stays_
        // unclassified: had the reshard left an inherited ancestor on this id, the walk
        // would instead read a version that does not exist under it and surface a bare
        // NotFound, which the frontend reads as transient and retries.
        auto provider = make_provider(tuple_id, schema_id);
        auto ds = provider->create_data_source(
                make_scan_range(tablet_id, /*base=*/kReshardVersion - 1, /*head=*/kReshardVersion + 1));
        ASSERT_OK(ds->open(_runtime_state.get()));
        Status st = drain_until_error(ds.get());
        expect_change_not_trackable(st, "cannot reach base");
        ds->close(_runtime_state.get());
    };

    run(DUP_KEYS);
    run(PRIMARY_KEYS);
    _keys_type = DUP_KEYS;
}

} // namespace starrocks::connector
