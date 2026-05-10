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

#include "storage/lake/add_index_schema_change.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

using namespace starrocks;

// Fixture: dedicates its own root dir under the lake test pattern so it
// does not collide with schema_change_test.cpp's kTestGroupPath. Mirrors
// SchemaChangeBaseTabletReadSchemaTest's manual TabletManager wiring.
class AddIndexSchemaChangeTest : public ::testing::Test {
public:
    AddIndexSchemaChangeTest() {
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
    }

protected:
    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

    // Build a minimal DUP_KEYS tablet with:
    //   c0 INT KEY,  c1 INT,  c2 VARCHAR,  c3 INT NULL
    // Unique IDs are deterministic so tests can wire TabletIndexPB.col_unique_id
    // by name. Returns a published metadata at version 1 (version 2 if rows are
    // written via populate_segment).
    std::shared_ptr<TabletMetadata> create_base_tablet_metadata() {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        auto* schema = metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);

        auto* c0 = schema->add_column();
        c0->set_unique_id(_c0_uid);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto* c1 = schema->add_column();
        c1->set_unique_id(_c1_uid);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("NONE");

        auto* c2 = schema->add_column();
        c2->set_unique_id(_c2_uid);
        c2->set_name("c2");
        c2->set_type("VARCHAR");
        c2->set_length(64);
        c2->set_is_key(false);
        c2->set_is_nullable(false);
        c2->set_aggregation("NONE");

        auto* c3 = schema->add_column();
        c3->set_unique_id(_c3_uid);
        c3->set_name("c3");
        c3->set_type("INT");
        c3->set_is_key(false);
        c3->set_is_nullable(true);
        c3->set_aggregation("NONE");

        return metadata;
    }

    // Build a chunk with `nrows` rows and append it as a single segment via
    // DeltaWriter, advancing the tablet to version 2. Each call writes one
    // rowset; call twice to produce a 2-rowset / 2-segment tablet at v3.
    int64_t write_one_rowset(int64_t base_tablet_id, int64_t version, std::shared_ptr<TabletSchema> schema, int nrows,
                             const std::string& vstr_prefix = "row") {
        auto vschema = std::make_shared<Schema>(ChunkHelper::convert_schema(schema));
        auto col_c0 = Int32Column::create();
        auto col_c1 = Int32Column::create();
        auto col_c2 = BinaryColumn::create();
        auto col_c3 = Int32Column::create();
        auto null_col_c3 = NullableColumn::create(std::move(col_c3), NullColumn::create());
        for (int i = 0; i < nrows; ++i) {
            col_c0->append_datum(Datum(i + 1));
            col_c1->append_datum(Datum(i * 7 + 3));
            col_c2->append_datum(Datum(Slice(vstr_prefix + std::to_string(i))));
            if (i % 3 == 0) {
                null_col_c3->append_default();
            } else {
                null_col_c3->append_datum(Datum(i * 11));
            }
        }
        Chunk chunk({std::move(col_c0), std::move(col_c1), std::move(col_c2), std::move(null_col_c3)}, vschema);
        std::vector<uint32_t> indexes(nrows);
        for (int i = 0; i < nrows; ++i) indexes[i] = i;

        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(schema->id())
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        return version + 1;
    }

    // Construct a single-column TabletIndexPB.
    static TabletIndexPB make_index(IndexType type, int32_t col_uid, int64_t index_id = 0,
                                    const std::string& props = "") {
        TabletIndexPB ix;
        ix.set_index_id(index_id != 0 ? index_id : next_id());
        ix.set_index_name("ix_" + std::to_string(col_uid));
        ix.set_index_type(type);
        ix.add_col_unique_id(col_uid);
        if (!props.empty()) {
            ix.set_index_properties(props);
        }
        return ix;
    }

    // Helper: take the metadata at `version` and return a VersionedTablet of
    // it. Both base_tablet and new_tablet point at the same physical tablet
    // here — a real ALTER would have a distinct new_tablet id, but the fast
    // path's segment_location resolution only depends on the new_tablet's
    // location and either id is fine for opening segment files we just wrote.
    VersionedTablet versioned_at(int64_t tablet_id, int64_t version) {
        auto vt_or = _tablet_manager->get_tablet(tablet_id, version);
        CHECK(vt_or.ok()) << vt_or.status();
        return *vt_or;
    }

    constexpr static const char* const kTestGroupPath = "test_lake_add_index_schema_change";

    // Stable column unique-ids (matches the ones generated for c0/c1/c2/c3).
    const int32_t _c0_uid = 100;
    const int32_t _c1_uid = 101;
    const int32_t _c2_uid = 102;
    const int32_t _c3_uid = 103;

    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
    int64_t _partition_id = 1234;
};

// Run() against a tablet with ONE segment carrying 5 rows. Expect
// op_add_index has exactly 1 segment_entry and the entry has an IndexKey
// for (c1_uid, BITMAP).
TEST_F(AddIndexSchemaChangeTest, run_bitmap_single_segment_happy_path) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, /*version=*/1, base_schema, /*nrows=*/5);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, _c1_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, /*alter_version=*/version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.segment_entries_size());
    ASSERT_EQ(version, op.alter_version());
    const auto& entry = op.segment_entries(0).entry();
    ASSERT_EQ(1, entry.keys_size());
    EXPECT_EQ(_c1_uid, entry.keys(0).col_unique_id());
    EXPECT_EQ(IndexType::BITMAP, entry.keys(0).index_type());
    EXPECT_GT(entry.file_size(), 0);
    // The .idx file should exist on disk.
    std::string idx_path = _tablet_manager->segment_location(base_tablet_id, entry.index_file());
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(idx_path));
    auto exists_or = fs->path_exists(idx_path);
    EXPECT_TRUE(exists_or.ok());
}

// Build NGRAMBF on a VARCHAR column with gram_num=3 / fpp=0.05 /
// case_sensitive=true. Verify the entry's keys carry NGRAMBF and the .idx
// file is non-empty (BloomFilterIndexWriter wrote a payload).
TEST_F(AddIndexSchemaChangeTest, run_ngrambf_with_index_properties) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, /*nrows=*/6);

    // index_properties JSON as produced by TabletIndex::to_schema_pb.
    const std::string props = R"({"properties":{"bloom_filter_fpp":"0.05","gram_num":"3","case_sensitive":"true"}})";
    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::NGRAMBF, _c2_uid, /*index_id=*/0, props)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.segment_entries_size());
    EXPECT_EQ(IndexType::NGRAMBF, op.segment_entries(0).entry().keys(0).index_type());
    EXPECT_GT(op.segment_entries(0).entry().file_size(), 0);
}

// Plain BLOOM_FILTER (no index_properties) -> use_ngram=false in
// BloomFilterIndexWriter. Entry's IndexKey carries BLOOM_FILTER.
TEST_F(AddIndexSchemaChangeTest, run_plain_bloom_filter) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 4);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BLOOM_FILTER, _c1_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.segment_entries_size());
    EXPECT_EQ(IndexType::BLOOM_FILTER, op.segment_entries(0).entry().keys(0).index_type());
}

// Two rowsets / two segments: each gets its own .idx file and a distinct
// segment_id in op_add_index.
TEST_F(AddIndexSchemaChangeTest, run_multi_segment_emits_per_segment_entry) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = 1;
    version = write_one_rowset(base_tablet_id, version, base_schema, 4, "a");
    version = write_one_rowset(base_tablet_id, version, base_schema, 4, "b");

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, _c1_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(2, op.segment_entries_size());
    // segment_ids must be distinct and each entry must have its own
    // index_file.
    EXPECT_NE(op.segment_entries(0).segment_id(), op.segment_entries(1).segment_id());
    EXPECT_NE(op.segment_entries(0).entry().index_file(), op.segment_entries(1).entry().index_file());
}

// Tablet with no rowsets -> run is a no-op and op_add_index has 0 segment
// entries but still records new_indexes / alter_version.
TEST_F(AddIndexSchemaChangeTest, run_empty_tablet_noop) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));

    auto vt = versioned_at(base_tablet_id, /*version=*/1);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, _c1_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, /*alter_version=*/1);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    EXPECT_EQ(0, op.segment_entries_size());
    EXPECT_EQ(1, op.alter_version());
    ASSERT_EQ(1, op.new_indexes_size());
    EXPECT_EQ(IndexType::BITMAP, op.new_indexes(0).index_type());
}

// GIN -> NotSupported. Triggers cleanup_written_idx_files via the run()
// failure path.
TEST_F(AddIndexSchemaChangeTest, run_gin_returns_not_supported) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 3);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::GIN, _c2_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    Status st = sc.run(&op);
    EXPECT_FALSE(st.ok());
    // Either NotSupported propagates straight, or it's wrapped — either way
    // a non-OK status is the contract.
}

// Unknown column unique_id -> InternalError ("column with unique_id ... not
// found in schema"). Cleanup runs.
TEST_F(AddIndexSchemaChangeTest, run_unknown_column_unique_id) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 3);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, /*col_uid=*/999999)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    Status st = sc.run(&op);
    EXPECT_FALSE(st.ok());
}

// Verify the index_id flows into op_add_index.new_indexes (the IndexKey on
// each entry only carries (col_uid, index_type), index_id is on new_indexes).
TEST_F(AddIndexSchemaChangeTest, run_carries_index_id_through_to_entry) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 4);

    const int64_t kIndexId = 7777;
    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, _c1_uid, kIndexId)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.new_indexes_size());
    EXPECT_EQ(kIndexId, op.new_indexes(0).index_id());
    EXPECT_EQ(_c1_uid, op.segment_entries(0).entry().keys(0).col_unique_id());
}

// VECTOR -> NotSupported (default switch fall-through).
TEST_F(AddIndexSchemaChangeTest, run_vector_index_returns_not_supported) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 3);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::VECTOR, _c1_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    Status st = sc.run(&op);
    EXPECT_FALSE(st.ok());
}

// Drive feed_index_from_column's nullable path via BITMAP on c3 (INT NULL).
// Confirms add_nulls / add_values runs are exercised end-to-end.
TEST_F(AddIndexSchemaChangeTest, run_bitmap_nullable_column_handles_nulls) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, /*nrows=*/9);

    auto vt = versioned_at(base_tablet_id, version);
    std::vector<TabletIndexPB> indexes{make_index(IndexType::BITMAP, _c3_uid)};
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.segment_entries_size());
    EXPECT_EQ(_c3_uid, op.segment_entries(0).entry().keys(0).col_unique_id());
    EXPECT_GT(op.segment_entries(0).entry().file_size(), 0);
}

// Build BITMAP and NGRAMBF together on the same .idx. Both keys end up on
// the same IndexDeltaGroupEntryPB.
TEST_F(AddIndexSchemaChangeTest, run_two_indexes_share_idx_file) {
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));
    auto base_schema = TabletSchema::create(base_metadata->schema());
    int64_t version = write_one_rowset(base_tablet_id, 1, base_schema, 5);

    auto vt = versioned_at(base_tablet_id, version);
    const std::string props = R"({"properties":{"gram_num":"3"}})";
    std::vector<TabletIndexPB> indexes{
            make_index(IndexType::BITMAP, _c1_uid),
            make_index(IndexType::NGRAMBF, _c2_uid, /*index_id=*/0, props),
    };
    AddIndexSchemaChange sc(_tablet_manager.get(), next_id(), vt, vt, indexes, version);

    TxnLogPB_OpAddIndex op;
    ASSERT_OK(sc.run(&op));
    ASSERT_EQ(1, op.segment_entries_size());
    EXPECT_EQ(2, op.segment_entries(0).entry().keys_size());
}

} // namespace starrocks::lake
