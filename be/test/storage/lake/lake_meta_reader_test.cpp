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

#include "storage/lake_meta_reader.h"

#include <gtest/gtest.h>

#include <limits>
#include <map>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeMetaReaderTest : public TestBase {
public:
    LakeMetaReaderTest() : TestBase(kTestDirectory) {}

    void SetUp() override { clear_and_init_test_dir(); }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_meta_reader";

    // Create a simple tablet with given keys type and write some data
    StatusOr<int64_t> create_tablet_with_data(KeysType keys_type, int num_rowsets = 1, int segments_per_rowset = 1) {
        auto tablet_metadata = generate_simple_tablet_metadata(keys_type);
        int64_t tablet_id = tablet_metadata->id();
        auto tablet_schema = TabletSchema::create(tablet_metadata->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

        RETURN_IF_ERROR(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

        ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id));

        // Write data
        for (int r = 0; r < num_rowsets; r++) {
            int64_t txn_id = next_id();
            ASSIGN_OR_RETURN(auto writer, tablet.new_writer(kHorizontal, txn_id));
            RETURN_IF_ERROR(writer->open());

            for (int s = 0; s < segments_per_rowset; s++) {
                // Create simple chunk
                std::vector<int> k0{1 + s * 10, 2 + s * 10, 3 + s * 10, 4 + s * 10, 5 + s * 10};
                std::vector<int> v0{2 + s * 10, 4 + s * 10, 6 + s * 10, 8 + s * 10, 10 + s * 10};

                auto c0 = Int32Column::create();
                auto c1 = Int32Column::create();
                c0->append_numbers(k0.data(), k0.size() * sizeof(int));
                c1->append_numbers(v0.data(), v0.size() * sizeof(int));

                Chunk chunk({std::move(c0), std::move(c1)}, schema);
                RETURN_IF_ERROR(writer->write(chunk));
                RETURN_IF_ERROR(writer->finish());
            }

            auto files = writer->segments();
            writer->close();

            // Publish version
            auto txn_log = std::make_shared<TxnLog>();
            txn_log->set_tablet_id(tablet_id);
            txn_log->set_txn_id(txn_id);
            auto op_write = txn_log->mutable_op_write();
            for (auto& file : files) {
                op_write->mutable_rowset()->add_segment_metas()->set_filename(file.path);
            }
            op_write->mutable_rowset()->set_num_rows(5 * segments_per_rowset);
            op_write->mutable_rowset()->set_data_size(100);
            op_write->mutable_rowset()->set_overlapped(true);

            RETURN_IF_ERROR(_tablet_mgr->put_txn_log(txn_log));

            int64_t version = r + 2;
            ASSIGN_OR_RETURN(auto new_metadata,
                             TEST_publish_single_version(_tablet_mgr.get(), tablet_id, version, txn_id));
        }

        return tablet_id;
    }
};

// Test _get_segments with primary key table
TEST_F(LakeMetaReaderTest, test_get_segments_with_primary_key_table) {
    // Create a primary key tablet with 1 rowset containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 1, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2, options_list.size());

    // Verify options for primary key table
    for (int i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_TRUE(options.is_primary_keys) << "Expected is_primary_keys to be true for PK table";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_EQ(2, options.version) << "Expected version to be 2";
        EXPECT_EQ(i, options.segment_id) << "Expected segment_id to be " << i;
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set for PK table";
    }
}

// Test _get_segments with non-primary key table (DUP_KEYS)
TEST_F(LakeMetaReaderTest, test_get_segments_with_dup_keys_table) {
    // Create a duplicate keys tablet with 1 rowset containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(DUP_KEYS, 1, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments
    ASSERT_EQ(2, segments.size());
    // Verify options for non-primary key table
    ASSERT_EQ(2, options_list.size()) << "Expected 2 options for non-PK table";

    // A non-PK tablet gets no delta column group loader in shared-data, but it still has to carry
    // the segment identity: SegmentMetaCollecter hands options.tablet_id and options.rss_id to the
    // virtual columns _tablet_id_ and _rss_id_, so leaving them at 0 makes a metadata scan answer
    // min/max of those columns with 0 instead of the real ids.
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ASSERT_EQ(1, metadata->rowsets_size());
    const int64_t rowset_id = metadata->rowsets(0).id();
    ASSERT_GT(rowset_id, 0);
    for (int i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_FALSE(options.is_primary_keys) << "Expected is_primary_keys to be false for a DUP table";
        EXPECT_EQ(nullptr, options.dcg_loader) << "Delta column group stays primary-key-only in shared-data";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to be set for a non-PK table";
        EXPECT_EQ(2, options.version) << "Expected version to be set for a non-PK table";
        EXPECT_EQ(i, options.segment_id) << "Expected segment_id to be " << i;
        // segment_idx is contiguous here, so the persisted index and the vector position agree. The
        // expectation is still phrased as the rowset id plus the segment own id, because that is what
        // an ordinary scan reports; test_get_segments_with_sparse_segment_idx covers the case where the
        // two disagree.
        ASSERT_NE(nullptr, segments[i]) << "at position " << i;
        EXPECT_EQ(static_cast<uint64_t>(i), segments[i]->id()) << "Expected a contiguous segment_idx here";
        EXPECT_EQ(rowset_id + static_cast<int64_t>(segments[i]->id()), options.rss_id)
                << "Expected rss_id to be the rowset id plus the id of the segment itself";
    }
}

// Test _get_segments derives rss_id from the persisted segment index, not the vector position.
// SegmentMetadataPB.segment_idx is non-positional once a rowset keeps only a subset of its original
// segments (tablet splitting, partial compaction), and Segment::id() is that persisted index. An
// ordinary scan reports _rss_id_ as rowset_id + Segment::id()
// (SegmentIterator::_init_virtual_column_iterator), so a metadata scan that added the vector
// position instead would answer min/max(_rss_id_) with a different value for the very same segment.
TEST_F(LakeMetaReaderTest, test_get_segments_with_sparse_segment_idx) {
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(DUP_KEYS, 1, 3));

    // Republish the same three segments with sparse, non-positional segment_idx values, the way
    // tablet splitting leaves them. The real filenames are kept so the segments still load.
    ASSIGN_OR_ABORT(auto base_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    auto metadata = std::make_shared<TabletMetadataPB>(*base_metadata);
    ASSERT_EQ(1, metadata->rowsets_size());
    auto* rowset_meta = metadata->mutable_rowsets(0);
    ASSERT_EQ(3, rowset_meta->segment_metas_size());
    const std::vector<uint32_t> kSegmentIdx{1, 4, 5};
    for (int i = 0; i < rowset_meta->segment_metas_size(); i++) {
        rowset_meta->mutable_segment_metas(i)->set_segment_idx(kSegmentIdx[i]);
    }
    metadata->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    // Segments are cached by file path and keep the segment id they were first loaded with, so drop
    // the cache to make sure the reader observes the rewritten indexes.
    _tablet_mgr->prune_metacache();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));

    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    ASSERT_EQ(3, segments.size());
    ASSERT_EQ(3, options_list.size());

    const int64_t rowset_id = rowset_meta->id();
    ASSERT_GT(rowset_id, 0);
    for (int i = 0; i < options_list.size(); i++) {
        ASSERT_NE(nullptr, segments[i]) << "at position " << i;
        // Segment::id() is the persisted segment_idx and is what an ordinary scan adds to the
        // rowset id; the metadata scan has to land on the same value.
        EXPECT_EQ(kSegmentIdx[i], segments[i]->id()) << "at position " << i;
        EXPECT_EQ(kSegmentIdx[i], options_list[i].segment_id) << "at position " << i;
        EXPECT_EQ(rowset_id + static_cast<int64_t>(segments[i]->id()), options_list[i].rss_id)
                << "rss_id must follow the persisted segment_idx, not the vector position " << i;
    }
}

// Test _get_segments with multiple rowsets
TEST_F(LakeMetaReaderTest, test_get_segments_with_multiple_rowsets) {
    // Create a primary key tablet with 3 rowsets, each containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 3, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 4));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 6 segments (3 rowsets * 2 segments each)
    ASSERT_EQ(6, segments.size());
    ASSERT_EQ(6, options_list.size());

    // Verify all options have correct primary key settings
    for (const auto& options : options_list) {
        EXPECT_TRUE(options.is_primary_keys);
        EXPECT_EQ(tablet_id, options.tablet_id);
        EXPECT_EQ(4, options.version);
        EXPECT_NE(nullptr, options.dcg_loader);
    }

    // Verify segment_ids are correctly set (should be 0, 1 for each rowset)
    int expected_seg_id = 0;
    int rowset_count = 0;
    for (const auto& options : options_list) {
        EXPECT_EQ(expected_seg_id, options.segment_id)
                << "Expected segment_id " << expected_seg_id << " at position " << rowset_count;
        expected_seg_id++;
        if (expected_seg_id >= 2) {
            expected_seg_id = 0;
            rowset_count++;
        }
    }
}

// Test _get_segments returns correct rowset ids for primary key table
TEST_F(LakeMetaReaderTest, test_get_segments_rowset_ids) {
    // Create a primary key tablet with 2 rowsets
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 2, 1));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments (2 rowsets * 1 segment each)
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2, options_list.size());

    // Verify rowset ids are different for different rowsets
    EXPECT_NE(options_list[0].pk_rowsetid, options_list[1].pk_rowsetid)
            << "Expected different rowset ids for different rowsets";
}

// experimental_lake_ignore_lost_segment: _init_seg_meta_collecters must skip a lost segment (the
// nullptr placeholder from load_segments) instead of building a SegmentMetaCollecter for it, so a
// metadata scan over a tablet with a physically-missing segment file does not crash.
TEST_F(LakeMetaReaderTest, test_init_seg_meta_collecters_ignore_lost_segment) {
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 1, 1));

    ASSIGN_OR_ABORT(auto meta, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
    ASSERT_GT(meta->rowsets_size(), 0);
    ASSERT_GT(meta->rowsets(0).segment_metas_size(), 0);
    auto seg_name = meta->rowsets(0).segment_metas(0).filename();
    _tablet_mgr->metacache()->prune();
    ASSERT_OK(FileSystem::Default()->delete_file(_tablet_mgr->segment_location(tablet_id, seg_name)));

    config::experimental_lake_ignore_lost_segment = true;
    DeferOp reset([] { config::experimental_lake_ignore_lost_segment = false; });

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
    LakeMetaReader reader;
    LakeMetaReaderParams params;
    // The sole segment is lost, so the collector loop must skip the null slot without crashing.
    ASSERT_OK(reader.TEST_init_seg_meta_collecters(tablet, params));
}

// Builds the access path the FE emits for get_json_string(j, '$.f2'): a ROOT node named after the
// JSON column carrying `extended`, with one FIELD child per subfield.
static ColumnAccessPathPtr make_extended_json_path(const std::string& root_column, const std::string& field,
                                                   const TypeDescriptor& value_type) {
    TColumnAccessPath tleaf;
    tleaf.__set_type(TAccessPathType::FIELD);
    tleaf.__set_from_predicate(false);
    tleaf.__set_extended(false);
    tleaf.__set_type_desc(value_type.to_thrift());

    TColumnAccessPath troot;
    troot.__set_type(TAccessPathType::ROOT);
    troot.__set_from_predicate(false);
    troot.__set_extended(true);
    troot.__set_type_desc(value_type.to_thrift());
    troot.__set_children({tleaf});

    std::vector<std::string> resolved = {root_column, field};
    size_t resolve_index = 0;
    auto resolver = [&](const TColumnAccessPath&) -> StatusOr<std::string> {
        CHECK_LT(resolve_index, resolved.size());
        return resolved[resolve_index++];
    };
    auto res = ColumnAccessPath::create(troot, resolver);
    CHECK(res.ok()) << res.status();
    return std::move(res).value();
}

// A tablet whose only value column is a JSON column, optionally carrying a default value.
static std::shared_ptr<TabletMetadataPB> generate_json_tablet_metadata(const std::string& json_default) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);
    auto schema = metadata->mutable_schema();
    schema->set_keys_type(DUP_KEYS);
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);

    auto c0 = schema->add_column();
    c0->set_unique_id(next_id());
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema->add_column();
    c1->set_unique_id(next_id());
    c1->set_name("j");
    c1->set_type("JSON");
    c1->set_is_key(false);
    c1->set_is_nullable(true);
    c1->set_aggregation("NONE");
    if (!json_default.empty()) {
        c1->set_default_value(json_default);
    }
    return metadata;
}

// A [_META_] scan appends a synthetic column for every extended JSON subfield path, exactly like the
// data scan does (extend_schema_by_access_paths / LakeDataSource::_extend_schema_by_access_paths).
// The synthetic column owns no storage, so in a segment written before `ADD COLUMN j JSON DEFAULT
// ...` it is served by a DefaultValueColumnIterator. Unless it inherits the root JSON column's
// default, that iterator reports only_nulls() and SegmentMetaCollecter::_collect_dict_for_column
// silently skips the segment, so the global dictionary the frontend caches is missing the default
// value -- and every row holding it then decodes to a bogus code 0: wrong sort/group order, or a
// "Dict Decode failed" error. Keep the meta path in step with the scan path.
TEST_F(LakeMetaReaderTest, test_extended_json_subfield_inherits_root_default_value) {
    auto metadata = generate_json_tablet_metadata(R"({"f2":"hello"})");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    std::vector<ColumnAccessPathPtr> paths;
    paths.emplace_back(make_extended_json_path("j", "f2", TypeDescriptor::create_varchar_type(65533)));
    ASSERT_EQ("j.f2", paths[0]->linear_path());
    ASSERT_TRUE(paths[0]->is_extended());

    std::map<int32_t, std::string> id_to_names;
    LakeMetaReaderParams params;
    params.tablet_manager = _tablet_mgr.get();
    params.tablet_id = metadata->id();
    params.version = Version(0, 1);
    params.id_to_names = &id_to_names;
    params.column_access_paths = &paths;
    // Same seed as next_uniq_id(): above every real column id, so the synthetic id cannot collide.
    params.next_uniq_id = std::numeric_limits<int32_t>::max() - 1000000;

    LakeMetaReader reader;
    ASSERT_OK(reader.init(params));

    const auto& schema = reader.TEST_tablet_schema();
    int32_t cid = schema->field_index("j.f2");
    ASSERT_GE(cid, 0);
    const auto& subfield = schema->column(static_cast<size_t>(cid));
    ASSERT_TRUE(subfield.is_extended());
    EXPECT_TRUE(subfield.has_default_value()) << "the subfield must inherit the root JSON column's default";
    EXPECT_EQ("hello", subfield.default_value());
}

// Control: a JSON column with no default must not hand the subfield one, otherwise the meta scan
// would invent a value the data scan never returns.
TEST_F(LakeMetaReaderTest, test_extended_json_subfield_without_root_default_value) {
    auto metadata = generate_json_tablet_metadata("");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    std::vector<ColumnAccessPathPtr> paths;
    paths.emplace_back(make_extended_json_path("j", "f2", TypeDescriptor::create_varchar_type(65533)));

    std::map<int32_t, std::string> id_to_names;
    LakeMetaReaderParams params;
    params.tablet_manager = _tablet_mgr.get();
    params.tablet_id = metadata->id();
    params.version = Version(0, 1);
    params.id_to_names = &id_to_names;
    params.column_access_paths = &paths;
    params.next_uniq_id = std::numeric_limits<int32_t>::max() - 1000000;

    LakeMetaReader reader;
    ASSERT_OK(reader.init(params));

    const auto& schema = reader.TEST_tablet_schema();
    int32_t cid = schema->field_index("j.f2");
    ASSERT_GE(cid, 0);
    EXPECT_FALSE(schema->column(static_cast<size_t>(cid)).has_default_value());
}

} // namespace starrocks::lake
