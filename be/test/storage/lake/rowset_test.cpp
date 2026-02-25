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

#include <optional>
#include <unordered_set>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/logging.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/transactions.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeRowsetTest : public TestBase {
public:
    LakeRowsetTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void create_rowsets_for_testing() {
        std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
        std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

        std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
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

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

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

            // segment #3
            ASSERT_OK(writer->write(chunk0));
            ASSERT_OK(writer->write(chunk1));
            ASSERT_OK(writer->finish());

            const auto& files = writer->segments();
            ASSERT_EQ(3, files.size());

            // add rowset metadata
            auto* rowset = _tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_next_compaction_offset(1);
            auto* segs = rowset->mutable_segments();
            for (const auto& file : writer->segments()) {
                segs->Add()->assign(file.path);
            }

            writer->close();
        }

        // write tablet metadata
        _tablet_metadata->set_version(2);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_rowset";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeRowsetTest, test_load_segments) {
    create_rowsets_for_testing();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    auto* cache = _tablet_mgr->metacache();

    ASSIGN_OR_ABORT(auto rowsets, tablet.get_rowsets(2));
    ASSERT_EQ(1, rowsets.size());
    auto& rowset = rowsets[0];

    // fill cache: false
    ASSIGN_OR_ABORT(auto segments1, rowset->segments(false));
    ASSERT_EQ(3, segments1.size());
    for (const auto& seg : segments1) {
        auto segment = cache->lookup_segment(seg->file_name());
        ASSERT_TRUE(segment == nullptr);
    }

    // fill data cache: false, fill metadata cache: true
    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = true};
    ASSIGN_OR_ABORT(auto segments2, rowset->segments(lake_io_opts));
    ASSERT_EQ(3, segments2.size());
    for (const auto& seg : segments2) {
        auto segment = cache->lookup_segment(seg->file_name());
        ASSERT_TRUE(segment != nullptr);
    }
}

TEST_F(LakeRowsetTest, test_segment_update_cache_size) {
    create_rowsets_for_testing();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    ASSIGN_OR_ABORT(auto rowsets, tablet.get_rowsets(2));
    ASSIGN_OR_ABORT(auto segments, rowsets[0]->segments(false));

    auto* cache = _tablet_mgr->metacache();

    // get the same segments from the rowset
    auto sample_segment = segments[0];
    std::string path = sample_segment->file_name();
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(path));
    auto schema = sample_segment->tablet_schema_share_ptr();

    // create a dummy segment with the same path to cache ahead in metacache,
    // the later segment open operation will not update the mem_usage due to instance mismatch.
    {
        // clean the cache
        cache->prune();
        //create the dummy segment and put it into metacache
        auto dummy_segment =
                std::make_shared<Segment>(fs, FileInfo{path}, sample_segment->id(), schema, _tablet_mgr.get());
        cache->cache_segment(path, dummy_segment);
        EXPECT_EQ(dummy_segment, cache->lookup_segment(path));
        auto sz1 = cache->memory_usage();

        auto mirror_segment =
                std::make_shared<Segment>(fs, FileInfo{path}, sample_segment->id(), schema, _tablet_mgr.get());
        LakeIOOptions lake_io_opts{.fill_data_cache = true};
        auto st = mirror_segment->open(nullptr, nullptr, lake_io_opts);
        EXPECT_TRUE(st.ok());
        auto sz2 = cache->memory_usage();
        // no memory_usage change, because the instance in metacache is different from this mirror_segment
        EXPECT_EQ(sz1, sz2);
    }
    // create the mirror_segment without open, and put it into metacache, get the cache memory_usage,
    // open the segment (during the open, the cache size will be updated), get the cache memory_usage again.
    {
        // clean the cache
        cache->prune();
        //create the dummy segment and put it into metacache
        auto mirror_segment =
                std::make_shared<Segment>(fs, FileInfo{path}, sample_segment->id(), schema, _tablet_mgr.get());
        cache->cache_segment(path, mirror_segment);
        auto sz1 = cache->memory_usage();
        auto ssz1 = mirror_segment->mem_usage();

        LakeIOOptions lake_io_opts{.fill_data_cache = true};
        auto st = mirror_segment->open(nullptr, nullptr, lake_io_opts);
        EXPECT_TRUE(st.ok());
        auto sz2 = cache->memory_usage();
        auto ssz2 = mirror_segment->mem_usage();
        // mem usage updated after the segment is opened.
        EXPECT_LT(sz1, sz2);
        EXPECT_EQ(ssz2 - ssz1, sz2 - sz1);
    }
}

TEST_F(LakeRowsetTest, test_partial_compaction) {
    create_rowsets_for_testing();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));

    // prepare writer
    {
        std::vector<int> k1{40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51};
        std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(k1.data(), k1.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

        ASSERT_OK(writer->open());
        // generate segment x
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());
        // generate segment y
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());
        ASSERT_EQ(2, writer->segments().size());
    }

    {
        TxnLogPB txn_log;
        auto op_compaction = txn_log.mutable_op_compaction();
        EXPECT_EQ(op_compaction->output_rowset().segments_size(), 0);

        auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1
                                                 /* compaction_segment_limit */);
        ASSERT_TRUE(rs->partial_segments_compaction());
        // segments in old rowset will be a b c
        // segments in new rowset will be a x y c
        // x and y should be deleted
        CompactionTaskContext context(txn_id, _tablet_metadata->id(), 456, false, false, nullptr);
        VersionedTablet vt(nullptr, _tablet_metadata);
        VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
        EXPECT_TRUE(task.fill_compaction_segment_info(op_compaction, writer.get()).ok());
        EXPECT_EQ(op_compaction->output_rowset().segments_size(), 4);
        EXPECT_EQ(op_compaction->new_segment_offset(), 1);
        EXPECT_EQ(op_compaction->new_segment_count(), 2);

        std::vector<string> files_to_delete;
        collect_files_in_log(_tablet_mgr.get(), txn_log, &files_to_delete);
        EXPECT_EQ(files_to_delete.size(), 2);
        EXPECT_TRUE(files_to_delete[0].find(writer->segments()[0].path) != std::string::npos);
        EXPECT_TRUE(files_to_delete[1].find(writer->segments()[1].path) != std::string::npos);
    }

    {
        TxnLogPB txn_log;
        auto op_compaction = txn_log.mutable_op_compaction();
        EXPECT_EQ(op_compaction->output_rowset().segments_size(), 0);

        auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                 0 /* compaction_segment_limit */);
        ASSERT_FALSE(rs->partial_segments_compaction());
        CompactionTaskContext context(txn_id, _tablet_metadata->id(), 456, false, false, nullptr);
        VersionedTablet vt(nullptr, _tablet_metadata);
        VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
        EXPECT_TRUE(task.fill_compaction_segment_info(op_compaction, writer.get()).ok());
        EXPECT_EQ(op_compaction->output_rowset().segments_size(), 2);
        EXPECT_EQ(op_compaction->new_segment_offset(), 0);
        EXPECT_EQ(op_compaction->new_segment_count(), 2);

        std::vector<string> files_to_delete;
        collect_files_in_log(_tablet_mgr.get(), txn_log, &files_to_delete);
        EXPECT_EQ(files_to_delete.size(), 2);
        EXPECT_TRUE(files_to_delete[0].find(writer->segments()[0].path) != std::string::npos);
        EXPECT_TRUE(files_to_delete[1].find(writer->segments()[1].path) != std::string::npos);
    }
}

TEST_F(LakeRowsetTest, test_partial_compaction_sparse_segment_id_no_collision) {
    create_rowsets_for_testing();
    auto* rowset_meta = _tablet_metadata->mutable_rowsets(0);
    rowset_meta->set_next_compaction_offset(2);
    rowset_meta->clear_segment_metas();
    rowset_meta->add_segment_metas()->set_segment_idx(0);
    rowset_meta->add_segment_metas()->set_segment_idx(2);
    rowset_meta->add_segment_metas()->set_segment_idx(4);

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    {
        std::vector<int> k1{40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51};
        std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(k1.data(), k1.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());
        ASSERT_EQ(2, writer->segments().size());
    }

    TxnLogPB txn_log;
    auto* op_compaction = txn_log.mutable_op_compaction();
    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* compaction_segment_limit */);
    ASSERT_TRUE(rs->partial_segments_compaction());
    CompactionTaskContext context(txn_id, _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    ASSERT_OK(task.fill_compaction_segment_info(op_compaction, writer.get()));

    const auto& output_rowset = op_compaction->output_rowset();
    ASSERT_EQ(4, output_rowset.segments_size());
    ASSERT_EQ(4, output_rowset.segment_metas_size());
    EXPECT_EQ(2, op_compaction->new_segment_offset());
    EXPECT_EQ(2, op_compaction->new_segment_count());

    std::unordered_set<uint32_t> segment_ids;
    for (int i = 0; i < output_rowset.segment_metas_size(); ++i) {
        segment_ids.insert(output_rowset.segment_metas(i).segment_idx());
    }
    EXPECT_EQ(static_cast<size_t>(output_rowset.segment_metas_size()), segment_ids.size());
    EXPECT_TRUE(segment_ids.contains(0));
    EXPECT_TRUE(segment_ids.contains(2));
    EXPECT_TRUE(segment_ids.contains(5));
    EXPECT_TRUE(segment_ids.contains(6));
}
namespace {

static VariantPB make_int_variant_pb(int32_t v) {
    VariantPB var;
    TypeDescriptor td(TYPE_INT);
    *var.mutable_type() = td.to_protobuf();
    var.set_value(std::to_string(v));
    return var;
}

static void set_tablet_range_int(TabletMetadata* tablet_meta, std::optional<int32_t> lower, bool lower_included,
                                 std::optional<int32_t> upper, bool upper_included) {
    auto* range = tablet_meta->mutable_range();
    range->Clear();
    if (lower.has_value()) {
        auto* tuple = range->mutable_lower_bound();
        *tuple->add_values() = make_int_variant_pb(lower.value());
        range->set_lower_bound_included(lower_included);
    }
    if (upper.has_value()) {
        auto* tuple = range->mutable_upper_bound();
        *tuple->add_values() = make_int_variant_pb(upper.value());
        range->set_upper_bound_included(upper_included);
    }
}

static void set_rowset_shared_segments(RowsetMetadataPB* rowset_meta, bool shared) {
    rowset_meta->clear_shared_segments();
    for (int i = 0; i < rowset_meta->segments_size(); i++) {
        rowset_meta->add_shared_segments(shared);
    }
}

static void set_rowset_range_int(RowsetMetadataPB* rowset_meta, std::optional<int32_t> lower, bool lower_included,
                                 std::optional<int32_t> upper, bool upper_included) {
    auto* range = rowset_meta->mutable_range();
    range->Clear();
    if (lower.has_value()) {
        auto* tuple = range->mutable_lower_bound();
        *tuple->add_values() = make_int_variant_pb(lower.value());
        range->set_lower_bound_included(lower_included);
    }
    if (upper.has_value()) {
        auto* tuple = range->mutable_upper_bound();
        *tuple->add_values() = make_int_variant_pb(upper.value());
        range->set_upper_bound_included(upper_included);
    }
}

static size_t count_rows_from_iters(const std::vector<ChunkIteratorPtr>& iters, int chunk_size = 1024) {
    size_t rows = 0;
    for (const auto& it : iters) {
        CHECK(it->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        CHECK(it->init_output_schema(std::unordered_set<uint32_t>()).ok());
        auto chunk = ChunkHelper::new_chunk(it->schema(), chunk_size);
        while (true) {
            chunk->reset();
            auto st = it->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK(st.ok()) << st.to_string();
            rows += chunk->num_rows();
        }
    }
    return rows;
}

} // namespace

TEST_F(LakeRowsetTest, test_tablet_range_pruning_only_for_shared_segments) {
    create_rowsets_for_testing();

    // Apply tablet range [10, 12) but do NOT mark segments as shared.
    // Because segments are not marked as shared, tablet range should NOT take effect.
    set_tablet_range_int(_tablet_metadata.get(), 10, true, 12, false);
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));
    ASSERT_EQ(count_rows_from_iters(iters), 3 * (22 + 12));

    // Mark all segments as shared, tablet range should prune rows.
    set_rowset_shared_segments(rs_meta, true);
    auto rowset2 =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    ASSIGN_OR_ABORT(auto iters2, rowset2->read(input_schema, rs_opts));
    // Now tablet range [10, 12) should prune rows: per segment keys {10, 11}, total segments = 3.
    ASSERT_EQ(count_rows_from_iters(iters2), 3 * 2);
}

TEST_F(LakeRowsetTest, test_tablet_range_pruning_inclusive_exclusive) {
    create_rowsets_for_testing();
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);

    auto make_rowset_and_count = [&](std::optional<int32_t> lower, bool lower_inc, std::optional<int32_t> upper,
                                     bool upper_inc) -> size_t {
        set_tablet_range_int(_tablet_metadata.get(), lower, lower_inc, upper, upper_inc);
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        RowsetReadOptions rs_opts;
        OlapReaderStatistics stats;
        rs_opts.stats = &stats;
        rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
        auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
        ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));
        return count_rows_from_iters(iters);
    };

    // Per segment keys include 10,11,12. Total segments = 3.
    // With tablet range [10, 12) (closed-open), each segment contributes keys {10, 11}.
    ASSERT_EQ(make_rowset_and_count(10, true, 12, false), 3 * 2);
}

TEST_F(LakeRowsetTest, test_rowset_range_overrides_tablet_range) {
    create_rowsets_for_testing();

    // tablet range [10, 12), rowset range [11, 13)
    set_tablet_range_int(_tablet_metadata.get(), 10, true, 12, false);
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);
    set_rowset_range_int(rs_meta, 11, true, 13, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));

    // If rowset range takes precedence, keep keys [11,13) => each segment contributes 11,12
    ASSERT_EQ(count_rows_from_iters(iters), 3 * 2);
}

TEST_F(LakeRowsetTest, test_tablet_range_pb_invalid_bounds) {
    create_rowsets_for_testing();
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);

    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});

    // 1) Missing type.
    {
        auto* range = _tablet_metadata->mutable_range();
        range->Clear();
        auto* lb = range->mutable_lower_bound();
        auto* v = lb->add_values();
        v->set_value("10");
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        auto res = rowset->read(input_schema, rs_opts);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_corruption());
    }

    // 2) Missing value.
    {
        auto* range = _tablet_metadata->mutable_range();
        range->Clear();
        auto* lb = range->mutable_lower_bound();
        auto* v = lb->add_values();
        TypeDescriptor td(TYPE_INT);
        *v->mutable_type() = td.to_protobuf();
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        auto res = rowset->read(input_schema, rs_opts);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_corruption());
    }

    // 3) Tuple size exceeds sort key size.
    {
        auto* range = _tablet_metadata->mutable_range();
        range->Clear();
        auto* lb = range->mutable_lower_bound();
        *lb->add_values() = make_int_variant_pb(1);
        *lb->add_values() = make_int_variant_pb(2); // sort key size is 1 in this test schema
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        auto res = rowset->read(input_schema, rs_opts);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_corruption());
    }
}

TEST_F(LakeRowsetTest, test_tablet_range_missing_does_not_prune) {
    create_rowsets_for_testing();
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);
    _tablet_metadata->clear_range();

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));
    ASSERT_EQ(count_rows_from_iters(iters), 3 * (22 + 12));
}

TEST_F(LakeRowsetTest, test_tablet_range_multi_column_range_pruning) {
    // Rebuild a tablet with two key columns so that TabletRangePB carries
    // a multi-column range and Rowset / SegmentIterator must honor it.
    _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    _tablet_metadata->set_id(next_id());
    _tablet_metadata->set_version(1);
    _tablet_metadata->mutable_schema()->set_id(next_id());

    auto* schema_pb = _tablet_metadata->mutable_schema();
    schema_pb->clear_column();

    // Key columns: (c0 INT, c1 INT), plus one non-key column.
    {
        auto* c0 = schema_pb->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    {
        auto* c1 = schema_pb->add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(true);
        c1->set_is_nullable(false);
    }
    {
        auto* c2 = schema_pb->add_column();
        c2->set_unique_id(next_id());
        c2->set_name("v");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
    }
    schema_pb->set_num_short_key_columns(2);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    auto tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto v = Int32Column::create();

    // Insert a small grid of (c0, c1) pairs:
    //   (0,0), (0,1), (0,2),
    //   (1,0), (1,1), (1,2),
    //   (2,0), (2,1), (2,2)
    for (int k0 = 0; k0 < 3; ++k0) {
        for (int k1 = 0; k1 < 3; ++k1) {
            c0->append(k0);
            c1->append(k1);
            v->append(k0 * 10 + k1);
        }
    }

    Chunk chunk({c0, c1, v}, schema);
    ASSERT_OK(writer->write(chunk));
    ASSERT_OK(writer->finish());
    auto files = writer->segments();
    writer->close();

    // Build rowset metadata for the written segment.
    _tablet_metadata->set_version(2);
    auto* rowset = _tablet_metadata->add_rowsets();
    rowset->set_overlapped(true);
    rowset->set_id(1);
    for (auto& file : files) {
        rowset->add_segments(std::move(file.path));
    }
    set_rowset_shared_segments(rowset, true);

    // Set tablet range: [ (1,1), (2,2) ) (closed-open).
    auto* range = _tablet_metadata->mutable_range();
    range->Clear();
    {
        auto* lb = range->mutable_lower_bound();
        *lb->add_values() = make_int_variant_pb(1);
        *lb->add_values() = make_int_variant_pb(1);
        range->set_lower_bound_included(true);
    }
    {
        auto* ub = range->mutable_upper_bound();
        *ub->add_values() = make_int_variant_pb(2);
        *ub->add_values() = make_int_variant_pb(2);
        range->set_upper_bound_included(false);
    }

    auto rowset_obj =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());

    // Read both key columns so that we can assert the effective key range.
    auto input_schema = ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>{0, 1});
    ASSIGN_OR_ABORT(auto iters, rowset_obj->read(input_schema, rs_opts));

    size_t rows = 0;
    for (const auto& it : iters) {
        ASSERT_OK(it->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        ASSERT_OK(it->init_output_schema(std::unordered_set<uint32_t>()));
        auto out_chunk = ChunkHelper::new_chunk(it->schema(), 16);
        while (true) {
            out_chunk->reset();
            auto st = it->get_next(out_chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            auto kc0 = down_cast<const Int32Column*>(out_chunk->get_column_raw_ptr_by_index(0));
            auto kc1 = down_cast<const Int32Column*>(out_chunk->get_column_raw_ptr_by_index(1));
            for (int i = 0; i < out_chunk->num_rows(); ++i) {
                int v0 = kc0->get_data()[i];
                int v1 = kc1->get_data()[i];
                ASSERT_GE(v0, 1);
                ASSERT_LE(v0, 2);
                if (v0 == 1) {
                    ASSERT_GE(v1, 1);
                }
                if (v0 == 2) {
                    ASSERT_LE(v1, 2);
                }
            }
            rows += out_chunk->num_rows();
        }
    }

    // The valid pairs within [ (1,1), (2,2) ) are:
    //   (1,1), (1,2), (2,0), (2,1) => 4 rows.
    ASSERT_EQ(rows, 4);
}

TEST_F(LakeRowsetTest, test_tablet_range_char_type_parsed_as_varchar) {
    // Build a new tablet whose sort key column type is CHAR so that Rowset::_get_tablet_range() will
    // take the TYPE_CHAR parsing branch.
    auto tablet_meta = generate_simple_tablet_metadata(DUP_KEYS);
    tablet_meta->set_id(next_id());
    tablet_meta->set_version(1);
    tablet_meta->mutable_schema()->set_id(next_id());

    auto* schema_pb = tablet_meta->mutable_schema();
    ASSERT_GE(schema_pb->column_size(), 1);
    auto* c0 = schema_pb->mutable_column(0);
    c0->set_type("CHAR");
    c0->set_length(20);
    c0->set_index_length(20);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_meta));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_meta->id()));

    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    auto tablet_schema = TabletSchema::create(tablet_meta->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto col0 = BinaryColumn::create();
    auto col1 = Int32Column::create();
    std::vector<std::string> keys = {"aa", "bb", "cc", "dd"};
    for (int i = 0; i < keys.size(); i++) {
        col0->append(Slice(keys[i]));
        col1->append(i);
    }
    Chunk chunk({std::move(col0), std::move(col1)}, schema);
    ASSERT_OK(writer->write(chunk));
    ASSERT_OK(writer->finish());
    auto files = writer->segments();
    writer->close();

    // Build rowset metadata for the written segment.
    tablet_meta->set_version(2);
    auto* rowset = tablet_meta->add_rowsets();
    rowset->set_overlapped(true);
    rowset->set_id(1);
    for (auto& file : files) {
        rowset->add_segments(std::move(file.path));
    }
    set_rowset_shared_segments(rowset, true);

    // Set tablet range: [bb, cc) (closed-open) with VariantPB type = CHAR.
    auto* range = tablet_meta->mutable_range();
    range->Clear();
    {
        auto* lb = range->mutable_lower_bound();
        auto* v = lb->add_values();
        TypeDescriptor td = TypeDescriptor::create_char_type(20);
        *v->mutable_type() = td.to_protobuf();
        v->set_value("bb");
        range->set_lower_bound_included(true);
    }
    {
        auto* ub = range->mutable_upper_bound();
        auto* v = ub->add_values();
        TypeDescriptor td = TypeDescriptor::create_char_type(20);
        *v->mutable_type() = td.to_protobuf();
        v->set_value("cc");
        range->set_upper_bound_included(false);
    }

    auto rowset_obj =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), tablet_meta, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = std::make_shared<const TabletSchema>(tablet_meta->schema());
    auto input_schema = ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset_obj->read(input_schema, rs_opts));
    // Keys are {"aa","bb","cc","dd"}, with [bb, cc) we only keep "bb".
    ASSERT_EQ(count_rows_from_iters(iters), 1);
}

TEST_F(LakeRowsetTest, test_get_each_segment_iterator_respects_tablet_range) {
    create_rowsets_for_testing();
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);

    // Apply tablet range [10, 12) and verify per-segment iterators see only keys in range.
    set_tablet_range_int(_tablet_metadata.get(), 10, true, 12, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    OlapReaderStatistics stats;
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto seg_iters, rowset->get_each_segment_iterator(input_schema, false /*file_data_cache*/, &stats));

    // Each of the 3 segments contributes keys 10, 11.
    ASSERT_EQ(count_rows_from_iters(seg_iters), 3 * 2);
}

TEST_F(LakeRowsetTest, test_get_each_segment_iterator_with_delvec_respects_tablet_range) {
    create_rowsets_for_testing();
    auto* rs_meta = _tablet_metadata->mutable_rowsets(0);
    set_rowset_shared_segments(rs_meta, true);

    // Apply tablet range [10, 12) and verify delvec-aware per-segment iterators
    // see only keys in range.
    set_tablet_range_int(_tablet_metadata.get(), 10, true, 12, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    OlapReaderStatistics stats;
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto seg_iters, rowset->get_each_segment_iterator_with_delvec(input_schema, 1, nullptr, &stats));

    ASSERT_EQ(count_rows_from_iters(seg_iters), 3 * 2);
}

// Test class for segment metadata filter and parallel load with skip_segment_idxs
class LakeRowsetSegmentMetadataFilterTest : public TestBase {
public:
    LakeRowsetSegmentMetadataFilterTest() : TestBase(kTestDirectory) {
        // Create a tablet schema with one INT sort key column
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_id(next_id());

        auto* c0 = schema_pb.add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto* c1 = schema_pb.add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("NONE");

        // Set sort key column index
        schema_pb.add_sort_key_idxes(0);

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_cumulative_point(0);
        _tablet_metadata->set_next_rowset_id(1);
        *_tablet_metadata->mutable_schema() = schema_pb;

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    // Create rowsets with segment_metas for testing
    // Creates 3 segments with sort key ranges: [0,10], [20,30], [40,50]
    void create_rowsets_with_segment_metas() {
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // Segment 0: keys [0, 10]
        {
            auto c0 = Int32Column::create();
            auto c1 = Int32Column::create();
            for (int i = 0; i <= 10; i++) {
                c0->append(i);
                c1->append(i * 2);
            }
            Chunk chunk({c0, c1}, _schema);
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());
        }

        // Segment 1: keys [20, 30]
        {
            auto c0 = Int32Column::create();
            auto c1 = Int32Column::create();
            for (int i = 20; i <= 30; i++) {
                c0->append(i);
                c1->append(i * 2);
            }
            Chunk chunk({c0, c1}, _schema);
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());
        }

        // Segment 2: keys [40, 50]
        {
            auto c0 = Int32Column::create();
            auto c1 = Int32Column::create();
            for (int i = 40; i <= 50; i++) {
                c0->append(i);
                c1->append(i * 2);
            }
            Chunk chunk({c0, c1}, _schema);
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());
        }

        const auto& files = writer->segments();
        ASSERT_EQ(3, files.size());

        // Build rowset metadata with segment_metas
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(1);
        rowset->set_num_rows(33); // 11 + 11 + 11

        for (const auto& file : files) {
            rowset->add_segments(file.path);
        }

        // Add segment_metas with sort_key_min/max
        // Segment 0: [0, 10]
        {
            auto* seg_meta = rowset->add_segment_metas();
            auto* min_tuple = seg_meta->mutable_sort_key_min();
            auto* min_var = min_tuple->add_values();
            TypeDescriptor td(TYPE_INT);
            *min_var->mutable_type() = td.to_protobuf();
            min_var->set_value("0");

            auto* max_tuple = seg_meta->mutable_sort_key_max();
            auto* max_var = max_tuple->add_values();
            *max_var->mutable_type() = td.to_protobuf();
            max_var->set_value("10");

            seg_meta->set_num_rows(11);
        }

        // Segment 1: [20, 30]
        {
            auto* seg_meta = rowset->add_segment_metas();
            auto* min_tuple = seg_meta->mutable_sort_key_min();
            auto* min_var = min_tuple->add_values();
            TypeDescriptor td(TYPE_INT);
            *min_var->mutable_type() = td.to_protobuf();
            min_var->set_value("20");

            auto* max_tuple = seg_meta->mutable_sort_key_max();
            auto* max_var = max_tuple->add_values();
            *max_var->mutable_type() = td.to_protobuf();
            max_var->set_value("30");

            seg_meta->set_num_rows(11);
        }

        // Segment 2: [40, 50]
        {
            auto* seg_meta = rowset->add_segment_metas();
            auto* min_tuple = seg_meta->mutable_sort_key_min();
            auto* min_var = min_tuple->add_values();
            TypeDescriptor td(TYPE_INT);
            *min_var->mutable_type() = td.to_protobuf();
            min_var->set_value("40");

            auto* max_tuple = seg_meta->mutable_sort_key_max();
            auto* max_var = max_tuple->add_values();
            *max_var->mutable_type() = td.to_protobuf();
            max_var->set_value("50");

            seg_meta->set_num_rows(11);
        }

        writer->close();

        _tablet_metadata->set_version(2);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_rowset_segment_metadata_filter";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

// Test: Segment metadata filter skips segments based on predicate
TEST_F(LakeRowsetSegmentMetadataFilterTest, test_segment_metadata_filter_skips_segments) {
    create_rowsets_with_segment_metas();

    // Ensure metadata filter is enabled
    ConfigResetGuard<bool> guard(&config::enable_lake_segment_metadata_filter, true);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = _tablet_schema;

    // Create predicate: c0 = 25 (only matches segment 1 with range [20, 30])
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "25"));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred.get()));
    rs_opts.pred_tree_for_zone_map = PredicateTree::create(std::move(root));

    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));

    // Verify stats: 2 segments should be filtered (segment 0 and segment 2)
    ASSERT_EQ(stats.segments_metadata_filtered, 2);
    // Filtered rows = 11 (segment 0) + 11 (segment 2) = 22
    ASSERT_EQ(stats.segment_metadata_filtered, 22);
    // Only 1 segment should be read
    ASSERT_EQ(stats.segments_read_count, 1);
}

// Test: Segment metadata filter disabled by config
TEST_F(LakeRowsetSegmentMetadataFilterTest, test_segment_metadata_filter_disabled) {
    create_rowsets_with_segment_metas();

    // Disable metadata filter
    ConfigResetGuard<bool> guard(&config::enable_lake_segment_metadata_filter, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = _tablet_schema;

    // Create predicate: c0 = 25
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "25"));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred.get()));
    rs_opts.pred_tree_for_zone_map = PredicateTree::create(std::move(root));

    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    ASSIGN_OR_ABORT(auto iters, rowset->read(input_schema, rs_opts));

    // No segments should be filtered when config is disabled
    ASSERT_EQ(stats.segments_metadata_filtered, 0);
    ASSERT_EQ(stats.segment_metadata_filtered, 0);
    // All 3 segments should be read
    ASSERT_EQ(stats.segments_read_count, 3);
}

// Test: load_segments with skip_segment_idxs in serial mode
TEST_F(LakeRowsetSegmentMetadataFilterTest, test_load_segments_with_skip_segment_idxs) {
    create_rowsets_with_segment_metas();

    // Disable parallel load to test serial mode
    ConfigResetGuard<bool> guard(&config::enable_load_segment_parallel, false);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    std::vector<SegmentPtr> segments;
    SegmentReadOptions seg_options;
    seg_options.lake_io_opts.fill_data_cache = false;
    seg_options.lake_io_opts.fill_metadata_cache = false;

    // Skip segment 1
    std::unordered_set<int> skip_segment_idxs = {1};

    ASSERT_OK(rowset->load_segments(&segments, seg_options, nullptr, &skip_segment_idxs));

    // Should have 3 entries (including nullptr placeholder)
    ASSERT_EQ(segments.size(), 3);

    // Segment 0 should be loaded
    ASSERT_NE(segments[0], nullptr);
    // Segment 1 should be skipped (nullptr)
    ASSERT_EQ(segments[1], nullptr);
    // Segment 2 should be loaded
    ASSERT_NE(segments[2], nullptr);
}

// Test: load_segments with skip_segment_idxs in parallel mode with index mapping
TEST_F(LakeRowsetSegmentMetadataFilterTest, test_load_segments_parallel_with_skip_segment_idxs) {
    create_rowsets_with_segment_metas();

    // Enable parallel load to test parallel mode with index mapping
    ConfigResetGuard<bool> guard(&config::enable_load_segment_parallel, true);

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    std::vector<SegmentPtr> segments;
    SegmentReadOptions seg_options;
    seg_options.lake_io_opts.fill_data_cache = false;
    seg_options.lake_io_opts.fill_metadata_cache = false;

    // Skip segment 1
    std::unordered_set<int> skip_segment_idxs = {1};

    ASSERT_OK(rowset->load_segments(&segments, seg_options, nullptr, &skip_segment_idxs));

    // Should have 3 entries with correct index mapping
    ASSERT_EQ(segments.size(), 3);

    // Segment 0 should be loaded at index 0
    ASSERT_NE(segments[0], nullptr);
    // Segment 1 should be skipped (nullptr) at index 1
    ASSERT_EQ(segments[1], nullptr);
    // Segment 2 should be loaded at index 2
    ASSERT_NE(segments[2], nullptr);
}

// ================================================================================
// Tests for Rowset segment range mode (large rowset split compaction)
// ================================================================================

TEST_F(LakeRowsetTest, test_segment_range_mode_basic) {
    create_rowsets_for_testing();

    // Create a rowset with segment range [0, 2) out of 3 segments
    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* segment_start */,
                                                 2 /* segment_end */);

    EXPECT_TRUE(rowset->is_segment_range_mode());
    EXPECT_EQ(0, rowset->segment_range_start());
    EXPECT_EQ(2, rowset->segment_range_end());
    EXPECT_EQ(2, rowset->num_segments()); // Only 2 segments in range
}

TEST_F(LakeRowsetTest, test_segment_range_mode_middle_range) {
    create_rowsets_for_testing();

    // Create a rowset with segment range [1, 3) out of 3 segments
    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* segment_start */,
                                                 3 /* segment_end */);

    EXPECT_TRUE(rowset->is_segment_range_mode());
    EXPECT_EQ(1, rowset->segment_range_start());
    EXPECT_EQ(3, rowset->segment_range_end());
    EXPECT_EQ(2, rowset->num_segments());
}

TEST_F(LakeRowsetTest, test_segment_range_mode_single_segment) {
    create_rowsets_for_testing();

    // Create a rowset with segment range [1, 2) - single segment
    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* segment_start */,
                                                 2 /* segment_end */);

    EXPECT_TRUE(rowset->is_segment_range_mode());
    EXPECT_EQ(1, rowset->segment_range_start());
    EXPECT_EQ(2, rowset->segment_range_end());
    EXPECT_EQ(1, rowset->num_segments());
}

TEST_F(LakeRowsetTest, test_segment_range_mode_load_segments) {
    create_rowsets_for_testing();

    // Create rowset with segment range [0, 2) out of 3 segments
    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* segment_start */,
                                                 2 /* segment_end */);

    // Load segments with fill_data_cache = false
    ASSIGN_OR_ABORT(auto segments, rowset->segments(false));
    ASSERT_EQ(2, segments.size()); // Only 2 segments loaded
}

TEST_F(LakeRowsetTest, test_segment_range_mode_vs_normal_mode) {
    create_rowsets_for_testing();

    // Normal mode: all 3 segments
    auto normal_rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    EXPECT_FALSE(normal_rowset->is_segment_range_mode());
    EXPECT_EQ(3, normal_rowset->num_segments());

    // Segment range mode: 2 segments [0, 2)
    auto range_rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* segment_start */,
                                                       2 /* segment_end */);
    EXPECT_TRUE(range_rowset->is_segment_range_mode());
    EXPECT_EQ(2, range_rowset->num_segments());

    // Compaction segment limit mode: 1 segment
    auto limit_rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* compaction_segment_limit */);
    EXPECT_FALSE(limit_rowset->is_segment_range_mode());
    EXPECT_TRUE(limit_rowset->partial_segments_compaction());
    EXPECT_EQ(1, limit_rowset->num_segments());
}

TEST_F(LakeRowsetTest, test_segment_range_mode_segment_ids) {
    create_rowsets_for_testing();

    // Create rowset with segment range [1, 3) - segments 1 and 2
    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* segment_start */,
                                                 3 /* segment_end */);

    ASSIGN_OR_ABORT(auto segments, rowset->segments(false));
    ASSERT_EQ(2, segments.size());

    // Verify segment IDs are correctly set (should be 1 and 2, not 0 and 1)
    // The segment ID is stored in the segment's metadata
    EXPECT_EQ(1, segments[0]->id());
    EXPECT_EQ(2, segments[1]->id());
}

} // namespace starrocks::lake
