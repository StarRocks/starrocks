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

#include <algorithm>
#include <initializer_list>
#include <limits>
#include <set>
#include <tuple>

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "storage/chunk_helper.h"
#include "storage/lake/metacache.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/query/range_group_task.h"
#include "storage/query/split_morsel_queue.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/short_key_range_option.h"
#include "test_util.h"

namespace starrocks::lake {
namespace {

TabletRangePB clip(std::optional<int64_t> lower, std::optional<int64_t> upper) {
    TabletRangePB range;
    auto bound = [](TuplePB* tuple, int64_t last) {
        for (auto v : {int64_t{7}, int64_t{8}, last}) {
            auto* value = tuple->add_values();
            *value->mutable_type() = TypeDescriptor(TYPE_BIGINT).to_protobuf();
            value->set_variant_type(VariantTypePB::NORMAL_VALUE);
            value->set_value(std::to_string(v));
        }
    };
    if (lower) {
        bound(range.mutable_lower_bound(), *lower);
        range.set_lower_bound_included(true);
    }
    if (upper) {
        bound(range.mutable_upper_bound(), *upper);
        range.set_upper_bound_included(false);
    }
    return range;
}

RangeGroupTaskPtr task(std::optional<int64_t> lower, std::optional<int64_t> upper) {
    return std::make_shared<const RangeGroupTask>(RangeGroupTask{0, clip(lower, upper), {100, 101, 102}});
}

// Include the nullable value and unique physical row identity, retaining duplicate C/u occurrences.
using Row = std::tuple<int64_t, int64_t, int64_t, std::optional<int64_t>, int64_t>;
using Rows = std::multiset<Row>;

} // namespace

class LakeGroupedRangeReaderTest : public TestBase {
public:
    LakeGroupedRangeReaderTest() : TestBase("test_lake_grouped_range_reader") {}
    void SetUp() override { clear_and_init_test_dir(); }
    void TearDown() override { remove_test_dir_ignore_error(); }

    std::shared_ptr<TabletMetadata> write_source(int64_t source, const std::vector<int64_t>& keys, bool bundle = false,
                                                 int num_rowsets = 1) {
        auto meta = std::make_shared<TabletMetadata>();
        meta->set_id(next_id());
        meta->set_version(2);
        meta->set_next_rowset_id(num_rowsets + 1);
        auto* pb = meta->mutable_schema();
        pb->set_id(next_id());
        pb->set_keys_type(DUP_KEYS);
        pb->set_num_short_key_columns(3);
        pb->set_num_rows_per_row_block(4);
        for (int i = 0; i < 5; ++i) {
            auto* col = pb->add_column();
            col->set_name("c" + std::to_string(i));
            col->set_unique_id(100 + i);
            col->set_type("BIGINT");
            col->set_is_key(i < 3);
            col->set_is_nullable(i == 3);
            col->set_aggregation("NONE");
            if (i < 3) pb->add_sort_key_idxes(i);
        }
        auto schema = TabletSchema::create(*pb);
        auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(schema), keys.size() * 3);
        for (auto key : keys) {
            // Two duplicate C/u occurrences plus one NULL DISTINCT argument at every C.
            for (int copy = 0; copy < 3; ++copy) {
                auto cols = chunk->columns();
                cols[0]->as_mutable_ptr()->append_datum(Datum(int64_t{7}));
                cols[1]->as_mutable_ptr()->append_datum(Datum(int64_t{8}));
                cols[2]->as_mutable_ptr()->append_datum(Datum(key));
                cols[3]->as_mutable_ptr()->append_datum(copy == 2 ? Datum() : Datum(int64_t{42}));
                cols[4]->as_mutable_ptr()->append_datum(Datum(source));
            }
        }
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*meta));
        VersionedTablet tablet(_tablet_mgr.get(), meta);
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
        CHECK_OK(writer->open());
        for (int segment = 0; segment < 2 * num_rowsets; ++segment) {
            // Keep identity distinct even when C/u are equal across copies or physical segments.
            auto identities = chunk->get_column_by_index(4)->as_mutable_ptr();
            identities->reset_column();
            for (int64_t row = 0; row < keys.size() * 3; ++row) {
                identities->append_datum(Datum(source * 100000 + segment * 10000 + row));
            }
            CHECK_OK(writer->write(*chunk));
            CHECK_OK(writer->finish());
        }
        RowsetMetadataPB* rowset = nullptr;
        for (size_t i = 0; i < writer->segments().size(); ++i) {
            if (i % 2 == 0) {
                rowset = meta->add_rowsets();
                rowset->set_id(i / 2 + 1);
                rowset->set_overlapped(true);
                rowset->set_num_rows(chunk->num_rows() * 2);
            }
            const auto& file = writer->segments()[i];
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(file.path);
            sm->set_size(*file.size);
            sm->set_shared(true);
        }
        for (auto& rowset_pb : *meta->mutable_rowsets()) {
            if (!bundle) break;
            auto* rowset = &rowset_pb;
            const std::string name = std::to_string(next_id()) + ".dat";
            auto fs = FileSystemFactory::CreateSharedFromString(_test_dir).value();
            ASSIGN_OR_ABORT(auto out, fs->new_writable_file(_tablet_mgr->segment_location(meta->id(), name)));
            int64_t offset = 0;
            for (auto& sm : *rowset->mutable_segment_metas()) {
                ASSIGN_OR_ABORT(auto in,
                                fs->new_random_access_file(_tablet_mgr->segment_location(meta->id(), sm.filename())));
                ASSIGN_OR_ABORT(auto data, in->read_all());
                CHECK_OK(out->append(Slice(data)));
                sm.set_filename(name);
                sm.set_bundle_file_offset(offset);
                offset += data.size();
            }
            CHECK_OK(out->close());
        }
        writer->close();
        return meta;
    }

    Status read(const std::shared_ptr<TabletMetadata>& meta, RangeGroupTaskPtr group, Rows* rows,
                bool prepared = false) {
        TabletReaderParams params;
        params.grouped_range_task = std::move(group);
        params.enable_prepared_physical_split_scan = prepared;
        return read_with_params(meta, params, rows);
    }

    Status read_with_params(const std::shared_ptr<TabletMetadata>& meta, const TabletReaderParams& params, Rows* rows) {
        auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
        TabletReader reader(_tablet_mgr.get(), meta, schema);
        RETURN_IF_ERROR(reader.prepare());
        RETURN_IF_ERROR(reader.open(params));
        auto chunk = ChunkFactory::new_chunk(schema, 16);
        while (true) {
            chunk->reset();
            auto status = reader.get_next(chunk.get());
            if (status.is_end_of_file()) break;
            RETURN_IF_ERROR(status);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                const auto row = chunk->get(i);
                rows->emplace(row[0].get_int64(), row[1].get_int64(), row[2].get_int64(),
                              row[3].is_null() ? std::nullopt : std::optional(row[3].get_int64()), row[4].get_int64());
            }
        }
        reader.close();
        return Status::OK();
    }

    // Drive the ordinary root's real PhysicalSplitMorselQueue, then reconstruct children as the connector does.
    void read_physical_children(const std::shared_ptr<TabletMetadata>& meta, TabletReaderParams params, Rows* rows) {
        const auto original_meta = meta->SerializeAsString();
        const auto original_group = params.grouped_range_task;
        const auto original_clip = original_group->key_range.SerializeAsString();
        const auto original_uids = original_group->sort_key_unique_ids;
        TInternalScanRange internal;
        internal.__set_tablet_id(meta->id());
        internal.__set_version(std::to_string(meta->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal);
        params.scan_range = &scan_range;
        params.plan_node_id = 1;
        params.scan_dop = 4;
        // A scoped reader parameter: no process-wide split-row setting survives this helper.
        params.splitted_scan_rows = 4;
        auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
        TabletReader root(_tablet_mgr.get(), meta, schema, true, true);
        ASSERT_OK(root.prepare());
        ASSERT_OK(root.open(params));
        auto chunk = ChunkFactory::new_chunk(schema, 16);
        ASSERT_TRUE(root.get_next(chunk.get()).is_end_of_file());
        ASSERT_EQ(0, chunk->num_rows());
        ASSERT_EQ(0, root.stats().raw_rows_read);
        std::vector<pipeline::ScanSplitContextPtr> children;
        root.get_split_tasks(&children);
        ASSERT_GT(children.size(), 1);
        root.close();
        for (auto& child : children) {
            auto* ctx = dynamic_cast<pipeline::LakeSplitContext*>(child.get());
            ASSERT_NE(nullptr, ctx);
            ASSERT_NE(nullptr, dynamic_cast<pipeline::PhysicalSplitMorselQueue*>(ctx->split_morsel_queue.get()));
            ASSERT_NE(nullptr, ctx->rowid_range);
            ASSERT_EQ(nullptr, ctx->short_key_range);
            ASSERT_EQ(pipeline::LakeSplitContext::RowidRangeSource::REGULAR, ctx->rowid_range_source);
            ASSERT_EQ(nullptr, ctx->prepared_tablet_read_state);
            ASSERT_EQ(nullptr, ctx->prepared_segment_read_state);
            params.rowid_range_option = ctx->rowid_range;
            // LakeDataSource clears these unused indices to zero for an ordinary nonprepared child.
            params.prepared_rowset_index = 0;
            params.prepared_segment_index = 0;
            Rows output;
            ASSERT_OK(read_with_params(meta, params, &output));
            for (const auto& row : output) {
                ASSERT_EQ(0, rows->count(row)) << "physical row appears in more than one child/clip";
                rows->insert(row);
            }
            EXPECT_EQ(original_group, params.grouped_range_task);
            EXPECT_EQ(original_clip, original_group->key_range.SerializeAsString());
            EXPECT_EQ(original_uids, original_group->sort_key_unique_ids);
        }
        EXPECT_EQ(original_meta, meta->SerializeAsString());
    }
    // A split planner must reject an incompatible source even when index pruning produces no child.
    void check_empty_query_schema_status(const std::shared_ptr<TabletMetadata>& meta,
                                         const std::vector<std::shared_ptr<Rowset>>& rowsets, bool invalid,
                                         std::initializer_list<bool> split_modes = {false, true}) {
        TInternalScanRange internal;
        internal.__set_tablet_id(meta->id());
        internal.__set_version(std::to_string(meta->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal);
        TabletReaderParams params;
        params.grouped_range_task = task(std::nullopt, std::nullopt);
        params.plan_node_id = 1;
        params.scan_range = &scan_range;
        params.scan_dop = 4;
        params.splitted_scan_rows = 4;
        params.range = TabletReaderParams::RangeStartOperation::GE;
        params.end_range = TabletReaderParams::RangeEndOperation::LT;
        // Prune on the leading column, independently of short-key suffix truncation.
        params.start_key.emplace_back(std::vector<std::string>{"0"});
        params.end_key.emplace_back(std::vector<std::string>{"1"});
        auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
        for (bool split : split_modes) {
            TabletReader reader(_tablet_mgr.get(), meta, schema, split, true, rowsets);
            ASSERT_OK(reader.prepare());
            const auto status = reader.open(params);
            if (invalid) {
                EXPECT_TRUE(status.is_invalid_argument()) << "physical root=" << split << ": " << status;
            } else {
                ASSERT_OK(status);
            }
            if (status.ok()) {
                auto chunk = ChunkFactory::new_chunk(schema, 16);
                EXPECT_TRUE(reader.get_next(chunk.get()).is_end_of_file());
                EXPECT_EQ(0, chunk->num_rows());
            }
            std::vector<pipeline::ScanSplitContextPtr> children;
            reader.get_split_tasks(&children);
            EXPECT_TRUE(children.empty());
            EXPECT_EQ(0, reader.stats().raw_rows_read);
            reader.close();
        }
    }
};

// Removing propagation, OR-ing the task with query ranges, or overwriting physical ownership
// duplicates boundary occurrences. Different source cuts deliberately cross the same segment.
TEST_F(LakeGroupedRangeReaderTest, UnalignedSourcesTileExactOccurrencesInSharedBundles) {
    auto a = write_source(1, {-2, -1, 0, 1, 2, 3, 4, 5}, true);
    auto b = write_source(2, {-2, -1, 0, 1, 2, 3, 4, 5}, true);
    *a->mutable_range() = clip(-1, 4);
    *b->mutable_range() = clip(0, 5);
    Rows baseline;
    ASSERT_OK(read(a, nullptr, &baseline));
    ASSERT_OK(read(b, nullptr, &baseline));
    ASSERT_EQ(60, baseline.size());
    Rows expected;
    for (int source : {1, 2}) {
        for (int key = source - 2; key < source + 3; ++key) {
            for (int segment = 0; segment < 2; ++segment) {
                const int64_t identity = source * 100000 + segment * 10000 + (key + 2) * 3;
                expected.emplace(7, 8, key, 42, identity);
                expected.emplace(7, 8, key, 42, identity + 1);
                expected.emplace(7, 8, key, std::nullopt, identity + 2);
            }
        }
    }
    ASSERT_EQ(expected, baseline);
    Rows grouped;
    for (const auto& meta : {a, b}) {
        // The outside clips are runtime empty after physical ownership is applied.
        for (auto [lo, hi] : {std::pair(-10, -1), {-1, 0}, {0, 2}, {2, 4}, {4, 5}, {5, 10}}) {
            ASSERT_OK(read(meta, task(lo, hi), &grouped));
        }
    }
    EXPECT_EQ(baseline, grouped);
}

TEST_F(LakeGroupedRangeReaderTest, ExtremesAndRuntimeEmptyClips) {
    const auto min = std::numeric_limits<int64_t>::min();
    const auto max = std::numeric_limits<int64_t>::max();
    auto meta = write_source(1, {min, -1, 0, max});
    Rows all, tiled, empty;
    ASSERT_OK(read(meta, nullptr, &all));
    ASSERT_EQ(24, all.size());
    ASSERT_OK(read(meta, task(std::nullopt, min), &empty));
    ASSERT_OK(read(meta, task(1, max), &empty));
    EXPECT_TRUE(empty.empty());
    for (auto group : {task(min, 0), task(0, max), task(max, std::nullopt)}) {
        ASSERT_OK(read(meta, group, &tiled));
    }
    EXPECT_EQ(all, tiled);
}

TEST_F(LakeGroupedRangeReaderTest, DeleteAndCapturedVersionVisibility) {
    auto old = write_source(1, {-1, 0, 1, 2});
    auto current = std::make_shared<TabletMetadata>(*old);
    current->set_version(3);
    auto* del = current->add_rowsets()->mutable_delete_predicate();
    del->set_version(-1);
    auto* pred = del->add_binary_predicates();
    pred->set_column_name("c2");
    pred->set_op("=");
    pred->set_value("1");
    Rows before, after, clipped;
    ASSERT_OK(read(old, task(0, 2), &before));
    ASSERT_EQ(12, before.size());
    ASSERT_OK(read(current, nullptr, &after));
    ASSERT_EQ(18, after.size());
    for (auto group : {task(std::nullopt, 0), task(0, 2), task(2, std::nullopt)}) {
        ASSERT_OK(read(current, group, &clipped));
    }
    EXPECT_EQ(after, clipped);
}

TEST_F(LakeGroupedRangeReaderTest, RejectsArchivedUidAndSortOrderMismatch) {
    auto meta = write_source(1, {0, 1});
    for (bool changed_uid : {false, true}) {
        auto changed = std::make_shared<TabletMetadata>(*meta);
        auto archived = changed->schema();
        archived.set_id(next_id());
        if (changed_uid)
            archived.mutable_column(1)->set_unique_id(999);
        else {
            archived.set_sort_key_idxes(0, 1);
            archived.set_sort_key_idxes(1, 0);
        }
        (*changed->mutable_rowset_to_schema())[1] = archived.id();
        (*changed->mutable_historical_schemas())[archived.id()] = archived;
        Rows rows;
        auto status = read(changed, task(0, 2), &rows);
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_TRUE(rows.empty());
    }
    Rows ordinary;
    ASSERT_OK(read(meta, nullptr, &ordinary));
    EXPECT_EQ(12, ordinary.size());
}

TEST_F(LakeGroupedRangeReaderTest, RechecksLoadedSegmentSchema) {
    auto meta = write_source(1, {0, 1});
    Rowset rowset(_tablet_mgr.get(), meta, 0, 0);
    auto pb = meta->schema();
    pb.mutable_column(1)->set_unique_id(999);
    auto archived = TabletSchema::create(pb);
    auto fs = FileSystemFactory::CreateSharedFromString(_test_dir).value();
    std::vector<SegmentPtr> segments;
    for (const auto& sm : meta->rowsets(0).segment_metas()) {
        ASSIGN_OR_ABORT(auto segment,
                        Segment::open(fs, FileInfo{_tablet_mgr->segment_location(meta->id(), sm.filename())},
                                      segments.size(), archived));
        segments.emplace_back(std::move(segment));
    }
    OlapReaderStatistics stats;
    RowsetReadOptions options;
    options.tablet_schema = TabletSchema::create(meta->schema());
    options.grouped_range_task = task(0, 2);
    options.stats = &stats;
    auto result = rowset.read(ChunkHelper::convert_schema(options.tablet_schema), options, segments);
    EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
}

TEST_F(LakeGroupedRangeReaderTest, PhysicalRootsValidateArchivedSchemaBeforeEmptyPruning) {
    for (int mismatch : {0, 1, 2}) {
        // Each archive has independent files/cache entries; a rejected schema must not poison a control.
        auto meta = write_source(1, {20, 21, 22, 23}, false, 2);
        auto changed = std::make_shared<TabletMetadata>(*meta);
        auto archived = changed->schema();
        archived.set_id(next_id());
        if (mismatch == 0) {
            archived.mutable_column(1)->set_unique_id(999);
        } else if (mismatch == 1) {
            archived.set_sort_key_idxes(0, 1);
            archived.set_sort_key_idxes(1, 0);
        }
        // A compatible archive with a different schema ID remains valid, including empty queries.
        for (const auto& rowset : changed->rowsets()) {
            (*changed->mutable_rowset_to_schema())[rowset.id()] = archived.id();
        }
        (*changed->mutable_historical_schemas())[archived.id()] = archived;
        ASSERT_NO_FATAL_FAILURE(check_empty_query_schema_status(
                changed, Rowset::get_rowsets(_tablet_mgr.get(), changed), mismatch != 2));
    }
}

TEST_F(LakeGroupedRangeReaderTest, PhysicalRootPrepassPreservesColdCachePolicyAndFooterStats) {
    auto meta = write_source(1, {20, 21, 22, 23});
    auto rowsets = Rowset::get_rowsets(_tablet_mgr.get(), meta);
    auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
    TabletReader root(_tablet_mgr.get(), meta, schema, true, true, rowsets);
    ASSERT_OK(root.prepare());
    _tablet_mgr->prune_metacache();
    TInternalScanRange internal;
    internal.__set_tablet_id(meta->id());
    internal.__set_version(std::to_string(meta->version()));
    TScanRange scan;
    scan.__set_internal_scan_range(internal);
    TabletReaderParams params;
    params.scan_range = &scan;
    params.plan_node_id = 1;
    params.grouped_range_task = task(std::nullopt, std::nullopt);
    params.scan_dop = 4;
    params.splitted_scan_rows = 4;
    params.lake_io_opts.fill_data_cache = false;
    params.lake_io_opts.fill_metadata_cache = false;
    ASSERT_OK(root.open(params));
    EXPECT_EQ(2, root.stats().segments_opened);
    EXPECT_GT(root.stats().io_ns, 0);
    for (const auto& sm : meta->rowsets(0).segment_metas()) {
        EXPECT_EQ(nullptr,
                  _tablet_mgr->metacache()->lookup_segment(_tablet_mgr->segment_location(meta->id(), sm.filename())));
    }
    // Physical planning has already consumed the prepass's memo, without a second footer load.
    ASSIGN_OR_ABORT(auto memo, rowsets[0]->get_segments_checked());
    ASSERT_EQ(2, memo.size());
    std::vector<pipeline::ScanSplitContextPtr> children;
    root.get_split_tasks(&children);
    EXPECT_GT(children.size(), 1);
    root.close();
}

TEST_F(LakeGroupedRangeReaderTest, PhysicalRootsValidateHeldSegmentSchemaBeforeEmptyPruning) {
    auto meta = write_source(1, {20, 21, 22, 23}, false, 2);
    auto bad_pb = meta->schema();
    bad_pb.set_id(next_id());
    bad_pb.mutable_column(1)->set_unique_id(999);
    auto bad_schema = TabletSchema::create(bad_pb);
    auto fs = FileSystemFactory::CreateSharedFromString(_test_dir).value();
    auto rowsets = Rowset::get_rowsets(_tablet_mgr.get(), meta);
    for (auto& rowset : rowsets) {
        ASSERT_OK(rowset->load());
        std::vector<SegmentPtr> segments;
        for (const auto& sm : rowset->metadata().segment_metas()) {
            ASSIGN_OR_ABORT(auto segment,
                            Segment::open(fs, FileInfo{_tablet_mgr->segment_location(meta->id(), sm.filename())},
                                          segments.size(), bad_schema));
            segments.emplace_back(std::move(segment));
        }
        // Model held segments whose actual schema differs from the valid rowset schema.
        rowset->_segments = std::move(segments);
        rowset->_segments_loaded = true;
    }
    // Split roots consume the held vector; ordinary readers reopen the valid files instead.
    // Evict the shared cache so this assertion cannot depend on retaining injected entries.
    _tablet_mgr->prune_metacache();
    ASSERT_NO_FATAL_FAILURE(check_empty_query_schema_status(meta, rowsets, true, {true}));
    ASSERT_NO_FATAL_FAILURE(check_empty_query_schema_status(meta, Rowset::get_rowsets(_tablet_mgr.get(), meta), false));
}

// A missing split guard narrowing fails at root open; missing rowid intersection duplicates physical rows.
TEST_F(LakeGroupedRangeReaderTest, PhysicalSplitChildrenTileExactOccurrences) {
    auto meta = write_source(1, {0, 0, 0, 0, 1, 1, 2, 3}, false, 2);
    Rows baseline, children;
    ASSERT_OK(read(meta, nullptr, &baseline));
    ASSERT_EQ(96, baseline.size());
    TabletReaderParams params;
    params.grouped_range_task = task(0, 4);
    ASSERT_NO_FATAL_FAILURE(read_physical_children(meta, params, &children));
    EXPECT_EQ(baseline, children);
    EXPECT_EQ(32, std::count_if(children.begin(), children.end(),
                                [](const Row& row) { return !std::get<3>(row).has_value(); }));
}

// Removing any independent AND, collapsing query OR, or refreshing the snapshot changes literal coverage.
TEST_F(LakeGroupedRangeReaderTest, PhysicalSplitChildrenPreserveQueryOrTabletAndTaskIntersection) {
    auto old = write_source(2, {-2, -1, 0, 1, 2, 3, 4, 5, 6}, true, 2);
    *old->mutable_range() = clip(-1, 6);
    auto current = std::make_shared<TabletMetadata>(*old);
    current->set_version(3);
    auto* del = current->add_rowsets()->mutable_delete_predicate();
    del->set_version(-1);
    auto* pred = del->add_binary_predicates();
    pred->set_column_name("c2");
    pred->set_op("=");
    pred->set_value("4");
    for (const auto& meta : {old, current}) {
        TabletReaderParams params;
        params.range = TabletReaderParams::RangeStartOperation::GE;
        params.end_range = TabletReaderParams::RangeEndOperation::LT;
        for (auto [lo, hi] : {std::pair(-2, 1), {3, 7}}) {
            params.start_key.emplace_back(std::vector<std::string>{"7", "8", std::to_string(lo)});
            params.end_key.emplace_back(std::vector<std::string>{"7", "8", std::to_string(hi)});
        }
        Rows baseline, expected, children;
        ASSERT_OK(read_with_params(meta, params, &baseline));
        for (int key : {-1, 0, 3, 4, 5}) {
            if (meta == current && key == 4) continue;
            for (int segment = 0; segment < 4; ++segment) {
                const int64_t identity = 200000 + segment * 10000 + (key + 2) * 3;
                expected.emplace(7, 8, key, 42, identity);
                expected.emplace(7, 8, key, 42, identity + 1);
                expected.emplace(7, 8, key, std::nullopt, identity + 2);
            }
        }
        ASSERT_EQ(expected, baseline);
        for (auto [lo, hi] : {std::pair(-5, 0), {0, 2}, {2, 4}, {4, 5}, {5, 9}}) {
            params.grouped_range_task = task(lo, hi);
            ASSERT_NO_FATAL_FAILURE(read_physical_children(meta, params, &children));
        }
        EXPECT_EQ(expected, children);
    }
}

TEST_F(LakeGroupedRangeReaderTest, RejectsMalformedOrIncompatibleSplitReaders) {
    auto meta = write_source(1, {0, 1, 2, 3});
    auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
    TabletReaderParams ordinary;
    ordinary.grouped_range_task = task(0, 4);
    Rows positive;
    ASSERT_NO_FATAL_FAILURE(read_physical_children(meta, ordinary, &positive));
    ASSERT_EQ(24, positive.size());
    auto logical = std::make_shared<ShortKeyRangesOption>(std::vector<ShortKeyRangeOptionPtr>{}, true);
    for (int invalid = 0; invalid < 9; ++invalid) {
        auto params = ordinary;
        params.rowid_range_option = std::make_shared<RowidRangeOption>();
        if (invalid == 0) {
            params.rowid_range_option = nullptr;
            params.short_key_ranges_option = logical;
        } else if (invalid == 1) {
            params.short_key_ranges_option = logical;
        } else if (invalid == 2) {
            params.enable_prepared_physical_split_scan = true;
        } else if (invalid == 3) {
            params.prepared_tablet_read_state = std::make_shared<PreparedTabletReadState>();
        } else if (invalid == 4) {
            params.prepared_segment_read_state = std::make_shared<PreparedSegmentReadState>();
        } else if (invalid == 5) {
            params.refine_initial_coarse_split_and_append_refined_tasks = true;
        } else if (invalid == 6) {
            params.grouped_range_task =
                    std::make_shared<const RangeGroupTask>(RangeGroupTask{0, clip(0, 4), {100, 999, 102}});
        } else if (invalid == 7) {
            params.rowid_range_option = nullptr;
        }
        // Reject logical roots and mixed root/child contexts before consuming a scan descriptor.
        TabletReader reader(_tablet_mgr.get(), meta, schema, invalid == 7 || invalid == 8, invalid == 8);
        ASSERT_OK(reader.prepare());
        const auto status = reader.open(params);
        if (invalid == 6) {
            EXPECT_TRUE(status.is_invalid_argument()) << status;
        } else {
            EXPECT_TRUE(status.is_not_supported()) << invalid << ": " << status;
        }
    }
    // Cached prepared ranges bypass ordinary key/tablet/task clipping and remain forbidden with rowid children.
    Rowset rowset(_tablet_mgr.get(), meta, 0, 0);
    ASSIGN_OR_ABORT(auto segments, rowset.segments(false));
    ASSERT_FALSE(segments.empty());
    ASSIGN_OR_ABORT(auto task_range,
                    decode_range_group_seek(*ordinary.grouped_range_task, TabletSchema::create(meta->schema())));
    const std::vector<std::optional<Range<rowid_t>>> seek_cache;
    const std::optional<Range<rowid_t>> tablet_cache;
    for (int cached = 0; cached < 3; ++cached) {
        OlapReaderStatistics stats;
        SegmentReadOptions options;
        options.stats = &stats;
        options.task_range = task_range;
        options.rowid_range_option = std::make_shared<SparseRange<>>(Range<>(0, 2));
        if (cached == 0) options.read_state_cache.scan_range = options.rowid_range_option;
        if (cached == 1) options.read_state_cache.seek_range_rowid_ranges = &seek_cache;
        if (cached == 2) options.read_state_cache.tablet_rowid_range = &tablet_cache;
        ASSIGN_OR_ABORT(auto iterator, segments[0]->new_iterator(schema, options));
        auto chunk = ChunkFactory::new_chunk(schema, 16);
        EXPECT_TRUE(iterator->get_next(chunk.get()).is_not_supported());
        iterator->close();
    }
}

TEST_F(LakeGroupedRangeReaderTest, RejectsPreparedScanEvenWithoutSplit) {
    auto meta = write_source(1, {0, 1});
    Rows rows;
    auto status = read(meta, task(0, 1), &rows, true);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_TRUE(rows.empty());
}

} // namespace starrocks::lake
