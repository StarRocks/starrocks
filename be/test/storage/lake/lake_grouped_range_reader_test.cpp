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

#include <limits>
#include <set>
#include <tuple>

#include "column/datum_tuple.h"
#include "storage/chunk_helper.h"
#include "storage/lake/metacache.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/query/range_group_task.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "test_util.h"
#include "testutil/assert.h"

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

    std::shared_ptr<TabletMetadata> write_source(int64_t source, const std::vector<int64_t>& keys,
                                                 bool bundle = false) {
        auto meta = std::make_shared<TabletMetadata>();
        meta->set_id(next_id());
        meta->set_version(2);
        meta->set_next_rowset_id(3);
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
        auto chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(schema), keys.size() * 3);
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
        for (int segment = 0; segment < 2; ++segment) {
            // Keep identity distinct even when C/u are equal across copies or physical segments.
            auto identities = chunk->get_column_by_index(4)->as_mutable_ptr();
            identities->reset_column();
            for (int64_t row = 0; row < keys.size() * 3; ++row) {
                identities->append_datum(Datum(source * 100000 + segment * 10000 + row));
            }
            CHECK_OK(writer->write(*chunk));
            CHECK_OK(writer->finish());
        }
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_overlapped(true);
        rowset->set_num_rows(chunk->num_rows() * 2);
        for (const auto& file : writer->segments()) {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(file.path);
            sm->set_size(*file.size);
            sm->set_shared(true);
        }
        if (bundle) {
            const std::string name = std::to_string(next_id()) + ".dat";
            auto fs = FileSystem::CreateSharedFromString(_test_dir).value();
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

    Status read(const std::shared_ptr<TabletMetadata>& meta, RangeGroupTaskPtr group, Rows* rows, bool split = false) {
        auto schema = ChunkHelper::convert_schema(TabletSchema::create(meta->schema()));
        TabletReader reader(_tablet_mgr.get(), meta, schema);
        RETURN_IF_ERROR(reader.prepare());
        TabletReaderParams params;
        params.grouped_range_task = std::move(group);
        if (split) params.rowid_range_option = std::make_shared<RowidRangeOption>();
        RETURN_IF_ERROR(reader.open(params));
        auto chunk = ChunkHelper::new_chunk(schema, 16);
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

TEST_F(LakeGroupedRangeReaderTest, RechecksCachedSegmentSchema) {
    auto meta = write_source(1, {0, 1});
    Rowset rowset(_tablet_mgr.get(), meta, 0, 0);
    auto pb = meta->schema();
    pb.mutable_column(1)->set_unique_id(999);
    auto archived = TabletSchema::create(pb);
    auto fs = FileSystem::CreateSharedFromString(_test_dir).value();
    for (const auto& sm : meta->rowsets(0).segment_metas()) {
        FileInfo info{_tablet_mgr->segment_location(meta->id(), sm.filename())};
        ASSIGN_OR_ABORT(auto segment, Segment::open(fs, info, 0, archived));
        _tablet_mgr->metacache()->cache_segment(info.cache_key(), segment);
    }
    OlapReaderStatistics stats;
    RowsetReadOptions options;
    options.tablet_schema = TabletSchema::create(meta->schema());
    options.grouped_range_task = task(0, 2);
    options.stats = &stats;
    auto result = rowset.read(ChunkHelper::convert_schema(options.tablet_schema), options);
    EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
}

TEST_F(LakeGroupedRangeReaderTest, RejectsSplitReader) {
    auto meta = write_source(1, {0, 1});
    Rows rows;
    auto status = read(meta, task(0, 1), &rows, true);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_TRUE(rows.empty());
}

} // namespace starrocks::lake
