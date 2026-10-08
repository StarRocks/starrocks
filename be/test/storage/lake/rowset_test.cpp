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

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "storage/chunk_helper.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/transactions.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

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

            auto files = writer->files();
            ASSERT_EQ(3, files.size());

            // add rowset metadata
            auto* rowset = _tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_next_compaction_offset(1);
            auto* segs = rowset->mutable_segments();
            for (auto& file : writer->files()) {
                segs->Add(std::move(file.path));
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

<<<<<<< HEAD
=======
// LakeIOOptions::hold_segments memoizes the loaded Segment objects on the Rowset, so every later
// pass of the same compaction task reuses those instances instead of reloading them; and because
// the held set replaces the shared metadata cache as the reuse mechanism, nothing is pushed into
// that cache. get_segments() (the flat-json compaction path) must reuse the held set too, or the
// task keeps a second full copy of every input segment and caches it after all.
TEST_F(LakeRowsetTest, test_hold_segments_reuses_and_skips_metacache) {
    create_rowsets_for_testing();

    auto* cache = _tablet_mgr->metacache();
    cache->prune();

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    ASSERT_EQ(3, rowset->num_segments());
    ASSERT_TRUE(rowset->can_hold_segments());

    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    ASSIGN_OR_ABORT(auto first, rowset->segments(lake_io_opts));
    ASSERT_EQ(3, first.size());
    for (const auto& seg : first) {
        EXPECT_TRUE(cache->lookup_segment(seg->file_name()) == nullptr);
    }

    // Second call: the very same Segment instances, no reload.
    ASSIGN_OR_ABORT(auto second, rowset->segments(lake_io_opts));
    ASSERT_EQ(first.size(), second.size());
    for (size_t i = 0; i < first.size(); i++) {
        EXPECT_EQ(first[i].get(), second[i].get());
    }

    // get_segments() must hand back the held set rather than loading (and caching) a second copy.
    auto via_get_segments = rowset->get_segments();
    ASSERT_EQ(first.size(), via_get_segments.size());
    for (size_t i = 0; i < first.size(); i++) {
        EXPECT_EQ(first[i].get(), via_get_segments[i].get());
    }
    for (const auto& seg : first) {
        EXPECT_TRUE(cache->lookup_segment(seg->file_name()) == nullptr);
    }
}

// A segment-range rowset (large-rowset split subtask of parallel compaction) cannot use the
// prepared-segments path, because that path derives each segment's metadata position from its index
// in the vector and this rowset's vector starts at _segment_range_start. Holding for it would pin a
// set the read path never consults -- and, since the compaction tasks turn the metadata cache off
// whenever they ask for holding, would leave every column-group pass reloading every segment from
// remote storage. hold_segments must therefore degrade to the pre-hold behavior here: no held set,
// metadata cache filled.
TEST_F(LakeRowsetTest, test_hold_segments_falls_back_for_segment_range_rowset) {
    create_rowsets_for_testing();

    auto* cache = _tablet_mgr->metacache();
    cache->prune();

    auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* segment_start */,
                                                 3 /* segment_end */);
    ASSERT_EQ(2, rowset->num_segments());
    ASSERT_FALSE(rowset->can_hold_segments());

    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    ASSIGN_OR_ABORT(auto first, rowset->segments(lake_io_opts));
    ASSERT_EQ(2, first.size());
    // The fallback keeps filling the shared metadata cache, which is what the read pass relies on.
    for (const auto& seg : first) {
        EXPECT_TRUE(cache->lookup_segment(seg->file_name()) != nullptr);
    }
    // And the reuse still works, through the cache rather than through a held set.
    ASSIGN_OR_ABORT(auto second, rowset->segments(lake_io_opts));
    ASSERT_EQ(first.size(), second.size());
    for (size_t i = 0; i < first.size(); i++) {
        EXPECT_EQ(first[i].get(), second[i].get());
    }
}

// The pinned-bytes gauge is the only way an operator sees this memory class: it is not in the
// metadata cache, so the cache's own usage metric never reports it. What matters is that it balances
// -- the exact amount reported when a set is held is taken back whether the task releases the set
// (fallback) or simply ends (Rowset destroyed) -- otherwise the gauge drifts and becomes useless.
TEST_F(LakeRowsetTest, test_held_segment_bytes_metric_balances) {
    create_rowsets_for_testing();

    auto* gauge = &StorageMetrics::instance()->lake_compaction_held_segment_bytes;
    const int64_t before = gauge->value();
    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};

    // Released explicitly, the way a task that stops holding does.
    {
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        ASSIGN_OR_ABORT(auto held, rowset->segments(lake_io_opts));
        int64_t expected = 0;
        for (const auto& seg : held) {
            expected += static_cast<int64_t>(seg->mem_usage());
        }
        EXPECT_EQ(before + expected, gauge->value());
        rowset->release_held_segments();
        EXPECT_EQ(before, gauge->value());
        // Releasing twice must not take the amount back twice.
        rowset->release_held_segments();
        EXPECT_EQ(before, gauge->value());
    }
    EXPECT_EQ(before, gauge->value());

    // Never released, just destroyed with the task: the destructor must still give it back.
    {
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        ASSIGN_OR_ABORT(auto held, rowset->segments(lake_io_opts));
        EXPECT_GT(gauge->value(), before);
    }
    EXPECT_EQ(before, gauge->value());
}

// Loading key and multi-page ordinal indexes changes the Segment's resident footprint after it was
// first held. Refresh both the sizing charge and the gauge, including when only the JSON memo owns
// the set after fallback, and settle the refreshed amount on release or destruction.
TEST_F(LakeRowsetTest, test_held_segment_bytes_tracks_loaded_column_indexes) {
    const int32_t saved_page_size = config::data_page_size;
    config::data_page_size = 64;
    DeferOp restore([&]() { config::data_page_size = saved_page_size; });

    std::vector<int> keys(2000);
    for (size_t i = 0; i < keys.size(); ++i) {
        keys[i] = static_cast<int>(i);
    }
    add_rowset_with_segment_keys({keys}, {false});
    _tablet_mgr->metacache()->prune();

    auto* gauge = &StorageMetrics::instance()->lake_compaction_held_segment_bytes;
    const int64_t before = gauge->value();
    const LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    for (bool keep_json_memo : {false, true}) {
        {
            auto rowset =
                    std::make_shared<Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
            ASSIGN_OR_ABORT(auto held, rowset->segments(lake_io_opts));
            ASSERT_EQ(1, held.size());
            const int64_t before_key_bytes = rowset->held_segments_bytes();
            ASSERT_GT(before_key_bytes, 0);
            EXPECT_EQ(before + before_key_bytes, gauge->value());

            if (keep_json_memo) {
                ASSIGN_OR_ABORT(auto memo, rowset->get_segments_checked());
                ASSERT_EQ(held[0], memo[0]);
                rowset->release_held_segments();
                EXPECT_EQ(before_key_bytes, rowset->held_segments_bytes());
            }

            ASSERT_OK(held[0]->load_index(lake_io_opts));
            const int64_t initial_bytes = static_cast<int64_t>(held[0]->mem_usage());
            ASSERT_GT(initial_bytes, before_key_bytes);
            EXPECT_EQ(initial_bytes, rowset->held_segments_bytes());
            EXPECT_EQ(before + initial_bytes, gauge->value());

            auto* reader = const_cast<ColumnReader*>(held[0]->column_with_uid(_tablet_schema->column(0).unique_id()));
            ASSERT_NE(nullptr, reader);
            ASSERT_EQ(0, reader->num_data_pages());
            ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(held[0]->file_name()));
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(held[0]->file_info()));
            OlapReaderStatistics stats;
            IndexReadOptions index_opts;
            index_opts.read_file = read_file->stream().get();
            index_opts.stats = &stats;
            index_opts.use_page_cache = false;
            ASSERT_OK(reader->load_ordinal_index(index_opts));
            ASSERT_GT(reader->num_data_pages(), 1);

            const int64_t current_bytes = static_cast<int64_t>(held[0]->mem_usage());
            ASSERT_GT(current_bytes, initial_bytes);
            EXPECT_EQ(current_bytes, rowset->held_segments_bytes());
            EXPECT_EQ(before + current_bytes, gauge->value());

            rowset->release_held_segments();
            EXPECT_EQ(keep_json_memo ? current_bytes : 0, rowset->held_segments_bytes());
            EXPECT_EQ(before + (keep_json_memo ? current_bytes : 0), gauge->value());
        }
        EXPECT_EQ(before, gauge->value());
    }
}

// A range sibling may choose fallback while the elected loader is outside the Rowset mutex doing
// IO. The caller still needs the loaded segments, but publication must not make them task-held again.
TEST_F(LakeRowsetTest, test_hold_segments_does_not_publish_after_fallback_during_load) {
    create_rowsets_for_testing();
    _tablet_mgr->metacache()->prune();
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    auto* gauge = &StorageMetrics::instance()->lake_compaction_held_segment_bytes;
    const int64_t before = gauge->value();

    std::promise<void> load_entered;
    std::promise<void> resume_load;
    auto load_entered_future = load_entered.get_future();
    auto resume_load_future = resume_load.get_future();
    SyncPoint::GetInstance()->SetCallBack("Rowset::segments::load_for_hold", [&](void*) {
        load_entered.set_value();
        resume_load_future.wait();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_sync_point([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::segments::load_for_hold");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    const LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    auto loader = std::async(std::launch::async, [&]() { return rowset->segments(lake_io_opts); });
    load_entered_future.wait();
    rowset->release_held_segments();
    resume_load.set_value();
    auto result = loader.get();

    ASSERT_OK(result.status());
    ASSERT_EQ(3, result->size());
    for (size_t i = 0; i < result->size(); ++i) {
        ASSERT_NE(nullptr, (*result)[i]);
        EXPECT_EQ(i, (*result)[i]->id());
    }
    EXPECT_TRUE(rowset->hold_disabled());
    EXPECT_TRUE(rowset->_held_segments.empty());
    EXPECT_EQ(0, rowset->held_segments_bytes());
    EXPECT_EQ(before, gauge->value());
}

// The eligibility check and election must observe fallback under the same mutex. In the memo case,
// republishing also used to add a second gauge charge while overwriting the first recorded amount,
// leaving a permanent residual after destruction.
TEST_F(LakeRowsetTest, test_hold_segments_does_not_repin_after_fallback_before_election) {
    create_rowsets_for_testing();
    auto* gauge = &StorageMetrics::instance()->lake_compaction_held_segment_bytes;
    const int64_t before = gauge->value();
    const LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};

    for (bool keep_json_memo : {false, true}) {
        _tablet_mgr->metacache()->prune();
        {
            auto rowset =
                    std::make_shared<Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
            ASSIGN_OR_ABORT(auto initial, rowset->segments(lake_io_opts));
            const int64_t held_bytes = rowset->held_segments_bytes();
            ASSERT_GT(held_bytes, 0);
            if (keep_json_memo) {
                ASSIGN_OR_ABORT(auto memo, rowset->get_segments_checked());
                ASSERT_EQ(initial, memo);
            }

            std::promise<void> before_election;
            std::promise<void> resume_election;
            auto before_election_future = before_election.get_future();
            auto resume_election_future = resume_election.get_future();
            SyncPoint::GetInstance()->SetCallBack("Rowset::segments::before_hold_election", [&](void*) {
                before_election.set_value();
                resume_election_future.wait();
            });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp clear_sync_point([]() {
                SyncPoint::GetInstance()->ClearCallBack("Rowset::segments::before_hold_election");
                SyncPoint::GetInstance()->DisableProcessing();
            });

            auto sibling = std::async(std::launch::async, [&]() { return rowset->segments(lake_io_opts); });
            before_election_future.wait();
            rowset->release_held_segments();
            resume_election.set_value();
            auto result = sibling.get();

            ASSERT_OK(result.status());
            EXPECT_EQ(initial.size(), result->size());
            EXPECT_TRUE(rowset->hold_disabled());
            EXPECT_TRUE(rowset->_held_segments.empty());
            EXPECT_EQ(keep_json_memo ? held_bytes : 0, rowset->held_segments_bytes());
            EXPECT_EQ(before + (keep_json_memo ? held_bytes : 0), gauge->value());
        }
        EXPECT_EQ(before, gauge->value());
    }
}

// The single-flight election must survive an exception unwinding out of the load: the allocator
// hook returns nullptr on a mem-tracker overrun, so operator new throws std::bad_alloc, and nothing
// on the compaction path catches it. A hand-written flag clear would be skipped by the unwind,
// leaving the election claimed with nobody to notify and every sibling subtask blocked forever.
// Here the throw is injected at the load sync point; without the guard the second call below would
// hang instead of loading.
TEST_F(LakeRowsetTest, test_hold_segments_election_released_on_exception) {
    create_rowsets_for_testing();
    _tablet_mgr->metacache()->prune();

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    bool should_throw = true;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::segments::load_for_hold", [&](void*) {
        if (should_throw) {
            throw std::bad_alloc();
        }
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::segments::load_for_hold");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    EXPECT_THROW((void)rowset->segments(lake_io_opts), std::bad_alloc);
    // The election was given back, so the next caller can take it rather than waiting on a flag no
    // one will ever clear.
    EXPECT_FALSE(rowset->_held_segments_loading);

    should_throw = false;
    ASSIGN_OR_ABORT(auto segments, rowset->segments(lake_io_opts));
    EXPECT_EQ(3, segments.size());
}

// Range-split parallel compaction shares one Rowset across concurrent subtasks; the held-segment
// load must be single-flight, or every subtask that misses loads and parses the complete input set
// (full remote IO plus a private copy each, with cache filling off). Elect-one semantics: N
// concurrent callers produce exactly one load, and everyone gets the same held instances. The
// elected load is held in flight long enough for every other thread to arrive, so the pre-fix
// behavior (each thread loading its own copy) would be caught as loads > 1.
TEST_F(LakeRowsetTest, test_hold_segments_single_flight) {
    create_rowsets_for_testing();
    _tablet_mgr->metacache()->prune();

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    std::atomic<int> loads{0};
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::segments::load_for_hold", [&](void*) {
        loads++;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::segments::load_for_hold");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    constexpr int kThreads = 4;
    std::vector<std::vector<SegmentPtr>> results(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t]() {
            auto res = rowset->segments(lake_io_opts);
            EXPECT_TRUE(res.ok());
            if (res.ok()) {
                results[t] = std::move(res).value();
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(1, loads.load());
    for (int t = 0; t < kThreads; t++) {
        ASSERT_EQ(3, results[t].size());
        for (size_t i = 0; i < results[t].size(); i++) {
            EXPECT_EQ(results[0][i].get(), results[t][i].get());
        }
    }
}

// experimental_lake_ignore_lost_segment: when a segment file is physically missing, load_segments
// must (a) fail hard when the flag is off, and (b) when the flag is on, skip the lost segment while
// keeping the result positionally aligned -- a null placeholder in the lost slot, size unchanged --
// so the PK-index callers' `size == num_segments` checks and rssid derivation keep working instead
// of hitting the CHECK/RETURN_ERROR_IF_FALSE that used to crash/fail the rebuild.
TEST_F(LakeRowsetTest, test_load_segments_ignore_lost_segment) {
    create_rowsets_for_testing();

    // Physically remove the middle segment file to simulate a lost segment.
    auto lost_seg_name = _tablet_metadata->rowsets(0).segment_metas(1).filename();
    auto lost_seg_path = _tablet_mgr->segment_location(_tablet_metadata->id(), lost_seg_name);
    // Drop any cached copy so the loader actually re-reads from the (now missing) file.
    _tablet_mgr->metacache()->prune();
    ASSERT_OK(FileSystem::Default()->delete_file(lost_seg_path));

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    ASSERT_EQ(rowset->num_segments(), 3);

    // Flag off: a missing segment is a hard error.
    {
        config::experimental_lake_ignore_lost_segment = false;
        std::vector<SegmentPtr> segments;
        auto st = rowset->load_segments(&segments, false);
        ASSERT_FALSE(st.ok());
    }

    // Flag on: the lost segment is skipped, but its slot is preserved as a null placeholder so the
    // vector stays aligned with the segment metadata.
    {
        config::experimental_lake_ignore_lost_segment = true;
        DeferOp reset([] { config::experimental_lake_ignore_lost_segment = false; });

        std::vector<SegmentPtr> segments;
        ASSERT_OK(rowset->load_segments(&segments, false));
        ASSERT_EQ(segments.size(), static_cast<size_t>(rowset->num_segments()));
        EXPECT_NE(segments[0], nullptr);
        EXPECT_EQ(segments[1], nullptr);
        EXPECT_NE(segments[2], nullptr);

        // The delvec iterator builder (the PK rebuild path) must not crash on the null slot and must
        // keep the same positional alignment: the lost segment yields a null iterator in its own slot.
        OlapReaderStatistics stats;
        auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
        ASSIGN_OR_ABORT(auto seg_iters,
                        rowset->get_each_segment_iterator_with_delvec(input_schema, 1, nullptr, &stats));
        ASSERT_EQ(seg_iters.size(), static_cast<size_t>(rowset->num_segments()));
        EXPECT_NE(seg_iters[0], nullptr);
        EXPECT_EQ(seg_iters[1], nullptr);
        EXPECT_NE(seg_iters[2], nullptr);

        // The non-delvec builder must keep the same alignment: the lost segment is a null placeholder.
        ASSIGN_OR_ABORT(auto plain_iters, rowset->get_each_segment_iterator(input_schema, false, &stats));
        ASSERT_EQ(plain_iters.size(), static_cast<size_t>(rowset->num_segments()));
        EXPECT_NE(plain_iters[0], nullptr);
        EXPECT_EQ(plain_iters[1], nullptr);
        EXPECT_NE(plain_iters[2], nullptr);
        for (auto& it : plain_iters) {
            if (it != nullptr) {
                it->close();
            }
        }

        // get_read_iterator_num must skip the null slot (2 live segments; overlapped, so no union).
        ASSIGN_OR_ABORT(auto iter_num, rowset->get_read_iterator_num());
        EXPECT_EQ(iter_num, 2);

        // get_non_null_segments() returns only the live segments (the lost slot is dropped), so
        // position-agnostic consumers (logical-split scan, compaction sizing) never see a null.
        auto non_null = rowset->get_non_null_segments();
        EXPECT_EQ(non_null.size(), 2);
        for (const auto& s : non_null) {
            EXPECT_NE(s, nullptr);
        }
    }
}

// The PK publish / partial-update paths wrap every slot returned by get_each_segment_iterator in a
// SegmentPKIterator, including the nullptr placeholder produced for a lost segment. SegmentPKIterator
// must tolerate a null underlying iterator: yield no rows and, crucially, not crash in close() (which
// used to call _iter->close() unconditionally).
TEST_F(LakeRowsetTest, test_segment_pk_iterator_tolerates_null_slot) {
    SegmentPKIterator it;
    ASSERT_OK(it.init(nullptr, *_schema, /*lazy_load=*/true, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                      /*defer_data_load=*/true));
    EXPECT_TRUE(it.done()); // an empty slot yields no rows
    it.close();             // must not crash on the null underlying iterator
}

// Covers add_partial_compaction_segments_info's null guards: a lost segment in either the
// already-compacted range or the uncompacted range must be skipped (file-size info dropped) rather
// than crashing on the null placeholder. With next_compaction_offset=1 and compaction_segment_limit=1
// over a 3-segment rowset, segment 0 is in the already-compacted range and segment 2 in the
// uncompacted range, so losing both exercises both guards.
TEST_F(LakeRowsetTest, test_ignore_lost_segment_partial_compaction) {
    create_rowsets_for_testing();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    // A writer providing the "new compacted" segment for section 2 of add_partial_compaction.
    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    {
        std::vector<int> k{100, 101, 102};
        std::vector<int> v{1, 2, 3};
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(k.data(), k.size() * sizeof(int));
        c1->append_numbers(v.data(), v.size() * sizeof(int));
        Chunk chunk({std::move(c0), std::move(c1)}, _schema);
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
    }

    // Lose segment 0 (already-compacted range) and segment 2 (uncompacted range).
    for (int idx : {0, 2}) {
        auto name = _tablet_metadata->rowsets(0).segment_metas(idx).filename();
        ASSERT_OK(FileSystem::Default()->delete_file(_tablet_mgr->segment_location(_tablet_metadata->id(), name)));
    }
    _tablet_mgr->metacache()->prune();

    config::experimental_lake_ignore_lost_segment = true;
    DeferOp reset([] { config::experimental_lake_ignore_lost_segment = false; });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 1 /* compaction_segment_limit */);
    ASSERT_TRUE(rs->partial_segments_compaction());

    TxnLogPB txn_log;
    auto op_compaction = txn_log.mutable_op_compaction();
    CompactionTaskContext context(txn_id, _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    // Must not crash on the lost (null) segments in the already-compacted / uncompacted ranges.
    ASSERT_OK(task.fill_compaction_segment_info(op_compaction, writer.get()));
    writer->close();
}

// Covers the null-segment skip in VerticalCompactionTask::calculate_chunk_size_for_column_group: a
// lost segment must be skipped instead of dereferencing null in column_with_uid. Uses
// compaction_segment_limit=0 so the rowset is not in partial-compaction mode (which would early-return
// before the segment loop).
TEST_F(LakeRowsetTest, test_ignore_lost_segment_vertical_chunk_size) {
    create_rowsets_for_testing();

    auto name = _tablet_metadata->rowsets(0).segment_metas(1).filename();
    ASSERT_OK(FileSystem::Default()->delete_file(_tablet_mgr->segment_location(_tablet_metadata->id(), name)));
    _tablet_mgr->metacache()->prune();

    config::experimental_lake_ignore_lost_segment = true;
    DeferOp reset([] { config::experimental_lake_ignore_lost_segment = false; });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    // Must skip the lost (null) segment instead of crashing on segment->column_with_uid.
    ASSIGN_OR_ABORT(auto chunk_size, task.calculate_chunk_size_for_column_group({0}));
    EXPECT_GT(chunk_size, 0);
}

// get_read_iterator_num() must answer from the rowset metadata when every segment in its window
// records num_rows: choose_compaction_algorithm() calls it before every compaction, and the old
// implementation parsed every input segment's footer only to throw the result away. The segment
// files are deleted before the call, so a successful count proves no segment was loaded; the
// legacy fallback is then proven engaged by stripping num_rows from one segment meta and watching
// the same call fail on the missing files.
TEST_F(LakeRowsetTest, test_get_read_iterator_num_from_metadata) {
    create_rowsets_for_testing();

    for (auto& seg_meta : *_tablet_metadata->mutable_rowsets(0)->mutable_segment_metas()) {
        seg_meta.set_num_rows(44);
    }

    // Remove the files: only a metadata-based count can still succeed.
    for (const auto& seg_meta : _tablet_metadata->rowsets(0).segment_metas()) {
        ASSERT_OK(FileSystem::Default()->delete_file(
                _tablet_mgr->segment_location(_tablet_metadata->id(), seg_meta.filename())));
    }
    _tablet_mgr->metacache()->prune();

    {
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        ASSIGN_OR_ABORT(auto num, rowset->get_read_iterator_num());
        ASSERT_EQ(3, num); // overlapped rowset: one iterator per non-empty segment
    }

    // A zero-row segment contributes no iterator, exactly like the footer-based count.
    {
        _tablet_metadata->mutable_rowsets(0)->mutable_segment_metas(1)->set_num_rows(0);
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        ASSIGN_OR_ABORT(auto num, rowset->get_read_iterator_num());
        ASSERT_EQ(2, num);
    }

    // Any segment without num_rows (e.g. written by cross-cluster replication) forces the legacy
    // loading path for the whole rowset -- which must now fail on the deleted files.
    {
        _tablet_metadata->mutable_rowsets(0)->mutable_segment_metas(1)->clear_num_rows();
        auto rowset = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0,
                                                     0 /* compaction_segment_limit */);
        ASSERT_FALSE(rowset->get_read_iterator_num().ok());
    }
}

// The held input set stays resident for the whole task, so it is charged against the same
// compaction_memory_limit_per_worker the read chunks are sized from -- but only up to a point: past
// the point where holding would shrink the read chunk by more than kMaxHeldChunkShrink, the task
// stops holding and sizes from the budget left after releasing segments. Driven with synthetic inputs so the
// arithmetic (including the non-positive remainder, which get_read_chunk_size would read as "no
// memory cap") is pinned independently of what the test segments happen to measure.
TEST_F(LakeRowsetTest, test_chunk_size_charges_held_segments) {
    create_rowsets_for_testing();

    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    DeferOp restore([&]() { config::compaction_memory_limit_per_worker = saved_mem_limit; });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);

    // Synthetic sizing inputs, so the arithmetic is exercised without depending on what the tiny
    // test segments happen to measure.
    const int64_t kLimit = 1000000;
    const int64_t kRows = 1000;
    const int64_t kFootprint = 100000;
    const size_t kSources = 10;
    const int32_t kCfgChunk = config::lake_compaction_chunk_size;
    config::compaction_memory_limit_per_worker = kLimit;

    const int32_t unheld = CompactionUtils::get_read_chunk_size(kLimit, kCfgChunk, kRows, kFootprint, kSources);

    // Half the budget held: charged against the sizing, and holding continues.
    task._hold_input_segments = true;
    const int64_t half = kLimit / 2;
    EXPECT_EQ(CompactionUtils::get_read_chunk_size(kLimit - half, kCfgChunk, kRows, kFootprint, kSources),
              task.chunk_size_with_held_segments(half, kRows, kFootprint, kSources));
    EXPECT_TRUE(task._hold_input_segments);

    // A memo can retain segments after holding is disabled; these bytes still reduce the budget.
    task._hold_input_segments = false;
    EXPECT_EQ(CompactionUtils::get_read_chunk_size(kLimit - half, kCfgChunk, kRows, kFootprint, kSources),
              task.chunk_size_with_held_segments(half, kRows, kFootprint, kSources));
    EXPECT_EQ(unheld, task.chunk_size_with_held_segments(0, kRows, kFootprint, kSources));
    EXPECT_EQ(1, task.chunk_size_with_held_segments(kLimit, kRows, kFootprint, kSources));
    EXPECT_EQ(1, task.chunk_size_with_held_segments(kLimit + 1, kRows, kFootprint, kSources));

    // Nearly the whole budget held: the read chunk would shrink by more than kMaxHeldChunkShrink,
    // so the task stops holding and sizes from the full budget instead.
    task._hold_input_segments = true;
    EXPECT_EQ(unheld, task.chunk_size_with_held_segments(kLimit - kLimit / 100, kRows, kFootprint, kSources));
    EXPECT_FALSE(task._hold_input_segments);

    // At or over budget: get_read_chunk_size() must never see the non-positive remainder (it reads
    // that as "no memory cap" and would answer with the largest chunk of all).
    task._hold_input_segments = true;
    EXPECT_EQ(unheld, task.chunk_size_with_held_segments(kLimit, kRows, kFootprint, kSources));
    EXPECT_FALSE(task._hold_input_segments);

    // A non-positive limit means "no memory cap" and must stay that way, holding or not.
    config::compaction_memory_limit_per_worker = -1;
    task._hold_input_segments = true;
    EXPECT_EQ(kCfgChunk, task.chunk_size_with_held_segments(half, kRows, kFootprint, kSources));
    EXPECT_TRUE(task._hold_input_segments);
    config::compaction_memory_limit_per_worker = 0;
    task._hold_input_segments = false;
    EXPECT_EQ(kCfgChunk, task.chunk_size_with_held_segments(half, kRows, kFootprint, kSources));
}

// When the input set does not fit the per-worker budget, holding it is worse than not holding:
// get_read_chunk_size() divides what is left by the per-row footprint, so a starved budget collapses
// the read chunk to a single row and the task crawls -- while pinning a set the metadata-cache LRU
// cannot reclaim. The task must stop holding instead: release the held sets, clear the flag (so the
// remaining passes fill the shared metadata cache again), and size the chunks from the full budget.
TEST_F(LakeRowsetTest, test_chunk_size_falls_back_when_held_segments_do_not_fit) {
    create_rowsets_for_testing();

    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    const bool saved_parallel = config::enable_load_segment_parallel;
    config::enable_load_segment_parallel = false;
    DeferOp restore([&]() {
        config::compaction_memory_limit_per_worker = saved_mem_limit;
        config::enable_load_segment_parallel = saved_parallel;
    });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);

    int64_t held_bytes = 0;
    {
        ASSIGN_OR_ABORT(auto segments, rs->segments(false));
        for (const auto& seg : segments) {
            held_bytes += static_cast<int64_t>(seg->mem_usage());
        }
    }
    ASSERT_GT(held_bytes, 4);

    // Half the held set: whatever the implementation measures on its own instances cannot fit.
    // Both legs run at this same budget, so the sizing is comparable: falling back must produce
    // exactly the not-holding chunk size, never the starved one the subtraction would have given.
    config::compaction_memory_limit_per_worker = held_bytes / 2;

    task._hold_input_segments = false;
    ASSIGN_OR_ABORT(auto chunk_no_hold, task.calculate_chunk_size_for_column_group({0}));

    task._hold_input_segments = true;
    ASSIGN_OR_ABORT(auto chunk_hold, task.calculate_chunk_size_for_column_group({0}));

    EXPECT_EQ(chunk_no_hold, chunk_hold);
    // Flag cleared, so the remaining passes fill the shared metadata cache again; nothing pinned.
    EXPECT_FALSE(task._hold_input_segments);
    EXPECT_TRUE(rs->_held_segments.empty());
    EXPECT_TRUE(rs->_held_delvecs == nullptr);
    EXPECT_EQ(0, rs->held_segments_bytes());

    // The decision is sticky and lives on the shared Rowset, not on the task: a range-split sibling
    // whose own flag is still set must not re-elect itself and pin the set again. It gets a plain
    // cache-backed load instead.
    _tablet_mgr->metacache()->prune();
    LakeIOOptions sibling_opts{.fill_data_cache = false, .fill_metadata_cache = false, .hold_segments = true};
    ASSIGN_OR_ABORT(auto sibling_segments, rs->segments(sibling_opts));
    EXPECT_EQ(3, sibling_segments.size());
    EXPECT_TRUE(rs->_held_segments.empty());
    EXPECT_EQ(0, rs->held_segments_bytes());
    // ... and the downgrade turned the metadata cache back on for it, so the reuse mechanism the
    // fallback restores is actually in place.
    for (const auto& seg : sibling_segments) {
        EXPECT_TRUE(_tablet_mgr->metacache()->lookup_segment(seg->file_name()) != nullptr);
    }
}

// Flat-JSON inspection memoizes the held segments before later column groups may trigger a
// fallback. Releasing the holder leaves that memo alive, so every later chunk must still reserve
// its bytes even though _hold_input_segments is now false.
TEST_F(LakeRowsetTest, test_chunk_size_charges_memo_after_fallback) {
    create_rowsets_for_testing();

    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    DeferOp restore([&]() { config::compaction_memory_limit_per_worker = saved_mem_limit; });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    {
        LakeIOOptions opts{.fill_metadata_cache = false, .hold_segments = true};
        ASSIGN_OR_ABORT(auto segments, rs->segments(opts));
        ASSIGN_OR_ABORT(auto memo, rs->get_segments_checked());
        ASSERT_EQ(segments, memo);
    }
    const int64_t held_bytes = rs->held_segments_bytes();
    ASSERT_GT(held_bytes, 4);

    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    task._hold_input_segments = true;
    config::compaction_memory_limit_per_worker = held_bytes + 1;

    // One byte left yields a one-row chunk and triggers fallback. The memo keeps the input set.
    EXPECT_EQ(1, task.chunk_size_with_held_segments(held_bytes, 1000, 1000, 1));
    EXPECT_FALSE(task._hold_input_segments);
    EXPECT_TRUE(rs->_held_segments.empty());
    EXPECT_FALSE(rs->_segments.empty());
    EXPECT_EQ(held_bytes, rs->held_segments_bytes());
    EXPECT_EQ(1, task.chunk_size_with_held_segments(rs->held_segments_bytes(), 1000, 1000, 1));

    // The disabled-holding path sizes from the actual remainder, including an exhausted budget.
    config::compaction_memory_limit_per_worker = held_bytes + 1000;
    EXPECT_EQ(CompactionUtils::get_read_chunk_size(1000, config::lake_compaction_chunk_size, 1000, 1000, 1),
              task.chunk_size_with_held_segments(rs->held_segments_bytes(), 1000, 1000, 1));
    config::compaction_memory_limit_per_worker = held_bytes;
    EXPECT_EQ(1, task.chunk_size_with_held_segments(rs->held_segments_bytes(), 1000, 1000, 1));
    config::compaction_memory_limit_per_worker = held_bytes - 1;
    EXPECT_EQ(1, task.chunk_size_with_held_segments(rs->held_segments_bytes(), 1000, 1000, 1));
}

// Every (input source, column) stream of a read pass gets its own buffer, so the configured size alone
// would let a large fan-in outgrow the per-worker budget. The buffer shrinks to share half the budget,
// bounded by kMinStreamBufferSize below and the configured size above.
TEST_F(LakeRowsetTest, test_stream_buffer_size_shares_half_the_budget) {
    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    const int64_t saved_buffer = config::lake_compaction_stream_buffer_size_bytes;
    DeferOp restore([&]() {
        config::compaction_memory_limit_per_worker = saved_mem_limit;
        config::lake_compaction_stream_buffer_size_bytes = saved_buffer;
    });
    const int64_t kMin = CompactionTask::kMinStreamBufferSize;
    config::compaction_memory_limit_per_worker = 2L * 1024 * 1024 * 1024;
    config::lake_compaction_stream_buffer_size_bytes = 1024 * 1024;

    // Small fan-in: the configured size fits and is kept.
    EXPECT_EQ(1024 * 1024, CompactionTask::stream_buffer_size(100));
    // 500 sources x 5 columns: 2500 MB of buffers would not fit, half the budget is shared out.
    EXPECT_EQ(config::compaction_memory_limit_per_worker / 2 / 2500, CompactionTask::stream_buffer_size(2500));
    EXPECT_LE(2500 * CompactionTask::stream_buffer_size(2500), config::compaction_memory_limit_per_worker / 2);
    // Huge fan-in: never below the floor.
    EXPECT_EQ(kMin, CompactionTask::stream_buffer_size(1000000));
    // Nothing to size, no budget, or an already-small configured buffer: unchanged.
    EXPECT_EQ(1024 * 1024, CompactionTask::stream_buffer_size(0));
    config::compaction_memory_limit_per_worker = 0;
    EXPECT_EQ(1024 * 1024, CompactionTask::stream_buffer_size(2500));
    config::compaction_memory_limit_per_worker = 2L * 1024 * 1024 * 1024;
    config::lake_compaction_stream_buffer_size_bytes = kMin / 2;
    EXPECT_EQ(kMin / 2, CompactionTask::stream_buffer_size(1000000));
    config::lake_compaction_stream_buffer_size_bytes = -1;
    EXPECT_EQ(-1, CompactionTask::stream_buffer_size(1000000));
}

// The read buffers are allocated whatever the chunk size is, so they come off the budget before the
// chunk is sized -- but the chunk always keeps at least a quarter of it.
TEST_F(LakeRowsetTest, test_chunk_size_charges_stream_buffers) {
    create_rowsets_for_testing();

    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    DeferOp restore([&]() { config::compaction_memory_limit_per_worker = saved_mem_limit; });

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    task._hold_input_segments = false;

    const int64_t kLimit = 1000000;
    const int64_t kRows = 1000;
    const int64_t kFootprint = 100000;
    const size_t kSources = 10;
    const int32_t kCfgChunk = config::lake_compaction_chunk_size;
    config::compaction_memory_limit_per_worker = kLimit;

    EXPECT_EQ(CompactionUtils::get_read_chunk_size(kLimit / 2, kCfgChunk, kRows, kFootprint, kSources),
              task.chunk_size_with_held_segments(0, kRows, kFootprint, kSources, kLimit / 2));
    EXPECT_EQ(CompactionUtils::get_read_chunk_size(kLimit / 4, kCfgChunk, kRows, kFootprint, kSources),
              task.chunk_size_with_held_segments(0, kRows, kFootprint, kSources, kLimit * 2));
    // Held segments come out of what the buffers leave.
    EXPECT_EQ(CompactionUtils::get_read_chunk_size(kLimit / 2 - kLimit / 8, kCfgChunk, kRows, kFootprint, kSources),
              task.chunk_size_with_held_segments(kLimit / 8, kRows, kFootprint, kSources, kLimit / 2));
}

// A pass over a large fan-in picks the shrunk buffer for both its own segment loads and the reader it
// feeds, and counts itself in the task stats.
TEST_F(LakeRowsetTest, test_vertical_pass_shrinks_stream_buffer) {
    create_rowsets_for_testing();

    const int64_t saved_mem_limit = config::compaction_memory_limit_per_worker;
    const int64_t saved_buffer = config::lake_compaction_stream_buffer_size_bytes;
    DeferOp restore([&]() {
        config::compaction_memory_limit_per_worker = saved_mem_limit;
        config::lake_compaction_stream_buffer_size_bytes = saved_buffer;
    });
    config::compaction_memory_limit_per_worker = 2L * 1024 * 1024 * 1024;
    config::lake_compaction_stream_buffer_size_bytes = 1024 * 1024;

    auto rs = std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    CompactionTaskContext context(next_id(), _tablet_metadata->id(), 456, false, false, nullptr);
    VersionedTablet vt(nullptr, _tablet_metadata);
    VerticalCompactionTask task(vt, {rs}, &context, _tablet_schema);
    task._hold_input_segments = false;

    // A handful of sources: the configured buffer stays and nothing is counted.
    task._total_input_segs = 3;
    ASSERT_OK(task.calculate_chunk_size_for_column_group({0}).status());
    EXPECT_EQ(1024 * 1024, task._stream_buffer_size);
    EXPECT_EQ(0, context.stats->stream_buffer_shrunk_passes);

    // A backlog of 5000 sources over a one-column group cannot all have 1 MB.
    task._total_input_segs = 5000;
    ASSERT_OK(task.calculate_chunk_size_for_column_group({0}).status());
    EXPECT_EQ(CompactionTask::stream_buffer_size(5000), task._stream_buffer_size);
    EXPECT_LT(task._stream_buffer_size, 1024 * 1024);
    EXPECT_EQ(1, context.stats->stream_buffer_shrunk_passes);
}

>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
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
        ASSERT_EQ(2, writer->files().size());
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
        EXPECT_TRUE(files_to_delete[0].find(writer->files()[0].path) != std::string::npos);
        EXPECT_TRUE(files_to_delete[1].find(writer->files()[1].path) != std::string::npos);
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
        EXPECT_TRUE(files_to_delete[0].find(writer->files()[0].path) != std::string::npos);
        EXPECT_TRUE(files_to_delete[1].find(writer->files()[1].path) != std::string::npos);
    }
}

TEST_F(LakeRowsetTest, test_read_by_column_hash_is_congruent) {
    create_rowsets_for_testing();

    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    TabletSchemaCSPtr opt_tablet_schema = std::make_shared<const TabletSchema>(_tablet_metadata->schema());
    rs_opts.tablet_schema = opt_tablet_schema;

    std::string col_name(_tablet_schema->column(0).name());
    std::vector<ColumnId> input_schema_cids;
    input_schema_cids.push_back(1);
    auto mutable_rowset_meta_ptr = const_cast<RowsetMetadataPB*>(&rowset->metadata());

    auto record_predicate_pb = mutable_rowset_meta_ptr->mutable_record_predicate();
    record_predicate_pb->set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
    auto column_hash_is_congruent_pb = record_predicate_pb->mutable_column_hash_is_congruent();
    column_hash_is_congruent_pb->set_modulus(2);
    column_hash_is_congruent_pb->set_remainder(0);
    column_hash_is_congruent_pb->add_column_names(col_name);

    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, input_schema_cids);
    ASSERT_OK(rowset->read(input_schema, rs_opts));
    ASSERT_ERROR(rowset->get_each_segment_iterator(input_schema, false, &stats));
    ASSERT_ERROR(rowset->get_each_segment_iterator_with_delvec(input_schema, 1, nullptr, &stats));
}

// Verify that DeferOp in load_segments waits for all parallel tasks
// before returning on error, preventing use-after-free of captured |this|.
TEST_F(LakeRowsetTest, test_parallel_load_error_waits_all_futures) {
    create_rowsets_for_testing();

    bool old_enable_load_segment_parallel = config::enable_load_segment_parallel;
    config::enable_load_segment_parallel = true;

    std::atomic<int> total_hits{0};

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([old_enable_load_segment_parallel] {
        config::enable_load_segment_parallel = old_enable_load_segment_parallel;
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->ClearTrace();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // First call injects error; subsequent calls succeed normally.
    // total_hits >= 2 proves DeferOp waited for all tasks before returning.
    SyncPoint::GetInstance()->SetCallBack("Rowset::load_segments::parallel_load", [&](void* arg) {
        if (total_hits.fetch_add(1) == 0) {
            *static_cast<Status*>(arg) = Status::IOError("injected");
        }
    });

    // Create rowset after config is set (Rowset captures _parallel_load in constructor)
    auto rowset =
            std::make_shared<lake::Rowset>(_tablet_mgr.get(), _tablet_metadata, 0, 0 /* compaction_segment_limit */);

    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.stats = &stats;
    rs_opts.tablet_schema = _tablet_schema;
    auto input_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{0});
    auto rs = rowset->read(input_schema, rs_opts);
    ASSERT_FALSE(rs.ok());
    ASSERT_GE(total_hits.load(), 2);
}

} // namespace starrocks::lake
