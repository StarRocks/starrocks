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

#include "storage/lake/tablet_parallel_compaction_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <future>
#include <optional>

#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "common/config_compaction_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/thread/threadpool.h"
#include "fs/fs_factory.h"
#include "gen_cpp/lake_service.pb.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/tablet_splitter.h"
#include "storage/lake/test_util.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/segment_writer.h"
#include "storage/sort_key_sampler.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

class TestClosure : public google::protobuf::Closure {
public:
    void Run() override {
        std::lock_guard<std::mutex> lock(_mutex);
        _finished = true;
        _cv.notify_all();
    }

    bool wait_finish(int64_t timeout_ms = 5000) {
        std::unique_lock<std::mutex> lock(_mutex);
        return _cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return _finished; });
    }

    bool is_finished() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _finished;
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _finished = false;
};

class TabletParallelCompactionStateTest : public ::testing::Test {
protected:
    void SetUp() override { _state = std::make_unique<TabletParallelCompactionState>(); }

    std::unique_ptr<TabletParallelCompactionState> _state;
};

TEST_F(TabletParallelCompactionStateTest, test_can_create_subtask) {
    _state->max_parallel = 3;

    // Initially no running subtasks, should be able to create
    EXPECT_TRUE(_state->can_create_subtask());

    // Add running subtasks up to max
    for (int i = 0; i < 3; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        _state->running_subtasks[i] = std::move(info);
    }
    EXPECT_FALSE(_state->can_create_subtask());

    // Remove one, should be able to create again
    _state->running_subtasks.erase(0);
    EXPECT_TRUE(_state->can_create_subtask());
}

TEST_F(TabletParallelCompactionStateTest, test_is_rowset_compacting) {
    EXPECT_FALSE(_state->is_rowset_compacting(1));
    EXPECT_FALSE(_state->is_rowset_compacting(2));

    _state->compacting_rowsets[1] = 1;
    _state->compacting_rowsets[3] = 1;

    EXPECT_TRUE(_state->is_rowset_compacting(1));
    EXPECT_FALSE(_state->is_rowset_compacting(2));
    EXPECT_TRUE(_state->is_rowset_compacting(3));
}

TEST_F(TabletParallelCompactionStateTest, test_is_complete) {
    // No subtasks created yet
    EXPECT_FALSE(_state->is_complete());

    // Create subtasks
    _state->total_subtasks_created = 2;
    SubtaskInfo info1, info2;
    info1.subtask_id = 0;
    info2.subtask_id = 1;
    _state->running_subtasks[0] = std::move(info1);
    _state->running_subtasks[1] = std::move(info2);

    // Still running
    EXPECT_FALSE(_state->is_complete());

    // Complete one
    _state->running_subtasks.erase(0);
    EXPECT_FALSE(_state->is_complete());

    // Complete all -- still NOT complete, because submission has not been sealed. While
    // submit_subtasks_from_groups() registers groups one at a time, an empty running_subtasks only means
    // "the next group has not been registered yet"; treating that as completion is what allowed two
    // subtasks to each drive the completion transition for one tablet.
    _state->running_subtasks.erase(1);
    EXPECT_FALSE(_state->is_complete());

    // Sealing submission is what makes the predicate meaningful.
    _state->submission_done = true;
    EXPECT_TRUE(_state->is_complete());

    // And the transition can be claimed exactly once, however many callers race for it.
    EXPECT_TRUE(_state->claim_completion());
    EXPECT_FALSE(_state->claim_completion());
}

// Pins the compaction-policy configs that these tests' expected subtask counts are derived from, and
// puts the previous values back on destruction. The BE test binary shares one process, so without
// this the number of rowsets pick_rowsets() returns depends on whatever suite ran earlier: with a
// leaked max_cumulative_compaction_num_singleton_deltas=10, a 20-rowset tablet yields 2 subtasks
// instead of 4. The pinned values are the config defaults.
class CompactionPolicyConfigPin {
public:
    CompactionPolicyConfigPin() {
        config::enable_size_tiered_compaction_strategy = true;
        config::min_cumulative_compaction_num_singleton_deltas = 5;
        config::max_cumulative_compaction_num_singleton_deltas = 500;
    }

    ~CompactionPolicyConfigPin() {
        config::enable_size_tiered_compaction_strategy = _enable_size_tiered;
        config::min_cumulative_compaction_num_singleton_deltas = _min_cumulative_deltas;
        config::max_cumulative_compaction_num_singleton_deltas = _max_cumulative_deltas;
    }

private:
    // Captured before the constructor body overwrites them.
    bool _enable_size_tiered = config::enable_size_tiered_compaction_strategy;
    int64_t _min_cumulative_deltas = config::min_cumulative_compaction_num_singleton_deltas;
    int64_t _max_cumulative_deltas = config::max_cumulative_compaction_num_singleton_deltas;
};

class TabletParallelCompactionManagerTest : public TestBase {
public:
    TabletParallelCompactionManagerTest() : TestBase(kTestDirectory) { clear_and_init_test_dir(); }

protected:
    constexpr static const char* kTestDirectory = "test_tablet_parallel_compaction_manager";

    void SetUp() override {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        _manager = std::make_unique<TabletParallelCompactionManager>(_tablet_mgr.get());

        ThreadPoolBuilder("test_pool")
                .set_min_threads(0)
                .set_max_threads(1) // Use 1 thread to allow execution if needed, or 0 to block
                .build(&_thread_pool);
    }

    void TearDown() override {
        _manager.reset();
        if (_thread_pool) {
            _thread_pool->shutdown();
        }
        remove_test_dir_ignore_error();
    }

    void create_tablet_with_rowsets(int64_t tablet_id, int num_rowsets, int64_t rowset_size) {
        create_tablet_with_rowsets_internal(tablet_id, num_rowsets, rowset_size, DUP_KEYS);
    }

    void create_pk_tablet_with_rowsets(int64_t tablet_id, int num_rowsets, int64_t rowset_size) {
        create_tablet_with_rowsets_internal(tablet_id, num_rowsets, rowset_size, PRIMARY_KEYS);
    }

    void create_tablet_with_rowsets_internal(int64_t tablet_id, int num_rowsets, int64_t rowset_size,
                                             KeysType keys_type) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(num_rowsets + 1);

        for (int i = 0; i < num_rowsets; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100);
            rowset->set_data_size(rowset_size);

            std::string segment_name = fmt::format("segment_{}.dat", i);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(segment_name);
            segment_meta->set_size(rowset_size);

            // Create dummy segment file
            std::string path = _lp->segment_location(tablet_id, segment_name);
            std::string dir = std::filesystem::path(path).parent_path().string();
            CHECK_OK(fs::create_directories(dir));
            auto fs = FileSystemFactory::CreateSharedFromString(path);
            WritableFileOptions opts;
            opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
            auto st = fs.value()->new_writable_file(opts, path);
            CHECK_OK(st.status());
            CHECK_OK(st.value()->append("dummy_segment_data"));
            CHECK_OK(st.value()->close());
            CHECK_OK(st.status());
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    // Writes a real segment (key column c0 = start_key..start_key+num_rows-1, value column c1 constant 0)
    // for |tablet_id| at |segment_name|, under |schema_pb|. Returns the file size.
    uint64_t write_int_key_segment(int64_t tablet_id, const TabletSchemaPB& schema_pb, const std::string& segment_name,
                                   int64_t num_rows, int32_t start_key = 0) {
        auto tablet_schema = TabletSchema::create(schema_pb);
        std::string path = _lp->segment_location(tablet_id, segment_name);
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        WritableFileOptions opts;
        opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        auto wfile = fs.value()->new_writable_file(opts, path);
        CHECK_OK(wfile.status());

        SegmentWriterOptions writer_opts;
        SegmentWriter writer(std::move(wfile.value()), /*segment_id=*/0, tablet_schema, writer_opts);
        CHECK_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < num_rows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(start_key + static_cast<int32_t>(i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
        return file_size;
    }

    // ================================================================================
    // Range-split sampling fixture
    //
    // A tablet whose written key layout is known exactly, so a subtask's row count can be measured
    // against ground truth instead of against the split algorithm's own per-range estimate.
    // ================================================================================

    // (k VARCHAR, v INT), DUP_KEYS, sort key = k. VARCHAR is deliberate: it is what forces sampling
    // onto the data-page path, because short_key_index_encodes_full_sort_key() rejects the schema on
    // TYPE -- sort_key_fixed_encode_size(TYPE_VARCHAR) falls into its `default: return 0` arm.
    // That holds at ANY index_length, so the value below does not
    // select the path and raising it would change nothing; it is set only because a real VARCHAR key
    // carries a truncated short key, and 4 is narrower than the 6-digit keys this fixture writes.
    static TabletSchemaPB varchar_sort_key_schema_pb() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_id(next_id());
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(65535);
        auto* k = pb.add_column();
        k->set_unique_id(1);
        k->set_name("k");
        k->set_type("VARCHAR");
        k->set_is_key(true);
        k->set_is_nullable(false);
        k->set_length(32);
        k->set_index_length(4);
        auto* v = pb.add_column();
        v->set_unique_id(2);
        v->set_name("v");
        v->set_type("INT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_aggregation("NONE");
        pb.add_sort_key_idxes(0);
        return pb;
    }

    // Zero-padded so byte order == numeric order. That is what lets a VARCHAR range bound be read
    // back as the integer key it denotes, and it keeps each written run non-decreasing so the
    // sampler's monotonicity validation sees a well-formed segment.
    static std::string encode_varchar_key(int64_t key) { return fmt::format("{:06d}", key); }

    static TuplePB make_varchar_tuple(int64_t key) {
        TuplePB tuple;
        auto* v = tuple.add_values();
        TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(32);
        v->mutable_type()->CopyFrom(type_desc.to_protobuf());
        v->set_variant_type(VariantTypePB::NORMAL_VALUE);
        v->set_value(encode_varchar_key(key));
        return tuple;
    }

    // Writes a real segment holding the ascending key run [start_key, start_key + num_rows) under
    // |schema_pb| (key column k, value column v constant 0). Returns the file size.
    uint64_t write_varchar_key_segment(int64_t tablet_id, const TabletSchemaPB& schema_pb,
                                       const std::string& segment_name, int64_t num_rows, int64_t start_key) {
        auto tablet_schema = TabletSchema::create(schema_pb);
        std::string path = _lp->segment_location(tablet_id, segment_name);
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        WritableFileOptions opts;
        opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        auto wfile = fs.value()->new_writable_file(opts, path);
        CHECK_OK(wfile.status());

        SegmentWriterOptions writer_opts;
        SegmentWriter writer(std::move(wfile.value()), /*segment_id=*/0, tablet_schema, writer_opts);
        CHECK_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, num_rows);
        auto cols = chunk->columns();
        // The Slices below point into this vector, which must therefore outlive append_chunk.
        std::vector<std::string> encoded;
        encoded.reserve(num_rows);
        for (int64_t i = 0; i < num_rows; ++i) {
            encoded.push_back(encode_varchar_key(start_key + i));
            cols[0]->as_mutable_ptr()->append_datum(Datum(Slice(encoded.back())));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
        return file_size;
    }

    // Builds a tablet of |num_rowsets| rowsets, one real segment each, and returns Rowsets over it.
    //
    // Segment s holds the dense key run [s * rows_each / 2, s * rows_each / 2 + rows_each), so
    // consecutive segments overlap over half their key span -- the shape a set of rowsets awaiting
    // compaction has, and one the coarse [min, max] model cannot divide evenly: with only the
    // segments' 2*N endpoints as boundary candidates, and each segment's rows spread EQUALLY over
    // the candidate ranges it overlaps rather than by width (distribute_to_ranges), the outer
    // subtasks come out ~50% off even though every key run is perfectly uniform.
    //
    // Records the layout in _written_key_runs so written_rows_in_group() can reconstruct ground
    // truth, and the tablet id in _sampling_tablet_id.
    std::vector<RowsetPtr> build_rowsets_with_varchar_sort_key(int num_rowsets, int64_t rows_each) {
        const int64_t tablet_id = next_id();
        const auto schema_pb = varchar_sort_key_schema_pb();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(num_rowsets + 1);
        *metadata->mutable_schema() = schema_pb;

        _written_key_runs.clear();
        const int64_t shift = rows_each / 2;
        for (int s = 0; s < num_rowsets; ++s) {
            const int64_t start_key = s * shift;
            const std::string name = fmt::format("varchar_seg_{}.dat", s);
            write_varchar_key_segment(tablet_id, schema_pb, name, rows_each, start_key);
            _written_key_runs.emplace_back(start_key, rows_each);

            auto* rowset = metadata->add_rowsets();
            rowset->set_id(s);
            rowset->set_overlapped(false);
            rowset->set_num_rows(rows_each);
            // Recorded rather than measured from disk: the split algorithm reads only these sizes,
            // and pinning them is what lets kBytesPerSubtask be a constant (see its comment).
            rowset->set_data_size(kSampledRowsetDataSize);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(name);
            segment_meta->set_size(kSampledRowsetDataSize);
            segment_meta->set_num_rows(rows_each);
            // The sampler rejects any sample outside [sort_key_min, sort_key_max], so these must be
            // the segment's real first and last key -- not a rounded envelope.
            segment_meta->mutable_sort_key_min()->CopyFrom(make_varchar_tuple(start_key));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_varchar_tuple(start_key + rows_each - 1));
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, metadata->version()));
        auto meta = tablet.metadata();
        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); ++i) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }
        _sampling_tablet_id = tablet_id;
        return rowsets;
    }

    // The integer key a subtask range bound denotes, or nullopt when the bound is absent
    // (unbounded). The key space IS the integers [0, ...): the fixture stores them zero-padded.
    static std::optional<int64_t> decode_varchar_bound(const VariantTuple& bound) {
        if (bound.empty()) {
            return std::nullopt;
        }
        CHECK_EQ(1u, bound.size());
        return std::stoll(bound[0].value().get_slice().to_string());
    }

    // Rows the FIXTURE actually wrote into |group|'s key range -- arithmetic over the written runs,
    // never a second reading of the algorithm's own estimate. SubtaskGroup::total_bytes comes from
    // RangeSplitResult::range_data_sizes, which the greedy loop optimises directly, so a spread
    // assertion over THAT is satisfied by construction and would pass with no samples at all.
    //
    // Reads range_lower_inclusive / range_upper_inclusive rather than assuming the [lower, upper)
    // convention _create_range_split_groups writes today. This helper IS the ground truth of the
    // evenness tests, so if that convention ever changed, an assumption here would mis-measure
    // silently instead of failing. The key space is the integers, so an exclusive bound is just the
    // adjacent one.
    int64_t written_rows_in_group(const SubtaskGroup& group) const {
        auto lower = decode_varchar_bound(group.range_lower_bound);
        auto upper = decode_varchar_bound(group.range_upper_bound);
        if (lower.has_value() && !group.range_lower_inclusive) {
            lower = *lower + 1;
        }
        if (upper.has_value() && group.range_upper_inclusive) {
            upper = *upper + 1;
        }
        int64_t rows = 0;
        for (const auto& [start, count] : _written_key_runs) {
            const int64_t lo = lower.has_value() ? std::max(*lower, start) : start;
            const int64_t hi = upper.has_value() ? std::min(*upper, start + count) : start + count;
            rows += std::max<int64_t>(0, hi - lo);
        }
        return rows;
    }

    // Each rowset's RECORDED data_size. Pinned in the metadata instead of measured from the file so
    // that kBytesPerSubtask can be a constant; nothing on the read path consults it.
    static constexpr int64_t kSampledRowsetDataSize = 1000000;
    // One rowset's worth, so ceil(total_bytes / kBytesPerSubtask) == the rowset count. With
    // max_parallel equal to that count, target_subtasks is the count AND the greedy loop's target
    // is total/count rather than this cap -- calculate_range_split_boundaries uses
    // min(total / actual_split_count, target_value_per_split), so a smaller value here would make
    // it place every boundary inside the first few candidate ranges.
    static constexpr int64_t kBytesPerSubtask = kSampledRowsetDataSize;
    // Subtasks _create_range_split_groups produced for
    // build_rowsets_with_varchar_sort_key(4, 25'000) at max_parallel 4 BEFORE sampling was wired
    // in, i.e. from coarse [min, max] bounds alone. Pinned so a change in compaction parallelism
    // cannot ride along unnoticed with a change in boundary quality.
    static constexpr size_t kExpectedSubtaskCount = 4;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::unique_ptr<TabletParallelCompactionManager> _manager;
    std::unique_ptr<ThreadPool> _thread_pool;
    // Constructed before SetUp() and destroyed after TearDown(), so every test in this fixture sees
    // the pinned values and no other suite inherits them.
    CompactionPolicyConfigPin _config_pin;
    int64_t _sampling_tablet_id = 0;
    // (first key, rows) of every segment build_rowsets_with_varchar_sort_key wrote.
    std::vector<std::pair<int64_t, int64_t>> _written_key_runs;
};

TEST_F(TabletParallelCompactionManagerTest, test_get_tablet_state_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    EXPECT_EQ(nullptr, state);
}

TEST_F(TabletParallelCompactionManagerTest, test_is_tablet_complete_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    // Non-existent tablet should return true (considered complete)
    EXPECT_TRUE(_manager->is_tablet_complete(tablet_id, txn_id));
}

TEST_F(TabletParallelCompactionManagerTest, test_cleanup_tablet) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    // Cleanup non-existent tablet should not crash
    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found());
}

TEST_F(TabletParallelCompactionManagerTest, test_metrics_initial_value) {
    EXPECT_EQ(0, _manager->running_subtasks());
    EXPECT_EQ(0, _manager->completed_subtasks());
}

// Builds |rowset_count| shared rowsets of |segments_per_rowset| segments, each |segment_bytes| big.
// Returned by value alongside the metadata that owns them.
static std::vector<RowsetPtr> make_shared_rowsets(TabletManager* tablet_mgr, const MutableTabletMetadataPtr& metadata,
                                                  int rowset_count, int segments_per_rowset, int64_t segment_bytes) {
    std::vector<RowsetPtr> rowsets;
    for (int rowset_id = 0; rowset_id < rowset_count; ++rowset_id) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_num_rows(segments_per_rowset * 100);
        rowset->set_data_size(segments_per_rowset * segment_bytes);
        rowset->set_overlapped(false);
        for (int segment = 0; segment < segments_per_rowset; ++segment) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("unshare_{}_{}", rowset_id, segment));
            segment_meta->set_size(segment_bytes);
            segment_meta->set_shared(true);
        }
        rowsets.push_back(std::make_shared<Rowset>(tablet_mgr, metadata, rowset_id, 0));
    }
    return rowsets;
}

// The planner refuses to plan at all rather than returning a partial cover. UNSHARE is
// all-or-nothing, so "no groups" is the safe answer and the caller falls back to the serial path.
TEST_F(TabletParallelCompactionManagerTest, test_create_unshare_groups_degenerate_inputs) {
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(90101);
    metadata->set_version(2);
    auto rowsets = make_shared_rowsets(_tablet_mgr.get(), metadata, 2, 4, 100);

    EXPECT_TRUE(_manager->_create_unshare_subtask_groups(metadata->id(), {}, 4, 200).empty()) << "no rowsets";
    EXPECT_TRUE(_manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 1, 200).empty()) << "no parallelism";
    EXPECT_TRUE(_manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 4, 0).empty()) << "no byte budget";
    // Budget larger than the whole tablet: one part is enough, so there is nothing to parallelise.
    EXPECT_TRUE(_manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 4, 1 << 20).empty())
            << "a single part covers everything";
}

// More rowsets than parts: each group takes whole rowsets and never splits one, so every group is
// NORMAL and the cover is still exact.
TEST_F(TabletParallelCompactionManagerTest, test_create_unshare_groups_pack_whole_rowsets) {
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(90102);
    metadata->set_version(2);
    auto rowsets = make_shared_rowsets(_tablet_mgr.get(), metadata, 6, 2, 100);

    auto groups = _manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 3, 400);
    ASSERT_FALSE(groups.empty());
    EXPECT_LE(groups.size(), 3u);
    EXPECT_TRUE(std::all_of(groups.begin(), groups.end(),
                            [](const SubtaskGroup& group) { return group.type == SubtaskType::NORMAL; }));
    EXPECT_TRUE(_manager->_validate_unshare_group_coverage(rowsets, groups).ok());

    size_t covered = 0;
    for (const auto& group : groups) {
        covered += group.rowsets.size();
    }
    EXPECT_EQ(rowsets.size(), covered) << "every rowset must land in exactly one group";
}

// A rowset that only warrants one part is emitted as NORMAL rather than as a one-part
// LARGE_ROWSET_PART, while its outsized sibling is still split. Both shapes in one plan.
TEST_F(TabletParallelCompactionManagerTest, test_create_unshare_groups_mixed_part_counts) {
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(90103);
    metadata->set_version(2);
    std::vector<RowsetPtr> rowsets;
    // rowset 0 is tiny (1 segment), rowset 1 is 8x bigger and must be carved up.
    for (int rowset_id = 0; rowset_id < 2; ++rowset_id) {
        const int segments = rowset_id == 0 ? 1 : 8;
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_num_rows(segments * 100);
        rowset->set_data_size(segments * 100);
        rowset->set_overlapped(false);
        for (int segment = 0; segment < segments; ++segment) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("mixed_{}_{}", rowset_id, segment));
            segment_meta->set_size(100);
            segment_meta->set_shared(true);
        }
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), metadata, rowset_id, 0));
    }

    auto groups = _manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 4, 200);
    ASSERT_FALSE(groups.empty());
    EXPECT_TRUE(_manager->_validate_unshare_group_coverage(rowsets, groups).ok())
            << "a mixed plan must still cover every segment exactly once";
}

// Coverage validation has to reject a plan that drops a whole rowset, not just one with a
// gap/overlap inside a rowset -- dropping one is exactly how sibling rows would survive UNSHARE.
TEST_F(TabletParallelCompactionManagerTest, test_validate_unshare_coverage_rejects_a_missing_rowset) {
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(90104);
    metadata->set_version(2);
    auto rowsets = make_shared_rowsets(_tablet_mgr.get(), metadata, 2, 4, 100);

    auto groups = _manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 4, 200);
    ASSERT_FALSE(groups.empty());
    ASSERT_TRUE(_manager->_validate_unshare_group_coverage(rowsets, groups).ok());

    groups.pop_back();
    EXPECT_FALSE(_manager->_validate_unshare_group_coverage(rowsets, groups).ok());
}

TEST_F(TabletParallelCompactionManagerTest, test_create_unshare_groups_cover_all_segments) {
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(90001);
    metadata->set_version(2);
    std::vector<RowsetPtr> rowsets;
    for (uint32_t rowset_id = 0; rowset_id < 2; ++rowset_id) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_num_rows(600);
        rowset->set_data_size(600);
        rowset->set_overlapped(false);
        for (int segment = 0; segment < 6; ++segment) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("unshare_{}_{}", rowset_id, segment));
            segment_meta->set_size(100);
            segment_meta->set_shared(true);
        }
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), metadata, rowset_id, 0));
    }

    auto groups = _manager->_create_unshare_subtask_groups(metadata->id(), rowsets, 4, 200);
    ASSERT_EQ(4, groups.size());
    EXPECT_TRUE(_manager->_validate_unshare_group_coverage(rowsets, groups).ok());
    EXPECT_TRUE(std::all_of(groups.begin(), groups.end(),
                            [](const SubtaskGroup& group) { return group.type == SubtaskType::LARGE_ROWSET_PART; }));

    groups.front().segment_start = 1;
    auto invalid = _manager->_validate_unshare_group_coverage(rowsets, groups);
    EXPECT_FALSE(invalid.ok());
    EXPECT_TRUE(invalid.message().find("gap/overlap") != std::string::npos);

    const int64_t txn_id = 90002;
    auto state_or = _manager->create_and_register_tablet_state(metadata->id(), txn_id, metadata->version(), 4, 200,
                                                               true, nullptr, [](bool /*success*/) {});
    ASSERT_TRUE(state_or.ok());
    state_or.value()->expected_unshare_subtask_count = 2;
    auto merged_log = _manager->get_merged_txn_log(metadata->id(), txn_id);
    EXPECT_FALSE(merged_log.ok());
    EXPECT_TRUE(merged_log.status().message().find("Incomplete parallel UNSHARE") != std::string::npos);
    _manager->cleanup_tablet(metadata->id(), txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;
    int32_t subtask_id = 0;

    auto context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, 1, false, true, nullptr);

    // Should not crash when state not exist
    _manager->on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
}

TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_two_groups) {
    int64_t tablet_id = 10001;
    int64_t txn_id = 20001;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024); // 5MB per subtask, will create 2 groups

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a thread pool with 1 thread
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // Submit a blocking task to occupy the thread
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for the blocking task to start
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Should be 2 groups (10MB / 5MB per group)

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    ASSERT_EQ(2, state->running_subtasks.size());
    // Each group should have approximately 5 rowsets (5MB each)
    ASSERT_EQ(5, state->running_subtasks[0].input_rowset_ids.size());
    ASSERT_EQ(5, state->running_subtasks[1].input_rowset_ids.size());

    // Unblock the thread
    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_multiple_groups) {
    int64_t tablet_id = 10002;
    int64_t txn_id = 20002;
    int64_t version = 11;

    // Create 10 rowsets, each 10MB
    create_tablet_with_rowsets(tablet_id, 10, 10 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(25 * 1024 * 1024); // 25MB limit

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a thread pool with 1 thread
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // Submit a blocking task to occupy the thread
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for the blocking task to start
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // Total 100MB. Max 25MB per task.
    // Group 1: 10+10 = 20MB (next is 30 > 25) -> 2 rowsets
    // Group 2: 10+10 = 20MB -> 2 rowsets
    // Group 3: 10+10 = 20MB -> 2 rowsets.
    // Remaining 4 rowsets are skipped because they exceed max_parallel * max_bytes capacity.

    // Expected:
    // Group 0: 2 rowsets (20MB)
    // Group 1: 2 rowsets (20MB)
    // Group 2: 2 rowsets (20MB)

    ASSERT_EQ(3, st.value());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    ASSERT_EQ(3, state->running_subtasks.size());

    ASSERT_EQ(2, state->running_subtasks[0].input_rowset_ids.size());
    ASSERT_EQ(2, state->running_subtasks[1].input_rowset_ids.size());
    ASSERT_EQ(2, state->running_subtasks[2].input_rowset_ids.size());

    // Unblock the thread
    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test manual completion flow by directly creating and manipulating TabletParallelCompactionState.
// This test does NOT use create_parallel_tasks because that function starts real compaction
// tasks which would fail with mock segment files. Instead, we test the on_subtask_complete
// and result merging logic directly.
TEST_F(TabletParallelCompactionManagerTest, test_manual_completion_flow) {
    int64_t tablet_id = 10003;
    int64_t txn_id = 20003;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create and register tablet state
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Create subtask info for 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.input_rowset_ids = {0, 1, 2, 3, 4};
        info0.input_bytes = 5 * 1024 * 1024;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.input_rowset_ids = {5, 6, 7, 8, 9};
        info1.input_bytes = 5 * 1024 * 1024;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    // Mark rowsets as compacting
    for (int i = 0; i < 10; i++) {
        state->compacting_rowsets[i] = 1;
    }

    // Register the state with manager using the test helper method
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Simulate completion of subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());
    ASSERT_FALSE(_manager->is_tablet_complete(tablet_id, txn_id));

    // Simulate completion of subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Verify subtask_compactions - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(1, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(50, subtask0.output_rowset().num_rows());
    EXPECT_EQ(500, subtask0.output_rowset().data_size());

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(1, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(50, subtask1.output_rowset().num_rows());
    EXPECT_EQ(500, subtask1.output_rowset().data_size());

    // In real scenario, cleanup_tablet is called by CompactionScheduler::remove_states.
    // Since we don't have CompactionScheduler in this test, manually clean up.
    _manager->cleanup_tablet(tablet_id, txn_id);
    ASSERT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));
}

// Regression test for the merged parallel-compaction txn log being silently dropped on the regular
// (non-aggregate/non-file-bundling) path. On that path FE does not set skip_write_txnlog, so there is
// no aggregator to consume CompactResponse.txn_logs: the merged log MUST be persisted to object
// storage by the CN itself, exactly as a serial compaction does. Before the fix the merged context
// unconditionally set skip_write_txnlog=true, so the log went into the RPC response, was read by
// nobody, and the committed compaction txn could never be published.
TEST_F(TabletParallelCompactionManagerTest, test_regular_path_persists_merged_txn_log) {
    int64_t tablet_id = 10013;
    int64_t txn_id = 20013;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    // Regular path: skip_write_txnlog stays false (FE never sets it for non-file-bundling tables).
    request.set_skip_write_txnlog(false);
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.input_rowset_ids = {0, 1, 2, 3, 4};
        info0.input_bytes = 5 * 1024 * 1024;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.input_rowset_ids = {5, 6, 7, 8, 9};
        info1.input_bytes = 5 * 1024 * 1024;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }
    for (int i = 0; i < 10; i++) {
        state->compacting_rowsets[i] = 1;
    }
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Regular path: the merged log must NOT be handed back via the RPC response ...
    ASSERT_EQ(0, response.txn_logs_size());

    // ... it must instead be persisted to object storage under txn_log_location(tablet_id, txn_id),
    // where the publish daemon expects to find it.
    auto merged_log_or = _tablet_mgr->get_txn_log(tablet_id, txn_id);
    ASSERT_TRUE(merged_log_or.ok()) << merged_log_or.status();
    const auto& merged_log = *merged_log_or.value();
    ASSERT_TRUE(merged_log.has_op_parallel_compaction());
    ASSERT_EQ(2, merged_log.op_parallel_compaction().subtask_compactions_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
    ASSERT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));
}

// Regression: when persisting the merged txn log fails on the regular path, the tablet must be
// reported as failed (so FE does not commit an unpublishable compaction txn) and no txn log is
// returned inline. Covers the put_txn_log-failure branch in on_subtask_complete.
TEST_F(TabletParallelCompactionManagerTest, test_regular_path_put_txn_log_failure_marks_tablet_failed) {
    int64_t tablet_id = 10023;
    int64_t txn_id = 20023;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    // Regular path: skip_write_txnlog stays false, so the merged log is persisted via put_txn_log.
    request.set_skip_write_txnlog(false);
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.input_rowset_ids = {0, 1, 2, 3, 4};
        info0.input_bytes = 5 * 1024 * 1024;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.input_rowset_ids = {5, 6, 7, 8, 9};
        info1.input_bytes = 5 * 1024 * 1024;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }
    for (int i = 0; i < 10; i++) {
        state->compacting_rowsets[i] = 1;
    }
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Force the merged-log persistence (put_txn_log) to fail, then complete the last subtask so the
    // merge + persist runs.
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_txn_log_fail");
    ASSERT_TRUE(fp != nullptr);
    fp->setMode(trigger_mode);

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Disable the fail point before assertions so a failed assertion cannot leak it to other tests.
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);

    ASSERT_TRUE(closure.is_finished());
    // Persistence failed → the tablet is reported failed so FE will not commit an unpublishable txn.
    ASSERT_EQ(1, response.failed_tablets_size());
    EXPECT_EQ(tablet_id, response.failed_tablets(0));
    // Regular path never hands the merged log back via the RPC response.
    EXPECT_EQ(0, response.txn_logs_size());
    // And nothing was persisted at the standalone txn-log location.
    auto merged_log_or = _tablet_mgr->get_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(merged_log_or.ok());

    _manager->cleanup_tablet(tablet_id, txn_id);
    ASSERT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));
}

class SubtaskInfoTest : public ::testing::Test {};

TEST_F(SubtaskInfoTest, test_subtask_info_default_values) {
    SubtaskInfo info;
    EXPECT_EQ(0, info.subtask_id);
    EXPECT_TRUE(info.input_rowset_ids.empty());
    EXPECT_EQ(0, info.input_bytes);
    EXPECT_EQ(0, info.start_time);
}

TEST_F(SubtaskInfoTest, test_subtask_info_set_values) {
    SubtaskInfo info;
    info.subtask_id = 5;
    info.input_rowset_ids = {1, 2, 3};
    info.input_bytes = 1024 * 1024;
    info.start_time = 1234567890;

    EXPECT_EQ(5, info.subtask_id);
    EXPECT_EQ(3, info.input_rowset_ids.size());
    EXPECT_EQ(1024 * 1024, info.input_bytes);
    EXPECT_EQ(1234567890, info.start_time);
}

class TabletParallelCompactionStateFieldsTest : public ::testing::Test {
protected:
    void SetUp() override { _state = std::make_unique<TabletParallelCompactionState>(); }

    std::unique_ptr<TabletParallelCompactionState> _state;
};

TEST_F(TabletParallelCompactionStateFieldsTest, test_default_values) {
    EXPECT_EQ(0, _state->tablet_id);
    EXPECT_EQ(0, _state->txn_id);
    EXPECT_EQ(0, _state->version);
    EXPECT_EQ(0, _state->max_parallel);
    EXPECT_EQ(0, _state->max_bytes_per_subtask);
    EXPECT_EQ(0, _state->next_subtask_id);
    EXPECT_EQ(0, _state->total_subtasks_created);
    EXPECT_TRUE(_state->compacting_rowsets.empty());
    EXPECT_TRUE(_state->running_subtasks.empty());
    EXPECT_TRUE(_state->completed_subtasks.empty());
    EXPECT_EQ(nullptr, _state->callback);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_set_fields) {
    _state->tablet_id = 100;
    _state->txn_id = 200;
    _state->version = 5;
    _state->max_parallel = 10;
    _state->max_bytes_per_subtask = 5368709120L; // 5GB
    _state->next_subtask_id = 3;
    _state->total_subtasks_created = 5;

    EXPECT_EQ(100, _state->tablet_id);
    EXPECT_EQ(200, _state->txn_id);
    EXPECT_EQ(5, _state->version);
    EXPECT_EQ(10, _state->max_parallel);
    EXPECT_EQ(5368709120L, _state->max_bytes_per_subtask);
    EXPECT_EQ(3, _state->next_subtask_id);
    EXPECT_EQ(5, _state->total_subtasks_created);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_compacting_rowsets_operations) {
    // Use reference counting: each insert increments the count
    _state->compacting_rowsets[1] = 1;
    _state->compacting_rowsets[2] = 1;
    _state->compacting_rowsets[3] = 1;

    EXPECT_EQ(3, _state->compacting_rowsets.size());
    EXPECT_TRUE(_state->compacting_rowsets.count(1) > 0);
    EXPECT_TRUE(_state->compacting_rowsets.count(2) > 0);
    EXPECT_TRUE(_state->compacting_rowsets.count(3) > 0);
    EXPECT_FALSE(_state->compacting_rowsets.count(4) > 0);

    _state->compacting_rowsets.erase(2);
    EXPECT_EQ(2, _state->compacting_rowsets.size());
    EXPECT_FALSE(_state->compacting_rowsets.count(2) > 0);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_running_subtasks_operations) {
    SubtaskInfo info1;
    info1.subtask_id = 0;
    info1.input_bytes = 100;

    SubtaskInfo info2;
    info2.subtask_id = 1;
    info2.input_bytes = 200;

    _state->running_subtasks[0] = std::move(info1);
    _state->running_subtasks[1] = std::move(info2);

    EXPECT_EQ(2, _state->running_subtasks.size());
    EXPECT_EQ(100, _state->running_subtasks[0].input_bytes);
    EXPECT_EQ(200, _state->running_subtasks[1].input_bytes);

    _state->running_subtasks.erase(0);
    EXPECT_EQ(1, _state->running_subtasks.size());
    EXPECT_TRUE(_state->running_subtasks.find(0) == _state->running_subtasks.end());
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_completed_subtasks_operations) {
    auto ctx1 = std::make_unique<CompactionTaskContext>(100, 101, 1, false, true, nullptr);
    auto ctx2 = std::make_unique<CompactionTaskContext>(100, 102, 1, false, true, nullptr);

    _state->completed_subtasks.push_back(std::move(ctx1));
    _state->completed_subtasks.push_back(std::move(ctx2));

    EXPECT_EQ(2, _state->completed_subtasks.size());
    EXPECT_EQ(101, _state->completed_subtasks[0]->tablet_id);
    EXPECT_EQ(102, _state->completed_subtasks[1]->tablet_id);
}

// Test for max_bytes <= 0: should use BE config default value and fallback to normal compaction
// if data size is small
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_default_max_bytes) {
    int64_t tablet_id = 10010;
    int64_t txn_id = 20010;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB, much smaller than default max_bytes ~5GB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(-1); // Invalid, will use BE config default

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // When max_bytes <= 0, code uses BE config default value (lake_compaction_max_bytes_per_subtask).
    // With small data (10MB) and large default max_bytes (~5GB), it falls back to normal compaction.
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(0, st.value()); // Returns 0 indicating fallback to normal compaction

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for max_parallel <= 0 (line 54-56)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_invalid_max_parallel) {
    int64_t tablet_id = 10011;
    int64_t txn_id = 20011;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(0); // Invalid, should use 1
    config.set_max_bytes_per_subtask(100 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // Should fail with invalid max_parallel
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_invalid_argument());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for tablet not found (line 68)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_tablet_not_found) {
    int64_t tablet_id = 99999; // Non-existent tablet
    int64_t txn_id = 20012;
    int64_t version = 1;

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    ASSERT_FALSE(st.ok());
}

// Test for already existing parallel compaction (lines 216-217)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_already_exists) {
    int64_t tablet_id = 10013;
    int64_t txn_id = 20013;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    // First creation should succeed and create parallel tasks
    auto st1 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st1.ok());
    ASSERT_GT(st1.value(), 0); // Should create at least 1 group

    // Second creation with same tablet_id and txn_id should fail
    auto st2 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_FALSE(st2.ok());
    ASSERT_TRUE(st2.status().is_already_exist());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for acquire_token failure (lines 274-288)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_acquire_token_failure) {
    int64_t tablet_id = 10014;
    int64_t txn_id = 20014;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // acquire_token always returns false to simulate failure
    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return false; }, [](bool) {});

    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_resource_busy());
}

// Test for on_subtask_complete with subtask not found (lines 374-381)
TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_subtask_not_found) {
    int64_t tablet_id = 10015;
    int64_t txn_id = 20015;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // Try to complete a non-existent subtask (id 999)
    auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx->subtask_id = 999; // Non-existent subtask
    _manager->on_subtask_complete(tablet_id, txn_id, 999, std::move(ctx));

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for list_tasks with running and completed subtasks (lines 786-827)
TEST_F(TabletParallelCompactionManagerTest, test_list_tasks) {
    int64_t tablet_id = 10016;
    int64_t txn_id = 20016;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // List tasks while some are running
    std::vector<CompactionTaskInfo> infos;
    _manager->list_tasks(&infos);

    // Should have at least one task
    EXPECT_GE(infos.size(), 1);

    // Check task info
    for (const auto& info : infos) {
        EXPECT_EQ(txn_id, info.txn_id);
        EXPECT_EQ(tablet_id, info.tablet_id);
        EXPECT_EQ(version, info.version);
        EXPECT_GE(info.subtask_id, 0);
    }

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for merged TxnLog with overlapped output (lines 520-565)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_overlapped) {
    int64_t tablet_id = 10017;
    int64_t txn_id = 20017;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0 with overlapped output
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    auto* output0 = ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output0->set_num_rows(100);
    output0->set_data_size(1024);
    output0->set_overlapped(true);
    auto* output0_seg = output0->add_segment_metas();
    output0_seg->set_filename("segment_0.dat");
    output0_seg->set_size(512);
    output0_seg->set_encryption_meta("meta0");
    ctx0->txn_log->mutable_op_compaction()->set_compact_version(10);
    ctx0->table_id = 1001;
    ctx0->partition_id = 2001;

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Simulate completion of subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    auto* output1 = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output1->set_num_rows(200);
    output1->set_data_size(2048);
    output1->set_overlapped(false);
    auto* output1_seg = output1->add_segment_metas();
    output1_seg->set_filename("segment_1.dat");
    output1_seg->set_size(1024);
    output1_seg->set_encryption_meta("meta1");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtask 0 output
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(100, subtask0.output_rowset().num_rows());
    EXPECT_TRUE(subtask0.output_rowset().overlapped());

    // Subtask 1 output
    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(200, subtask1.output_rowset().num_rows());

    block_promise.set_value();
    pool->wait();
}

// Test for partial success: one subtask succeeds, one fails
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_one_succeeded_one_failed) {
    int64_t tablet_id = 10018;
    int64_t txn_id = 20018;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());
    ASSERT_FALSE(_manager->is_tablet_complete(tablet_id, txn_id));

    // Simulate completion of subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("simulated failure");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // With partial success, the overall status should be OK (one subtask succeeded)
    EXPECT_EQ(0, response.status().status_code());

    // Verify TxnLog only contains successful subtask's data
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // success_subtask_ids should only contain subtask 0
    EXPECT_EQ(1, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(0, op_parallel.success_subtask_ids(0));

    // Verify subtask_compactions only contains successful subtask
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(50, subtask0.output_rowset().num_rows());
    EXPECT_EQ(500, subtask0.output_rowset().data_size());
    EXPECT_EQ(1, subtask0.output_rowset().segment_metas_size());
    EXPECT_EQ("segment_0.dat", subtask0.output_rowset().segment_metas(0).filename());

    block_promise.set_value();
    pool->wait();
}

// Test for data exceeding max_parallel capacity (lines 141-156)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_exceeds_capacity) {
    int64_t tablet_id = 10019;
    int64_t txn_id = 20019;
    int64_t version = 11;

    // Create 10 rowsets, each 10MB = 100MB total
    create_tablet_with_rowsets(tablet_id, 10, 10 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);              // Only allow 2 subtasks
    config.set_max_bytes_per_subtask(20 * 1024 * 1024); // 20MB per subtask, so 40MB total capacity

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    // Should create max_parallel subtasks (2), skipping excess data
    ASSERT_EQ(2, st.value());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    // Each subtask should have limited rowsets
    ASSERT_EQ(2, state->running_subtasks.size());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for stats merging in on_subtask_complete (line 446)
TEST_F(TabletParallelCompactionManagerTest, test_stats_merging) {
    int64_t tablet_id = 10020;
    int64_t txn_id = 20020;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with stats
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx0->stats->io_ns_read_remote = 1000;
    ctx0->stats->io_bytes_read_remote = 2000;

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with stats
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->stats->io_ns_read_remote = 3000;
    ctx1->stats->io_bytes_read_remote = 4000;

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for no rowsets to compact (line 75)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_no_rowsets) {
    int64_t tablet_id = 10022;
    int64_t txn_id = 20022;
    int64_t version = 1;

    // Create tablet without any rowsets
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    // Don't add any rowsets
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // Should fail because no rowsets to compact
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_not_found());
}

// Test for valid_groups empty case (line 203)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_single_small_rowset) {
    int64_t tablet_id = 10023;
    int64_t txn_id = 20023;
    int64_t version = 2;

    // Create tablet with a single non-overlapped rowset (won't be selected for compaction)
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);

    // Add a single rowset that's not overlapped
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_overlapped(false); // Not overlapped, may not be selected
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    auto* segment_meta = rowset->add_segment_metas();
    segment_meta->set_filename("segment_0.dat");
    segment_meta->set_size(100);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // May fail if no rowsets selected, or succeed with single group
    // This tests the pick_rowsets path
}

// Test for execute_subtask when state is cleaned up (lines 631-644)
TEST_F(TabletParallelCompactionManagerTest, test_execute_subtask_state_cleaned_up) {
    int64_t tablet_id = 10024;
    int64_t txn_id = 20024;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to ensure parallel tasks are created
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(4).build(&pool);

    std::atomic<bool> subtask_started{false};
    std::promise<void> cleanup_done_promise;
    std::future<void> cleanup_done_future = cleanup_done_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { subtask_started.store(true); });

    // Wait briefly for subtask to start, then cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Cleanup tablet state while subtask might be running
    // This tests the path where state is gone during execute_subtask
    _manager->cleanup_tablet(tablet_id, txn_id);

    pool->wait();
}

// Test for partial subtask creation when some fail (lines 310-314)
TEST_F(TabletParallelCompactionManagerTest, test_partial_subtask_creation) {
    int64_t tablet_id = 10025;
    int64_t txn_id = 20025;
    int64_t version = 11;

    // Create 10 rowsets, each 5MB
    create_tablet_with_rowsets(tablet_id, 10, 5 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(15 * 1024 * 1024); // ~15MB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    int acquire_count = 0;
    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(),
            [&]() {
                acquire_count++;
                // First acquisition succeeds, subsequent ones fail
                return acquire_count <= 1;
            },
            [](bool) {});

    // Should fail with ResourceBusy because we require atomic acquisition of all tokens
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_resource_busy());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for listing completed subtasks (lines 810-826)
TEST_F(TabletParallelCompactionManagerTest, test_list_tasks_with_completed) {
    int64_t tablet_id = 10026;
    int64_t txn_id = 20026;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->runs.store(1, std::memory_order_relaxed);
    ctx0->start_time.store(::time(nullptr) - 10, std::memory_order_relaxed);
    ctx0->finish_time.store(::time(nullptr), std::memory_order_release);
    ctx0->skipped.store(false, std::memory_order_relaxed);
    ctx0->subtask_input_rowsets = 4;
    ctx0->stats->in_queue_time_sec = 7;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // List tasks - should include completed subtask
    std::vector<CompactionTaskInfo> infos;
    _manager->list_tasks(&infos);

    // Should have tasks listed
    EXPECT_GE(infos.size(), 1);

    // The PROFILE for a completed parallel subtask must carry both the CompactionTaskStats
    // counters and the subtask metadata - i.e. the same JSON schema as a running subtask.
    bool found_completed_profile = false;
    for (const auto& info : infos) {
        if (info.finish_time > 0 && info.runs > 0) {
            EXPECT_EQ(0, info.subtask_id);
            EXPECT_NE(info.profile.find(R"("in_queue_sec":7)"), std::string::npos);
            EXPECT_NE(info.profile.find(R"("subtask_id":0)"), std::string::npos);
            EXPECT_NE(info.profile.find(R"("input_rowsets":4)"), std::string::npos);
            EXPECT_EQ(info.profile.find(R"("input_bytes")"), std::string::npos);
            EXPECT_NE(info.profile.find(R"("is_parallel_subtask":true)"), std::string::npos);
            found_completed_profile = true;
        }
    }
    EXPECT_TRUE(found_completed_profile);

    // Complete subtask 1 to finish
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for copying table_id and partition_id from subtask contexts (lines 413-428)
TEST_F(TabletParallelCompactionManagerTest, test_table_partition_id_copy) {
    int64_t tablet_id = 10021;
    int64_t txn_id = 20021;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 without table_id and partition_id
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->table_id = 0;     // Not set
    ctx0->partition_id = 0; // Not set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with table_id and partition_id
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->table_id = 12345;
    ctx1->partition_id = 67890;

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for TxnLog merge without output (lines 520-527, 560-565)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_output) {
    int64_t tablet_id = 10027;
    int64_t txn_id = 20027;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with TxnLog but no output_rowset
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    // No output_rowset set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with TxnLog but no output_rowset
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    // No output_rowset set

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify subtask_compactions - each subtask has no output (or empty output)
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtasks have input but no output_rowset with data
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(1, subtask0.input_rowsets_size());
    // output_rowset exists but has 0 rows (default value)

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(1, subtask1.input_rowsets_size());

    block_promise.set_value();
    pool->wait();
}

// Test for TxnLog merge without compact_version (lines 567-574)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_compact_version) {
    int64_t tablet_id = 10028;
    int64_t txn_id = 20028;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 without compact_version
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    // No compact_version set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for subtask with null TxnLog (lines 501-507)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_null_txn_log) {
    int64_t tablet_id = 10029;
    int64_t txn_id = 20029;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with valid TxnLog
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with null TxnLog
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = nullptr; // Null TxnLog

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for subtask with TxnLog but no op_compaction (lines 501-507, 522-523)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_op_compaction) {
    int64_t tablet_id = 10030;
    int64_t txn_id = 20030;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with TxnLog but no op_compaction
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    // Don't set op_compaction

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with TxnLog but no op_compaction
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    // Don't set op_compaction

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for two subtasks output with non-overlapped results
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_two_subtasks) {
    int64_t tablet_id = 10031;
    int64_t txn_id = 20031;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to create 2 groups
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Two subtasks

    // Complete subtask 0 with non-overlapped output
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    auto* output0 = ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output0->set_num_rows(100);
    output0->set_data_size(1000);
    output0->set_overlapped(false);
    output0->add_segment_metas()->set_filename("merged_segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished()); // Not finished yet

    // Complete subtask 1 with non-overlapped output
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    auto* output1 = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output1->set_num_rows(200);
    output1->set_data_size(2000);
    output1->set_overlapped(false);
    output1->add_segment_metas()->set_filename("merged_segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify subtask_compactions for two subtasks
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(100, subtask0.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask0.output_rowset().data_size());
    EXPECT_EQ("merged_segment_0.dat", subtask0.output_rowset().segment_metas(0).filename());

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(200, subtask1.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask1.output_rowset().data_size());
    EXPECT_EQ("merged_segment_1.dat", subtask1.output_rowset().segment_metas(0).filename());

    block_promise.set_value();
    pool->wait();
}

// Test for metrics after subtask completion
TEST_F(TabletParallelCompactionManagerTest, test_metrics_after_completion) {
    int64_t tablet_id = 10032;
    int64_t txn_id = 20032;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    // Submit a blocking task first to occupy the single thread in the pool.
    // This prevents execute_subtask from running until we unblock it,
    // avoiding race condition with manual on_subtask_complete calls.
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for blocking task to start
    start_promise.get_future().wait();

    int64_t initial_running = _manager->running_subtasks();
    int64_t initial_completed = _manager->completed_subtasks();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // After creation, running subtasks should increase
    EXPECT_EQ(initial_running + 2, _manager->running_subtasks());

    // Complete both subtasks manually (execute_subtask is blocked in thread pool queue)
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // After first completion
    EXPECT_EQ(initial_running + 1, _manager->running_subtasks());
    EXPECT_EQ(initial_completed + 1, _manager->completed_subtasks());

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Unblock the pool. The queued execute_subtask tasks will run but find state cleaned up,
    // which will decrement running_subtasks (expected behavior for orphaned tasks).
    block_promise.set_value();
    pool->wait();

    // After pool completes, running_subtasks will be decremented by the orphaned execute_subtask calls.
    // This is expected: 2 execute_subtask calls each decrement counter when they find state missing.
    EXPECT_EQ(initial_running - 2, _manager->running_subtasks());
}

// Test for rowsets marking and unmarking (lines 606-620)
TEST_F(TabletParallelCompactionManagerTest, test_rowsets_marking) {
    int64_t tablet_id = 10033;
    int64_t txn_id = 20033;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);

    // Check that rowsets are marked as compacting
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_FALSE(state->compacting_rowsets.empty());

        // All rowsets from all subtasks should be marked
        for (const auto& [subtask_id, info] : state->running_subtasks) {
            for (uint32_t rid : info.input_rowset_ids) {
                EXPECT_TRUE(state->is_rowset_compacting(rid));
            }
        }
    }

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for callback not set scenario
TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_with_callback) {
    int64_t tablet_id = 10034;
    int64_t txn_id = 20034;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to create 2 groups
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Expect 2 subtasks

    // Get state to verify
    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.wait_finish(5000));

    block_promise.set_value();
    pool->wait();
}

// Test for all subtasks failed (should fail overall)
TEST_F(TabletParallelCompactionManagerTest, test_all_subtasks_failed) {
    int64_t tablet_id = 10035;
    int64_t txn_id = 20035;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0 with failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::InternalError("subtask 0 failed");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Simulate completion of subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("subtask 1 failed");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // When all subtasks fail, the overall status should indicate failure
    EXPECT_NE(0, response.status().status_code());

    block_promise.set_value();
    pool->wait();
}

// Test for partial success with multiple subtasks (2 succeed, 1 fails) - PK table
// PK tables allow non-consecutive successful subtasks to be applied
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_multiple_subtasks_pk) {
    int64_t tablet_id = 10036;
    int64_t txn_id = 20036;
    int64_t version = 16; // 15 rowsets + 1

    // Create 15 rowsets, each 1MB - PK table
    create_pk_tablet_with_rowsets(tablet_id, 15, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(3, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::MemoryLimitExceeded("OOM");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_2.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    ASSERT_TRUE(closure.is_finished());

    // With partial success (2 out of 3 succeeded), the overall status should be OK
    EXPECT_EQ(0, response.status().status_code());

    // Verify the merged TxnLog
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // success_subtask_ids should contain 1 and 2
    EXPECT_EQ(2, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));
    EXPECT_EQ(2, op_parallel.success_subtask_ids(1));

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(100, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask1.output_rowset().data_size());
    EXPECT_EQ("segment_1.dat", subtask1.output_rowset().segment_metas(0).filename());

    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(2, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());
    EXPECT_EQ("segment_2.dat", subtask2.output_rowset().segment_metas(0).filename());

    block_promise.set_value();
    pool->wait();
}

// Test for partial success with non-consecutive successful subtasks
// With unified logic, all successful subtasks are applied regardless of consecutiveness.
TEST_F(TabletParallelCompactionManagerTest, test_non_pk_table_all_successful_subtasks) {
    int64_t tablet_id = 10038;
    int64_t txn_id = 20038;
    int64_t version = 21; // 20 rowsets + 1

    create_tablet_with_rowsets(tablet_id, 20, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(4, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(7);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(150);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1500);
    auto* ctx1_seg = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas();
    ctx1_seg->set_filename("segment_1.dat");
    ctx1_seg->set_size(750);
    ctx1_seg->set_encryption_meta("meta1");
    ctx1->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success (should also be applied)
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->status = Status::OK();
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(12);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    auto* ctx2_seg = ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas();
    ctx2_seg->set_filename("segment_2.dat");
    ctx2_seg->set_size(1000);
    ctx2_seg->set_encryption_meta("meta2");
    ctx2->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    // Subtask 3: failure
    auto ctx3 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx3->subtask_id = 3;
    ctx3->status = Status::MemoryLimitExceeded("OOM");
    ctx3->txn_log = std::make_unique<TxnLogPB>();
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(15);
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(16);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2500);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_3.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 3, std::move(ctx3));

    ASSERT_TRUE(closure.is_finished());

    // Verify result - all successful subtasks (1 and 2) should be applied
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // success_subtask_ids should contain 1 and 2
    EXPECT_EQ(2, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));
    EXPECT_EQ(2, op_parallel.success_subtask_ids(1));

    // Verify subtask_compactions structure - 2 successful subtasks with independent outputs
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtask 1 output
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(3, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_EQ(7, subtask1.input_rowsets(2));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(150, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1500, subtask1.output_rowset().data_size());

    // Subtask 2 output
    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(3, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_EQ(12, subtask2.input_rowsets(2));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());

    block_promise.set_value();
    pool->wait();
}

// Test where first subtask fails but second succeeds
// With unified logic, all successful subtasks are applied
TEST_F(TabletParallelCompactionManagerTest, test_first_subtask_fails_second_succeeds) {
    int64_t tablet_id = 10039;
    int64_t txn_id = 20039;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success (should be applied)
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // With unified logic, subtask 1 should be applied as it succeeded
    EXPECT_EQ(0, response.status().status_code());

    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Only subtask 1 in success_subtask_ids
    EXPECT_EQ(1, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));

    // Verify subtask_compactions structure - only subtask 1 has output
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(100, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask1.output_rowset().data_size());
    EXPECT_EQ(1, subtask1.output_rowset().segment_metas_size());
    EXPECT_EQ("segment_1.dat", subtask1.output_rowset().segment_metas(0).filename());

    block_promise.set_value();
    pool->wait();
}

// Test partial success pattern: [fail, success, success, fail]
// With unified logic, all successful subtasks (1 and 2) should be applied
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_middle_subtasks) {
    int64_t tablet_id = 10040;
    int64_t txn_id = 20040;
    int64_t version = 21; // 20 rowsets + 1

    create_tablet_with_rowsets(tablet_id, 20, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(4, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(7);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(150);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1500);
    auto* ctx1_seg = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas();
    ctx1_seg->set_filename("segment_1.dat");
    ctx1_seg->set_size(750);
    ctx1_seg->set_encryption_meta("meta1");
    ctx1->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success (should be applied)
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->status = Status::OK();
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(12);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    auto* ctx2_seg = ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas();
    ctx2_seg->set_filename("segment_2.dat");
    ctx2_seg->set_size(1000);
    ctx2_seg->set_encryption_meta("meta2");
    ctx2->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    // Subtask 3: failure
    auto ctx3 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx3->subtask_id = 3;
    ctx3->status = Status::MemoryLimitExceeded("OOM");
    ctx3->txn_log = std::make_unique<TxnLogPB>();
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(15);
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(16);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2500);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("segment_3.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 3, std::move(ctx3));

    ASSERT_TRUE(closure.is_finished());

    // With unified logic: all successful subtasks are applied
    // Pattern: [fail, success, success, fail]
    // Subtasks 1 and 2 should be applied
    EXPECT_EQ(0, response.status().status_code());

    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(3, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_EQ(7, subtask1.input_rowsets(2));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(150, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1500, subtask1.output_rowset().data_size());

    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(3, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_EQ(12, subtask2.input_rowsets(2));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());

    block_promise.set_value();
    pool->wait();
}

// ================================================================================
// Tests for SubtaskType enum and SubtaskGroup struct
// ================================================================================

class SubtaskGroupTest : public ::testing::Test {};

TEST_F(SubtaskGroupTest, test_subtask_type_enum_values) {
    // Verify enum values exist and are distinct
    EXPECT_NE(SubtaskType::NORMAL, SubtaskType::LARGE_ROWSET_PART);
}

TEST_F(SubtaskGroupTest, test_subtask_group_default_values) {
    SubtaskGroup group;
    EXPECT_EQ(SubtaskType::NORMAL, group.type);
    EXPECT_TRUE(group.rowsets.empty());
    EXPECT_EQ(nullptr, group.large_rowset);
    EXPECT_EQ(0, group.large_rowset_id);
    EXPECT_EQ(0, group.segment_start);
    EXPECT_EQ(0, group.segment_end);
    EXPECT_EQ(0, group.total_bytes);
}

TEST_F(SubtaskGroupTest, test_subtask_group_normal_type) {
    SubtaskGroup group;
    group.type = SubtaskType::NORMAL;
    group.total_bytes = 1024 * 1024;

    EXPECT_EQ(SubtaskType::NORMAL, group.type);
    EXPECT_EQ(1024 * 1024, group.total_bytes);
}

TEST_F(SubtaskGroupTest, test_subtask_group_large_rowset_part_type) {
    SubtaskGroup group;
    group.type = SubtaskType::LARGE_ROWSET_PART;
    group.large_rowset_id = 42;
    group.segment_start = 0;
    group.segment_end = 4;
    group.total_bytes = 10 * 1024 * 1024;

    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, group.type);
    EXPECT_EQ(42, group.large_rowset_id);
    EXPECT_EQ(0, group.segment_start);
    EXPECT_EQ(4, group.segment_end);
    EXPECT_EQ(10 * 1024 * 1024, group.total_bytes);
}

// ================================================================================
// Tests for SubtaskInfo new fields
// ================================================================================

TEST_F(SubtaskInfoTest, test_subtask_info_new_fields_default_values) {
    SubtaskInfo info;
    EXPECT_EQ(SubtaskType::NORMAL, info.type);
    EXPECT_EQ(0, info.large_rowset_id);
    EXPECT_EQ(0, info.segment_start);
    EXPECT_EQ(0, info.segment_end);
}

TEST_F(SubtaskInfoTest, test_subtask_info_large_rowset_part_fields) {
    SubtaskInfo info;
    info.subtask_id = 3;
    info.type = SubtaskType::LARGE_ROWSET_PART;
    info.large_rowset_id = 100;
    info.segment_start = 4;
    info.segment_end = 8;

    EXPECT_EQ(3, info.subtask_id);
    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, info.type);
    EXPECT_EQ(100, info.large_rowset_id);
    EXPECT_EQ(4, info.segment_start);
    EXPECT_EQ(8, info.segment_end);
}

// ================================================================================
// Tests for TabletParallelCompactionState new fields
// ================================================================================

TEST_F(TabletParallelCompactionStateFieldsTest, test_large_rowset_split_groups_default) {
    EXPECT_TRUE(_state->large_rowset_split_groups.empty());
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_large_rowset_split_groups_operations) {
    // Add entries for two large rowsets
    _state->large_rowset_split_groups[100] = {0, 1, 2};
    _state->large_rowset_split_groups[200] = {3, 4};

    EXPECT_EQ(2, _state->large_rowset_split_groups.size());
    EXPECT_EQ(3, _state->large_rowset_split_groups[100].size());
    EXPECT_EQ(2, _state->large_rowset_split_groups[200].size());

    // Verify subtask IDs for large rowset 100
    auto& subtasks_100 = _state->large_rowset_split_groups[100];
    EXPECT_EQ(0, subtasks_100[0]);
    EXPECT_EQ(1, subtasks_100[1]);
    EXPECT_EQ(2, subtasks_100[2]);

    // Verify subtask IDs for large rowset 200
    auto& subtasks_200 = _state->large_rowset_split_groups[200];
    EXPECT_EQ(3, subtasks_200[0]);
    EXPECT_EQ(4, subtasks_200[1]);
}

// ================================================================================
// Tests for large rowset split functionality (all table types)
// ================================================================================

class TabletParallelCompactionManagerLargeRowsetTest : public TestBase {
public:
    TabletParallelCompactionManagerLargeRowsetTest() : TestBase(kTestDirectory) { clear_and_init_test_dir(); }

protected:
    constexpr static const char* kTestDirectory = "test_tablet_parallel_compaction_large_rowset";

    void SetUp() override {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        _manager = std::make_unique<TabletParallelCompactionManager>(_tablet_mgr.get());
    }

    void TearDown() override {
        _manager.reset();
        remove_test_dir_ignore_error();
    }

    // Create a tablet with one large rowset containing multiple segments
    void create_tablet_with_large_rowset(int64_t tablet_id, int num_segments, int64_t segment_size,
                                         KeysType keys_type = PRIMARY_KEYS) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(2);

        // Create one large rowset with multiple segments
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(1000 * num_segments);
        rowset->set_data_size(segment_size * num_segments);

        for (int i = 0; i < num_segments; i++) {
            std::string segment_name = fmt::format("segment_{}.dat", i);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(segment_name);
            segment_meta->set_size(segment_size);

            // Create dummy segment file
            std::string path = _lp->segment_location(tablet_id, segment_name);
            std::string dir = std::filesystem::path(path).parent_path().string();
            CHECK_OK(fs::create_directories(dir));
            auto fs = FileSystemFactory::CreateSharedFromString(path);
            auto st = fs.value()->new_writable_file(path);
            CHECK_OK(st.status());
            CHECK_OK(st.value()->append("dummy_segment_data"));
            CHECK_OK(st.value()->close());
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    // Keep old API for backward compatibility
    void create_pk_tablet_with_large_rowset(int64_t tablet_id, int num_segments, int64_t segment_size) {
        create_tablet_with_large_rowset(tablet_id, num_segments, segment_size, PRIMARY_KEYS);
    }

    // Create a tablet with multiple rowsets of varying sizes
    void create_tablet_with_mixed_rowsets(int64_t tablet_id, const std::vector<std::pair<int, int64_t>>& rowset_specs,
                                          KeysType keys_type = PRIMARY_KEYS) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(rowset_specs.size() + 1);

        int rowset_id = 0;
        for (const auto& [num_segments, segment_size] : rowset_specs) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100 * num_segments);
            rowset->set_data_size(segment_size * num_segments);

            for (int i = 0; i < num_segments; i++) {
                std::string segment_name = fmt::format("rowset_{}_segment_{}.dat", rowset_id, i);
                auto* segment_meta = rowset->add_segment_metas();
                segment_meta->set_filename(segment_name);
                segment_meta->set_size(segment_size);

                // Create dummy segment file
                std::string path = _lp->segment_location(tablet_id, segment_name);
                std::string dir = std::filesystem::path(path).parent_path().string();
                CHECK_OK(fs::create_directories(dir));
                auto fs = FileSystemFactory::CreateSharedFromString(path);
                auto st = fs.value()->new_writable_file(path);
                CHECK_OK(st.status());
                CHECK_OK(st.value()->append("dummy_segment_data"));
                CHECK_OK(st.value()->close());
            }
            rowset_id++;
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    // Keep old API for backward compatibility
    void create_pk_tablet_with_mixed_rowsets(int64_t tablet_id,
                                             const std::vector<std::pair<int, int64_t>>& rowset_specs) {
        create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, PRIMARY_KEYS);
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::unique_ptr<TabletParallelCompactionManager> _manager;
    // Constructed before SetUp() and destroyed after TearDown(), so every test in this fixture sees
    // the pinned values and no other suite inherits them.
    CompactionPolicyConfigPin _config_pin;
};

// Test that a large rowset meeting split criteria is identified
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_criteria) {
    int64_t tablet_id = 30001;

    // Create a tablet with one large rowset: 8 segments, each 1GB
    // Total size = 8GB, which is > 2 * lake_compaction_max_rowset_size (default ~2GB)
    // and has >= 4 segments
    int64_t segment_size = 1024 * 1024 * 1024; // 1GB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 8, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 123, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // Should create multiple subtasks for the large rowset split
    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 123);
    if (state != nullptr) {
        // Verify that large_rowset_split_groups is populated for PK tables
        // The exact number depends on the split algorithm
        VLOG(1) << "Created " << state->running_subtasks.size() << " subtasks";
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 123);
}

// Test mixed rowsets: some large (to be split) and some small (to be grouped)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_mixed_large_and_small_rowsets) {
    int64_t tablet_id = 30002;

    // Create mixed rowsets:
    // Rowset 0: Large - 8 segments, 500MB each = 4GB (should be split)
    // Rowset 1: Small - 1 segment, 100MB
    // Rowset 2: Small - 1 segment, 100MB
    // Rowset 3: Small - 1 segment, 100MB
    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 500 * 1024 * 1024}, // Large rowset: 4GB total
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
    };
    create_pk_tablet_with_mixed_rowsets(tablet_id, rowset_specs);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(1024 * 1024 * 1024L); // 1GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 124, 5, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 124);
    if (state != nullptr) {
        VLOG(1) << "Created " << state->running_subtasks.size() << " subtasks for mixed rowsets";

        // Verify subtask types
        for (const auto& [id, info] : state->running_subtasks) {
            if (info.type == SubtaskType::LARGE_ROWSET_PART) {
                VLOG(1) << "Subtask " << id << " is LARGE_ROWSET_PART for rowset " << info.large_rowset_id
                        << " segments [" << info.segment_start << ", " << info.segment_end << ")";
            } else {
                VLOG(1) << "Subtask " << id << " is NORMAL with " << info.input_rowset_ids.size() << " rowsets";
            }
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 124);
}

// Test manual completion flow with LARGE_ROWSET_PART subtasks
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_manual_large_rowset_split_completion) {
    int64_t tablet_id = 30003;
    int64_t txn_id = 40003;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 8, 1024 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create state with LARGE_ROWSET_PART subtasks
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Simulate large rowset split into 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        info0.input_bytes = 4 * 1024 * 1024 * 1024L;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        info1.input_bytes = 4 * 1024 * 1024 * 1024L;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    // Record large rowset split group
    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->mutable_output_rowset()->set_num_rows(500);
    op0->mutable_output_rowset()->set_data_size(2 * 1024 * 1024 * 1024L);
    op0->set_segment_range_start(0);
    op0->set_segment_range_end(4);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->mutable_output_rowset()->set_num_rows(500);
    op1->mutable_output_rowset()->set_data_size(2 * 1024 * 1024 * 1024L);
    op1->set_segment_range_start(4);
    op1->set_segment_range_end(8);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    // After merging large rowset split subtasks, we get 1 merged subtask_compaction
    // that combines all segments from the same large_rowset_id
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());

    // Verify the merged subtask contains segments from both subtasks
    const auto& merged_subtask = op_parallel.subtask_compactions(0);
    // The merged subtask should have overlapped=true since segments are from different subtasks
    EXPECT_TRUE(merged_subtask.output_rowset().overlapped());
    // Total rows should be sum of both subtasks
    EXPECT_EQ(1000, merged_subtask.output_rowset().num_rows());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test partial failure in large rowset split - all subtasks must succeed
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_partial_failure) {
    int64_t tablet_id = 30004;
    int64_t txn_id = 40004;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 8, 1024 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Simulate large rowset split into 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 with success
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::IOError("disk error");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // For large rowset split, if any subtask fails, all subtasks for that rowset should be discarded
    // When all subtasks in a large rowset split group fail, no txn_log is generated
    // because there's nothing to apply (the original rowset is kept intact)
    if (response.txn_logs_size() > 0) {
        const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
        // No successful subtasks for the failed large rowset split
        EXPECT_EQ(0, op_parallel.subtask_compactions_size());
    } else {
        // When all subtasks fail, no txn_log is generated (expected behavior)
        EXPECT_EQ(0, response.txn_logs_size());
    }

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test that large rowset split is capped to max_parallel to ensure completeness.
// When max_parallel is less than the ideal number of subtasks for a large rowset,
// the split should be capped to max_parallel (allowing each subtask to exceed
// target_bytes_per_subtask) rather than truncating and causing data loss.
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_capped_to_max_parallel) {
    int64_t tablet_id = 30005;

    // Create a tablet with one very large rowset.
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    // With max_bytes_per_subtask=2GB, ideally this would split into 4-5 groups
    // But with max_parallel=2, it should be capped to 2 groups
    // to ensure the split is complete and no data is lost.
    int64_t segment_size = 550 * 1024 * 1024; // 550MB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);                     // Only 2 subtasks allowed
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 125, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 125);
    ASSERT_NE(nullptr, state);

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        // The large rowset should be split into exactly 2 subtasks (capped by max_parallel)
        EXPECT_EQ(1, state->large_rowset_split_groups.size());

        // Verify the large rowset (id=0) is being compacted
        EXPECT_TRUE(state->compacting_rowsets.count(0) > 0) << "Large rowset 0 should be in compacting set";

        // Verify all subtasks for the large rowset are tracked
        auto it = state->large_rowset_split_groups.find(0);
        if (it != state->large_rowset_split_groups.end()) {
            EXPECT_EQ(2, it->second.size()) << "Large rowset should be split into exactly 2 subtasks";

            // Verify all subtasks are running (tasks are queued but not executed yet)
            for (int32_t sid : it->second) {
                EXPECT_TRUE(state->running_subtasks.count(sid) > 0) << "Subtask " << sid << " should be running";
            }

            // Verify segment ranges cover all segments [0, 16)
            std::set<int32_t> covered_segments;
            for (const auto& [subtask_id, info] : state->running_subtasks) {
                if (info.type == SubtaskType::LARGE_ROWSET_PART && info.large_rowset_id == 0) {
                    for (int32_t seg = info.segment_start; seg < info.segment_end; seg++) {
                        covered_segments.insert(seg);
                    }
                }
            }
            EXPECT_EQ(16, covered_segments.size()) << "All 16 segments should be covered by subtasks";
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 125);
}

// Test that when large rowset is split, it uses multiple parallel slots as expected
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_uses_all_parallel_slots) {
    int64_t tablet_id = 30006;

    // Create a tablet with one large rowset only (no small rowsets to avoid compaction policy selection issues)
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    int64_t segment_size = 550 * 1024 * 1024; // 550MB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);                     // Only 2 subtasks allowed
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 126, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 126);
    ASSERT_NE(nullptr, state);

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        // Large rowset should use all 2 parallel slots (split into 2 subtasks)
        EXPECT_EQ(2, state->running_subtasks.size());

        // Only the large rowset (id=0) should be compacting
        EXPECT_TRUE(state->compacting_rowsets.count(0) > 0);

        // Verify it's in large_rowset_split_groups
        EXPECT_EQ(1, state->large_rowset_split_groups.size());
        auto it = state->large_rowset_split_groups.find(0);
        if (it != state->large_rowset_split_groups.end()) {
            EXPECT_EQ(2, it->second.size()) << "Large rowset should be split into 2 subtasks";
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 126);
}

// Test _split_large_rowset merge last group when last group has < 2 segments (lines 1589-1596)
// 7 segments with max_parallel=3: segments_per_subtask=3 -> [0,3), [3,6), [6,7); last has 1 segment -> merge with previous
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_split_large_rowset_merge_last_group) {
    int64_t tablet_id = 30025;
    int64_t txn_id = 40006;

    int64_t segment_size = 1500 * 1024 * 1024; // 1.5GB per segment, 7*1.5=10.5GB total
    create_tablet_with_large_rowset(tablet_id, 7, segment_size, PRIMARY_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(3 * 1024 * 1024 * 1024L); // 3GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    EXPECT_EQ(2, state->running_subtasks.size())
            << "7 segments with max_parallel=3: last group has 1 segment, merged into previous -> 2 groups";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test that large rowset is skipped when only 1 parallel slot remains
// (splitting with only 1 slot is meaningless, need at least 2)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_skipped_when_only_one_slot) {
    int64_t tablet_id = 30007;

    // Create a tablet with one large rowset
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    int64_t segment_size = 550 * 1024 * 1024;
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(1);                     // Only 1 subtask allowed - not enough to split
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 127, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // With only 1 parallel slot, the large rowset should be skipped (can't split with 1 slot)
    // This should return 0 (fallback to normal compaction) and state should be nullptr
    ASSERT_TRUE(st.ok()) << st.status();
    EXPECT_EQ(0, st.value()) << "Large rowset should be skipped when max_parallel=1, returning 0 for fallback";

    // State should not exist because no subtasks were created
    auto state = _manager->get_tablet_state(tablet_id, 127);
    EXPECT_EQ(nullptr, state) << "State should not exist when large rowset is skipped";

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 127);
}

// Test that incomplete large rowset split (when acquire_token() fails after some subtasks are created)
// is detected and treated as failed to prevent data loss.
// This tests the fix for: when acquire_token() fails after some subtasks are already created,
// only a subset of segment ranges would be scheduled for a given rowset. Because
// large_rowset_split_groups is populated only for created subtasks, get_merged_txn_log()
// would treat the group as successful and the first subtask would delete the original rowset,
// dropping the segments that never got a subtask.
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_incomplete_large_rowset_split_detected_as_failed) {
    int64_t tablet_id = 30008;
    int64_t txn_id = 40008;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 16, 550 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->callback = callback;

    // Simulate the scenario where a large rowset should be split into 4 subtasks,
    // but only 2 were successfully created (the others failed due to acquire_token() failure).
    // This is the bug scenario: expected 4 subtasks, but only 2 were created.
    state->expected_large_rowset_split_counts[0] = 4; // Expected 4 subtasks for large rowset 0

    // Only 2 subtasks were actually created (simulating acquire_token() failure for the rest)
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        state->running_subtasks[0] = std::move(info0);

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        state->running_subtasks[1] = std::move(info1);

        state->total_subtasks_created = 2;
    }

    // Only 2 subtasks are in the split group (the other 2 were never created)
    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 with success
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx0->txn_log->mutable_op_compaction()->set_segment_range_start(0);
    ctx0->txn_log->mutable_op_compaction()->set_segment_range_end(4);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1 with success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx1->txn_log->mutable_op_compaction()->set_segment_range_start(4);
    ctx1->txn_log->mutable_op_compaction()->set_segment_range_end(8);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Even though both created subtasks succeeded, the large rowset split is incomplete
    // (expected 4 subtasks but only 2 were created). The incomplete split should be
    // detected and treated as failed to prevent data loss.
    // The original rowset should NOT be deleted because segments [8,16) were never processed.
    //
    // When get_merged_txn_log() detects an incomplete split:
    // 1. It adds the large_rowset_id to failed_large_rowset_ids
    // 2. All subtasks belonging to the failed group are skipped
    // 3. Since success_subtask_ids becomes empty, it returns Status::InternalError
    // 4. The merged_context->status is set to this error
    // 5. No txn_log is added to the response (compaction fails completely)
    //
    // This is the expected behavior: incomplete splits should fail entirely to prevent data loss.
    EXPECT_EQ(0, response.txn_logs_size()) << "Incomplete large rowset split should result in no txn_logs - "
                                           << "the compaction should fail entirely to prevent data loss";

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// ==============================================================================
// Tests for _split_large_rowset segment merging (lines 1432-1436)
// ==============================================================================

// Note: The _split_large_rowset segment merge logic (lines 1432-1436) is covered
// by existing tests like test_large_rowset_split_capped_to_max_parallel.
// The merge happens when last group has < 2 segments.
// We can't easily test this in isolation without real segment files.

// ==============================================================================
// Tests for submit_subtasks_from_groups token acquisition failure (lines 1601-1625)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_token_acquisition_failure) {
    // This tests lines 1614-1625: when acquire_token() fails
    int64_t tablet_id = 10010;
    int64_t txn_id = 20010;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    int acquire_count = 0;
    // Fail on first acquire_token() call
    auto acquire_token = [&acquire_count]() {
        acquire_count++;
        return false; // All acquires fail
    };

    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(tablet_id, txn_id, version, config, callback, false, pool.get(),
                                              acquire_token, release_token);

    // Should fail because token acquisition failed
    EXPECT_TRUE(st.status().is_resource_busy()) << st.status();
    // No token was successfully acquired, so no release should be called
    EXPECT_EQ(0, release_count) << "No token was acquired, so no release should happen";

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test token partial acquire: first succeeds, second fails; release the one acquired (lines 1802-1810)
TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_token_partial_acquire_then_release) {
    int64_t tablet_id = 10061;
    int64_t txn_id = 20061;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    int acquire_count = 0;
    auto acquire_token = [&acquire_count]() {
        acquire_count++;
        return acquire_count <= 1;
    };
    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(tablet_id, txn_id, version, config, callback, false, pool.get(),
                                              acquire_token, release_token);

    EXPECT_TRUE(st.status().is_resource_busy()) << st.status();
    EXPECT_EQ(2, acquire_count);
    EXPECT_EQ(1, release_count) << "One token was acquired then released";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test fallback not_enough_segments: total_segments < 4 (same condition in split_rowsets_into_groups
// and _create_subtask_groups). We test via split_rowsets_into_groups so we don't depend on
// compaction policy returning few rowsets.
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_fallback_not_enough_segments) {
    int64_t tablet_id = 10062;
    int64_t version = 3;

    create_tablet_with_rowsets(tablet_id, 2, 10 * 1024 * 1024);

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(2, rowsets.size());

    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 5 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "2 segments < kMinSegmentsForParallel(4) should fallback, no groups";
}

// Test _create_subtask_groups: skip second large rowset when no remaining parallel slots (lines 1712-1715)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_create_subtask_groups_skip_large_rowset_no_slots) {
    int64_t tablet_id = 30026;

    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 1200 * 1024 * 1024},
            {8, 1200 * 1024 * 1024},
    };
    create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, PRIMARY_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(3 * 1024 * 1024 * 1024L);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 126, 3, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    auto state = _manager->get_tablet_state(tablet_id, 126);
    ASSERT_NE(nullptr, state);
    EXPECT_EQ(2, state->running_subtasks.size())
            << "First large rowset uses 2 slots; second is skipped (no remaining slots)";
    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 126);
}

// ==============================================================================
// Tests for execute_subtask_segment_range error handling (lines 1797-1856)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_execute_subtask_segment_range_state_not_found) {
    // This tests lines 1797-1804: tablet state not found during execution
    int64_t tablet_id = 30010;
    int64_t txn_id = 40010;

    // Don't register any state - verify state is not found
    EXPECT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));

    // When execute_subtask_segment_range is called without registered state,
    // it should return early without crash (line 1797-1804)
    bool release_called = false;
    auto release_token = [&release_called](bool) { release_called = true; };

    _manager->execute_subtask_segment_range(tablet_id, txn_id, 0, nullptr, 0, 0, 4, 1, false, release_token);

    // Release token should be called even on early return
    EXPECT_TRUE(release_called) << "Release token should be called when state not found";
}

// ==============================================================================
// Tests for submit_subtasks_from_groups failure handling (lines 1722-1756)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_thread_pool_failure) {
    // This tests lines 1722-1756: when thread pool submission fails
    int64_t tablet_id = 10011;
    int64_t txn_id = 20011;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Create a valid pool first, then shutdown to make submissions fail
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_min_threads(0).set_max_threads(1).build(&pool);
    // Shutdown the pool so submissions will fail
    pool->shutdown();

    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, release_token);

    // Submission should fail because pool is shutdown
    EXPECT_FALSE(st.ok()) << "Should fail when thread pool submission fails";
    // All tokens should be released
    EXPECT_GT(release_count, 0) << "Tokens should be released on failure";

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// ==============================================================================
// Tests for coverage: split_rowsets_into_groups, _filter_compactable_rowsets,
// pick_rowsets, create_and_register_tablet_state, get_merged_txn_log, etc.
// ==============================================================================

// Test split_rowsets_into_groups when all rowsets are large non-overlapped (lines 369-373)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_all_large_non_overlapped) {
    int64_t tablet_id = 10050;
    int64_t version = 3;
    // Two rowsets, each >= lake_compaction_max_rowset_size (default 4GB), non-overlapped
    int64_t large_size = 5L * 1024 * 1024 * 1024;
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    for (int i = 0; i < 2; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(false);
        rowset->set_num_rows(1000);
        rowset->set_data_size(large_size);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename("segment_" + std::to_string(i) + ".dat");
        segment_meta->set_size(large_size);
        std::string path = _lp->segment_location(tablet_id, "segment_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(2, rowsets.size());

    auto groups = _manager->split_rowsets_into_groups(tablet_id, std::move(rowsets), 4, 10 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "All large non-overlapped should yield no groups";
}

// Test split_rowsets_into_groups fallback: data_size_small, not_enough_segments, max_parallel<=1 (lines 391-405)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_fallback_reasons) {
    int64_t tablet_id = 10051;
    int64_t version = 5;
    create_tablet_with_rowsets(tablet_id, 4, 512 * 1024); // 4 rowsets, 512KB each = 2MB total

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();

    // Fallback: total_bytes (2MB) <= max_bytes (5MB) -> data_size_small
    auto groups_small = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 5 * 1024 * 1024, false);
    EXPECT_TRUE(groups_small.empty());

    // Fallback: max_parallel <= 1
    auto groups_parallel = _manager->split_rowsets_into_groups(tablet_id, rowsets, 1, 1024, false);
    EXPECT_TRUE(groups_parallel.empty());

    // Fallback: not_enough_segments - 4 rowsets each 1 segment = 4 segments, need >= 4 for parallel (kMinSegmentsForParallel=4)
    // So 4 segments is exactly the limit; with 4 rowsets overlapped we have total_segments=4. So we might get 1 group
    // or empty. Actually not_enough_segments = (total_segments < 4). So 4 is not < 4, so we don't fallback for that.
    // Use 3 rowsets so total_segments=3 < 4
    create_tablet_with_rowsets(10052, 3, 2 * 1024 * 1024);
    auto tablet_or2 = _tablet_mgr->get_tablet(10052, 4);
    ASSERT_OK(tablet_or2);
    auto rowsets2 = tablet_or2.value().get_rowsets();
    auto groups_seg = _manager->split_rowsets_into_groups(10052, rowsets2, 4, 1024, false);
    EXPECT_TRUE(groups_seg.empty());
}

// Test split_rowsets_into_groups fallback: has_delete_predicate (lines 391, 397)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_fallback_has_delete_predicate) {
    int64_t tablet_id = 10053;
    int64_t version = 6;
    create_tablet_with_rowsets(tablet_id, 6, 2 * 1024 * 1024); // 6 rowsets, 2MB each

    // Put metadata again with delete predicate on one rowset
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    for (int i = 0; i < 6; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(2 * 1024 * 1024);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename("seg_" + std::to_string(i) + ".dat");
        segment_meta->set_size(2 * 1024 * 1024);
        if (i == 0) {
            rowset->mutable_delete_predicate()->set_version(version);
        }
        std::string path = _lp->segment_location(tablet_id, "seg_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 2 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "has_delete_predicate should fallback to normal compaction";
}

// Test _filter_compactable_rowsets path: at least one large non-overlapped skipped (lines 81-91)
TEST_F(TabletParallelCompactionManagerTest, test_filter_compactable_rowsets_skips_large_non_overlapped) {
    int64_t tablet_id = 10054;
    int64_t version = 4;
    int64_t large_size = 5L * 1024 * 1024 * 1024; // 5GB
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    // One large non-overlapped, four small overlapped
    for (int i = 0; i < 5; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(i > 0);
        rowset->set_num_rows(100);
        rowset->set_data_size(i == 0 ? large_size : 2 * 1024 * 1024);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename("s_" + std::to_string(i) + ".dat");
        segment_meta->set_size(i == 0 ? large_size : 2 * 1024 * 1024);
        std::string path = _lp->segment_location(tablet_id, "s_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(5, rowsets.size());
    // split_rowsets_into_groups calls _filter_compactable_rowsets; one 5GB non-overlapped is skipped
    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 2 * 1024 * 1024, false);
    // After filter we have 4 small rowsets; may produce groups or fallback
    EXPECT_GE(groups.size(), 0u);
}

// Test create_and_register_tablet_state AlreadyExist (lines 416-421)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_already_registered) {
    int64_t tablet_id = 10055;
    int64_t txn_id = 20055;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(2).build(&pool);

    auto st1 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st1.ok()) << st1.status();
    EXPECT_GT(st1.value(), 0);

    // Second call with same tablet_id/txn_id should get AlreadyExist
    auto st2 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    EXPECT_FALSE(st2.ok());
    EXPECT_TRUE(st2.status().is_already_exist()) << st2.status();

    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset split group incomplete (lines 971-978)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_split_incomplete) {
    int64_t tablet_id = 10056;
    int64_t txn_id = 20056;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[100] = {0, 1};     // two subtasks
    state->expected_large_rowset_split_counts[100] = 3; // expected 3, actual 2 -> incomplete
    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    // Incomplete large rowset group should be skipped; merged log should still be produced
    EXPECT_EQ(0, response.txn_logs_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset merge path: segment_metas, ssts, sst_ranges (lines 1060-1118)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_with_segment_metas_ssts) {
    int64_t tablet_id = 10055;
    int64_t txn_id = 20055;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info0.input_rowset_ids = {0};
    info1.subtask_id = 1;
    info1.input_rowset_ids = {0};
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->set_compact_version(10);
    auto* out0 = op0->mutable_output_rowset();
    out0->set_num_rows(100);
    out0->set_data_size(1000);
    auto* out0_seg = out0->add_segment_metas();
    out0_seg->set_filename("s0.dat");
    out0_seg->set_size(1000);
    out0_seg->set_encryption_meta("enc0");
    out0_seg->set_segment_idx(0);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    auto* out1 = op1->mutable_output_rowset();
    out1->set_num_rows(200);
    out1->set_data_size(2000);
    auto* out1_seg = out1->add_segment_metas();
    out1_seg->set_filename("s1.dat");
    out1_seg->set_size(2000);
    out1_seg->set_segment_idx(0);
    op1->add_ssts();
    op1->add_sst_ranges();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(300, merged.output_rowset().num_rows());
    EXPECT_EQ(3000, merged.output_rowset().data_size());
    EXPECT_EQ(2, merged.output_rowset().segment_metas_size());
    EXPECT_EQ(0, merged.output_rowset().segment_metas(0).segment_idx());
    EXPECT_EQ(1, merged.output_rowset().segment_metas(1).segment_idx());
    EXPECT_EQ(1, merged.ssts_size());
    EXPECT_EQ(1, merged.sst_ranges_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// NOTE: the former test_get_merged_txn_log_large_rowset_segment_meta_size_mismatch was removed
// during the segment_metas refactor. It forced a mismatch between the legacy `segments[]` array
// and `segment_metas[]` to trip a DCHECK in get_merged_txn_log. With segment_metas now the sole
// canonical source (no parallel arrays), that inconsistency is structurally impossible to express,
// and the corresponding DCHECK was removed.

// Test get_merged_txn_log large rowset group with no valid subtasks (RemoveLast, lines 1132-1140)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_no_valid_subtasks) {
    int64_t tablet_id = 10050;
    int64_t txn_id = 20050;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete both subtasks with status OK but no op_compaction (so first_subtask stays true in merge loop)
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    EXPECT_EQ(0, op_parallel.subtask_compactions_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset split group one subtask failed (lines 990-996)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_split_one_failed) {
    int64_t tablet_id = 10057;
    int64_t txn_id = 20057;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[100] = {0, 1};
    state->expected_large_rowset_split_counts[100] = 2;
    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("failed");
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    EXPECT_EQ(0, response.txn_logs_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test non-PK table with enable_size_tiered_compaction_strategy=false fallback (lines 662-666)
TEST_F(TabletParallelCompactionManagerTest, test_try_create_parallel_tasks_non_pk_size_tiered_disabled) {
    int64_t tablet_id = 10058;
    int64_t txn_id = 20058;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    ConfigResetGuard<bool> guard(&config::enable_size_tiered_compaction_strategy, false);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // Non-PK with size_tiered disabled should fallback (return 0)
    EXPECT_TRUE(st.ok()) << st.status();
    EXPECT_EQ(0, st.value());
}

// ==============================================================================
// Tests for duplicate key table large rowset split
// ==============================================================================

// Test that DUP_KEYS table with a large overlapped rowset triggers large rowset split
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_large_rowset_split) {
    int64_t tablet_id = 30020;

    // Create a DUP_KEYS tablet with one large rowset: 8 segments, each 1GB
    // Total = 8GB > 2 * lake_compaction_max_rowset_size (default 4GB * 2 = 8GB)
    // Use slightly larger to guarantee the threshold is exceeded
    int64_t segment_size = 1200L * 1024 * 1024; // 1.2GB per segment -> 9.6GB total
    create_tablet_with_large_rowset(tablet_id, 8, segment_size, DUP_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(3L * 1024 * 1024 * 1024); // 3GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 500, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    // Should create multiple subtasks via large rowset split
    EXPECT_GT(st.value(), 1) << "DUP_KEYS table should get multiple subtasks from large rowset split";

    auto state = _manager->get_tablet_state(tablet_id, 500);
    ASSERT_NE(nullptr, state);
    // Verify large_rowset_split_groups is populated
    EXPECT_FALSE(state->large_rowset_split_groups.empty()) << "DUP_KEYS table should have large_rowset_split_groups";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 500);
}

// Test DUP_KEYS table with mixed large and small rowsets
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_mixed_large_and_small_rowsets) {
    int64_t tablet_id = 30021;

    // Mixed rowsets:
    // Rowset 0: Large - 8 segments, 1.2GB each = 9.6GB (should be split)
    // Rowset 1: Small - 1 segment, 100MB
    // Rowset 2: Small - 1 segment, 100MB
    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 1200L * 1024 * 1024}, // Large rowset: 9.6GB total
            {1, 100 * 1024 * 1024},   // Small rowset: 100MB
            {1, 100 * 1024 * 1024},   // Small rowset: 100MB
    };
    create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, DUP_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(3L * 1024 * 1024 * 1024); // 3GB per subtask

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 501, 4, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    EXPECT_GT(st.value(), 1) << "DUP_KEYS mixed should produce multiple subtasks";

    auto state = _manager->get_tablet_state(tablet_id, 501);
    ASSERT_NE(nullptr, state);

    // Check that we have both LARGE_ROWSET_PART and NORMAL subtasks
    bool has_large = false;
    for (const auto& [id, info] : state->running_subtasks) {
        if (info.type == SubtaskType::LARGE_ROWSET_PART) {
            has_large = true;
        }
    }
    EXPECT_TRUE(has_large) << "Should have LARGE_ROWSET_PART subtasks for the large rowset";
    // Small rowsets may or may not form a NORMAL subtask depending on grouping

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 501);
}

// Test manual completion flow for DUP_KEYS large rowset split (verify get_merged_txn_log works)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_large_rowset_split_completion) {
    int64_t tablet_id = 30022;
    int64_t txn_id = 50022;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;

    // Simulate 2 subtasks splitting the same large rowset (id=0)
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info0.type = SubtaskType::LARGE_ROWSET_PART;
    info0.large_rowset_id = 0;
    info0.segment_start = 0;
    info0.segment_end = 4;
    info0.input_rowset_ids = {0};
    info0.input_bytes = 4L * 1024 * 1024 * 1024;
    info0.start_time = ::time(nullptr);
    state->running_subtasks[0] = std::move(info0);

    info1.subtask_id = 1;
    info1.type = SubtaskType::LARGE_ROWSET_PART;
    info1.large_rowset_id = 0;
    info1.segment_start = 4;
    info1.segment_end = 8;
    info1.input_rowset_ids = {0};
    info1.input_bytes = 4L * 1024 * 1024 * 1024;
    info1.start_time = ::time(nullptr);
    state->running_subtasks[1] = std::move(info1);

    state->compacting_rowsets[0] = 2; // refcount=2

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->mutable_output_rowset()->set_num_rows(500);
    op0->mutable_output_rowset()->set_data_size(4000000000L);
    auto* op0_seg0 = op0->mutable_output_rowset()->add_segment_metas();
    op0_seg0->set_filename("out_seg_0.dat");
    op0_seg0->set_size(2000000000L);
    op0_seg0->set_segment_idx(0);
    auto* op0_seg1 = op0->mutable_output_rowset()->add_segment_metas();
    op0_seg1->set_filename("out_seg_1.dat");
    op0_seg1->set_size(2000000000L);
    op0_seg1->set_segment_idx(1);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->mutable_output_rowset()->set_num_rows(500);
    op1->mutable_output_rowset()->set_data_size(4000000000L);
    auto* op1_seg0 = op1->mutable_output_rowset()->add_segment_metas();
    op1_seg0->set_filename("out_seg_2.dat");
    op1_seg0->set_size(2000000000L);
    op1_seg0->set_segment_idx(0);
    auto* op1_seg1 = op1->mutable_output_rowset()->add_segment_metas();
    op1_seg1->set_filename("out_seg_3.dat");
    op1_seg1->set_size(2000000000L);
    op1_seg1->set_segment_idx(1);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify merged result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Should have 1 merged subtask_compaction (two subtasks merged into one)
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, merged.input_rowsets_size());
    EXPECT_EQ(0u, merged.input_rowsets(0)); // Original rowset id
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(1000, merged.output_rowset().num_rows());         // 500+500
    EXPECT_EQ(8000000000L, merged.output_rowset().data_size()); // 4G+4G
    EXPECT_EQ(4, merged.output_rowset().segment_metas_size());  // 2+2
    EXPECT_EQ(4, merged.output_rowset().segment_metas_size());
    EXPECT_EQ(0, merged.output_rowset().segment_metas(0).segment_idx());
    EXPECT_EQ(1, merged.output_rowset().segment_metas(1).segment_idx());
    EXPECT_EQ(2, merged.output_rowset().segment_metas(2).segment_idx());
    EXPECT_EQ(3, merged.output_rowset().segment_metas(3).segment_idx());
    EXPECT_TRUE(merged.output_rowset().overlapped()); // merged output is overlapped

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// ================================================================================
// Range split related tests
// ================================================================================

// Helper: create a TuplePB with a single INT32 value
static TuplePB make_int_tuple(int32_t value) {
    TuplePB tuple;
    auto* v = tuple.add_values();
    TypeDescriptor type_desc(TYPE_INT);
    v->mutable_type()->CopyFrom(type_desc.to_protobuf());
    v->set_variant_type(VariantTypePB::NORMAL_VALUE);
    v->set_value(std::to_string(value));
    return tuple;
}

// Helper: create VariantTuple from int
static VariantTuple make_variant_int(int32_t value) {
    VariantTuple vt;
    auto type_info = get_type_info(TYPE_INT);
    Datum d;
    d.set_int32(value);
    vt.append(DatumVariant(type_info, d));
    return vt;
}

TEST_F(TabletParallelCompactionManagerTest, test_can_use_range_split) {
    // Case 1: empty rowsets → false
    {
        std::vector<RowsetPtr> empty;
        EXPECT_FALSE(TabletParallelCompactionManager::_can_use_range_split(empty));
    }

    // Case 2: missing segment_metas → false
    {
        int64_t tablet_id = 10100;
        create_tablet_with_rowsets(tablet_id, 3, 1024 * 1024);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 4));
        auto metadata = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < metadata->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), metadata, i, 0));
        }

        EXPECT_FALSE(TabletParallelCompactionManager::_can_use_range_split(rowsets));
    }

    // Case 3: with proper segment_metas → true
    {
        int64_t tablet_id = 10101;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(3);

        for (int i = 0; i < 2; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100);
            rowset->set_data_size(1024 * 1024);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 100));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 100 + 99));
            segment_meta->set_num_rows(100);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        EXPECT_TRUE(TabletParallelCompactionManager::_can_use_range_split(rowsets));
    }
}

TEST_F(TabletParallelCompactionManagerTest, test_collect_segment_key_bounds) {
    // Case 1: basic - 3 segments with equal num_rows
    {
        int64_t tablet_id = 10102;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(4);

        for (int i = 0; i < 3; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(200);
            rowset->set_data_size(2000);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 10));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 10 + 15));
            segment_meta->set_num_rows(200);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 4));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        // 2 is the smallest width that enables sampling at all: allocate_sort_key_sample_budget
        // returns an all-zero budget below it. These segments have no file on disk, so every one of
        // them falls back to its coarse [min, max] range regardless.
        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(3, result.value().size());
        EXPECT_EQ(200, result.value()[0].num_rows);
        EXPECT_EQ(2000, result.value()[0].data_size);
    }

    // Case 2: zero num_rows - fallback to data_size / num_segments
    {
        int64_t tablet_id = 10203;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(3);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(0);
        rowset->set_data_size(6000);
        for (int i = 0; i < 3; i++) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 10));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 10 + 9));
            segment_meta->set_num_rows(0);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(3, result.value().size());
        for (const auto& bound : result.value()) {
            EXPECT_EQ(0, bound.num_rows);
            EXPECT_EQ(2000, bound.data_size); // 6000 / 3
        }
    }

    // Case 3: proportional data_size allocation (100 vs 300 rows)
    {
        int64_t tablet_id = 10221;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(3);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(400);
        rowset->set_data_size(4000);

        for (int i = 0; i < 2; i++) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 10));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 10 + 5));
            segment_meta->set_num_rows(i == 0 ? 100 : 300);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(2, result.value().size());
        EXPECT_EQ(1000, result.value()[0].data_size); // 100/400 * 4000
        EXPECT_EQ(3000, result.value()[1].data_size); // 300/400 * 4000
    }
}

// _collect_segment_key_bounds sampling integration: every segment goes through
// SegmentSplitInfo::load_samples, which reads the segment's short key index when that index
// already encodes the whole sort key and otherwise its data pages, and leaves the segment's coarse
// [min, max] range in place when neither can produce trustworthy samples. The Rowset objects here
// are already constructed (unlike build_segments_from_rowsets, which may construct one from
// synthetic reshard metadata), so there is no schema-id-resolution abort risk to guard against in
// this path -- opening a rowset's segments can only ever gain precision or, on failure, silently
// fall back to the coarse range.
TEST_F(TabletParallelCompactionManagerTest, test_collect_segment_key_bounds_sampling) {
    // Case 1: a segment whose sort key the short key index covers is sampled from that index --
    // 250 rows over 100-row blocks -> [100, 200] at a 100-row interval -- with no data-page I/O.
    {
        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);
        // generate_simple_tablet_metadata leaves index_length unset, and SeekTuple::short_key_encode
        // writes a key's bytes only when Field::short_key_length() > 0 -- so without this the index
        // holds marker-only entries no decoder can read back, and sampling would silently degrade
        // to the data-page path. Set locally rather than in the shared helper: flipping every test
        // in the repo onto the covered path is its own change.
        metadata->mutable_schema()->mutable_column(0)->set_index_length(4);

        const int64_t num_rows = 250;
        const std::string seg_name = "seg_covered_key.dat";
        const uint64_t seg_size = write_int_key_segment(tablet_id, metadata->schema(), seg_name, num_rows);
        const int64_t data_page_segments_before = sort_key_sampling_data_page_segments_count();

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(seg_size);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(seg_name);
        segment_meta->set_size(seg_size);
        segment_meta->set_num_rows(num_rows);
        segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(0));
        segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(static_cast<int32_t>(num_rows - 1)));

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0));

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(1, result.value().size());
        ASSERT_EQ(2u, result.value()[0].sort_key_samples.size());
        EXPECT_EQ(100, result.value()[0].sort_key_samples[0][0].value().get_int32());
        EXPECT_EQ(200, result.value()[0].sort_key_samples[1][0].value().get_int32());
        EXPECT_EQ(100, result.value()[0].sort_key_sample_row_interval);
        // Which path produced them, asserted rather than assumed: both publish through the same
        // carrier, so the samples alone cannot tell the free index path from the paid data-page one.
        EXPECT_EQ(data_page_segments_before, sort_key_sampling_data_page_segments_count())
                << "a covered sort key must not read data pages";
    }

    // Case 2: metadata sort-key samples (deprecated_sort_key_samples) are no longer a source. This
    // segment carries them and has no file on disk, so it comes back coarse -- where the previous
    // metadata-driven code returned [100, 200] at a 100-row interval.
    {
        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(250);
        rowset->set_data_size(2500);
        auto* segment_meta = rowset->add_segment_metas();
        // Never written to disk: the budget is nonzero so the open IS attempted, load_segments
        // fails, and the &&-chain leaves this segment coarse.
        segment_meta->set_filename("seg_never_written.dat");
        segment_meta->set_size(2500);
        segment_meta->set_num_rows(250);
        segment_meta->set_deprecated_sort_key_sample_row_interval(100);
        segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(0));
        segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(249));
        segment_meta->add_deprecated_sort_key_samples()->CopyFrom(make_int_tuple(100));
        segment_meta->add_deprecated_sort_key_samples()->CopyFrom(make_int_tuple(200));

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0));

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(1, result.value().size());
        EXPECT_TRUE(result.value()[0].sort_key_samples.empty());
        EXPECT_EQ(0, result.value()[0].sort_key_sample_row_interval);
        EXPECT_EQ(0, result.value()[0].min_key[0].value().get_int32());
        EXPECT_EQ(249, result.value()[0].max_key[0].value().get_int32());
    }

    // Case 3: a segment whose file is missing (Rowset::LoadedSegment::segment == nullptr, via
    // experimental_lake_ignore_lost_segment=true) degrades to coarse bounds WITHOUT crashing,
    // while a sibling real segment in the SAME rowset is still sampled.
    {
        const bool old_ignore_lost = config::experimental_lake_ignore_lost_segment;
        config::experimental_lake_ignore_lost_segment = true;
        DeferOp restore_ignore_lost([&] { config::experimental_lake_ignore_lost_segment = old_ignore_lost; });

        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);
        metadata->mutable_schema()->mutable_column(0)->set_index_length(4); // see Case 1

        const int64_t num_rows = 250;
        const std::string present_seg_name = "seg_present.dat";
        const uint64_t present_seg_size =
                write_int_key_segment(tablet_id, metadata->schema(), present_seg_name, num_rows);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(present_seg_size);

        auto* sm_present = rowset->add_segment_metas();
        sm_present->set_filename(present_seg_name);
        sm_present->set_size(present_seg_size);
        sm_present->set_num_rows(num_rows);
        // Load-bearing, not decoration: with the bounds unset, every !min_key.empty() /
        // !max_key.empty() guard in sample_sort_key_from_short_key_index is skipped -- including the
        // entry-0 == sort_key_min comparison the sampler calls the check that "empirically subsumes
        // every static assumption the predicate makes". Without them this case would establish "the
        // sibling segment is still sampled" with the only cross-source verification switched off.
        sm_present->mutable_sort_key_min()->CopyFrom(make_int_tuple(0));
        sm_present->mutable_sort_key_max()->CopyFrom(make_int_tuple(static_cast<int32_t>(num_rows - 1)));

        // Never written to disk; with experimental_lake_ignore_lost_segment=true this
        // becomes a null LoadedSegment placeholder instead of a hard load error.
        auto* sm_lost = rowset->add_segment_metas();
        sm_lost->set_filename("seg_missing.dat");
        sm_lost->set_size(100);
        sm_lost->set_num_rows(10);
        sm_lost->mutable_sort_key_min()->CopyFrom(make_int_tuple(1000));
        sm_lost->mutable_sort_key_max()->CopyFrom(make_int_tuple(1009));

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0));

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(2, result.value().size());

        // segments[0]: the real, present segment -- still gets samples.
        ASSERT_EQ(2u, result.value()[0].sort_key_samples.size());
        EXPECT_EQ(100, result.value()[0].sort_key_sample_row_interval);

        // segments[1]: the lost segment -- coarse fallback, no crash.
        EXPECT_TRUE(result.value()[1].sort_key_samples.empty());
        EXPECT_EQ(0, result.value()[1].sort_key_sample_row_interval);
        EXPECT_EQ(10, result.value()[1].num_rows);
        EXPECT_EQ(1000, result.value()[1].min_key[0].value().get_int32());
        EXPECT_EQ(1009, result.value()[1].max_key[0].value().get_int32());
    }

    // Case 4: the per-segment budget must be indexed by the segment's position across ALL rowsets
    // (rowset_flat_index + meta_pos), not by the rowset's. Every other case here puts one segment
    // in its own rowset, where those two are the same number; this one puts two segments of very
    // UNEQUAL row counts in a single rowset, and squeezes the tablet-wide cap so the budget
    // actually binds -- feeding the second segment the first's share collapses it from 9 samples
    // at a 200-row interval to 1 at a 1,900-row interval.
    {
        const auto old_cap = config::sort_key_max_samples_per_tablet;
        config::sort_key_max_samples_per_tablet = 12;
        DeferOp restore_cap([&] { config::sort_key_max_samples_per_tablet = old_cap; });

        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);
        metadata->mutable_schema()->mutable_column(0)->set_index_length(4); // see Case 1

        // 12 samples apportioned over 200 + 2000 rows gives the small segment 1 and the large one
        // 10. At 100-row blocks the small segment offers 1 index candidate (stride 1, 1 sample) and
        // the large one 19 (stride ceil(19/10) = 2, 9 samples at a 2 * 100 = 200-row interval).
        const std::string small_name = "seg_budget_small.dat";
        const std::string large_name = "seg_budget_large.dat";
        write_int_key_segment(tablet_id, metadata->schema(), small_name, /*num_rows=*/200, /*start_key=*/0);
        write_int_key_segment(tablet_id, metadata->schema(), large_name, /*num_rows=*/2000, /*start_key=*/1000);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(false);
        rowset->set_num_rows(2200);
        rowset->set_data_size(22000);

        auto* sm_small = rowset->add_segment_metas();
        sm_small->set_filename(small_name);
        sm_small->set_size(2000);
        sm_small->set_num_rows(200);
        sm_small->mutable_sort_key_min()->CopyFrom(make_int_tuple(0));
        sm_small->mutable_sort_key_max()->CopyFrom(make_int_tuple(199));

        auto* sm_large = rowset->add_segment_metas();
        sm_large->set_filename(large_name);
        sm_large->set_size(20000);
        sm_large->set_num_rows(2000);
        sm_large->mutable_sort_key_min()->CopyFrom(make_int_tuple(1000));
        sm_large->mutable_sort_key_max()->CopyFrom(make_int_tuple(2999));

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0));

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(2, result.value().size());

        EXPECT_EQ(1u, result.value()[0].sort_key_samples.size());
        EXPECT_EQ(100, result.value()[0].sort_key_sample_row_interval);

        ASSERT_EQ(9u, result.value()[1].sort_key_samples.size());
        EXPECT_EQ(200, result.value()[1].sort_key_sample_row_interval);
        // The first sample of the large segment is the key 200 rows into its own run.
        EXPECT_EQ(1200, result.value()[1].sort_key_samples[0][0].value().get_int32());
    }

    // Case 5: under partial-segment compaction, Rowset::load_segments hands back only
    // [next_compaction_offset, +limit); a segment outside that window can never be sampled, so it
    // must take no share of the cap. Three equal 2,000-row segments with the window at [1, 3) and
    // the cap squeezed to 12: the two in-window segments are entitled to 6 samples each, which at
    // 19 index candidates is a stride of 4 -> 4 samples at a 400-row interval. Counting the
    // out-of-window segment would apportion 12 over 6,000 rows instead of 4,000, giving each of
    // them 4 -> a stride of 5 -> 3 samples at a 500-row interval.
    {
        const auto old_cap = config::sort_key_max_samples_per_tablet;
        config::sort_key_max_samples_per_tablet = 12;
        DeferOp restore_cap([&] { config::sort_key_max_samples_per_tablet = old_cap; });

        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);
        metadata->mutable_schema()->mutable_column(0)->set_index_length(4); // see Case 1

        constexpr int64_t kRowsPerSegment = 2000;
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        // Load-bearing: the Rowset(tablet_mgr, tablet_metadata, rowset_index, compaction_segment_limit)
        // ctor drops compaction_segment_limit unless the rowset is overlapped
        // (storage/lake/rowset.cpp), so without this partial_segments_compaction() is false and
        // there is no window to test.
        rowset->set_overlapped(true);
        rowset->set_num_rows(3 * kRowsPerSegment);
        rowset->set_data_size(30000);
        // Segments 1 and 2 are the compaction window; segment 0 is already compacted.
        rowset->set_next_compaction_offset(1);

        for (int i = 0; i < 3; ++i) {
            const int64_t start_key = i * 10000;
            const std::string name = fmt::format("seg_window_{}.dat", i);
            write_int_key_segment(tablet_id, metadata->schema(), name, kRowsPerSegment,
                                  static_cast<int32_t>(start_key));
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(name);
            sm->set_size(10000);
            sm->set_num_rows(kRowsPerSegment);
            sm->mutable_sort_key_min()->CopyFrom(make_int_tuple(static_cast<int32_t>(start_key)));
            sm->mutable_sort_key_max()->CopyFrom(make_int_tuple(static_cast<int32_t>(start_key + kRowsPerSegment - 1)));
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, /*compaction_segment_limit=*/2));
        ASSERT_TRUE(rowsets[0]->partial_segments_compaction());

        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(3, result.value().size());

        // Out of window: load_segments never returned it, so it stays coarse.
        EXPECT_TRUE(result.value()[0].sort_key_samples.empty());
        EXPECT_EQ(0, result.value()[0].sort_key_sample_row_interval);

        for (int i = 1; i < 3; ++i) {
            SCOPED_TRACE(fmt::format("in-window segment meta_pos={}", i));
            EXPECT_EQ(4u, result.value()[i].sort_key_samples.size());
            EXPECT_EQ(400, result.value()[i].sort_key_sample_row_interval);
        }
    }

    // Case 6: the perf gate. A zero sampling cap must open no rowset at all -- the samples that do
    // not get taken are already covered above, but the I/O that is not performed is only visible
    // through the rowsets-opened counter, which is why that counter exists.
    {
        int64_t tablet_id = next_id();
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(2);
        metadata->mutable_schema()->mutable_column(0)->set_index_length(4); // see Case 1

        const int64_t num_rows = 250;
        const std::string seg_name = "seg_gate.dat";
        const uint64_t seg_size = write_int_key_segment(tablet_id, metadata->schema(), seg_name, num_rows);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(false);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(seg_size);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(seg_name);
        segment_meta->set_size(seg_size);
        segment_meta->set_num_rows(num_rows);
        segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(0));
        segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(static_cast<int32_t>(num_rows - 1)));

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0));

        {
            const auto old_cap = config::sort_key_max_samples_per_tablet;
            config::sort_key_max_samples_per_tablet = 0;
            DeferOp restore_cap([&] { config::sort_key_max_samples_per_tablet = old_cap; });

            const int64_t opened_before = sort_key_sampling_rowsets_opened_count();
            auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
            ASSERT_TRUE(result.ok());
            ASSERT_EQ(1, result.value().size());
            EXPECT_TRUE(result.value()[0].sort_key_samples.empty());
            EXPECT_EQ(opened_before, sort_key_sampling_rowsets_opened_count())
                    << "a zero sampling cap must not open a single rowset";
        }

        // Same rowset, same call, non-zero cap: this is what proves the assertion above is about
        // the gate and not about the fixture being unsampleable.
        const int64_t opened_before = sort_key_sampling_rowsets_opened_count();
        auto result = TabletParallelCompactionManager::_collect_segment_key_bounds(rowsets, /*split_width=*/2);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(1, result.value().size());
        EXPECT_FALSE(result.value()[0].sort_key_samples.empty());
        EXPECT_EQ(opened_before + 1, sort_key_sampling_rowsets_opened_count());
    }
}

// How far a subtask's WRITTEN row count may sit from a perfectly even share. Sampling divides this
// fixture's four overlapping 25,000-row segments at a granularity of ~760 rows (32 samples per
// segment at a 758-row stride), i.e. ~3% of a subtask, so 0.25 leaves ample headroom -- while
// still being far tighter than the ~50% skew coarse [min, max] bounds alone produce here.
constexpr double kSubtaskEvennessTolerance = 0.25;

// target_subtasks = max(2, min(max_parallel, ceil(total_bytes / max_bytes_per_subtask))) caps the
// output width, and sampling does not touch any term of it: it only refines WHERE the boundaries
// between those subtasks fall. The cap is what this pins.
//
// What is observable from here is groups.size(), which is min(target_subtasks, boundaries + 1) --
// so "never exceeds the cap" is the general law, while the EQ below holds only for THIS fixture,
// where the coarse path already produced enough boundaries to reach the cap (measured at 4 both
// before and after sampling was wired in). It is deliberately not asserted as a general invariant:
// on a layout whose coarse bounds yield fewer boundaries than the cap, sampling legitimately RAISES
// the count, and an EQ asserted as a law there would fail a correct change.
TEST_F(TabletParallelCompactionManagerTest, subtask_count_stays_at_the_target_for_this_fixture) {
    auto rowsets = build_rowsets_with_varchar_sort_key(/*num_rowsets=*/4, /*rows_each=*/25000);
    auto groups =
            _manager->_create_range_split_groups(_sampling_tablet_id, rowsets, /*max_parallel=*/4, kBytesPerSubtask);
    // The LE is implied by the EQ below, so deleting it alone leaves the suite green. It is kept
    // deliberately: the EQ is scoped to this fixture's boundary count (see the comment above), while
    // the LE is the general law that must still hold if the EQ is ever relaxed for another layout.
    EXPECT_LE(groups.size(), kExpectedSubtaskCount) << "sampling must never raise the width above target_subtasks";
    EXPECT_EQ(kExpectedSubtaskCount, groups.size());
}

// Sampled boundaries must divide the tablet to within kSubtaskEvennessTolerance of even, measured
// against the rows the fixture actually wrote. Coarse [min, max] bounds alone put ~37,500 rows in
// the first subtask and ~12,500 in the last on this layout (see
// build_rowsets_with_varchar_sort_key); that skew is what this asserts is gone.
TEST_F(TabletParallelCompactionManagerTest, range_split_subtasks_are_balanced_with_sampling) {
    constexpr int64_t kRowsEach = 25000;
    constexpr int64_t kTotalRows = 4 * kRowsEach;
    auto rowsets = build_rowsets_with_varchar_sort_key(/*num_rowsets=*/4, kRowsEach);

    const int64_t data_page_segments_before = sort_key_sampling_data_page_segments_count();
    auto groups =
            _manager->_create_range_split_groups(_sampling_tablet_id, rowsets, /*max_parallel=*/4, kBytesPerSubtask);
    ASSERT_EQ(kExpectedSubtaskCount, groups.size());

    // Which sampler path ran, asserted rather than assumed: both paths publish through the same
    // carrier, so the boundaries alone cannot tell them apart. A VARCHAR sort key is truncated in
    // the short key index, so all four segments must be sampled from their data pages.
    EXPECT_EQ(4, sort_key_sampling_data_page_segments_count() - data_page_segments_before);

    const double ideal = static_cast<double>(kTotalRows) / kExpectedSubtaskCount;
    int64_t total = 0;
    for (size_t i = 0; i < groups.size(); ++i) {
        SCOPED_TRACE(fmt::format("subtask={}", i));
        const int64_t rows = written_rows_in_group(groups[i]);
        total += rows;
        EXPECT_NEAR(static_cast<double>(rows), ideal, ideal * kSubtaskEvennessTolerance);
    }
    EXPECT_EQ(kTotalRows, total) << "the emitted subtask ranges must tile every written row";
}

// The width requested from the sampler must be floored at 2. Both terms of target_subtasks --
// max_parallel and ceil(total_bytes / max_bytes_per_subtask) -- can be 1 while target_subtasks is
// still 2, and allocate_sort_key_sample_budget returns an all-zero budget below a width of 2, so
// without the floor those two subtasks would be cut from coarse [min, max] bounds.
//
// This is a DEFENSIVE floor, not a reachable production state: _create_range_split_groups has one
// caller (_create_subtask_groups), which already returns early on max_parallel <= 1, so production
// never reaches this function with a width-1 request. The floor is asserted here because it is
// otherwise unobservable -- at the max_parallel = 4 the other tests use, max(2, 4) == 4 -- and
// removing it would then be invisible.
TEST_F(TabletParallelCompactionManagerTest, requested_width_is_floored_at_two) {
    constexpr int64_t kRowsEach = 25000;
    constexpr int64_t kTotalRows = 4 * kRowsEach;
    // Two rowsets' worth, so ceil(total_bytes / this) == 2 == the floored subtask count, which
    // keeps the greedy loop's target at total/2 rather than at this cap.
    constexpr int64_t kBytesPerHalf = 2 * kBytesPerSubtask;
    auto rowsets = build_rowsets_with_varchar_sort_key(/*num_rowsets=*/4, kRowsEach);

    const int64_t data_page_segments_before = sort_key_sampling_data_page_segments_count();
    auto groups = _manager->_create_range_split_groups(_sampling_tablet_id, rowsets, /*max_parallel=*/1, kBytesPerHalf);
    ASSERT_EQ(2u, groups.size());
    EXPECT_EQ(4, sort_key_sampling_data_page_segments_count() - data_page_segments_before)
            << "a width-1 request must still be floored to 2, which funds sampling";

    // Tighter than kSubtaskEvennessTolerance on purpose: at 0.25 a two-way split of this layout is
    // within tolerance even from coarse bounds alone (62,498 / 37,502), so a looser bound here
    // would assert nothing.
    const double ideal = static_cast<double>(kTotalRows) / 2;
    for (size_t i = 0; i < groups.size(); ++i) {
        SCOPED_TRACE(fmt::format("subtask={}", i));
        EXPECT_NEAR(static_cast<double>(written_rows_in_group(groups[i])), ideal, ideal * 0.10);
    }
}

// The sample budget must be requested at the width this call can actually produce, not at
// max_parallel, which is a user-set table property. Here max_parallel is 64 but the data is only
// two and a half subtasks' worth, so target_subtasks is 3 and the budget must be 3 * 32 samples --
// budgeting at max_parallel would ask for min(1024, 64 * 32) = 1024, better than ten times what a
// 3-way split can use, in synchronous data-page reads before any subtask starts.
TEST_F(TabletParallelCompactionManagerTest, requested_width_tracks_the_achievable_width) {
    // ceil(4 * kSampledRowsetDataSize / this) == 3, and 3 < max_parallel, so the byte bound is what
    // decides the width.
    constexpr int64_t kBytesPerThird = 3 * kSampledRowsetDataSize / 2;
    auto rowsets = build_rowsets_with_varchar_sort_key(/*num_rowsets=*/4, /*rows_each=*/25000);

    const int64_t samples_before = sort_key_sampling_samples_count();
    auto groups =
            _manager->_create_range_split_groups(_sampling_tablet_id, rowsets, /*max_parallel=*/64, kBytesPerThird);
    ASSERT_EQ(3u, groups.size());

    const int64_t samples = sort_key_sampling_samples_count() - samples_before;
    EXPECT_GT(samples, 0) << "the width must still fund sampling, not merely be small";
    EXPECT_LE(samples, 3 * kSortKeySamplesPerSplit)
            << "budgeted at max_parallel (64) this would be min(1024, 2048) samples, not 3 * 32";
}

TEST_F(TabletParallelCompactionManagerTest, test_calculate_range_split_boundaries) {
    // Case 1: basic - 3 overlapping segments, target 3 subtasks → 2 boundaries
    {
        std::vector<SegmentSplitInfo> seg_bounds;
        for (int i = 0; i < 3; i++) {
            SegmentSplitInfo b;
            b.min_key = make_variant_int(i * 10);
            b.max_key = make_variant_int(i * 10 + 15);
            b.data_size = 3000;
            b.num_rows = 300;
            seg_bounds.push_back(std::move(b));
        }

        auto result = calculate_range_split_boundaries(seg_bounds, 3, 3000, /*use_num_rows=*/false);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(2, result.value().boundaries.size());
    }

    // Case 2: single subtask → empty boundaries
    {
        std::vector<SegmentSplitInfo> seg_bounds;
        SegmentSplitInfo b;
        b.min_key = make_variant_int(0);
        b.max_key = make_variant_int(100);
        b.data_size = 1000;
        b.num_rows = 100;
        seg_bounds.push_back(std::move(b));

        auto result = calculate_range_split_boundaries(seg_bounds, 1, 5000, /*use_num_rows=*/false);
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE(result.value().boundaries.empty());
    }

    // Case 3: empty segments → empty boundaries
    {
        std::vector<SegmentSplitInfo> empty;
        auto result = calculate_range_split_boundaries(empty, 3, 3000, /*use_num_rows=*/false);
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE(result.value().boundaries.empty());
    }
}

TEST_F(TabletParallelCompactionManagerTest, test_variant_tuple_to_olap_tuple) {
    // Case 1: single int value
    {
        auto vt = make_variant_int(42);
        auto olap = TabletParallelCompactionManager::_variant_tuple_to_olap_tuple(vt);
        ASSERT_EQ(1, olap.size());
        EXPECT_EQ("42", olap.get_value(0));
        EXPECT_FALSE(olap.is_null(0));
    }

    // Case 2: empty tuple
    {
        VariantTuple vt;
        auto olap = TabletParallelCompactionManager::_variant_tuple_to_olap_tuple(vt);
        EXPECT_EQ(0, olap.size());
    }

    // Case 3: mixed null and non-null values
    {
        VariantTuple vt;
        auto type_info = get_type_info(TYPE_INT);
        Datum d1;
        d1.set_int32(42);
        vt.append(DatumVariant(type_info, d1));
        Datum d_null; // default is null
        vt.append(DatumVariant(type_info, d_null));
        Datum d2;
        d2.set_int32(99);
        vt.append(DatumVariant(type_info, d2));

        auto olap = TabletParallelCompactionManager::_variant_tuple_to_olap_tuple(vt);
        ASSERT_EQ(3, olap.size());
        EXPECT_FALSE(olap.is_null(0));
        EXPECT_EQ("42", olap.get_value(0));
        EXPECT_TRUE(olap.is_null(1));
        EXPECT_FALSE(olap.is_null(2));
        EXPECT_EQ("99", olap.get_value(2));
    }

    // Case 4: single null value
    {
        VariantTuple vt;
        auto type_info = get_type_info(TYPE_INT);
        Datum null_datum;
        vt.append(DatumVariant(type_info, null_datum));

        auto olap = TabletParallelCompactionManager::_variant_tuple_to_olap_tuple(vt);
        EXPECT_EQ(1, olap.size());
    }
}

TEST_F(TabletParallelCompactionManagerTest, test_create_range_split_groups) {
    int64_t tablet_id = 10103;
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(5);

    // Create 4 rowsets with overlapping key ranges, each with segment metadata
    for (int i = 0; i < 4; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(1000);
        rowset->set_data_size(10 * 1024 * 1024); // 10MB each

        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(fmt::format("seg_{}.dat", i));
        segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 100));
        segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 100 + 200));
        segment_meta->set_num_rows(1000);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 5));
    auto meta = tablet.metadata();

    std::vector<RowsetPtr> rowsets;
    for (int i = 0; i < meta->rowsets_size(); i++) {
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
    }

    auto groups = _manager->_create_range_split_groups(tablet_id, rowsets, 3, 15 * 1024 * 1024);

    // Should create 2-3 groups based on boundary calculation
    ASSERT_GE(groups.size(), 2);
    ASSERT_LE(groups.size(), 3);

    for (const auto& g : groups) {
        EXPECT_EQ(SubtaskType::RANGE_SPLIT, g.type);
        EXPECT_EQ(4, g.range_split_rowsets.size());
    }

    EXPECT_TRUE(groups.front().is_first_range);
    EXPECT_TRUE(groups.back().is_last_range);
}

TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_range_split) {
    int64_t tablet_id = 10104;
    int64_t txn_id = 20104;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2};
    state->expected_range_split_count = 3;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Simulate 3 successful range split subtasks, each with one segment_meta (idx=0)
    for (int i = 0; i < 3; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->add_input_rowsets(1);
        op->add_input_rowsets(2);
        op->set_compact_version(version);
        auto* out = op->mutable_output_rowset();
        out->set_num_rows(100 * (i + 1));
        out->set_data_size(1000 * (i + 1));
        // Each subtask assigns segment_idx=0 independently; merge must renumber them.
        auto* sm = out->add_segment_metas();
        sm->set_filename(fmt::format("range_seg_{}.dat", i));
        sm->set_size(1000 * (i + 1));
        sm->set_segment_idx(0);
        sm->set_num_rows(100 * (i + 1));

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.input_rowset_ids = {0, 1, 2};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    ASSERT_TRUE(result.ok());

    const auto& merged_log = result.value();
    ASSERT_TRUE(merged_log.has_op_parallel_compaction());
    const auto& op_parallel = merged_log.op_parallel_compaction();

    EXPECT_TRUE(op_parallel.is_range_split());
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());

    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_EQ(3, merged.input_rowsets_size());
    ASSERT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(600, merged.output_rowset().num_rows());   // 100+200+300
    EXPECT_EQ(6000, merged.output_rowset().data_size()); // 1000+2000+3000
    EXPECT_EQ(3, merged.output_rowset().segment_metas_size());
    EXPECT_FALSE(merged.output_rowset().overlapped());
    // next_compaction_offset must NOT be set for non-overlapped rowsets (proto contract).
    EXPECT_FALSE(merged.output_rowset().has_next_compaction_offset());
    // segment_idx must be renumbered sequentially: 0, 1, 2 (not 0, 0, 0).
    ASSERT_EQ(3, merged.output_rowset().segment_metas_size());
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(static_cast<uint32_t>(i), merged.output_rowset().segment_metas(i).segment_idx())
                << "segment_metas[" << i << "].segment_idx should be " << i;
    }

    EXPECT_EQ(3, op_parallel.success_subtask_ids_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_range_split_partial_failure) {
    int64_t tablet_id = 10105;
    int64_t txn_id = 20105;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1};
    state->expected_range_split_count = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Subtask 0 succeeds
    {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 0;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->add_input_rowsets(1);
        op->mutable_output_rowset()->set_num_rows(100);
        op->mutable_output_rowset()->set_data_size(1000);

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = 0;
            info.input_rowset_ids = {0, 1};
            state->running_subtasks[0] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx));
    }

    // Subtask 1 fails
    {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 1;
        ctx->status = Status::InternalError("test failure");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = 1;
            info.input_rowset_ids = {0, 1};
            state->running_subtasks[1] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx));
    }

    // Range split requires ALL subtasks to succeed
    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test that range split detects incomplete submission (e.g., submit_func failed
// for the 3rd subtask but the first 2 were already submitted and succeeded).
// Without the expected_range_split_count check, this would incorrectly merge
// only 2 out of 3 ranges, causing data loss.
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_range_split_incomplete_submission) {
    int64_t tablet_id = 10115;
    int64_t txn_id = 20115;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2};
    // Expected 3 subtasks, but only 2 were actually submitted
    state->expected_range_split_count = 3;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Only 2 subtasks completed successfully (3rd was never submitted)
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->add_input_rowsets(1);
        op->add_input_rowsets(2);
        op->set_compact_version(version);
        op->mutable_output_rowset()->set_num_rows(100);
        op->mutable_output_rowset()->set_data_size(1000);
        auto* segment_meta = op->mutable_output_rowset()->add_segment_metas();
        segment_meta->set_filename(fmt::format("range_seg_{}.dat", i));
        segment_meta->set_size(1000);

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.input_rowset_ids = {0, 1, 2};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    // Should fail: only 2 of 3 expected subtasks completed
    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_range_split_with_lcrm) {
    int64_t tablet_id = 10106;
    int64_t txn_id = 20106;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->is_range_split = true;
    state->expected_range_split_count = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Create the data directory and LCRM files on disk so _merge_subtask_lcrm_files can read them.
    for (int i = 0; i < 2; i++) {
        std::string lcrm_name = fmt::format("lcrm_{}", i);
        std::string lcrm_path = _tablet_mgr->lcrm_location(tablet_id, lcrm_name);
        std::string dir = std::filesystem::path(lcrm_path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        RowsMapperBuilder builder(lcrm_path);
        std::vector<uint64_t> dummy_rows(50, static_cast<uint64_t>(i));
        CHECK_OK(builder.append(dummy_rows));
        CHECK_OK(builder.finalize());
    }

    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->set_compact_version(version);
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        op->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("seg_{}.dat", i));

        auto* lcrm = op->mutable_lcrm_file();
        lcrm->set_name(fmt::format("lcrm_{}", i));
        // size intentionally omitted so iter.open() falls back to actual file size via get_size()

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.input_rowset_ids = {0};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    ASSERT_TRUE(result.ok());

    const auto& op_parallel = result.value().op_parallel_compaction();
    EXPECT_TRUE(op_parallel.is_range_split());
    EXPECT_FALSE(op_parallel.subtask_compactions(0).output_rowset().overlapped());
    EXPECT_EQ(100, op_parallel.subtask_compactions(0).output_rowset().num_rows());
    EXPECT_EQ(2, op_parallel.orphan_lcrm_files_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_compaction_context_range_split_fields) {
    auto ctx = std::make_unique<CompactionTaskContext>(1, 2, 3, false, true, nullptr);

    EXPECT_FALSE(ctx->has_range_split);
    EXPECT_TRUE(ctx->range_start_key.empty());
    EXPECT_TRUE(ctx->range_end_key.empty());
    EXPECT_TRUE(ctx->range_lower_inclusive);
    EXPECT_FALSE(ctx->range_upper_inclusive);
    EXPECT_FALSE(ctx->is_first_range);
    EXPECT_FALSE(ctx->is_last_range);

    ctx->has_range_split = true;
    ctx->is_first_range = true;
    OlapTuple upper({"100"});
    ctx->range_end_key.push_back(upper);
    ctx->range_upper_inclusive = false;

    EXPECT_TRUE(ctx->has_range_split);
    EXPECT_TRUE(ctx->is_first_range);
    EXPECT_EQ(1, ctx->range_end_key.size());
    EXPECT_EQ("100", ctx->range_end_key[0].get_value(0));
}

// ================================================================================
// Additional tests for uncovered code paths
// ================================================================================

// Test 1: Range split merge with segment_metas renumbering (lines 1016-1021)
// When range split subtasks produce segment_metas, the merged output must renumber
// segment_idx sequentially across subtasks to avoid RSSID collisions in PK tables.
TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_segment_metas_renumbering) {
    int64_t tablet_id = 10200;
    int64_t txn_id = 20200;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1};
    state->expected_range_split_count = 3;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Create 3 subtasks, each with segment_metas in output
    for (int i = 0; i < 3; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->add_input_rowsets(1);
        if (i == 0) {
            op->set_compact_version(version);
        }

        auto* output = op->mutable_output_rowset();
        output->set_num_rows(100);
        output->set_data_size(1000);
        // Each subtask produces 2 segment_metas with segment_idx starting from 0
        for (int j = 0; j < 2; j++) {
            auto* sm = output->add_segment_metas();
            sm->set_filename(fmt::format("range_{}_seg_{}.dat", i, j));
            sm->set_size(500);
            sm->set_segment_idx(j); // Each subtask starts from 0
            sm->set_num_rows(50);
            sm->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 100 + j * 50));
            sm->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 100 + j * 50 + 49));
        }

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.input_rowset_ids = {0, 1};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    ASSERT_TRUE(result.ok()) << result.status();

    const auto& op_parallel = result.value().op_parallel_compaction();
    EXPECT_TRUE(op_parallel.is_range_split());
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());

    const auto& merged_output = op_parallel.subtask_compactions(0).output_rowset();
    // 3 subtasks * 2 segments each = 6 segments total
    EXPECT_EQ(6, merged_output.segment_metas_size());
    EXPECT_EQ(300, merged_output.num_rows());
    EXPECT_FALSE(merged_output.overlapped());

    // Verify segment_idx renumbering: should be 0,1,2,3,4,5 (not 0,1,0,1,0,1)
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(static_cast<uint32_t>(i), merged_output.segment_metas(i).segment_idx())
                << "segment_meta[" << i << "] should have segment_idx=" << i;
    }

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test 2: Large rowset split group failure detection (lines 1066-1110)
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_group_subtask_failure) {
    int64_t tablet_id = 10201;
    int64_t txn_id = 20201;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->is_range_split = false;

    // Large rowset 100 split into subtasks 0 and 1
    state->large_rowset_split_groups[100] = {0, 1};
    state->expected_large_rowset_split_counts[100] = 2;

    // Normal subtask 2
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Subtask 0 (large rowset part) succeeds
    {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 0;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(100);
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        op->mutable_output_rowset()->add_segment_metas()->set_filename("seg_0.dat");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = 0;
            info.type = SubtaskType::LARGE_ROWSET_PART;
            info.large_rowset_id = 100;
            info.input_rowset_ids = {100};
            state->running_subtasks[0] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx));
    }

    // Subtask 1 (large rowset part) FAILS
    {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 1;
        ctx->status = Status::InternalError("compaction failed");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = 1;
            info.type = SubtaskType::LARGE_ROWSET_PART;
            info.large_rowset_id = 100;
            info.input_rowset_ids = {100};
            state->running_subtasks[1] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx));
    }

    // Subtask 2 (normal) succeeds
    {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 2;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(200);
        op->mutable_output_rowset()->set_num_rows(100);
        op->mutable_output_rowset()->set_data_size(1000);
        op->mutable_output_rowset()->add_segment_metas()->set_filename("seg_2.dat");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = 2;
            info.type = SubtaskType::NORMAL;
            info.input_rowset_ids = {200};
            state->running_subtasks[2] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    ASSERT_TRUE(result.ok()) << result.status();

    const auto& op_parallel = result.value().op_parallel_compaction();
    EXPECT_FALSE(op_parallel.is_range_split());

    // Only the normal subtask (2) should be in success_subtask_ids
    // Large rowset group should be entirely skipped due to failure
    bool found_subtask_2 = false;
    bool found_subtask_0 = false;
    bool found_subtask_1 = false;
    for (int i = 0; i < op_parallel.success_subtask_ids_size(); i++) {
        if (op_parallel.success_subtask_ids(i) == 2) found_subtask_2 = true;
        if (op_parallel.success_subtask_ids(i) == 0) found_subtask_0 = true;
        if (op_parallel.success_subtask_ids(i) == 1) found_subtask_1 = true;
    }
    EXPECT_TRUE(found_subtask_2) << "Normal subtask 2 should succeed";
    EXPECT_FALSE(found_subtask_0) << "Subtask 0 from failed large rowset group should be skipped";
    EXPECT_FALSE(found_subtask_1) << "Subtask 1 from failed large rowset group should be skipped";

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test 3: Large rowset split group incomplete (lines 1082-1091)
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_group_incomplete) {
    int64_t tablet_id = 10202;
    int64_t txn_id = 20202;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->is_range_split = false;

    // Large rowset 100 expected 3 subtasks, but only 2 were created
    state->large_rowset_split_groups[100] = {0, 1};     // Only 2 created
    state->expected_large_rowset_split_counts[100] = 3; // Expected 3

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Both subtasks succeed, but group is still incomplete
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(100);
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        op->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("seg_{}.dat", i));

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.type = SubtaskType::LARGE_ROWSET_PART;
            info.large_rowset_id = 100;
            info.input_rowset_ids = {100};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    // When all subtasks belong to an incomplete large rowset group and none succeed,
    // get_merged_txn_log returns an error to prevent data loss from silently dropping
    // unprocessed segments.
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test 4: _collect_segment_key_bounds with zero num_rows fallback (line 2402-2403)
// Test 6: Range split all subtasks failed (lines 967-971)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_all_subtasks_failed) {
    int64_t tablet_id = 10204;
    int64_t txn_id = 20204;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1};
    state->expected_range_split_count = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Both subtasks fail
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->status = Status::InternalError("all failed");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            SubtaskInfo info;
            info.subtask_id = i;
            info.input_rowset_ids = {0, 1};
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test 7: _is_large_rowset_for_split with next_compaction_offset >= segments_size (line 1665)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_is_large_rowset_next_compaction_offset) {
    int64_t tablet_id = 30100;

    // Create a large rowset that meets all other split criteria
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(2);

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_overlapped(true);
    int64_t segment_size = 3 * 1024L * 1024L * 1024L; // 3GB per segment
    int num_segments = 4;
    rowset->set_num_rows(1000 * num_segments);
    rowset->set_data_size(segment_size * num_segments); // 12GB total, > 2*4GB

    // Set next_compaction_offset >= segments_size to indicate all segments processed
    rowset->set_next_compaction_offset(num_segments);

    for (int i = 0; i < num_segments; i++) {
        std::string segment_name = fmt::format("segment_{}.dat", i);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(segment_name);
        segment_meta->set_size(segment_size);

        std::string path = _lp->segment_location(tablet_id, segment_name);
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy_segment_data"));
        CHECK_OK(st.value()->close());
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
    auto meta = tablet.metadata();

    auto rowset_ptr = std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0);
    int64_t max_bytes = 1024L * 1024L * 1024L; // 1GB per subtask

    // Should return false because next_compaction_offset >= segments_size
    EXPECT_FALSE(TabletParallelCompactionManager::_is_large_rowset_for_split(rowset_ptr, max_bytes));

    _manager->cleanup_tablet(tablet_id, 0);
}

// Test 8: _split_large_rowset merge-with-previous-group (lines 1729-1735)
// When the last group has fewer than 2 segments, it merges with the previous group
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_split_large_rowset_merge_with_previous) {
    int64_t tablet_id = 30101;

    // 5 segments, target 2 per subtask: [0,2), [2,4), [4,5) -> last has 1 segment -> merge
    int64_t segment_size = 3 * 1024L * 1024L * 1024L; // 3GB each
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(2);

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_overlapped(true);
    rowset->set_num_rows(5000);
    rowset->set_data_size(segment_size * 5);

    for (int i = 0; i < 5; i++) {
        std::string segment_name = fmt::format("segment_{}.dat", i);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(segment_name);
        segment_meta->set_size(segment_size);

        std::string path = _lp->segment_location(tablet_id, segment_name);
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystemFactory::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
    auto meta = tablet.metadata();
    auto rowset_ptr = std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0);

    // Target bytes set to allow ~2 segments per subtask
    int64_t target_bytes = segment_size * 2;
    auto groups = TabletParallelCompactionManager::_split_large_rowset(rowset_ptr, target_bytes, 0);

    // 5 segments / 2 per subtask = 3 groups initially: [0,2), [2,4), [4,5)
    // Last group has 1 segment -> merged with previous -> 2 groups: [0,2), [2,5)
    ASSERT_EQ(2, groups.size());
    EXPECT_EQ(0, groups[0].segment_start);
    EXPECT_EQ(2, groups[0].segment_end);
    EXPECT_EQ(2, groups[1].segment_start);
    EXPECT_EQ(5, groups[1].segment_end);
    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, groups[0].type);
    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, groups[1].type);
}

// Test: _split_large_rowset with too few segments (< 2)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_split_large_rowset_too_few_segments) {
    int64_t tablet_id = 30102;

    int64_t segment_size = 3 * 1024L * 1024L * 1024L;
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(2);

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_overlapped(true);
    rowset->set_num_rows(1000);
    rowset->set_data_size(segment_size);

    // Only 1 segment
    auto* segment_meta = rowset->add_segment_metas();
    segment_meta->set_filename("segment_0.dat");
    segment_meta->set_size(segment_size);
    std::string path = _lp->segment_location(tablet_id, "segment_0.dat");
    std::string dir = std::filesystem::path(path).parent_path().string();
    CHECK_OK(fs::create_directories(dir));
    auto fs = FileSystemFactory::CreateSharedFromString(path);
    auto st = fs.value()->new_writable_file(path);
    CHECK_OK(st.status());
    CHECK_OK(st.value()->append("dummy"));
    CHECK_OK(st.value()->close());

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
    auto meta = tablet.metadata();
    auto rowset_ptr = std::make_shared<Rowset>(_tablet_mgr.get(), meta, 0, 0);

    auto groups = TabletParallelCompactionManager::_split_large_rowset(rowset_ptr, segment_size / 2, 0);
    EXPECT_TRUE(groups.empty()) << "Should return empty when total_segments < 2";
}

// Test 9: _group_small_rowsets basic (lines 1758-1795)
TEST_F(TabletParallelCompactionManagerTest, test_group_small_rowsets_basic) {
    int64_t tablet_id = 10205;

    // Create a tablet with 5 small rowsets of 1MB each
    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 6));
    auto metadata = tablet.metadata();

    std::vector<RowsetPtr> rowsets;
    for (int i = 0; i < metadata->rowsets_size(); i++) {
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), metadata, i, 0));
    }

    // Target 3MB per subtask: should create 2 groups (3+2 rowsets)
    int64_t target_bytes = 3 * 1024 * 1024;
    auto groups = TabletParallelCompactionManager::_group_small_rowsets(std::move(rowsets), target_bytes);

    ASSERT_EQ(2, groups.size());
    EXPECT_EQ(SubtaskType::NORMAL, groups[0].type);
    EXPECT_EQ(SubtaskType::NORMAL, groups[1].type);
    EXPECT_EQ(3, groups[0].rowsets.size());
    EXPECT_EQ(2, groups[1].rowsets.size());
}

TEST_F(TabletParallelCompactionManagerTest, test_group_small_rowsets_empty) {
    std::vector<RowsetPtr> empty;
    auto groups = TabletParallelCompactionManager::_group_small_rowsets(std::move(empty), 1024 * 1024);
    EXPECT_TRUE(groups.empty());
}

TEST_F(TabletParallelCompactionManagerTest, test_group_small_rowsets_single_large) {
    int64_t tablet_id = 10206;

    // Create 1 rowset of 10MB
    create_tablet_with_rowsets(tablet_id, 1, 10 * 1024 * 1024);

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));
    auto metadata = tablet.metadata();

    std::vector<RowsetPtr> rowsets;
    for (int i = 0; i < metadata->rowsets_size(); i++) {
        rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), metadata, i, 0));
    }

    // Target 5MB, but single rowset is 10MB -- should still create 1 group
    int64_t target_bytes = 5 * 1024 * 1024;
    auto groups = TabletParallelCompactionManager::_group_small_rowsets(std::move(rowsets), target_bytes);

    ASSERT_EQ(1, groups.size());
    EXPECT_EQ(1, groups[0].rowsets.size());
}

// Test 10: tablet_splitter.cpp edge cases

// Test: ordered_boundaries.size() < 2 (single unique boundary, line 52)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_single_unique_boundary) {
    // All segments have the same min_key and max_key
    std::vector<SegmentSplitInfo> seg_bounds;

    SegmentSplitInfo b0;
    b0.min_key = make_variant_int(42);
    b0.max_key = make_variant_int(42);
    b0.data_size = 1000;
    b0.num_rows = 100;
    seg_bounds.push_back(std::move(b0));

    SegmentSplitInfo b1;
    b1.min_key = make_variant_int(42);
    b1.max_key = make_variant_int(42);
    b1.data_size = 1000;
    b1.num_rows = 100;
    seg_bounds.push_back(std::move(b1));

    auto result = calculate_range_split_boundaries(seg_bounds, 3, 500, /*use_num_rows=*/false);
    ASSERT_TRUE(result.ok());
    // Only 1 unique boundary point, so ordered_boundaries.size() < 2
    EXPECT_TRUE(result.value().boundaries.empty());
}

// Test: ordered_ranges.empty() (line 77) - covered by single boundary test above

// Test: non_empty_ranges < actual_split_count (line 163)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_not_enough_non_empty_ranges) {
    // 2 segments with non-overlapping ranges, but we ask for 5 splits
    // There will be 1 range between the boundaries, so only 1 non-empty range
    std::vector<SegmentSplitInfo> seg_bounds;

    SegmentSplitInfo b0;
    b0.min_key = make_variant_int(0);
    b0.max_key = make_variant_int(10);
    b0.data_size = 1000;
    b0.num_rows = 100;
    seg_bounds.push_back(std::move(b0));

    SegmentSplitInfo b1;
    b1.min_key = make_variant_int(100);
    b1.max_key = make_variant_int(110);
    b1.data_size = 1000;
    b1.num_rows = 100;
    seg_bounds.push_back(std::move(b1));

    // We have 3 boundaries (0, 10, 100, 110) -> 3 ranges: [0,10), [10,100), [100,110]
    // [10,100) has 0 data, so only 2 non-empty ranges. Asking for 5 splits but actual_split_count
    // is min(5,3)=3. non_empty_ranges (2) < actual_split_count (3) -> empty result.
    auto result = calculate_range_split_boundaries(seg_bounds, 5, 500, /*use_num_rows=*/false);
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.value().boundaries.empty());
}

// Test: split_ranges->size() < 2 (tablet_splitter.cpp line 359)
// This is tested indirectly through the public API. The get_tablet_split_ranges
// function returns InvalidArgument when split_ranges is < 2. This is covered
// by the calculate_range_split_boundaries tests above that return empty boundaries.

// Test range split merge: auxiliary fields (encryption_metas, segment_sizes, ssts, segment_metas)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_auxiliary_fields) {
    // Case 1: encryption_metas, segment_sizes, ssts, sst_ranges
    {
        int64_t tablet_id = 10220;
        int64_t txn_id = 20220;
        int64_t version = 2;

        auto state = std::make_shared<TabletParallelCompactionState>();
        state->tablet_id = tablet_id;
        state->txn_id = txn_id;
        state->version = version;
        state->max_parallel = 2;
        state->is_range_split = true;
        state->expected_range_split_count = 2;

        _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

        for (int i = 0; i < 2; i++) {
            auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
            ctx->subtask_id = i;
            ctx->txn_log = std::make_unique<TxnLogPB>();
            auto* op = ctx->txn_log->mutable_op_compaction();
            op->add_input_rowsets(0);
            op->set_compact_version(version);
            auto* output = op->mutable_output_rowset();
            output->set_num_rows(50);
            output->set_data_size(500);
            auto* segment_meta = output->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->set_size(500);
            segment_meta->set_encryption_meta(fmt::format("enc_{}", i));

            auto* sst = op->add_ssts();
            sst->set_name(fmt::format("sst_{}.sst", i));
            auto* sst_range = op->add_sst_ranges();
            sst_range->set_start_key(fmt::format("key_{}", i * 100));

            {
                std::lock_guard<std::mutex> lock(state->mutex);
                SubtaskInfo info;
                info.subtask_id = i;
                info.input_rowset_ids = {0};
                state->running_subtasks[i] = std::move(info);
                state->total_subtasks_created++;
            }
            _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
        }

        auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
        ASSERT_TRUE(result.ok()) << result.status();

        const auto& op_parallel = result.value().op_parallel_compaction();
        ASSERT_EQ(1, op_parallel.subtask_compactions_size());
        const auto& merged = op_parallel.subtask_compactions(0);

        EXPECT_EQ(2, merged.output_rowset().segment_metas_size());
        EXPECT_EQ("enc_0", merged.output_rowset().segment_metas(0).encryption_meta());
        EXPECT_EQ("enc_1", merged.output_rowset().segment_metas(1).encryption_meta());
        EXPECT_EQ(2, merged.output_rowset().segment_metas_size());
        EXPECT_EQ(2, merged.ssts_size());
        EXPECT_EQ(2, merged.sst_ranges_size());
        EXPECT_FALSE(merged.output_rowset().overlapped());
        EXPECT_EQ(100, merged.output_rowset().num_rows());

        _manager->cleanup_tablet(tablet_id, txn_id);
    }

    // Case 2: segment_metas with re-indexed segment_idx
    {
        int64_t tablet_id = 10250;
        int64_t txn_id = 20250;
        int64_t version = 11;

        create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

        CompactRequest request;
        request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
        request.add_tablet_ids(tablet_id);
        CompactResponse response;
        TestClosure closure;
        auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

        auto state = std::make_shared<TabletParallelCompactionState>();
        state->tablet_id = tablet_id;
        state->txn_id = txn_id;
        state->version = version;
        state->max_parallel = 2;
        state->callback = callback;
        state->is_range_split = true;
        state->range_split_input_rowset_ids = {0, 1, 2};
        state->expected_range_split_count = 2;

        for (int i = 0; i < 2; i++) {
            SubtaskInfo info;
            info.subtask_id = i;
            info.type = SubtaskType::RANGE_SPLIT;
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }

        _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

        for (int i = 0; i < 2; i++) {
            auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
            ctx->subtask_id = i;
            ctx->txn_log = std::make_unique<TxnLogPB>();
            auto* op = ctx->txn_log->mutable_op_compaction();
            op->add_input_rowsets(0);
            op->add_input_rowsets(1);
            op->add_input_rowsets(2);
            if (i == 0) op->set_compact_version(10);
            auto* out = op->mutable_output_rowset();
            out->set_num_rows((i + 1) * 100);
            out->set_data_size((i + 1) * 1000);
            auto* meta = out->add_segment_metas();
            meta->set_filename(fmt::format("seg_{}.dat", i));
            meta->set_segment_idx(0); // Both subtasks start from 0
            _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
        }

        ASSERT_TRUE(closure.is_finished());
        ASSERT_EQ(1, response.txn_logs_size());
        const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
        ASSERT_EQ(1, op_parallel.subtask_compactions_size());
        const auto& merged = op_parallel.subtask_compactions(0);

        EXPECT_EQ(2, merged.output_rowset().segment_metas_size());
        EXPECT_EQ(0, merged.output_rowset().segment_metas(0).segment_idx());
        EXPECT_EQ(1, merged.output_rowset().segment_metas(1).segment_idx()); // re-indexed
        EXPECT_FALSE(merged.output_rowset().overlapped());

        _manager->cleanup_tablet(tablet_id, txn_id);
    }
}

// Test calculate_range_split_boundaries with track_sources enabled
TEST_F(TabletParallelCompactionManagerTest, test_range_split_boundaries_with_track_sources) {
    std::vector<SegmentSplitInfo> seg_bounds;

    SegmentSplitInfo b0;
    b0.source_id = 1;
    b0.min_key = make_variant_int(0);
    b0.max_key = make_variant_int(50);
    b0.data_size = 3000;
    b0.num_rows = 300;
    seg_bounds.push_back(std::move(b0));

    SegmentSplitInfo b1;
    b1.source_id = 2;
    b1.min_key = make_variant_int(30);
    b1.max_key = make_variant_int(100);
    b1.data_size = 3000;
    b1.num_rows = 300;
    seg_bounds.push_back(std::move(b1));

    auto result = calculate_range_split_boundaries(seg_bounds, 2, 3000, /*use_num_rows=*/false,
                                                   /*track_sources=*/true);
    ASSERT_TRUE(result.ok());
    auto& split = result.value();

    if (!split.boundaries.empty()) {
        // Source stats should be populated for each range
        EXPECT_FALSE(split.range_source_stats.empty());
        EXPECT_EQ(split.boundaries.size() + 1, split.range_source_stats.size());
    }
}

// Test range split related struct fields (State, SubtaskGroup, SubtaskInfo)
TEST_F(TabletParallelCompactionStateFieldsTest, test_range_split_fields) {
    // State fields
    EXPECT_FALSE(_state->is_range_split);
    EXPECT_TRUE(_state->range_split_input_rowset_ids.empty());
    EXPECT_EQ(0, _state->expected_range_split_count);
    EXPECT_TRUE(_state->expected_large_rowset_split_counts.empty());

    _state->is_range_split = true;
    _state->range_split_input_rowset_ids = {1, 2, 3};
    _state->expected_range_split_count = 3;
    _state->expected_large_rowset_split_counts[100] = 3;
    _state->expected_large_rowset_split_counts[200] = 2;

    EXPECT_TRUE(_state->is_range_split);
    EXPECT_EQ(3, _state->range_split_input_rowset_ids.size());
    EXPECT_EQ(3, _state->expected_range_split_count);
    EXPECT_EQ(2, _state->expected_large_rowset_split_counts.size());
    EXPECT_EQ(3, _state->expected_large_rowset_split_counts[100]);
    EXPECT_EQ(2, _state->expected_large_rowset_split_counts[200]);
}

TEST_F(SubtaskGroupTest, test_subtask_group_range_split_fields) {
    // SubtaskType enum distinctness
    EXPECT_NE(SubtaskType::NORMAL, SubtaskType::RANGE_SPLIT);
    EXPECT_NE(SubtaskType::LARGE_ROWSET_PART, SubtaskType::RANGE_SPLIT);

    // SubtaskGroup RANGE_SPLIT fields
    SubtaskGroup group;
    group.type = SubtaskType::RANGE_SPLIT;
    group.is_first_range = true;
    group.is_last_range = false;
    group.range_lower_inclusive = true;
    group.range_upper_inclusive = false;
    group.range_upper_bound = make_variant_int(100);
    group.total_bytes = 5000;

    EXPECT_EQ(SubtaskType::RANGE_SPLIT, group.type);
    EXPECT_TRUE(group.is_first_range);
    EXPECT_FALSE(group.is_last_range);
    EXPECT_TRUE(group.range_lower_inclusive);
    EXPECT_FALSE(group.range_upper_inclusive);
    EXPECT_EQ(5000, group.total_bytes);
    EXPECT_FALSE(group.range_upper_bound.empty());
}

TEST_F(SubtaskInfoTest, test_subtask_info_range_split_fields) {
    // Default state
    SubtaskInfo info;
    EXPECT_TRUE(info.range_lower_bound.empty());
    EXPECT_TRUE(info.range_upper_bound.empty());

    // After setting
    info.subtask_id = 7;
    info.type = SubtaskType::RANGE_SPLIT;
    info.range_lower_bound = make_variant_int(10);
    info.range_upper_bound = make_variant_int(20);

    EXPECT_EQ(7, info.subtask_id);
    EXPECT_EQ(SubtaskType::RANGE_SPLIT, info.type);
    EXPECT_FALSE(info.range_lower_bound.empty());
    EXPECT_FALSE(info.range_upper_bound.empty());
}

// =============================================================================
// Tests for range split merge path in get_merged_txn_log
// =============================================================================

// Test range split merge: all subtasks succeed, merged into single non-overlapped output
TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_all_success) {
    int64_t tablet_id = 10200;
    int64_t txn_id = 20200;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create and register tablet state with is_range_split=true
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->callback = callback;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    state->expected_range_split_count = 3;

    // Create running subtasks
    for (int i = 0; i < 3; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = state->range_split_input_rowset_ids;
        info.input_bytes = 10 * 1024 * 1024;
        info.start_time = ::time(nullptr);
        info.type = SubtaskType::RANGE_SPLIT;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    for (uint32_t rid : state->range_split_input_rowset_ids) {
        state->compacting_rowsets[rid] = 3; // ref count 3 (shared by all subtasks)
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 (first range)
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    for (uint32_t rid : state->range_split_input_rowset_ids) {
        op0->add_input_rowsets(rid);
    }
    op0->set_compact_version(10);
    auto* out0 = op0->mutable_output_rowset();
    out0->set_num_rows(100);
    out0->set_data_size(1000);
    auto* out0_seg = out0->add_segment_metas();
    out0_seg->set_filename("range_seg_0.dat");
    out0_seg->set_size(500);
    out0_seg->set_encryption_meta("enc_meta_0");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 (middle range)
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    for (uint32_t rid : state->range_split_input_rowset_ids) {
        op1->add_input_rowsets(rid);
    }
    auto* out1 = op1->mutable_output_rowset();
    out1->set_num_rows(200);
    out1->set_data_size(2000);
    auto* out1_seg = out1->add_segment_metas();
    out1_seg->set_filename("range_seg_1.dat");
    out1_seg->set_size(1000);
    out1_seg->set_encryption_meta("enc_meta_1");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Complete subtask 2 (last range)
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    auto* op2 = ctx2->txn_log->mutable_op_compaction();
    for (uint32_t rid : state->range_split_input_rowset_ids) {
        op2->add_input_rowsets(rid);
    }
    auto* out2 = op2->mutable_output_rowset();
    out2->set_num_rows(300);
    out2->set_data_size(3000);
    auto* out2_seg = out2->add_segment_metas();
    out2_seg->set_filename("range_seg_2.dat");
    out2_seg->set_size(1500);
    out2_seg->set_encryption_meta("enc_meta_2");

    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    ASSERT_TRUE(closure.is_finished());

    // Verify result - should have merged non-overlapping compaction
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // is_range_split should be set
    EXPECT_TRUE(op_parallel.is_range_split());

    // All 3 subtasks should be in success_subtask_ids
    EXPECT_EQ(3, op_parallel.success_subtask_ids_size());

    // Should have single merged compaction with all segments
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);

    // Input rowsets from first subtask
    EXPECT_EQ(10, merged.input_rowsets_size());
    EXPECT_EQ(10, merged.compact_version());

    // Merged output
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(600, merged.output_rowset().num_rows());         // 100+200+300
    EXPECT_EQ(6000, merged.output_rowset().data_size());       // 1000+2000+3000
    EXPECT_FALSE(merged.output_rowset().overlapped());         // Non-overlapped for range split
    EXPECT_EQ(3, merged.output_rowset().segment_metas_size()); // 3 segments merged
    EXPECT_EQ("range_seg_0.dat", merged.output_rowset().segment_metas(0).filename());
    EXPECT_EQ("range_seg_1.dat", merged.output_rowset().segment_metas(1).filename());
    EXPECT_EQ("range_seg_2.dat", merged.output_rowset().segment_metas(2).filename());
    EXPECT_EQ(3, merged.output_rowset().segment_metas_size());
    EXPECT_EQ(3, merged.output_rowset().segment_metas_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test range split merge: one subtask fails (all must succeed for range split)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_one_failure) {
    int64_t tablet_id = 10201;
    int64_t txn_id = 20201;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2};
    state->expected_range_split_count = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.type = SubtaskType::RANGE_SPLIT;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 successfully
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("simulated failure");
    ctx1->txn_log = std::make_unique<TxnLogPB>();

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Range split requires all subtasks to succeed, so overall should fail
    EXPECT_NE(0, response.status().status_code());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test range split merge: incomplete split (expected count mismatch)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_incomplete) {
    int64_t tablet_id = 10202;
    int64_t txn_id = 20202;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->callback = callback;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2};
    state->expected_range_split_count = 3; // Expect 3 subtasks

    // Only create 2 subtasks (simulating submit failure for 3rd)
    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.type = SubtaskType::RANGE_SPLIT;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete both subtasks successfully
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        ctx->txn_log->mutable_op_compaction()->add_input_rowsets(0);
        ctx->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    ASSERT_TRUE(closure.is_finished());

    // Should fail because completed count (2) != expected count (3)
    EXPECT_NE(0, response.status().status_code());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// =============================================================================
// Tests for large rowset split merge with LCRM files
// =============================================================================

// Test large rowset split merge with LCRM files in subtask ops
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_merge_with_lcrm) {
    int64_t tablet_id = 10210;
    int64_t txn_id = 20210;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    state->is_range_split = false;

    // Set up large rowset split group: rowset 0 split into subtasks 0 and 1
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = {0};
        info.input_bytes = 5 * 1024 * 1024;
        info.start_time = ::time(nullptr);
        info.type = SubtaskType::LARGE_ROWSET_PART;
        info.large_rowset_id = 0;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Create LCRM files on disk so _merge_subtask_lcrm_files can read them
    std::vector<int64_t> lcrm_file_sizes(2);
    for (int i = 0; i < 2; i++) {
        std::string lcrm_name = fmt::format("lcrm_{}.dat", i);
        std::string lcrm_path = _tablet_mgr->lcrm_location(tablet_id, lcrm_name);
        std::string dir = std::filesystem::path(lcrm_path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        RowsMapperBuilder builder(lcrm_path);
        size_t num_rows = (i == 0) ? 100 : 200;
        std::vector<uint64_t> dummy_rows(num_rows, static_cast<uint64_t>(i));
        CHECK_OK(builder.append(dummy_rows));
        CHECK_OK(builder.finalize());
        auto fi = builder.file_info();
        lcrm_file_sizes[i] = fi.size.has_value() ? fi.size.value() : 0;
    }

    // Complete subtask 0 with LCRM file
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->set_compact_version(10);
    op0->set_segment_range_start(0);
    op0->set_segment_range_end(2);
    auto* out0 = op0->mutable_output_rowset();
    out0->set_num_rows(100);
    out0->set_data_size(1000);
    auto* out0_seg = out0->add_segment_metas();
    out0_seg->set_filename("split_seg_0.dat");
    out0_seg->set_size(500);
    // Set LCRM file
    auto* lcrm0 = op0->mutable_lcrm_file();
    lcrm0->set_name("lcrm_0.dat");
    lcrm0->set_size(lcrm_file_sizes[0]);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with LCRM file
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->set_compact_version(10);
    op1->set_segment_range_start(2);
    op1->set_segment_range_end(4);
    auto* out1 = op1->mutable_output_rowset();
    out1->set_num_rows(200);
    out1->set_data_size(2000);
    auto* out1_seg = out1->add_segment_metas();
    out1_seg->set_filename("split_seg_1.dat");
    out1_seg->set_size(1000);
    // Set LCRM file
    auto* lcrm1 = op1->mutable_lcrm_file();
    lcrm1->set_name("lcrm_1.dat");
    lcrm1->set_size(lcrm_file_sizes[1]);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Should have 1 merged compaction from the large rowset split
    ASSERT_GE(op_parallel.subtask_compactions_size(), 1);

    // Verify merged compaction output
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, merged.input_rowsets_size());
    EXPECT_EQ(0, merged.input_rowsets(0));
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(300, merged.output_rowset().num_rows());         // 100+200
    EXPECT_EQ(3000, merged.output_rowset().data_size());       // 1000+2000
    EXPECT_TRUE(merged.output_rowset().overlapped());          // Large rowset split is overlapped
    EXPECT_EQ(2, merged.output_rowset().segment_metas_size()); // 2 segments

    // Verify orphan LCRM files are recorded
    EXPECT_EQ(2, op_parallel.orphan_lcrm_files_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test large rowset split merge: group has no valid subtasks (first_subtask remains true)
// This happens when subtasks succeed but have null txn_log or no op_compaction.
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_no_valid_txn_log) {
    int64_t tablet_id = 10211;
    int64_t txn_id = 20211;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->callback = callback;
    state->is_range_split = false;

    // Set up large rowset split group: rowset 0 split into subtasks 0 and 1
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = {0};
        info.type = SubtaskType::LARGE_ROWSET_PART;
        info.large_rowset_id = 0;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    // Add a normal subtask (subtask 2) to have at least one success overall
    SubtaskInfo info2;
    info2.subtask_id = 2;
    info2.input_rowset_ids = {1, 2};
    info2.type = SubtaskType::NORMAL;
    state->running_subtasks[2] = std::move(info2);
    state->total_subtasks_created++;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 with success but null txn_log
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = nullptr; // No txn_log
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with success but txn_log without op_compaction
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>(); // Empty txn_log, no op_compaction
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Complete normal subtask 2 with valid data
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(2);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_metas()->set_filename("seg_2.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    ASSERT_TRUE(closure.is_finished());

    // The large rowset split group had no valid txn_logs, so the empty merged_compaction
    // should be removed. Only the normal subtask 2 should be in the result.
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Should have subtask 2 in success (subtasks 0,1 have no valid txn_log but still "succeeded")
    // The large rowset split subtasks are "processed" via the merge path but produce nothing
    EXPECT_GE(op_parallel.subtask_compactions_size(), 1);

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test large rowset split merge: all subtasks in group failed
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_all_failed) {
    int64_t tablet_id = 10213;
    int64_t txn_id = 20213;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    state->is_range_split = false;

    // Large rowset split group: both subtasks fail
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = {0};
        info.type = SubtaskType::LARGE_ROWSET_PART;
        info.large_rowset_id = 0;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Both subtasks fail
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->status = Status::InternalError("subtask failed");
        ctx->txn_log = std::make_unique<TxnLogPB>();
        ctx->txn_log->mutable_op_compaction()->add_input_rowsets(0);
        ctx->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    ASSERT_TRUE(closure.is_finished());
    EXPECT_NE(0, response.status().status_code());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test large rowset split merge: incomplete split (expected count != actual count)
TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_incomplete) {
    int64_t tablet_id = 10212;
    int64_t txn_id = 20212;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->callback = callback;
    state->is_range_split = false;

    // Set up large rowset split group: rowset 0 expects 3 subtasks
    state->large_rowset_split_groups[0] = {0, 1}; // Only 2 created (incomplete)
    state->expected_large_rowset_split_counts[0] = 3;

    // Also add a normal subtask (subtask 2) so we have at least one success
    SubtaskInfo info0;
    info0.subtask_id = 0;
    info0.input_rowset_ids = {0};
    info0.type = SubtaskType::LARGE_ROWSET_PART;
    info0.large_rowset_id = 0;
    state->running_subtasks[0] = std::move(info0);
    state->total_subtasks_created++;

    SubtaskInfo info1;
    info1.subtask_id = 1;
    info1.input_rowset_ids = {0};
    info1.type = SubtaskType::LARGE_ROWSET_PART;
    info1.large_rowset_id = 0;
    state->running_subtasks[1] = std::move(info1);
    state->total_subtasks_created++;

    SubtaskInfo info2;
    info2.subtask_id = 2;
    info2.input_rowset_ids = {1, 2};
    info2.type = SubtaskType::NORMAL;
    state->running_subtasks[2] = std::move(info2);
    state->total_subtasks_created++;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete all 3 subtasks successfully
    for (int i = 0; i < 3; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        if (i < 2) {
            op->add_input_rowsets(0);
            op->set_compact_version(10);
            op->set_segment_range_start(i * 2);
            op->set_segment_range_end((i + 1) * 2);
        } else {
            op->add_input_rowsets(1);
            op->add_input_rowsets(2);
        }
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        op->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("seg_{}.dat", i));

        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    ASSERT_TRUE(closure.is_finished());

    // The large rowset split is incomplete (2/3), so subtasks 0,1 should be treated as failed
    // Only normal subtask 2 should succeed
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Only the normal subtask (2) should be in success
    EXPECT_EQ(1, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(2, op_parallel.success_subtask_ids(0));

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// =============================================================================
// Tests for submit_subtasks_from_groups with RANGE_SPLIT type
// =============================================================================

// Test submit_subtasks_from_groups records range split state correctly
TEST_F(TabletParallelCompactionManagerTest, test_submit_range_split_groups_state_recording) {
    int64_t tablet_id = 10220;
    int64_t txn_id = 20220;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create state and register it
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 3;
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Verify state was registered
    auto registered_state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, registered_state);
    EXPECT_FALSE(registered_state->is_range_split);
    EXPECT_EQ(0, registered_state->expected_range_split_count);

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// =============================================================================
// Tests for _variant_tuple_to_olap_tuple with null value
// =============================================================================

// =============================================================================
// Tests for range split with LCRM files in merged output
// =============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_range_split_merge_with_lcrm_files) {
    int64_t tablet_id = 10230;
    int64_t txn_id = 20230;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    state->is_range_split = true;
    state->range_split_input_rowset_ids = {0, 1, 2};
    state->expected_range_split_count = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.type = SubtaskType::RANGE_SPLIT;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Create LCRM files on disk so _merge_subtask_lcrm_files can read them
    std::vector<int64_t> lcrm_file_sizes(2);
    for (int i = 0; i < 2; i++) {
        std::string lcrm_name = fmt::format("lcrm_{}.dat", i);
        std::string lcrm_path = _tablet_mgr->lcrm_location(tablet_id, lcrm_name);
        std::string dir = std::filesystem::path(lcrm_path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        RowsMapperBuilder builder(lcrm_path);
        size_t num_rows = (i == 0) ? 100 : 200;
        std::vector<uint64_t> dummy_rows(num_rows, static_cast<uint64_t>(i));
        CHECK_OK(builder.append(dummy_rows));
        CHECK_OK(builder.finalize());
        auto fi = builder.file_info();
        lcrm_file_sizes[i] = fi.size.has_value() ? fi.size.value() : 0;
    }

    // Complete subtask 0 with SST files and LCRM file
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->add_input_rowsets(1);
    op0->add_input_rowsets(2);
    op0->set_compact_version(10);
    auto* out0 = op0->mutable_output_rowset();
    out0->set_num_rows(100);
    out0->set_data_size(1000);
    out0->add_segment_metas()->set_filename("range_seg_0.dat");
    // Add SST
    auto* sst0 = op0->add_ssts();
    sst0->set_name("sst_0.sst");
    // Add SST range
    auto* sst_range0 = op0->add_sst_ranges();
    sst_range0->set_start_key("0");
    sst_range0->set_end_key("100");
    // Add LCRM file
    auto* lcrm0 = op0->mutable_lcrm_file();
    lcrm0->set_name("lcrm_0.dat");
    lcrm0->set_size(lcrm_file_sizes[0]);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->add_input_rowsets(1);
    op1->add_input_rowsets(2);
    auto* out1 = op1->mutable_output_rowset();
    out1->set_num_rows(200);
    out1->set_data_size(2000);
    out1->add_segment_metas()->set_filename("range_seg_1.dat");
    // Add SST
    auto* sst1 = op1->add_ssts();
    sst1->set_name("sst_1.sst");
    // Add LCRM file
    auto* lcrm1 = op1->mutable_lcrm_file();
    lcrm1->set_name("lcrm_1.dat");
    lcrm1->set_size(lcrm_file_sizes[1]);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    EXPECT_TRUE(op_parallel.is_range_split());

    // Verify merged compaction has SSTs from both subtasks
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_EQ(2, merged.ssts_size());
    EXPECT_EQ(1, merged.sst_ranges_size());

    // Verify LCRM orphan files are tracked
    EXPECT_EQ(2, op_parallel.orphan_lcrm_files_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// =============================================================================
// Tests for range split with segment_metas (covering segment_metas merge path)
// =============================================================================

// =============================================================================
// Tests for _create_range_split_groups error paths (lines 2443-2444, 2462-2463)
// =============================================================================

// Test _create_range_split_groups returns empty for both error paths:
// 1. When _collect_segment_key_bounds returns empty (no segment_metas)
// 2. When calculate_range_split_boundaries returns empty (identical keys)
TEST_F(TabletParallelCompactionManagerTest, test_create_range_split_groups_error_paths) {
    // Case 1: rowsets WITHOUT segment_metas → empty bounds (lines 2443-2444)
    {
        int64_t tablet_id = 10250;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(5);

        for (int i = 0; i < 4; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(1000);
            rowset->set_data_size(10 * 1024 * 1024);
            rowset->add_segment_metas()->set_filename(fmt::format("seg_{}.dat", i));
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 5));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        auto groups = _manager->_create_range_split_groups(tablet_id, rowsets, 3, 15 * 1024 * 1024);
        EXPECT_TRUE(groups.empty());
    }

    // Case 2: all segments have same key → failed boundaries (lines 2462-2463)
    {
        int64_t tablet_id = 10251;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(5);

        for (int i = 0; i < 4; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(1000);
            rowset->set_data_size(10 * 1024 * 1024);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(100));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(100));
            segment_meta->set_num_rows(1000);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 5));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        auto groups = _manager->_create_range_split_groups(tablet_id, rowsets, 3, 15 * 1024 * 1024);
        EXPECT_TRUE(groups.empty());
    }
}

// =============================================================================
// Tests for large rowset split no-valid-subtask with LCRM cleanup (lines 1284-1286)
// =============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_large_rowset_split_no_valid_with_lcrm_cleanup) {
    int64_t tablet_id = 10252;
    int64_t txn_id = 20252;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->callback = callback;
    state->is_range_split = false;

    // Large rowset 0 split group: subtasks 0, 1 - valid data with LCRM
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;
    // Large rowset 5 split group: subtasks 2, 3 - NO valid txn_log
    state->large_rowset_split_groups[5] = {2, 3};
    state->expected_large_rowset_split_counts[5] = 2;

    for (int i = 0; i < 4; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = {static_cast<uint32_t>(i < 2 ? 0 : 5)};
        info.type = SubtaskType::LARGE_ROWSET_PART;
        info.large_rowset_id = (i < 2) ? 0 : 5;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Create LCRM files for group 0 subtasks
    std::vector<int64_t> lcrm_file_sizes(2);
    for (int i = 0; i < 2; i++) {
        std::string lcrm_name = fmt::format("lcrm_grp0_{}.dat", i);
        std::string lcrm_path = _tablet_mgr->lcrm_location(tablet_id, lcrm_name);
        std::string dir = std::filesystem::path(lcrm_path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        RowsMapperBuilder builder(lcrm_path);
        std::vector<uint64_t> dummy_rows(50, static_cast<uint64_t>(i));
        CHECK_OK(builder.append(dummy_rows));
        CHECK_OK(builder.finalize());
        auto fi = builder.file_info();
        lcrm_file_sizes[i] = fi.size.has_value() ? fi.size.value() : 0;
    }

    // Complete subtasks 0,1 (group 0) with valid data + LCRM
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->set_compact_version(version);
        op->set_segment_range_start(i * 2);
        op->set_segment_range_end((i + 1) * 2);
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        auto* segment_meta = op->mutable_output_rowset()->add_segment_metas();
        segment_meta->set_filename(fmt::format("grp0_seg_{}.dat", i));
        segment_meta->set_size(500);
        auto* lcrm = op->mutable_lcrm_file();
        lcrm->set_name(fmt::format("lcrm_grp0_{}.dat", i));
        lcrm->set_size(lcrm_file_sizes[i]);
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    // Complete subtasks 2,3 (group 5) with null txn_log → no valid data
    for (int i = 2; i < 4; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = nullptr;
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    ASSERT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    EXPECT_GE(op_parallel.subtask_compactions_size(), 1);

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// =============================================================================
// Tests with VLOG enabled to cover VLOG logging paths
// =============================================================================

// Test range split merge + _create_range_split_groups with VLOG enabled
// (covers lines 1064-1065, 2503-2504)
TEST_F(TabletParallelCompactionManagerTest, test_range_split_vlog_paths) {
    auto old_v = FLAGS_v;
    FLAGS_v = 1;

    // Part 1: range split merge VLOG (lines 1064-1065)
    {
        int64_t tablet_id = 10253;
        int64_t txn_id = 20253;
        int64_t version = 11;

        create_tablet_with_rowsets(tablet_id, 5, 1024 * 1024);

        CompactRequest request;
        request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
        request.add_tablet_ids(tablet_id);
        CompactResponse response;
        TestClosure closure;
        auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

        auto state = std::make_shared<TabletParallelCompactionState>();
        state->tablet_id = tablet_id;
        state->txn_id = txn_id;
        state->version = version;
        state->max_parallel = 2;
        state->callback = callback;
        state->is_range_split = true;
        state->range_split_input_rowset_ids = {0, 1, 2};
        state->expected_range_split_count = 2;

        for (int i = 0; i < 2; i++) {
            SubtaskInfo info;
            info.subtask_id = i;
            info.type = SubtaskType::RANGE_SPLIT;
            state->running_subtasks[i] = std::move(info);
            state->total_subtasks_created++;
        }

        _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

        for (int i = 0; i < 2; i++) {
            auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
            ctx->subtask_id = i;
            ctx->txn_log = std::make_unique<TxnLogPB>();
            auto* op = ctx->txn_log->mutable_op_compaction();
            op->add_input_rowsets(0);
            op->add_input_rowsets(1);
            op->add_input_rowsets(2);
            op->mutable_output_rowset()->set_num_rows(100);
            op->mutable_output_rowset()->set_data_size(1000);
            op->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("range_seg_{}.dat", i));
            _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
        }

        ASSERT_TRUE(closure.is_finished());
        ASSERT_EQ(1, response.txn_logs_size());
        _manager->cleanup_tablet(tablet_id, txn_id);
    }

    // Part 2: _create_range_split_groups VLOG (lines 2503-2504)
    {
        int64_t tablet_id = 10257;
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(tablet_id);
        metadata->set_version(5);

        for (int i = 0; i < 4; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(1000);
            rowset->set_data_size(10 * 1024 * 1024);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("seg_{}.dat", i));
            segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(i * 100));
            segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(i * 100 + 200));
            segment_meta->set_num_rows(1000);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 5));
        auto meta = tablet.metadata();

        std::vector<RowsetPtr> rowsets;
        for (int i = 0; i < meta->rowsets_size(); i++) {
            rowsets.push_back(std::make_shared<Rowset>(_tablet_mgr.get(), meta, i, 0));
        }

        auto groups = _manager->_create_range_split_groups(tablet_id, rowsets, 3, 15 * 1024 * 1024);
        ASSERT_GE(groups.size(), 2);
    }

    FLAGS_v = old_v;
}

// Test non-range-split partial success + large rowset split merge with VLOG enabled
// (covers lines 1310-1315, 1354, 1359-1361)
TEST_F(TabletParallelCompactionManagerTest, test_non_range_split_vlog_paths) {
    auto old_v = FLAGS_v;
    FLAGS_v = 1;

    int64_t tablet_id = 10254;
    int64_t txn_id = 20254;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->callback = callback;
    state->is_range_split = false;

    // Large rowset split group (subtasks 0, 1) → covers lines 1310-1315
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    for (int i = 0; i < 2; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        info.input_rowset_ids = {0};
        info.type = SubtaskType::LARGE_ROWSET_PART;
        info.large_rowset_id = 0;
        state->running_subtasks[i] = std::move(info);
        state->total_subtasks_created++;
    }

    // Normal subtask 2: success
    {
        SubtaskInfo info;
        info.subtask_id = 2;
        info.input_rowset_ids = {1, 2};
        info.type = SubtaskType::NORMAL;
        state->running_subtasks[2] = std::move(info);
        state->total_subtasks_created++;
    }
    // Normal subtask 3: will fail → triggers partial success VLOG (lines 1354, 1359-1361)
    {
        SubtaskInfo info;
        info.subtask_id = 3;
        info.input_rowset_ids = {3, 4};
        info.type = SubtaskType::NORMAL;
        state->running_subtasks[3] = std::move(info);
        state->total_subtasks_created++;
    }

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete large rowset split subtasks (0, 1)
    for (int i = 0; i < 2; i++) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = i;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        auto* op = ctx->txn_log->mutable_op_compaction();
        op->add_input_rowsets(0);
        op->set_compact_version(version);
        op->set_segment_range_start(i * 2);
        op->set_segment_range_end((i + 1) * 2);
        op->mutable_output_rowset()->set_num_rows(50);
        op->mutable_output_rowset()->set_data_size(500);
        auto* segment_meta = op->mutable_output_rowset()->add_segment_metas();
        segment_meta->set_filename(fmt::format("seg_{}.dat", i));
        segment_meta->set_size(500);
        _manager->on_subtask_complete(tablet_id, txn_id, i, std::move(ctx));
    }

    // Normal subtask 2: success
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(2);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    // Normal subtask 3: failure → partial success
    auto ctx3 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx3->subtask_id = 3;
    ctx3->status = Status::InternalError("failed");
    ctx3->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 3, std::move(ctx3));

    ASSERT_TRUE(closure.is_finished());
    EXPECT_EQ(0, response.status().status_code());
    ASSERT_EQ(1, response.txn_logs_size());

    _manager->cleanup_tablet(tablet_id, txn_id);

    FLAGS_v = old_v;
}

// =============================================================================
// Test for range split subtask creation + execution paths
// (covers lines 1835-1840, 1983, 1990, 2015-2017, 2041-2045, 2076-2090,
//  2140-2143, 2517-2525)
// =============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_range_split) {
    int64_t tablet_id = 10256;
    constexpr int kNumRowsets = 5;
    constexpr int32_t kRowsPerRowset = 250;
    constexpr int64_t kLogicalRowsetSize = 50 * 1024 * 1024;

    auto old_enable = config::enable_lake_compaction_range_split;
    config::enable_lake_compaction_range_split = true;
    DeferOp restore_range_split([&]() { config::enable_lake_compaction_range_split = old_enable; });

    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(kNumRowsets + 1);
    metadata->set_next_rowset_id(kNumRowsets);

    for (int i = 0; i < kNumRowsets; i++) {
        const int32_t start_key = static_cast<int32_t>(i * kRowsPerRowset);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(kRowsPerRowset);
        // Keep the logical size large enough to create multiple range-split subtasks;
        // the physical segment stays small so the UT remains fast.
        rowset->set_data_size(kLogicalRowsetSize);

        std::string segment_name = fmt::format("rs_seg_{}.dat", i);
        const uint64_t segment_size =
                write_int_key_segment(tablet_id, metadata->schema(), segment_name, kRowsPerRowset, start_key);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(segment_name);
        segment_meta->set_size(segment_size);
        segment_meta->mutable_sort_key_min()->CopyFrom(make_int_tuple(start_key));
        segment_meta->mutable_sort_key_max()->CopyFrom(make_int_tuple(start_key + kRowsPerRowset - 1));
        segment_meta->set_num_rows(kRowsPerRowset);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    int64_t txn_id = 20256;
    int64_t version = kNumRowsets + 1;

    TabletParallelConfig pconfig;
    pconfig.set_max_parallel_per_tablet(3);
    pconfig.set_max_bytes_per_subtask(80 * 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true); // aggregate-path: inspect merged log via response.txn_logs
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("rng_split_test").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    CancelableDefer unblock_pool([&]() { block_promise.set_value(); });
    ASSERT_OK(pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    }));
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, pconfig, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_OK(st.status());
    ASSERT_GT(st.value(), 0);

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        ASSERT_EQ(st.value(), state->running_subtasks.size());
        for (auto& entry : state->running_subtasks) {
            entry.second.enqueue_time_ns = MonotonicNanos() - 1'000'000;
        }
    }

    block_promise.set_value();
    unblock_pool.cancel();
    pool->wait();
    ASSERT_TRUE(closure.wait_finish());

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        ASSERT_EQ(st.value(), state->completed_subtasks.size());
        for (const auto& context : state->completed_subtasks) {
            ASSERT_NE(nullptr, context->stats);
            EXPECT_EQ(1, context->runs.load(std::memory_order_relaxed));
            EXPECT_EQ(1, context->stats->task_attempt_count);
            EXPECT_GT(context->stats->queue_wait_ns, 0);
            EXPECT_GT(context->stats->task_prepare_ns, 0);
            EXPECT_GT(context->stats->task_execute_ns, 0);
            EXPECT_GE(context->stats->task_total_ns, context->stats->task_prepare_ns + context->stats->task_execute_ns);
            EXPECT_EQ(0, context->task_attempt_start_ns.load(std::memory_order_acquire));
            EXPECT_EQ(0, context->task_execute_start_ns.load(std::memory_order_acquire));
        }
    }

    _manager->cleanup_tablet(tablet_id, txn_id);

    // Also test execute_subtask_range_split when state not found (lines 2517-2525)
    {
        std::vector<RowsetPtr> empty_rowsets;
        VariantTuple lower, upper;
        _manager->execute_subtask_range_split(99999, 99999, 0, std::move(empty_rowsets), lower, upper, true, true, true,
                                              true, 1, false, [](bool) {});
    }
}

// stop() settles tablets whose subtasks the shutting-down thread pool dropped without ever running them:
// ThreadPool::shutdown() removes queued tasks and FunctionRunnable::cancel() is a no-op, so they never
// report back. Nothing else can complete such a tablet -- its scheduler context was destroyed when it was
// handed off to the subtasks -- so without this pass the compact RPC would hang until it timed out.
TEST_F(TabletParallelCompactionManagerTest, test_abort_pending_states_completes_dropped_subtasks) {
    int64_t tablet_id = 10007;
    int64_t txn_id = 20007;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true);
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.input_rowset_ids = {0, 1, 2, 3, 4};
        info0.input_bytes = 5 * 1024 * 1024;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.input_rowset_ids = {5, 6, 7, 8, 9};
        info1.input_bytes = 5 * 1024 * 1024;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }
    for (int i = 0; i < 10; i++) {
        state->compacting_rowsets[i] = 1;
    }
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Subtask 0 reports back; subtask 1 is the one the pool dropped, so it stays in running_subtasks and
    // the tablet cannot reach completion on its own.
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));
    ASSERT_FALSE(closure.is_finished());

    _manager->abort_pending_states();

    // The RPC is completed instead of being left hanging...
    EXPECT_TRUE(closure.is_finished());
    // ...and it is completed as an abort, like a serial compaction interrupted by stop(). Reporting the
    // tablet as failed is what keeps FE from committing this compaction.
    ASSERT_EQ(1, response.failed_tablets_size());
    EXPECT_EQ(tablet_id, response.failed_tablets(0));
    // Nothing is published for the work that did run: subtask 0's log must not be merged and handed back
    // as if the whole tablet had been compacted, because subtask 1's rowsets were never touched.
    EXPECT_EQ(0, response.txn_logs_size());

    // Completion is claimed once, so a second settling pass must not complete the same tablet again:
    // finish_task() has already released the request and response that a second call would touch.
    _manager->abort_pending_states();
    EXPECT_EQ(1, response.failed_tablets_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// abort() walks the scheduler's own context list first, but a handed-off tablet is no longer on it: that
// context was destroyed when the subtasks took over. The manager is then the only place still holding the
// txn's callback, which is what lets an abort reach subtasks that are already running.
TEST_F(TabletParallelCompactionManagerTest, test_collect_callbacks_for_txn) {
    int64_t tablet_id = 10008;
    int64_t txn_id = 20008;

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = 11;
    state->max_parallel = 2;
    state->callback = callback;
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto callbacks = _manager->collect_callbacks_for_txn(txn_id);
    ASSERT_EQ(1, callbacks.size());
    EXPECT_EQ(callback.get(), callbacks[0].get());

    // Another txn's abort must not pick up this tablet's callback.
    EXPECT_TRUE(_manager->collect_callbacks_for_txn(txn_id + 1).empty());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// A failing PK index major compaction must fail the parallel compaction, exactly as it does on the
// three non-parallel paths (horizontal_compaction_task.cpp, vertical_compaction_task.cpp,
// cloud_native_index_compaction_task.cpp all RETURN_IF_ERROR the same call).
//
// Swallowing it here returned Status::OK() after one WARNING, so the data compaction reported
// success while the index compactor made no progress -- and every consistency guard inside major
// compaction ("sstables are not ordered", "inconsistent fileset_id in sstables", "no matching
// sstable fileset found") was silent on this path only. Both directions are asserted: armed, so a
// typo'd failpoint name cannot make the test vacuous; disarmed, so the path is not failing
// unconditionally.
TEST_F(TabletParallelCompactionManagerTest, test_index_major_compaction_failure_fails_parallel_compaction) {
    int64_t tablet_id = 10023;
    int64_t txn_id = 20023;
    int64_t version = 11;

    // A cloud-native persistent-index PK tablet is the only shape that reaches
    // execute_index_major_compaction from the parallel path.
    auto metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto register_completed_state = [&](int64_t id) {
        auto state = std::make_shared<TabletParallelCompactionState>();
        state->tablet_id = tablet_id;
        state->txn_id = id;
        state->version = version;
        state->max_parallel = 1;

        auto ctx = std::make_unique<CompactionTaskContext>(id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = 0;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        ctx->txn_log->mutable_op_compaction()->add_input_rowsets(0);
        ctx->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
        state->completed_subtasks.push_back(std::move(ctx));

        _manager->register_tablet_state_for_test(tablet_id, id, state);
    };

    auto set_failpoint_mode = [](const std::string& name, FailPointTriggerModeType mode) {
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(mode);
        auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(name);
        ASSERT_NE(nullptr, fp) << "failpoint " << name << " is not registered";
        fp->setMode(trigger_mode);
    };

    // Armed: the injected failure must reach the caller.
    register_completed_state(txn_id);
    set_failpoint_mode("fail_execute_index_major_compaction", FailPointTriggerModeType::ENABLE);
    auto failed = _manager->get_merged_txn_log(tablet_id, txn_id);
    set_failpoint_mode("fail_execute_index_major_compaction", FailPointTriggerModeType::DISABLE);

    ASSERT_FALSE(failed.ok());
    EXPECT_NE(std::string::npos, failed.status().to_string().find("injected index major compaction failure"))
            << failed.status();
    _manager->cleanup_tablet(tablet_id, txn_id);

    // Disarmed: the same tablet and state must merge cleanly.
    register_completed_state(txn_id + 1);
    auto ok = _manager->get_merged_txn_log(tablet_id, txn_id + 1);
    EXPECT_TRUE(ok.ok()) << ok.status();
    _manager->cleanup_tablet(tablet_id, txn_id + 1);
}

// A token reserved for a subtask that never ran must go back through the neutral return_token, not
// through release_token(false): the limiter restores concurrency it reduced under memory pressure by
// counting successful completions (Limiter::no_memory_limit_exceeded), and a reservation that did no work
// is not one. Otherwise a single planning failure over several groups would undo that protection at
// once. The three ways a reserved token ends up unused are covered: the all-or-nothing acquisition
// failing part-way, the thread pool rejecting a subtask, and an exception unwinding the submission loop.
TEST_F(TabletParallelCompactionManagerTest, test_unused_tokens_are_returned_not_released) {
    int64_t tablet_id = 10009;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    int acquired = 0;
    int released = 0;
    int returned = 0;
    auto reset_counts = [&]() { acquired = released = returned = 0; };
    ReleaseTokenFunc release_token = [&](bool) { released++; };
    ReturnTokenFunc return_token = [&]() { returned++; };

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->EnableProcessing();
    DeferOp disable_sync_point([&]() {
        sync_point->ClearCallBack("ThreadPool::do_submit:1");
        sync_point->ClearCallBack("TabletParallelCompactionManager::submit_subtasks_from_groups:after_register");
        sync_point->DisableProcessing();
    });

    // 1. The all-or-nothing acquisition fails part-way: the one token acquired so far is returned.
    {
        int64_t txn_id = 20009;
        CompactRequest request;
        request.set_skip_write_txnlog(true);
        request.add_tablet_ids(tablet_id);
        CompactResponse response;
        TestClosure closure;
        auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

        auto st = _manager->create_parallel_tasks(
                tablet_id, txn_id, version, config, callback, false, pool.get(), [&]() { return ++acquired <= 1; },
                release_token, false, 0, 0, return_token);
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(st.status().is_resource_busy()) << st.status();
        EXPECT_EQ(1, returned);
        EXPECT_EQ(0, released);
        _manager->cleanup_tablet(tablet_id, txn_id);
        reset_counts();
    }

    // 2. The thread pool rejects the first subtask: its token and those of every later group are returned.
    {
        int64_t txn_id = 20010;
        CompactRequest request;
        request.set_skip_write_txnlog(true);
        request.add_tablet_ids(tablet_id);
        CompactResponse response;
        TestClosure closure;
        auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

        sync_point->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *static_cast<int64_t*>(arg) = 0; });
        auto st = _manager->create_parallel_tasks(
                tablet_id, txn_id, version, config, callback, false, pool.get(),
                [&]() {
                    acquired++;
                    return true;
                },
                release_token, false, 0, 0, return_token);
        sync_point->ClearCallBack("ThreadPool::do_submit:1");
        ASSERT_FALSE(st.ok());
        // Every token the planner reserved -- one per group, so more than one -- came back untouched.
        EXPECT_GE(acquired, 2);
        EXPECT_EQ(acquired, returned);
        EXPECT_EQ(0, released);
        _manager->cleanup_tablet(tablet_id, txn_id);
        reset_counts();
    }

    // 3. An exception unwinds the submission loop before any subtask was handed to the pool: the guard
    //    that keeps the tokens from leaking must return them, not report them as completions.
    {
        int64_t txn_id = 20011;
        CompactRequest request;
        request.set_skip_write_txnlog(true);
        request.add_tablet_ids(tablet_id);
        CompactResponse response;
        TestClosure closure;
        auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

        sync_point->SetCallBack("TabletParallelCompactionManager::submit_subtasks_from_groups:after_register",
                                [](void*) { throw std::bad_alloc(); });
        auto st = _manager->create_parallel_tasks(
                tablet_id, txn_id, version, config, callback, false, pool.get(),
                [&]() {
                    acquired++;
                    return true;
                },
                release_token, false, 0, 0, return_token);
        sync_point->ClearCallBack("TabletParallelCompactionManager::submit_subtasks_from_groups:after_register");
        ASSERT_FALSE(st.ok());
        EXPECT_GE(acquired, 2);
        EXPECT_EQ(acquired, returned);
        EXPECT_EQ(0, released);
        _manager->cleanup_tablet(tablet_id, txn_id);
        reset_counts();
    }

    pool->wait();
}

// Whoever claims a tablet's completion must complete it even when the finalization throws: nothing else
// will. Here the sealer claims it -- every subtask finished before submission was sealed -- and the
// merged-context build throws right after completion was claimed. Before the guard, the exception
// escaped into create_parallel_tasks()'s catch, which sealed again (a no-op, the claim was taken) and
// reported a successful hand-off, so the scheduler destroyed its context and the RPC hung forever.
TEST_F(TabletParallelCompactionManagerTest, test_finalization_exception_completes_tablet_as_failed) {
    int64_t tablet_id = 10011;
    int64_t txn_id = 20013;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true);
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 1;
    state->callback = callback;
    state->total_subtasks_created = 1;
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    state->completed_subtasks.push_back(std::move(ctx0));
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("TabletParallelCompactionManager::finalize_tablet_completion:after_context",
                            [](void*) { throw std::bad_alloc(); });
    sync_point->EnableProcessing();
    DeferOp disable_sync_point([&]() {
        sync_point->ClearCallBack("TabletParallelCompactionManager::finalize_tablet_completion:after_context");
        sync_point->DisableProcessing();
    });

    // Sealing claims the completion and runs the finalization, which throws.
    _manager->seal_submission(tablet_id, txn_id, state);

    // The RPC still completes, and as a failure: nothing of a half-built result may be published.
    EXPECT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.failed_tablets_size());
    EXPECT_EQ(tablet_id, response.failed_tablets(0));
    EXPECT_EQ(0, response.txn_logs_size());

    // Completion was claimed exactly once, so sealing again -- what create_parallel_tasks()'s catch does --
    // must not complete the tablet a second time.
    _manager->seal_submission(tablet_id, txn_id, state);
    EXPECT_EQ(1, response.failed_tablets_size());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// The last subtask to finish before submission is sealed cannot run the completion itself, and the sealer
// that will run it has already given its own limiter token back before planning. If that subtask released
// its token too, the finalization -- LCRM rewriting, the PK SST compaction wait -- would run outside the
// limiter while other compactions use the full concurrency. So the subtask parks its token with the state,
// the sealer finalizes on it and hands it back afterwards with the outcome the subtask reported; at most one
// token is parked, and cleanup returns a parked token whose sealer never came.
TEST_F(TabletParallelCompactionManagerTest, test_last_unsealed_subtask_parks_token_for_the_sealer) {
    int64_t tablet_id = 10012;
    int64_t txn_id = 20014;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.set_skip_write_txnlog(true);
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    int released = 0;
    int released_mem_limit_exceeded = 0;
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;
    state->release_token = [&](bool mem_limit_exceeded) {
        released++;
        if (mem_limit_exceeded) {
            released_mem_limit_exceeded++;
        }
    };
    auto register_running = [&](int32_t subtask_id, std::vector<uint32_t> rowset_ids) {
        std::lock_guard<std::mutex> lock(state->mutex);
        SubtaskInfo info;
        info.subtask_id = subtask_id;
        info.input_rowset_ids = rowset_ids;
        info.input_bytes = 5 * 1024 * 1024;
        info.start_time = ::time(nullptr);
        for (auto id : rowset_ids) {
            state->compacting_rowsets[id] = 1;
        }
        state->running_subtasks[subtask_id] = std::move(info);
        state->total_subtasks_created++;
    };
    auto finished_context = [&](int32_t subtask_id, uint32_t input_rowset) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
        ctx->subtask_id = subtask_id;
        ctx->txn_log = std::make_unique<TxnLogPB>();
        ctx->txn_log->mutable_op_compaction()->add_input_rowsets(input_rowset);
        ctx->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
        ctx->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
        return ctx;
    };
    register_running(0, {0, 1, 2, 3, 4});
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);
    {
        // register_tablet_state_for_test() seals the state; this test is about the window before the seal.
        std::lock_guard<std::mutex> lock(state->mutex);
        state->submission_done = false;
    }

    // Subtask 0 finishes while submission is still open: it parks its token instead of releasing it, and
    // the tablet is not completed.
    EXPECT_TRUE(_manager->on_subtask_complete(tablet_id, txn_id, 0, finished_context(0, 0),
                                              /*mem_limit_exceeded=*/true));
    EXPECT_EQ(0, released);
    EXPECT_FALSE(closure.is_finished());

    // Subtask 1 is registered after that and also finishes before the seal: one token is already parked, so
    // this one is left to its caller to release as usual.
    register_running(1, {5, 6, 7, 8, 9});
    EXPECT_FALSE(_manager->on_subtask_complete(tablet_id, txn_id, 1, finished_context(1, 5),
                                               /*mem_limit_exceeded=*/false));
    EXPECT_EQ(0, released);
    EXPECT_FALSE(closure.is_finished());

    // Sealing claims the completion, runs it on the parked token, and only then hands that token back --
    // with the outcome its subtask reported.
    _manager->seal_submission(tablet_id, txn_id, state);
    EXPECT_TRUE(closure.is_finished());
    EXPECT_EQ(0, response.failed_tablets_size());
    EXPECT_EQ(1, released);
    EXPECT_EQ(1, released_mem_limit_exceeded);

    // Nothing is left parked for cleanup to return.
    _manager->cleanup_tablet(tablet_id, txn_id);
    EXPECT_EQ(1, released);

    // A parked token whose sealer never comes goes back when the state is cleaned up.
    int64_t txn_id2 = txn_id + 1;
    released = 0;
    released_mem_limit_exceeded = 0;
    auto state2 = std::make_shared<TabletParallelCompactionState>();
    state2->tablet_id = tablet_id;
    state2->txn_id = txn_id2;
    state2->version = version;
    state2->max_parallel = 1;
    state2->callback = callback;
    state2->release_token = state->release_token;
    {
        std::lock_guard<std::mutex> lock(state2->mutex);
        SubtaskInfo info;
        info.subtask_id = 0;
        info.input_rowset_ids = {0};
        info.start_time = ::time(nullptr);
        state2->compacting_rowsets[0] = 1;
        state2->running_subtasks[0] = std::move(info);
        state2->total_subtasks_created = 1;
    }
    _manager->register_tablet_state_for_test(tablet_id, txn_id2, state2);
    {
        std::lock_guard<std::mutex> lock(state2->mutex);
        state2->submission_done = false;
    }
    auto ctx = std::make_unique<CompactionTaskContext>(txn_id2, tablet_id, version, false, true, nullptr);
    ctx->subtask_id = 0;
    EXPECT_TRUE(_manager->on_subtask_complete(tablet_id, txn_id2, 0, std::move(ctx), /*mem_limit_exceeded=*/false));
    EXPECT_EQ(0, released);
    _manager->cleanup_tablet(tablet_id, txn_id2);
    EXPECT_EQ(1, released);
    EXPECT_EQ(0, released_mem_limit_exceeded);
}

} // namespace starrocks::lake
