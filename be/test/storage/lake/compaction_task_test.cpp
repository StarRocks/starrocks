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

#include <atomic>
#include <memory>
#include <random>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/config_compaction_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/logging.h"
#include "fs/fs_factory.h"
#include "fs/fs_memory.h"
#include "io/seekable_input_stream.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_test_utils.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/init_test_env.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeCompactionTest : public TestBase, testing::WithParamInterface<CompactionParam> {
public:
    LakeCompactionTest(std::string test_dir) : TestBase(test_dir) {}

    void check_task(CompactionTaskPtr& task) {
        if (GetParam().algorithm == HORIZONTAL_COMPACTION) {
            ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(task.get()) != nullptr);
        } else {
            ASSERT_EQ(GetParam().algorithm, VERTICAL_COMPACTION);
            ASSERT_TRUE(dynamic_cast<VerticalCompactionTask*>(task.get()) != nullptr);
        }
    }

protected:
    void SetUp() override {
        config::enable_size_tiered_compaction_strategy = GetParam().enable_size_tiered_compaction_strategy;
        config::vertical_compaction_max_columns_per_group = GetParam().vertical_compaction_max_columns_per_group;
        config::min_cumulative_compaction_num_singleton_deltas = 1;
        clear_and_init_test_dir();
    }

    void TearDown() override {
        remove_test_dir_ignore_error();
        config::enable_size_tiered_compaction_strategy = _enable_size_tiered_compaction_strategy;
        config::vertical_compaction_max_columns_per_group = _vertical_compaction_max_columns_per_group;
        config::min_cumulative_compaction_num_singleton_deltas = _min_cumulative_compaction_num_singleton_deltas;
    }

    RuntimeProfile _dummy_runtime_profile{"dummy"};

private:
    bool _enable_size_tiered_compaction_strategy = config::enable_size_tiered_compaction_strategy;
    int64_t _vertical_compaction_max_columns_per_group = config::vertical_compaction_max_columns_per_group;
    int64_t _min_cumulative_compaction_num_singleton_deltas = config::min_cumulative_compaction_num_singleton_deltas;
};

class LakeDuplicateKeyCompactionTest : public LakeCompactionTest {
public:
    LakeDuplicateKeyCompactionTest() : LakeCompactionTest(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_task";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        LakeCompactionTest::SetUp();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakeDuplicateKeyCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().enable_size_tiered_compaction_strategy) {
        ASSERT_EQ(0, new_tablet_metadata->cumulative_point());
    } else {
        ASSERT_EQ(1, new_tablet_metadata->cumulative_point());
    }
    ASSERT_EQ(1, new_tablet_metadata->rowsets_size());
}

// Pins the cache-fill flags that reach SegmentReadOptions from the compaction read path:
//  - fill_data_cache must follow the per-algorithm config, not a hardcoded value;
//  - with hold_segments on (the default), neither algorithm may fill the shared metadata cache:
//    the task reuses the Segment objects held on the Rowset instance, and its inputs are deleted
//    right after compaction, so filling would only evict neighbors' entries;
//  - with the hold_segments kill switch off, horizontal compaction must keep fill_metadata_cache
//    on, so the read pass reuses the segments already opened by calculate_chunk_size() instead of
//    re-reading every footer (covered by test_compaction_read_cache_options_no_hold below).
// The two data-cache configs are deliberately set to opposite values so that a hardcoded flag on
// either side would fail here rather than accidentally match the expectation.
TEST_P(LakeDuplicateKeyCompactionTest, test_compaction_read_cache_options) {
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto write_txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(write_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, write_txn_id).status());
        version++;
    }

    const bool horizontal = GetParam().algorithm == HORIZONTAL_COMPACTION;
    const bool saved_horizontal_fill = config::lake_enable_horizontal_compaction_fill_data_cache;
    const bool saved_vertical_fill = config::lake_enable_vertical_compaction_fill_data_cache;
    const bool saved_hold = config::lake_compaction_hold_input_segments;
    config::lake_enable_horizontal_compaction_fill_data_cache = true;
    config::lake_enable_vertical_compaction_fill_data_cache = false;
    config::lake_compaction_hold_input_segments = true;
    DeferOp restore_config([&]() {
        config::lake_enable_horizontal_compaction_fill_data_cache = saved_horizontal_fill;
        config::lake_enable_vertical_compaction_fill_data_cache = saved_vertical_fill;
        config::lake_compaction_hold_input_segments = saved_hold;
    });

    bool seen = false;
    bool fill_data_cache = !horizontal;
    bool fill_metadata_cache = true;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::read::seg_options", [&](void* arg) {
        auto* seg_options = static_cast<SegmentReadOptions*>(arg);
        seen = true;
        fill_data_cache = seg_options->lake_io_opts.fill_data_cache;
        fill_metadata_cache = seg_options->lake_io_opts.fill_metadata_cache;
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::read::seg_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    ASSERT_TRUE(seen);
    EXPECT_EQ(horizontal, fill_data_cache);
    EXPECT_FALSE(fill_metadata_cache);
}

// The kill-switch leg of the contract pinned above: with hold_segments off, the task depends on
// the shared metadata cache again, so horizontal compaction must fill it (cross-phase reuse) and
// vertical must not (its reader path never did).
TEST_P(LakeDuplicateKeyCompactionTest, test_compaction_read_cache_options_no_hold) {
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto write_txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(write_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, write_txn_id).status());
        version++;
    }

    const bool horizontal = GetParam().algorithm == HORIZONTAL_COMPACTION;
    const bool saved_horizontal_fill = config::lake_enable_horizontal_compaction_fill_data_cache;
    const bool saved_vertical_fill = config::lake_enable_vertical_compaction_fill_data_cache;
    const bool saved_hold = config::lake_compaction_hold_input_segments;
    config::lake_enable_horizontal_compaction_fill_data_cache = true;
    config::lake_enable_vertical_compaction_fill_data_cache = false;
    config::lake_compaction_hold_input_segments = false;
    DeferOp restore_config([&]() {
        config::lake_enable_horizontal_compaction_fill_data_cache = saved_horizontal_fill;
        config::lake_enable_vertical_compaction_fill_data_cache = saved_vertical_fill;
        config::lake_compaction_hold_input_segments = saved_hold;
    });

    bool seen = false;
    bool fill_data_cache = !horizontal;
    bool fill_metadata_cache = !horizontal;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::read::seg_options", [&](void* arg) {
        auto* seg_options = static_cast<SegmentReadOptions*>(arg);
        seen = true;
        fill_data_cache = seg_options->lake_io_opts.fill_data_cache;
        fill_metadata_cache = seg_options->lake_io_opts.fill_metadata_cache;
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::read::seg_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    ASSERT_TRUE(seen);
    EXPECT_EQ(horizontal, fill_data_cache);
    EXPECT_EQ(horizontal, fill_metadata_cache);
}

TEST_P(LakeDuplicateKeyCompactionTest, test_skip_write_txnlog) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    EXPECT_TRUE(task_context->skip_write_txnlog);
    EXPECT_TRUE(task_context->txn_log != nullptr);
}

INSTANTIATE_TEST_SUITE_P(LakeDuplicateKeyCompactionTest, LakeDuplicateKeyCompactionTest,
                         ::testing::Values(CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = false},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = false}),
                         to_string_param_name);

TEST_P(LakeDuplicateKeyCompactionTest, test_empty_tablet) {
    auto version = 1;
    ASSERT_EQ(0, read(version));

    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(0, read(version));
}

// Regression test for SIGFPE in compaction when rowset metadata reports
// num_rows=0 but the underlying physical segments still contain readable
// rows. This simulates the state after an online tablet split where sub-tablet
// metadata inherits zeroed stats while the shared physical segments remain
// populated. Before the fix, the divide-by-zero in progress.update triggered
// a crash; after the fix, the guards skip the progress update and compaction
// completes normally.
TEST_P(LakeDuplicateKeyCompactionTest, test_zero_num_rows_no_crash) {
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    // Simulate post-split metadata inconsistency: zero the per-rowset stats
    // while leaving the physical segments intact.
    {
        ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto mutated = std::make_shared<TabletMetadata>(*tablet_metadata);
        for (auto& rowset : *mutated->mutable_rowsets()) {
            rowset.set_num_rows(0);
            rowset.set_data_size(0);
        }
        mutated->set_version(version + 1);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*mutated));
        version++;
    }

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
}

// Exercise the parallel prefill with three INT columns and an ARRAY column:
// INT-only passes coalesce onto the segment-wide
// stream and become resident, the ARRAY pass stays off it (semi-typed columns keep their plain file)
// and takes the full read on the pool with a read-ahead pump, and a horizontal task mixes both in
// one pass. Rows, sums and array contents must match the serial result either way.
class LakeWideDuplicateKeyCompactionTest : public LakeCompactionTest {
public:
    LakeWideDuplicateKeyCompactionTest() : LakeCompactionTest(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        auto* schema = _tablet_metadata->mutable_schema();
        auto* c2 = schema->add_column();
        c2->set_unique_id(next_id());
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_nullable(false);
        c2->set_is_key(false);
        c2->set_aggregation("NONE");
        auto* c3 = schema->add_column();
        c3->set_unique_id(next_id());
        c3->set_name("c3");
        c3->set_type("ARRAY");
        c3->set_is_nullable(true);
        c3->set_is_key(false);
        c3->set_aggregation("NONE");
        auto* element = c3->add_children_columns();
        element->set_unique_id(next_id());
        element->set_name("element");
        element->set_type("INT");
        element->set_is_nullable(true);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_wide_compaction_task";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        LakeCompactionTest::SetUp();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    // c0 = a permutation of [0, n), c1 = 3 * c0, c2 = 5 * c0, c3 = [c0, c0 + 1].
    Chunk generate_data(int n) {
        std::vector<int> v0(n);
        for (int i = 0; i < n; i++) {
            v0[i] = i;
        }
        std::shuffle(v0.begin(), v0.end(), std::default_random_engine{});
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto elements = Int32Column::create();
        auto element_nulls = NullColumn::create();
        auto offsets = UInt32Column::create();
        auto array_nulls = NullColumn::create();
        offsets->append(0);
        for (int i = 0; i < n; i++) {
            c0->append(v0[i]);
            c1->append(v0[i] * 3);
            c2->append(v0[i] * 5);
            elements->append(v0[i]);
            elements->append(v0[i] + 1);
            element_nulls->append(0);
            element_nulls->append(0);
            offsets->append((i + 1) * 2);
            array_nulls->append(0);
        }
        auto array = ArrayColumn::create(NullableColumn::create(std::move(elements), std::move(element_nulls)),
                                         std::move(offsets));
        auto c3 = NullableColumn::create(std::move(array), std::move(array_nulls));
        return Chunk({std::move(c0), std::move(c1), std::move(c2), std::move(c3)}, _schema);
    }

    struct Totals {
        int64_t rows = 0;
        int64_t c1 = 0;
        int64_t c2 = 0;
        int64_t elements = 0;
        int64_t element_sum = 0;
    };

    Totals read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        Totals t;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                t.rows++;
                t.c1 += chunk->get_column_by_index(1)->get(i).get_int32();
                t.c2 += chunk->get_column_by_index(2)->get(i).get_int32();
                const auto array_datum = chunk->get_column_by_index(3)->get(i);
                for (const auto& e : array_datum.get_array()) {
                    t.elements++;
                    t.element_sum += e.get_int32();
                }
            }
            chunk->reset();
        }
        return t;
    }

    // Three identical rowsets of kChunkSize rows: c0 sums to s per rowset.
    void expect_totals(const Totals& t) {
        const int64_t s = static_cast<int64_t>(kChunkSize) * (kChunkSize - 1) / 2;
        EXPECT_EQ(3 * kChunkSize, t.rows);
        EXPECT_EQ(3 * 3 * s, t.c1);
        EXPECT_EQ(3 * 5 * s, t.c2);
        EXPECT_EQ(3 * 2 * kChunkSize, t.elements);
        EXPECT_EQ(3 * (2 * s + kChunkSize), t.element_sum);
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakeWideDuplicateKeyCompactionTest, test_parallel_prefill_and_coalesced_read) {
    const bool saved_prefill = config::enable_compaction_parallel_merge_init;
    const int32_t saved_buffers = config::compaction_merge_child_buffers;
    config::enable_compaction_parallel_merge_init = true;
    config::compaction_merge_child_buffers = 2;
    DeferOp restore_config([&]() {
        config::enable_compaction_parallel_merge_init = saved_prefill;
        config::compaction_merge_child_buffers = saved_buffers;
    });

    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    expect_totals(read(version));

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    version++;
    expect_totals(read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSERT_EQ(1, new_tablet_metadata->rowsets_size());
}

INSTANTIATE_TEST_SUITE_P(LakeWideDuplicateKeyCompactionTest, LakeWideDuplicateKeyCompactionTest,
                         ::testing::Values(CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = true}),
                         to_string_param_name);

// Counts every read that reaches the file, so a test can tell which step of a read issued IO.
class ReadCountingStream final : public io::SeekableInputStreamWrapper {
public:
    ReadCountingStream(std::shared_ptr<io::SeekableInputStream> stream, std::atomic<int64_t>* reads)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _reads(reads) {}

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        _reads->fetch_add(1);
        return _stream->read_at(offset, out, count);
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        _reads->fetch_add(1);
        return _stream->read_at_fully(offset, out, count);
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    std::atomic<int64_t>* _reads;
};

class ReadCountingMemoryFileSystem final : public MemoryFileSystem {
public:
    explicit ReadCountingMemoryFileSystem(std::atomic<int64_t>* reads) : _reads(reads) {}

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ASSIGN_OR_RETURN(auto file, MemoryFileSystem::new_random_access_file(opts, url));
        auto stream = std::make_shared<ReadCountingStream>(file->stream(), _reads);
        return std::make_unique<RandomAccessFile>(std::move(stream), url);
    }

private:
    std::atomic<int64_t>* _reads;
};

class LakeCompactionPrefetchTest : public TestBase {
public:
    LakeCompactionPrefetchTest() : TestBase(kTestDirectory) {
        // c0 INT key, c1 VARCHAR value: VARCHAR is dictionary-encoded by default.
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        auto* c1 = _tablet_metadata->mutable_schema()->mutable_column(1);
        c1->set_type("VARCHAR");
        c1->set_length(64);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_prefetch";
    constexpr static const int kNumRows = 4096;
    constexpr static const int kNumWords = 16;

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    static std::string word(int row) { return "word_" + std::to_string(row % kNumWords); }

    // Writes one segment and reopens it from a file system that counts the reads reaching the file.
    void open_counted_segment() {
        auto c0 = Int32Column::create();
        auto c1 = BinaryColumn::create();
        for (int i = 0; i < kNumRows; i++) {
            c0->append(i);
            c1->append(Slice(word(i)));
        }
        Chunk chunk({std::move(c0), std::move(c1)}, _schema);

        VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
        auto files = writer->segments();
        ASSERT_EQ(1, files.size());
        writer->close();

        ASSIGN_OR_ABORT(auto local_fs, FileSystemFactory::CreateSharedFromString(kTestDirectory));
        ASSIGN_OR_ABORT(auto source, local_fs->new_random_access_file(
                                             _tablet_mgr->segment_location(_tablet_metadata->id(), files[0].path)));
        ASSIGN_OR_ABORT(auto content, source->read_all());
        auto fs = std::make_shared<ReadCountingMemoryFileSystem>(&_reads);
        const std::string segment_path = "/test_lake_compaction_prefetch/segment.dat";
        ASSERT_OK(fs->create_dir_recursive("/test_lake_compaction_prefetch"));
        ASSERT_OK(fs->append_file(segment_path, content));
        _fs = fs;

        FileInfo file_info;
        file_info.path = segment_path;
        file_info.size = static_cast<int64_t>(content.size());
        ASSIGN_OR_ABORT(_segment, Segment::open(_fs, file_info, 0, _tablet_schema, nullptr, nullptr, LakeIOOptions{},
                                                _tablet_mgr.get()));
        const ColumnReader* c1_reader = _segment->column(1);
        ASSERT_TRUE(c1_reader != nullptr);
        ASSERT_EQ(DICT_ENCODING, c1_reader->encoding_info()->encoding());
        ASSERT_GT(c1_reader->get_dict_page_pointer().size, 0U);
    }

    // A vertical compaction value group: the VARCHAR column alone, without the key column in front.
    SegmentReadOptions read_options() {
        SegmentReadOptions opts;
        opts.fs = _fs;
        opts.tablet_id = _tablet_metadata->id();
        opts.stats = &_statistics;
        opts.chunk_size = kNumRows;
        opts.reader_type = READER_CUMULATIVE_COMPACTION;
        opts.lake_io_opts.fill_data_cache = false;
        return opts;
    }

    void expect_words(const Chunk& chunk) {
        ASSERT_EQ(kNumRows, chunk.num_rows());
        for (int i = 0; i < kNumRows; i++) {
            EXPECT_EQ(word(i), chunk.get_column_by_index(0)->get(i).get_slice().to_string());
        }
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::atomic<int64_t> _reads{0};
    std::shared_ptr<FileSystem> _fs;
    SegmentSharedPtr _segment;
    OlapReaderStatistics _statistics;
};

// A vertical compaction value group reads a column without its neighbours, so nothing registered
// in front of a dictionary-encoded column happens to cover its dictionary page. prefetch() returning
// true promises that the following read needs no IO; the dictionary page must be part of that.
TEST_F(LakeCompactionPrefetchTest, test_prefetch_covers_dict_page) {
    open_counted_segment();
    ASSERT_FALSE(HasFatalFailure());

    auto opts = read_options();
    opts.lake_io_opts.coalesce_across_columns = true;
    Schema value_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{1});
    ASSIGN_OR_ABORT(auto iter, _segment->new_iterator(value_schema, opts));
    std::atomic<int64_t> budget{64 * 1024 * 1024};
    ASSIGN_OR_ABORT(bool covered, iter->prefetch(&budget));
    ASSERT_TRUE(covered);

    const int64_t reads_after_prefetch = _reads.load();
    auto read_chunk = ChunkFactory::new_chunk(value_schema, kNumRows);
    ASSERT_OK(iter->get_next(read_chunk.get()));
    EXPECT_EQ(reads_after_prefetch, _reads.load());
    expect_words(*read_chunk);
    iter->close();
}

// A per-column stream registers data pages only, so with the dictionary page still to be loaded the
// input must not report itself covered: its first read then stays off the merge thread.
TEST_F(LakeCompactionPrefetchTest, test_per_column_stream_leaves_dict_page_uncovered) {
    const bool saved_coalesce = config::io_coalesce_lake_read_enable;
    config::io_coalesce_lake_read_enable = true;
    DeferOp restore_config([&]() { config::io_coalesce_lake_read_enable = saved_coalesce; });

    open_counted_segment();
    ASSERT_FALSE(HasFatalFailure());

    auto opts = read_options();
    Schema value_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<ColumnId>{1});
    ASSIGN_OR_ABORT(auto iter, _segment->new_iterator(value_schema, opts));
    std::atomic<int64_t> budget{64 * 1024 * 1024};
    ASSIGN_OR_ABORT(bool covered, iter->prefetch(&budget));
    EXPECT_FALSE(covered);

    auto read_chunk = ChunkFactory::new_chunk(value_schema, kNumRows);
    ASSERT_OK(iter->get_next(read_chunk.get()));
    expect_words(*read_chunk);
    iter->close();
}

class LakeDuplicateKeyOverlapSegmentsCompactionTest : public LakeCompactionTest {
public:
    LakeDuplicateKeyOverlapSegmentsCompactionTest() : LakeCompactionTest(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_task_duplicate_overlap_segments";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        LakeCompactionTest::SetUp();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakeDuplicateKeyOverlapSegmentsCompactionTest, test) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        for (int j = 0; j < i + 1; ++j) {
            ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->flush());
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 6, read(version));

    // Cancelled compaction task
    {
        auto txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        auto st = task->execute(CompactionTask::kCancelledFn);
        EXPECT_EQ(0, task_context->progress.value());
        EXPECT_TRUE(st.is_aborted()) << st;
    }
    // Completed compaction task without error
    {
        auto txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
        version++;
        ASSERT_EQ(kChunkSize * 6, read(version));
    }

    // check metadata
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().enable_size_tiered_compaction_strategy) {
        ASSERT_EQ(0, new_tablet_metadata->cumulative_point());
    } else {
        ASSERT_EQ(1, new_tablet_metadata->cumulative_point());
    }
    ASSERT_EQ(1, new_tablet_metadata->rowsets_size());
    ASSERT_EQ(1, new_tablet_metadata->rowsets(0).segment_metas_size());

    // check data
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));
    auto chunk = ChunkFactory::new_chunk(*_schema, 128);
    auto st = reader->get_next(chunk.get());
    ASSERT_FALSE(st.is_end_of_file());
    ASSERT_EQ(kChunkSize * 6, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); ++i) {
        auto row = chunk->get(i);
        ASSERT_EQ(i / 6, row.get(0).get_int32());
        ASSERT_EQ(i / 6 * 3, row.get(1).get_int32());
    }
    chunk->reset();
    st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

INSTANTIATE_TEST_SUITE_P(LakeDuplicateKeyOverlapSegmentsCompactionTest, LakeDuplicateKeyOverlapSegmentsCompactionTest,
                         ::testing::Values(CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = false},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = false}),
                         to_string_param_name);

class LakeUniqueKeyCompactionTest : public LakeCompactionTest {
public:
    LakeUniqueKeyCompactionTest() : LakeCompactionTest(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(UNIQUE_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_task_unique";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        LakeCompactionTest::SetUp();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakeUniqueKeyCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read(version));

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().enable_size_tiered_compaction_strategy) {
        ASSERT_EQ(0, new_tablet_metadata->cumulative_point());
    } else {
        ASSERT_EQ(1, new_tablet_metadata->cumulative_point());
    }
    ASSERT_EQ(1, new_tablet_metadata->rowsets_size());
}

INSTANTIATE_TEST_SUITE_P(LakeUniqueKeyCompactionTest, LakeUniqueKeyCompactionTest,
                         ::testing::Values(CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = false},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = false}),
                         to_string_param_name);

class LakeUniqueKeyCompactionWithDeleteTest : public LakeCompactionTest {
public:
    LakeUniqueKeyCompactionWithDeleteTest() : LakeCompactionTest(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(UNIQUE_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_compaction_task_unique_with_delete";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        LakeCompactionTest::SetUp();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            chunk->reset();
        }
        return ret;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakeUniqueKeyCompactionWithDeleteTest, test_base_compaction_with_delete) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read(version));

    // add delete rowset version
    {
        ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_delete_metadata = std::make_shared<TabletMetadata>(*tablet_metadata);
        auto* rowset = new_delete_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);

        auto* delete_predicate = rowset->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        // delete c0 < 4
        auto* binary_predicate = delete_predicate->add_binary_predicates();
        binary_predicate->set_column_name("c0");
        binary_predicate->set_op("<");
        binary_predicate->set_value("4");

        new_delete_metadata->set_version(version + 1);
        new_delete_metadata->set_cumulative_point(new_delete_metadata->rowsets_size());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*new_delete_metadata));

        version++;
    }

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize - 4, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().enable_size_tiered_compaction_strategy) {
        ASSERT_EQ(0, new_tablet_metadata->cumulative_point());
    } else {
        ASSERT_EQ(1, new_tablet_metadata->cumulative_point());
    }
    ASSERT_EQ(1, new_tablet_metadata->rowsets_size());
}

INSTANTIATE_TEST_SUITE_P(LakeUniqueKeyCompactionWithDeleteTest, LakeUniqueKeyCompactionWithDeleteTest,
                         ::testing::Values(CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = HORIZONTAL_COMPACTION,
                                                           .enable_size_tiered_compaction_strategy = false},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = true},
                                           CompactionParam{.algorithm = VERTICAL_COMPACTION,
                                                           .vertical_compaction_max_columns_per_group = 1,
                                                           .enable_size_tiered_compaction_strategy = false}),
                         to_string_param_name);

} // namespace starrocks::lake

int main(int argc, char** argv) {
    starrocks::init_test_env(argc, argv);
}
