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

#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_test_utils.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
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
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(0, read(version));
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
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        auto st = task->execute(CompactionTask::kCancelledFn);
        EXPECT_EQ(0, task_context->progress.value());
        EXPECT_TRUE(st.is_aborted()) << st;
    }
    // Completed compaction task without error
    {
        auto txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
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
    ASSERT_EQ(1, new_tablet_metadata->rowsets(0).segments_size());

    // check data
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));
    auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
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
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
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
