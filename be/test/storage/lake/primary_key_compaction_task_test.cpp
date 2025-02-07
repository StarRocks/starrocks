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

#include <algorithm>
#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_test_utils.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/lake_primary_key_recover.h"
#include "storage/lake/primary_key_compaction_policy.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/rows_mapper.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class LakePrimaryKeyCompactionTest : public TestBase, public testing::WithParamInterface<CompactionParam> {
public:
    LakePrimaryKeyCompactionTest() : TestBase(kTestDirectory), _partition_id(next_id()) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_metadata->set_enable_persistent_index(GetParam().enable_persistent_index);
        _tablet_metadata->set_persistent_index_type(GetParam().persistent_index_type);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_pk_compaction_task";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        config::vertical_compaction_max_columns_per_group = GetParam().vertical_compaction_max_columns_per_group;
        (void)fs::remove_all(kTestDirectory);
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        // Turn it down so we don't need to generate too much rowset for test.
        config::lake_pk_compaction_min_input_segments = 2;
    }

    void TearDown() override {
        config::lake_pk_compaction_min_input_segments = 5;
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), 0);
        (void)fs::remove_all(kTestDirectory);
    }

    Chunk generate_data(int64_t chunk_size, int shift) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
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
        return Chunk({c0, c1}, _schema);
    }

    Chunk generate_data2(int64_t chunk_size, int interval, int shift) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i * interval + shift;
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
        return Chunk({c0, c1}, _schema);
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

    void get_key_list(int64_t version, std::vector<int>& key_list) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);

        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (int i = 0; i < chunk->num_rows(); i++) {
                key_list.push_back(chunk->columns()[0]->get(i).get_int32());
            }
            chunk->reset();
        }
    }

    void check_task(CompactionTaskPtr& task) {
        if (GetParam().algorithm == HORIZONTAL_COMPACTION) {
            ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(task.get()) != nullptr);
        } else {
            ASSERT_EQ(GetParam().algorithm, VERTICAL_COMPACTION);
            ASSERT_TRUE(dynamic_cast<VerticalCompactionTask*>(task.get()) != nullptr);
        }
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

// each time overwrite last rows
TEST_P(LakePrimaryKeyCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize, 0);
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
    ASSIGN_OR_ABORT(auto new_tablet_metadata1, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata1->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata1->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata1->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata1->rowsets(2).num_dels(), 0);

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // make sure delvecs have been generated
    for (int i = 0; i < 2; i++) {
        auto itr = new_tablet_metadata1->delvec_meta().version_to_file().find(version - i);
        EXPECT_TRUE(itr != new_tablet_metadata1->delvec_meta().version_to_file().end());
        auto delvec_file = itr->second;
        EXPECT_TRUE(fs::path_exist(_lp->delvec_location(tablet_id, delvec_file.name())));
    }

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(kChunkSize, read(version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata2, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata2->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata2->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata2->has_prev_garbage_version());
    EXPECT_EQ(new_tablet_metadata2->rowsets(0).num_dels(), 0);
}

// test write 3 diff chunk
TEST_P(LakePrimaryKeyCompactionTest, test2) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
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
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
}

// test write empty chunk
TEST_P(LakePrimaryKeyCompactionTest, test3) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        if (i == 1) {
            chunks.push_back(generate_data(0, 0));
        } else {
            chunks.push_back(generate_data(kChunkSize, i));
        }
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    auto indexes_empty = std::vector<uint32_t>();
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
        if (i == 1) {
            ASSERT_OK(delta_writer->write(chunks[i], indexes_empty.data(), indexes_empty.size()));
        } else {
            ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 2, read(version));

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    ASSERT_EQ(kChunkSize * 2, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    EXPECT_EQ(2, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));
    ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_metadata, false /* force_base_compaction */));
    config::lake_pk_compaction_max_input_rowsets = 1000;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(3, input_rowsets.size());

    config::lake_pk_compaction_max_input_rowsets = 2;
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets());
    EXPECT_EQ(2, input_rowsets2.size());

    config::lake_pk_compaction_max_input_rowsets = 1;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets());
    EXPECT_EQ(1, input_rowsets3.size());
    config::lake_pk_compaction_max_input_rowsets = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy2) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    std::vector<std::vector<uint32_t>> indexes_list;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize * (i + 1), i));
        auto indexes = std::vector<uint32_t>(kChunkSize * (i + 1));
        for (int j = 0; j < kChunkSize * (i + 1); j++) {
            indexes[j] = j;
        }
        indexes_list.push_back(indexes);
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes_list[i].data(), indexes_list[i].size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
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
        ASSERT_OK(delta_writer->write(chunks[0], indexes_list[0].data(), indexes_list[0].size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 6, read(version));

    config::lake_pk_compaction_max_input_rowsets = 4;
    ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets.size());

    // check the rowset order, pick rowset#1 first, because it is empty.
    // Next order is rowset#4#2#3, by their byte size.
    EXPECT_EQ(input_rowsets[0]->id(), 1);
    EXPECT_EQ(input_rowsets[1]->id(), 4);
    EXPECT_EQ(input_rowsets[2]->id(), 2);
    EXPECT_EQ(input_rowsets[3]->id(), 3);
    config::lake_pk_compaction_max_input_rowsets = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy3) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    std::vector<std::vector<uint32_t>> indexes_list;
    for (int i = 0; i < 6; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
        auto indexes = std::vector<uint32_t>(kChunkSize);
        for (int j = 0; j < kChunkSize; j++) {
            indexes[j] = j;
        }
        indexes_list.push_back(indexes);
    }

    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    size_t chunk_index = 0;
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
        for (int seg_cnt = 0; seg_cnt <= i; seg_cnt++) {
            ASSERT_OK(delta_writer->write(chunks[chunk_index], indexes_list[chunk_index].data(),
                                          indexes_list[chunk_index].size()));
            chunk_index++;
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
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
        ASSERT_OK(delta_writer->write(chunks[0], indexes_list[0].data(), indexes_list[0].size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize * 6, read(version));

    config::lake_pk_compaction_max_input_rowsets = 4;
    ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_metadata, false /* force_base_compaction */));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(1, input_rowsets[0]->num_segments());
    EXPECT_EQ(1, input_rowsets[1]->num_segments());
    EXPECT_EQ(1, input_rowsets[2]->num_segments());
    EXPECT_EQ(1, input_rowsets[3]->num_segments());

    // check the rowset order, pick rowset#1 first, because it is empty.
    // Next order is rowset#7#2#4, by their segment count.
    EXPECT_EQ(input_rowsets[0]->id(), 1);
    EXPECT_EQ(input_rowsets[1]->id(), 4);
    EXPECT_EQ(input_rowsets[2]->id(), 2);
    EXPECT_EQ(input_rowsets[3]->id(), 3);
    config::lake_pk_compaction_max_input_rowsets = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy_min_input) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 4; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 4; i++) {
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 4, read(version));
    ASSIGN_OR_ABORT(auto tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_metadata, false /* force_base_compaction */));
    config::lake_pk_compaction_min_input_segments = 1;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(4, compaction_score(_tablet_mgr.get(), tablet_metadata));

    config::lake_pk_compaction_min_input_segments = 4;
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets2.size());
    EXPECT_EQ(4, compaction_score(_tablet_mgr.get(), tablet_metadata));

    config::lake_pk_compaction_min_input_segments = 5;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets());
    EXPECT_EQ(0, input_rowsets3.size());
    EXPECT_EQ(0, compaction_score(_tablet_mgr.get(), tablet_metadata));

    {
        // do compaction without input rowsets
        auto txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
        version++;
        ASSERT_EQ(kChunkSize * 4, read(version));
        if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
            check_local_persistent_index_meta(tablet_id, version);
        }

        ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 4);
        EXPECT_EQ(0, new_tablet_metadata->compaction_inputs_size());
    }
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_score_by_policy) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));
    ASSIGN_OR_ABORT(auto tablet_meta, _tablet_mgr->get_tablet_metadata(tablet_id, version));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_meta, false /* force_base_compaction */));
    config::lake_pk_compaction_max_input_rowsets = 1000;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::lake_pk_compaction_max_input_rowsets = 2;
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets());
    EXPECT_EQ(2, input_rowsets2.size());
    EXPECT_EQ(3, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::lake_pk_compaction_max_input_rowsets = 1;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets());
    EXPECT_EQ(1, input_rowsets3.size());
    EXPECT_EQ(3, compaction_score(_tablet_mgr.get(), tablet_meta));
    config::lake_pk_compaction_max_input_rowsets = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_sorted) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data2(kChunkSize, 3, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }
    // write chunk without order
    std::vector<int> chunk_write_without_order = {1, 0, 2};

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
        ASSERT_OK(delta_writer->write(chunks[chunk_write_without_order[i]], indexes.data(), indexes.size()));
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
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    EXPECT_EQ(100, task_context->progress.value());
    // check compaction state
    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    Rowset output_rowset(_tablet_mgr.get(), _tablet_metadata->id(), &txn_log->op_compaction().output_rowset(), -1,
                         _tablet_schema);
    {
        auto compaction_state = std::make_unique<CompactionState>();
        EXPECT_TRUE(_update_mgr->compaction_state_mem_tracker() != nullptr);
        auto prev = _update_mgr->compaction_state_mem_tracker()->consumption();
        for (size_t i = 0; i < output_rowset.num_segments(); i++) {
            ASSERT_OK(compaction_state->load_segments(&output_rowset, _update_mgr.get(), _tablet_schema, i));
            auto& pk_col = compaction_state->pk_cols[i];
            EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), pk_col->memory_usage() + prev);
            compaction_state->release_segments(i);
            EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), prev);
        }
    }
    // publish version
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());

    // check compact to one rowset with in order
    std::vector<int> key_list;
    get_key_list(version, key_list);
    for (int i = 0; i < key_list.size() - 1; i++) {
        EXPECT_TRUE(key_list[i] < key_list[i + 1]);
    }

    // Make sure all memory consume by compaction have return.
    EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), 0);
}

TEST_P(LakePrimaryKeyCompactionTest, test_remove_compaction_state) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize, 0);
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
    ASSIGN_OR_ABORT(auto new_tablet_metadata1, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata1->rowsets_size(), 3);

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // make sure delvecs have been generated
    for (int i = 0; i < 2; i++) {
        auto itr = new_tablet_metadata1->delvec_meta().version_to_file().find(version - i);
        EXPECT_TRUE(itr != new_tablet_metadata1->delvec_meta().version_to_file().end());
        auto delvec_file = itr->second;
        EXPECT_TRUE(fs::path_exist(_lp->delvec_location(tablet_id, delvec_file.name())));
    }

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    // remove compaction state
    _update_mgr->TEST_remove_compaction_cache(tablet_id, txn_id);
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(kChunkSize, read(version));
    // write again
    {
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
    // check again
    ASSERT_EQ(kChunkSize, read(version));
}

TEST_P(LakePrimaryKeyCompactionTest, test_abort_txn) {
    SyncPoint::GetInstance()->EnableProcessing();
    if (!config::enable_light_pk_compaction_publish) {
        SyncPoint::GetInstance()->LoadDependency(
                {{"UpdateManager::preload_compaction_state:return", "transactions::abort_txn:enter"}});
    }
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize, 0);
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
    ASSIGN_OR_ABORT(auto new_tablet_metadata1, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata1->rowsets_size(), 3);

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // make sure delvecs have been generated
    for (int i = 0; i < 2; i++) {
        auto itr = new_tablet_metadata1->delvec_meta().version_to_file().find(version - i);
        EXPECT_TRUE(itr != new_tablet_metadata1->delvec_meta().version_to_file().end());
        auto delvec_file = itr->second;
        EXPECT_TRUE(fs::path_exist(_lp->delvec_location(tablet_id, delvec_file.name())));
    }

    auto txn_id = next_id();
    std::thread t1([&]() {
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
    });

    std::thread t2([&]() {
        AbortTxnRequest request;
        request.add_tablet_ids(tablet_id);
        request.add_txn_ids(txn_id);
        request.set_skip_cleanup(false);
        AbortTxnResponse response;
        auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), _tablet_mgr.get());
        lake_service.abort_txn(nullptr, &request, &response, nullptr);
    });

    t1.join();
    t2.join();

    ASSERT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(LakePrimaryKeyCompactionTest, test_multi_output_seg) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    auto txn_id = next_id();
    // make sure compact can generate more than one segment in output rowset
    config::max_segment_file_size = 50;
    config::vector_chunk_size = 10;
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    config::max_segment_file_size = 1073741824;
    config::vector_chunk_size = 4096;
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    // make sure compact can generate more than one segment in output rowset
    EXPECT_TRUE(new_tablet_metadata->rowsets(0).segments_size() > 1);
}

TEST_P(LakePrimaryKeyCompactionTest, test_pk_recover_rowset_order_after_compact) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata1, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata1->rowsets_size(), 3);
    EXPECT_FALSE(new_tablet_metadata1->rowsets(0).has_max_compact_input_rowset_id());
    EXPECT_FALSE(new_tablet_metadata1->rowsets(1).has_max_compact_input_rowset_id());
    EXPECT_FALSE(new_tablet_metadata1->rowsets(2).has_max_compact_input_rowset_id());

    config::lake_pk_compaction_max_input_rowsets = 2;
    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(3 * kChunkSize, read(version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata2, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata2->rowsets_size(), 2);
    std::vector<RowsetPtr> rs_list = Rowset::get_rowsets(_tablet_mgr.get(), new_tablet_metadata2);
    EXPECT_EQ(2, rs_list.size());
    ASSERT_OK(LakePrimaryKeyRecover::sort_rowsets(&rs_list));
    EXPECT_TRUE(rs_list[0]->metadata().has_max_compact_input_rowset_id());
    EXPECT_FALSE(rs_list[1]->metadata().has_max_compact_input_rowset_id());
    EXPECT_TRUE(rs_list[0]->metadata().id() > rs_list[1]->metadata().id());
    EXPECT_TRUE(rs_list[0]->metadata().max_compact_input_rowset_id() < rs_list[1]->metadata().id());

    config::lake_pk_compaction_max_input_rowsets = 1000;
}

static void generate_test_rowsets(std::vector<int64_t> bytes, std::vector<RowsetMetadataPB>* rowset_metas,
                                  std::vector<RowsetCandidate>* rowset_vec) {
    uint32_t id = 0;
    for (int64_t byte : bytes) {
        RowsetMetadataPB rm;
        rm.set_id(id++);
        rm.set_overlapped(false);
        rm.set_num_rows(1000);
        rm.set_data_size(byte);
        rowset_metas->push_back(rm);
    }
    for (const RowsetMetadataPB& rowset_pb : *rowset_metas) {
        RowsetStat stat;
        stat.num_rows = rowset_pb.num_rows();
        stat.bytes = rowset_pb.data_size();
        stat.num_dels = rowset_pb.num_dels();
        rowset_vec->emplace_back(&rowset_pb, stat, rowset_pb.id());
    }
}

TEST_P(LakePrimaryKeyCompactionTest, test_size_tiered_compaction_strategy) {
    const bool old_val = config::enable_pk_size_tiered_compaction_strategy;
    config::enable_pk_size_tiered_compaction_strategy = true;
    // Case-1
    // 1000MB, 200MB, 40MB, 8MB, 1MB, 200KB, 10KB
    std::vector<RowsetMetadataPB> rowset_metas;
    std::vector<RowsetCandidate> rowset_vec;
    generate_test_rowsets({1000 * 1024 * 1024, 200 * 1024 * 1024, 40 * 1024 * 1024, 8 * 1024 * 1024, 1 * 1024 * 1024,
                           200 * 1024, 10 * 1024},
                          &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(auto pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(false, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 2);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 6);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 5);

    // calculate score
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(true, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 7);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 6);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 5);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 4);
    rowset_metas.clear();
    rowset_vec.clear();

    // Case-2
    // 500MB, 500MB, 400MB, 100KB, 100KB, 50KB, 10KB
    generate_test_rowsets(
            {500 * 1024 * 1024, 500 * 1024 * 1024, 400 * 1024 * 1024, 100 * 1024, 100 * 1024, 50 * 1024, 10 * 1024},
            &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(false, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 4);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 6);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 5);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 4);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 3);
    pick_level_ptr->rowsets.pop();
    rowset_metas.clear();
    rowset_vec.clear();

    // Case-3
    // 400MB, 100KB, 100KB, 50KB, 10KB, 500MB, 500MB
    generate_test_rowsets(
            {400 * 1024 * 1024, 100 * 1024, 100 * 1024, 50 * 1024, 10 * 1024, 500 * 1024 * 1024, 500 * 1024 * 1024},
            &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(false, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 4);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 4);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 3);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 2);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 1);
    pick_level_ptr->rowsets.pop();
    rowset_metas.clear();
    rowset_vec.clear();

    // Case-4
    // 127KB, 50KB, 10KB, 1KB, 128
    generate_test_rowsets({127 * 1024, 50 * 1024, 10 * 1024, 1024, 128}, &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(false, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 5);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 4);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 3);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 2);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 1);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 0);
    pick_level_ptr->rowsets.pop();
    rowset_metas.clear();
    rowset_vec.clear();

    // Case-5
    // 400MB, 100KB, 10KB, 50KB, 40KB, 500MB, 500MB
    generate_test_rowsets(
            {400 * 1024 * 1024, 100 * 1024, 10 * 1024, 50 * 1024, 40 * 1024, 500 * 1024 * 1024, 500 * 1024 * 1024},
            &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(false, rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 4);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 2);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 4);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 3);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 1);
    pick_level_ptr->rowsets.pop();
    rowset_metas.clear();
    rowset_vec.clear();
    config::enable_pk_size_tiered_compaction_strategy = old_val;
}

TEST_P(LakePrimaryKeyCompactionTest, test_rows_mapper) {
    // Prepare data for writing
    Chunk chunks[3];
    chunks[0] = generate_data2(kChunkSize, 100, 0);
    chunks[1] = generate_data2(kChunkSize, 100, 1);
    chunks[2] = generate_data2(kChunkSize, 100, 2);
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    {
        // check rows mapper files
        uint32_t counter = 0;
        RowsMapperIterator iterator;
        ASSIGN_OR_ABORT(auto filename, lake_rows_mapper_filename(tablet_id, txn_id));
        ASSERT_OK(iterator.open(filename));
        for (uint32_t i = 0; i < kChunkSize * 3; i += 3) {
            std::vector<uint64_t> rows_mapper;
            ASSERT_OK(iterator.next_values(3, &rows_mapper));
            ASSERT_TRUE(rows_mapper.size() == 3);
            for (const auto& each : rows_mapper) {
                ASSERT_TRUE((each >> 32) == (counter % 3) + 1);
                ASSERT_TRUE((each & 0xFFFFFFFF) == counter / 3);
                counter++;
            }
        }
        ASSERT_OK(iterator.status());
        // should eof
        std::vector<uint64_t> rows_mapper;
        ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
    }
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_data_load_conc) {
    // Prepare data for writing
    Chunk chunks[2];
    chunks[0] = generate_data2(kChunkSize, 100, 0);
    chunks[1] = generate_data2(kChunkSize, 100, 1);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 2; i++) {
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 2, read(version));

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    // upsert chunk 0 again without publish
    auto load_txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(load_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[0], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // compact rowset 1 & 2 without publish
    auto compact_txn_id = next_id();
    {
        auto task_context = std::make_unique<CompactionTaskContext>(compact_txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
    }

    // publish loading txn
    ASSERT_OK(publish_single_version(tablet_id, version + 1, load_txn_id).status());
    version++;
    // publish compact txn
    ASSERT_OK(publish_single_version(tablet_id, version + 1, compact_txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, compact_txn_id));
    version++;

    ASSERT_EQ(kChunkSize * 2, read(version));

    // check rowset result
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(2, new_tablet_metadata->compaction_inputs_size());
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_rows(), kChunkSize * 2);
}

TEST_P(LakePrimaryKeyCompactionTest, test_major_compaction) {
    if (!GetParam().enable_persistent_index || GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        return;
    }
    // Prepare data for writing
    std::vector<Chunk> chunks;
    int N = 10;
    for (int i = 0; i < N; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < N; i++) {
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * N, read(version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), N);

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    version++;
    ASSERT_EQ(kChunkSize * N, read(version));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(N, new_tablet_metadata->orphan_files_size());

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_P(LakePrimaryKeyCompactionTest, test_major_compaction_thread_safe) {
    if (!GetParam().enable_persistent_index || GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        return;
    }
    // Prepare data for writing
    std::vector<Chunk> chunks;
    int N = 10;
    for (int i = 0; i < N; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < N; i++) {
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
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * N, read(version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), N);

    std::vector<std::thread> workers;
    workers.emplace_back([&]() {
        for (int i = 0; i < N; i++) {
            auto txn_id = next_id() + N * 10;
            auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
            ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
            check_task(task);
            ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
            EXPECT_EQ(100, task_context->progress.value());
            _update_mgr->TEST_remove_compaction_cache(tablet_id, txn_id);
        }
    });
    workers.emplace_back([&]() {
        for (int i = 0; i < N; i++) {
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
            ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            // Publish version
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
            version++;
        }
    });
    for (auto& worker : workers) {
        worker.join();
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

INSTANTIATE_TEST_SUITE_P(
        LakePrimaryKeyCompactionTest, LakePrimaryKeyCompactionTest,
        ::testing::Values(CompactionParam{HORIZONTAL_COMPACTION, 5, false},
                          CompactionParam{VERTICAL_COMPACTION, 1, false},
                          CompactionParam{HORIZONTAL_COMPACTION, 5, true},
                          CompactionParam{VERTICAL_COMPACTION, 1, true},
                          CompactionParam{HORIZONTAL_COMPACTION, 5, true, PersistentIndexTypePB::CLOUD_NATIVE},
                          CompactionParam{VERTICAL_COMPACTION, 1, true, PersistentIndexTypePB::CLOUD_NATIVE}),
        to_string_param_name);

} // namespace starrocks::lake
