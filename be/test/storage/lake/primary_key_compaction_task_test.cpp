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

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_compaction_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_storage_fwd.h"
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
        ExecEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
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
        return Chunk({std::move(c0), std::move(c1)}, _schema);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    EXPECT_EQ(2, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::lake_pk_compaction_max_input_rowsets = 1;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets());
    EXPECT_EQ(1, input_rowsets3.size());
    EXPECT_EQ(1, compaction_score(_tablet_mgr.get(), tablet_meta));
    config::lake_pk_compaction_max_input_rowsets = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_score_by_policy2) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 2; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
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

    {
        std::vector<Chunk> chunks2;
        for (int i = 0; i < 2; i++) {
            chunks2.push_back(generate_data(kChunkSize * 10, i));
        }
        auto indexes2 = std::vector<uint32_t>(kChunkSize * 10);
        for (int i = 0; i < kChunkSize * 10; i++) {
            indexes2[i] = i;
        }
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
            ASSERT_OK(delta_writer->write(chunks2[i], indexes2.data(), indexes2.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            // Publish version
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
            version++;
        }
    }

    ASSERT_EQ(kChunkSize * 20, read(version));
    ASSIGN_OR_ABORT(auto tablet_meta, _tablet_mgr->get_tablet_metadata(tablet_id, version));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create(_tablet_mgr.get(), tablet_meta, false /* force_base_compaction */));

    config::lake_pk_compaction_max_input_rowsets = 1000;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(6, compaction_score(_tablet_mgr.get(), tablet_meta));

    auto non_const_tablet_meta = const_cast<TabletMetadataPB*>(tablet_meta.get());
    non_const_tablet_meta->set_compaction_strategy(CompactionStrategyPB::REAL_TIME);
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets2.size());
    EXPECT_EQ(4, compaction_score(_tablet_mgr.get(), tablet_meta));

    auto old_val = config::size_tiered_min_level_size;
    config::size_tiered_min_level_size = 2;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets());
    EXPECT_EQ(2, input_rowsets3.size());
    EXPECT_EQ(2, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::update_compaction_ratio_threshold = 2;
    config::update_compaction_delvec_file_io_amp_ratio = 0;
    ASSIGN_OR_ABORT(auto input_rowsets4, compaction_policy->pick_rowsets());
    EXPECT_EQ(4, input_rowsets4.size());
    EXPECT_EQ(4, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::lake_pk_compaction_max_input_rowsets = 2;
    config::update_compaction_delvec_file_io_amp_ratio = 2;
    ASSIGN_OR_ABORT(auto input_rowsets5, compaction_policy->pick_rowsets());
    EXPECT_EQ(2, input_rowsets5.size());
    EXPECT_EQ(2, compaction_score(_tablet_mgr.get(), tablet_meta));

    config::lake_pk_compaction_max_input_rowsets = 1000;
    config::size_tiered_min_level_size = old_val;
    config::update_compaction_ratio_threshold = 0.5;
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        check_task(task);
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
    });

    std::thread t2([&]() {
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_combined_txn_log(false);
        std::vector<TxnInfoPB> txn_infos{txn_info};
        abort_txn(_tablet_mgr.get(), tablet_id, txn_infos);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    ASSIGN_OR_ABORT(auto pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 2);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 6);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 5);

    // calculate score
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
    EXPECT_TRUE(pick_level_ptr != nullptr);
    EXPECT_EQ(pick_level_ptr->rowsets.size(), 2);
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 6);
    pick_level_ptr->rowsets.pop();
    EXPECT_EQ(pick_level_ptr->rowsets.top().rowset_index, 5);
    rowset_metas.clear();
    rowset_vec.clear();

    // Case-2
    // 500MB, 500MB, 400MB, 100KB, 100KB, 50KB, 10KB
    generate_test_rowsets(
            {500 * 1024 * 1024, 500 * 1024 * 1024, 400 * 1024 * 1024, 100 * 1024, 100 * 1024, 50 * 1024, 10 * 1024},
            &rowset_metas, &rowset_vec);
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
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
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
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
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
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
    ASSIGN_OR_ABORT(pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
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

// Helper: build a tablet_metadata-like vector of rowset PBs with explicit num_dels set,
// so pick_rowset_indexes does not need to consult UpdateManager for delete counts.
static void build_rowsets_with_dels(const std::vector<std::pair<int64_t /*bytes*/, int64_t /*dels*/>>& specs,
                                    std::vector<RowsetMetadataPB>* rowset_metas) {
    uint32_t id = 0;
    for (const auto& spec : specs) {
        RowsetMetadataPB rm;
        rm.set_id(id++);
        rm.set_overlapped(false);
        rm.set_num_rows(1000);
        rm.set_data_size(spec.first);
        rm.set_num_dels(spec.second);
        rowset_metas->push_back(rm);
    }
}

TEST_P(LakePrimaryKeyCompactionTest, test_min_level_score_skips_sparse_mid_tier) {
    // Reproduce the pathological case observed on a 13 GB 10T-tier tablet:
    //   size-tiered selector picks a level holding only a handful of large
    //   non-overlapped rowsets, the merge rewrites GBs of base data with
    //   negligible IO-count reduction, and write amplification balloons.
    // The new lake_pk_compaction_min_level_score config skips these picks.

    const bool old_strategy = config::enable_pk_size_tiered_compaction_strategy;
    const double old_threshold = config::lake_pk_compaction_min_level_score;
    config::enable_pk_size_tiered_compaction_strategy = true;
    DeferOp restore([&] {
        config::enable_pk_size_tiered_compaction_strategy = old_strategy;
        config::lake_pk_compaction_min_level_score = old_threshold;
    });

    // Sparse mid-tier: 4 rowsets at ~700 MB each, no overlap, no deletes.
    // Per-rowset score = 1 MB / ~700 MB ≈ 0.00143
    // Level score      ≈ 0.0057  (well below any non-trivial threshold)
    {
        std::vector<RowsetMetadataPB> rowset_metas;
        std::vector<RowsetCandidate> rowset_vec;
        generate_test_rowsets({700LL * 1024 * 1024, 700LL * 1024 * 1024, 700LL * 1024 * 1024, 700LL * 1024 * 1024},
                              &rowset_metas, &rowset_vec);
        ASSIGN_OR_ABORT(auto pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
        ASSERT_NE(pick_level_ptr, nullptr);
        EXPECT_EQ(pick_level_ptr->rowsets.size(), 4);
        // Score is the axis the new config gates on; document the regime.
        EXPECT_LT(pick_level_ptr->score, 0.01);
    }

    // High-overhead L0: 10 small rowsets at 10 KB each, no overlap, no deletes.
    // Per-rowset score = 1 MB / 10 KB ≈ 102.4
    // Level score      ≈ 1024  (any sane threshold leaves this alone)
    {
        std::vector<RowsetMetadataPB> rowset_metas;
        std::vector<RowsetCandidate> rowset_vec;
        std::vector<int64_t> small_bytes(10, 10 * 1024);
        generate_test_rowsets(small_bytes, &rowset_metas, &rowset_vec);
        ASSIGN_OR_ABORT(auto pick_level_ptr, PrimaryCompactionPolicy::pick_max_level(rowset_vec));
        ASSERT_NE(pick_level_ptr, nullptr);
        EXPECT_GT(pick_level_ptr->score, 100.0);
    }

    // Now exercise the gate end-to-end via pick_rowset_indexes: with threshold 0,
    // the sparse mid-tier should still be picked (legacy behavior preserved).
    auto build_metadata = [&](const std::vector<std::pair<int64_t, int64_t>>& specs) {
        auto md = std::make_shared<TabletMetadataPB>();
        md->set_id(_tablet_metadata->id());
        md->set_version(1);
        std::vector<RowsetMetadataPB> rowset_metas;
        build_rowsets_with_dels(specs, &rowset_metas);
        for (auto& rm : rowset_metas) {
            *md->add_rowsets() = rm;
        }
        return md;
    };

    // Lower min_input_segments gate so 4 rowsets pass the basic eligibility check.
    const int64_t old_min_segs = config::lake_pk_compaction_min_input_segments;
    config::lake_pk_compaction_min_input_segments = 2;
    DeferOp restore_segs([&] { config::lake_pk_compaction_min_input_segments = old_min_segs; });

    // Sparse mid-tier metadata: 4 large rowsets, no deletes.
    auto sparse_md = build_metadata(
            {{700LL * 1024 * 1024, 0}, {700LL * 1024 * 1024, 0}, {700LL * 1024 * 1024, 0}, {700LL * 1024 * 1024, 0}});

    PrimaryCompactionPolicy policy(_tablet_mgr.get(), sparse_md, /*force_base_compaction=*/false);

    // (a) threshold=0 (default): legacy behavior — picks the 4 rowsets.
    config::lake_pk_compaction_min_level_score = 0.0;
    {
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(sparse_md, &has_dels));
        EXPECT_EQ(picked.size(), 4);
    }

    // (b) threshold=0.01: gate trips, no rowsets picked, FE-side score collapses to 0.
    config::lake_pk_compaction_min_level_score = 0.01;
    {
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(sparse_md, &has_dels));
        EXPECT_EQ(picked.size(), 0);
    }

    // (c) High-overhead L0 still compacts even at the same threshold (high score).
    auto small_md = build_metadata({{10 * 1024, 0}, {10 * 1024, 0}, {10 * 1024, 0}, {10 * 1024, 0}, {10 * 1024, 0}});
    PrimaryCompactionPolicy small_policy(_tablet_mgr.get(), small_md, /*force_base_compaction=*/false);
    {
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, small_policy.pick_rowset_indexes(small_md, &has_dels));
        EXPECT_GE(picked.size(), 1);
    }

    // (d) Sparse mid-tier WITH deletes bypasses the gate (delete vectors must compact).
    auto delete_md = build_metadata({{700LL * 1024 * 1024, 0},
                                     {700LL * 1024 * 1024, 0},
                                     {700LL * 1024 * 1024, 0},
                                     {700LL * 1024 * 1024, 500 /* dels */}});
    PrimaryCompactionPolicy delete_policy(_tablet_mgr.get(), delete_md, /*force_base_compaction=*/false);
    {
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, delete_policy.pick_rowset_indexes(delete_md, &has_dels));
        EXPECT_GE(picked.size(), 1);
    }
}

// PR-1' v2 coverage:
//   - delete_ratio folded into bcr (no more binary has_deletes collapse)
//   - real_benefit_segs uses (input_segs - output_segs), not raw input_segs
//   - io_mb uses raw bytes, not read_bytes (which subtracts delete_bytes)
//   - size_overflow_ratio override uses max_rowset_bytes from picked level
//     (not the stale compact_level after pick_max_level merges levels)
TEST_P(LakePrimaryKeyCompactionTest, test_pr1prime_v2_gate_overrides) {
    const bool old_strategy = config::enable_pk_size_tiered_compaction_strategy;
    const double old_mls = config::lake_pk_compaction_min_level_score;
    const double old_bcr = config::lake_pk_compaction_min_benefit_cost_ratio;
    const double old_em = config::lake_pk_compaction_emergency_score;
    const double old_size_overflow = config::lake_pk_compaction_size_overflow_ratio;
    const double old_delvec_w = config::lake_pk_compaction_delvec_benefit_weight;
    const int64_t old_min_segs = config::lake_pk_compaction_min_input_segments;
    config::enable_pk_size_tiered_compaction_strategy = true;
    config::lake_pk_compaction_min_input_segments = 2;
    DeferOp restore([&] {
        config::enable_pk_size_tiered_compaction_strategy = old_strategy;
        config::lake_pk_compaction_min_level_score = old_mls;
        config::lake_pk_compaction_min_benefit_cost_ratio = old_bcr;
        config::lake_pk_compaction_emergency_score = old_em;
        config::lake_pk_compaction_size_overflow_ratio = old_size_overflow;
        config::lake_pk_compaction_delvec_benefit_weight = old_delvec_w;
        config::lake_pk_compaction_min_input_segments = old_min_segs;
    });

    auto build_metadata = [&](const std::vector<std::pair<int64_t, int64_t>>& specs) {
        auto md = std::make_shared<TabletMetadataPB>();
        md->set_id(_tablet_metadata->id());
        md->set_version(1);
        std::vector<RowsetMetadataPB> rowset_metas;
        build_rowsets_with_dels(specs, &rowset_metas);
        for (auto& rm : rowset_metas) {
            *md->add_rowsets() = rm;
        }
        return md;
    };

    // ===== Case 1: Low delete_ratio (5%) — gate should still skip =====
    // 8 mid-tier rowsets, each 500MB, 100 dels per 1000 rows → delete_ratio = 0.10.
    // wait — let me make it 5%: 50 dels / 1000 rows = 5% delete_ratio per rowset.
    // benefit_score = real_benefit_segs (7) + 0.05 * 8 * 12 = 7 + 4.8 = 11.8
    // io_mb = 4096
    // bcr = 11.8 / 4096 ≈ 0.00288 < 0.005 default threshold → bcr should NOT fire
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        auto md = build_metadata({{500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50},
                                  {500LL * 1024 * 1024, 50}});
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_EQ(picked.size(), 0)
                << "low delete_ratio (5%) sparse mid-tier should still skip — bcr override must not fire";
    }

    // ===== Case 2: High delete_ratio (20%) — bcr should fire and ALLOW compaction =====
    // 8 mid-tier rowsets at 500MB, 200 dels per 1000 rows → delete_ratio = 0.20.
    // benefit_score = 7 + 0.20 * 8 * 12 = 7 + 19.2 = 26.2
    // bcr = 26.2 / 4096 ≈ 0.0064 > 0.005 → bcr fires
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        auto md = build_metadata({{500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200},
                                  {500LL * 1024 * 1024, 200}});
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_GE(picked.size(), 1)
                << "high delete_ratio (20%) should let bcr fire and allow compaction";
    }

    // ===== Case 3: 1 single delete in 1 rowset out of 8 (binary-style collapse case) =====
    // delete_ratio = 1 / (8 * 1000) = 0.000125 (tiny)
    // benefit_score = 7 + 0.000125 * 8 * 12 = 7.012, bcr = 0.00171 < 0.005 → skip
    // (PR-1' v1 would have force-compacted on this; v2 correctly skips.)
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        auto md = build_metadata({{500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 0},
                                  {500LL * 1024 * 1024, 1 /* single delete */}});
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_EQ(picked.size(), 0)
                << "single delete in one of 8 rowsets has negligible delete_ratio, gate must still skip "
                << "(this case is the v1 design hole that v2 fixes)";
    }

    // ===== Case 4: size_overflow fires when accumulation crosses threshold =====
    // 20 mid-tier rowsets at 500MB each, 10GB total, no deletes.
    // size_overflow basis = max_rowset_bytes = 500MB → next_level_target = 500MB * 5 = 2.5GB
    // overflow_ratio = 10000MB / 2500MB = 4.0 ≥ alpha=2.0 → fires
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        std::vector<std::pair<int64_t, int64_t>> specs(20, {500LL * 1024 * 1024, 0});
        auto md = build_metadata(specs);
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_GE(picked.size(), 1)
                << "20 x 500MB accumulation should trigger size_overflow (alpha=2.0, total=10GB > 5GB threshold)";
    }

    // ===== Case 5: size_overflow does NOT fire below threshold =====
    // 8 mid-tier rowsets at 500MB, 4GB total. size_overflow = 4000 / 2500 = 1.6 < 2.0 → skip
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        std::vector<std::pair<int64_t, int64_t>> specs(8, {500LL * 1024 * 1024, 0});
        auto md = build_metadata(specs);
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_EQ(picked.size(), 0)
                << "8 x 500MB = 4GB stays below alpha=2.0 size_overflow threshold (5GB), gate must skip";
    }

    // ===== Case 6: size_overflow uses max_rowset_bytes (NOT stale compact_level after merge) =====
    // This is the bug-fix regression: pick_max_level merges single-rowset top with second
    // level, but compact_level retains the original top's level_size. Using compact_level
    // directly would give size_overflow = total / (small_top_size * 5) ≫ alpha, falsely
    // triggering the override and re-introducing the pathology our gate is meant to prevent.
    //
    // Recipe: large base 5GB + sparse mid-tier 8 x 500MB. pick_max_level's "top has 1 rowset,
    // not multi-segment overlapped" branch merges the 5GB level into the 500MB level.
    // After merge, compact_level == 5GB but max_rowset_bytes (correctly used) == 5GB also.
    // To force the bug-prone path we want a scenario where the original top level_size is
    // small relative to the merged contents: this requires the smaller (L0-like) level to
    // win on score but get merged with the larger mid-tier level.
    //
    // Score = io_count * 1MB / read_bytes ≈ 1 / size_MB. Smaller rowset → higher score.
    // A single 20MB rowset has score 0.05; 8 x 500MB has score 8 * 1/500 = 0.016.
    // Top = 20MB level (1 rowset, single segment) → merged with 500MB level.
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.005;
        config::lake_pk_compaction_size_overflow_ratio = 2.0;
        config::lake_pk_compaction_delvec_benefit_weight = 12.0;
        config::lake_pk_compaction_emergency_score = 50.0;

        // 1 x 20MB (the L0-like rowset, will be picked top + merged) + 8 x 500MB mid-tier.
        // Total = 4020MB. Using max_rowset_bytes (500MB): overflow = 4020 / 2500 = 1.61 < 2.0
        //   → gate correctly skips.
        // Using stale compact_level (20MB): overflow = 4020 / 100 = 40 ≫ 2.0
        //   → gate would falsely fire and re-create the pathology.
        std::vector<std::pair<int64_t, int64_t>> specs;
        specs.push_back({20LL * 1024 * 1024, 0});
        for (int i = 0; i < 8; i++) {
            specs.push_back({500LL * 1024 * 1024, 0});
        }
        auto md = build_metadata(specs);
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_EQ(picked.size(), 0)
                << "size_overflow must use max_rowset_bytes (500MB), not stale compact_level (20MB) "
                << "after pick_max_level merge — total 4GB is below the 5GB threshold and gate should skip";
    }

    // ===== Case 7: All v2 overrides at 0 (legacy ba3328b behavior) =====
    // With every override disabled, gate skips for low-score clean levels just like ba3328b.
    {
        config::lake_pk_compaction_min_level_score = 2.0;
        config::lake_pk_compaction_min_benefit_cost_ratio = 0.0;
        config::lake_pk_compaction_size_overflow_ratio = 0.0;
        config::lake_pk_compaction_delvec_benefit_weight = 0.0;
        config::lake_pk_compaction_emergency_score = 0.0;

        std::vector<std::pair<int64_t, int64_t>> specs(8, {500LL * 1024 * 1024, 0});
        auto md = build_metadata(specs);
        PrimaryCompactionPolicy policy(_tablet_mgr.get(), md, /*force_base_compaction=*/false);
        std::vector<bool> has_dels;
        ASSIGN_OR_ABORT(auto picked, policy.pick_rowset_indexes(md, &has_dels));
        EXPECT_EQ(picked.size(), 0)
                << "with all overrides disabled, gate should skip low-score clean level";
    }
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    {
        // check rows mapper files
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        uint32_t counter = 0;
        RowsMapperIterator iterator;
        ASSIGN_OR_ABORT(auto filename, lake_rows_mapper_filename(_tablet_mgr.get(), tablet_id,
                                                                 txn_log->op_compaction().lcrm_file().name()));
        ASSERT_OK(iterator.open(FileInfo{.path = filename, .size = txn_log->op_compaction().lcrm_file().size()}));
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
        auto task_context =
                std::make_unique<CompactionTaskContext>(compact_txn_id, tablet_id, version, false, false, nullptr);
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
        chunks.push_back(generate_data(kChunkSize, N - i - 1));
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
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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
    EXPECT_EQ(9, new_tablet_metadata->orphan_files_size());

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
            auto task_context =
                    std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
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

TEST_P(LakePrimaryKeyCompactionTest, test_should_enable_pk_index_eager_build) {
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

    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    // check should_enable_pk_index_eager_build
    if (!GetParam().enable_persistent_index || GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(0));
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes - 1));
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes));
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes + 1));
    } else {
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(0));
        EXPECT_FALSE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes - 1));
        EXPECT_TRUE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes));
        EXPECT_TRUE(task->should_enable_pk_index_eager_build(config::pk_index_eager_build_threshold_bytes + 1));
    }
}

TEST_P(LakePrimaryKeyCompactionTest, test_concurrent_compaction_and_publish) {
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

    // two concurrent compaction tasks
    // task-1
    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    }
    // task-2
    auto txn_id2 = next_id();
    auto task_context2 = std::make_unique<CompactionTaskContext>(txn_id2, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task2, _tablet_mgr->compact(task_context2.get()));
    check_task(task2);
    ASSERT_OK(task2->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context2->progress.value());
    if (!config::enable_light_pk_compaction_publish) {
        EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id2));
    }
    // publish task-1
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id));
    // task-2 publish should fail
    ASSERT_ERROR(publish_single_version(tablet_id, version + 2, txn_id2));
    version++;
    ASSERT_EQ(kChunkSize, read(version));
}

TEST_P(LakePrimaryKeyCompactionTest, test_publish_compaction_with_invalid_rowset_id) {
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
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    ASSERT_EQ(kChunkSize, read(version));
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(metadata->rowsets_size(), 3);

    auto txn_id = next_id();
    TxnLogPB txn_log;
    txn_log.set_tablet_id(tablet_id);
    txn_log.set_txn_id(txn_id);
    auto* op_compaction = txn_log.mutable_op_compaction();

    op_compaction->add_input_rowsets(0);
    op_compaction->add_input_rowsets(1);
    op_compaction->add_input_rowsets(999);

    auto* output_rowset = op_compaction->mutable_output_rowset();
    output_rowset->set_num_rows(kChunkSize);
    output_rowset->set_data_size(1024);
    output_rowset->add_segments("fake_segment.dat");
    output_rowset->add_segment_size(1024);

    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    auto res = publish_single_version(tablet_id, version + 1, txn_id);
    EXPECT_TRUE(res.status().is_internal_error());
}

TEST_P(LakePrimaryKeyCompactionTest, test_preload_compaction_state_slow_log) {
    // Set slow log threshold to 0 and disable light compaction publish
    // so that preload_compaction_state runs with trace and triggers slow log
    auto saved_slow_log_ms = config::lake_publish_version_slow_log_ms;
    auto saved_light_compaction = config::enable_light_pk_compaction_publish;
    config::lake_publish_version_slow_log_ms = 0;
    config::enable_light_pk_compaction_publish = false;
    DeferOp defer([&] {
        config::lake_publish_version_slow_log_ms = saved_slow_log_ms;
        config::enable_light_pk_compaction_publish = saved_light_compaction;
    });

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
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read(version));

    // Run compaction which triggers preload_compaction_state with trace and slow log
    auto txn_id = next_id();
    auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    check_task(task);
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());
    EXPECT_FALSE(_update_mgr->TEST_check_compaction_cache_absent(tablet_id, txn_id));
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize, read(version));
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
