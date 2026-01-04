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

#include "storage/lake/pk_tablet_sst_writer.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <random>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"

namespace starrocks::lake {

using namespace starrocks;

inline std::shared_ptr<TabletMetadataPB> generate_tablet_metadata(KeysType keys_type) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    //
    //  | column | type | KEY | NULL |
    //  +--------+------+-----+------+
    //  |   c0   |  VARCHAR | YES |  NO  |
    //  |   c1   |  INT | NO  |  NO  |
    auto schema = metadata->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("VARCHAR");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        c0->set_length(20);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
    }
    return metadata;
}

class PkTabletSSTWriterTest : public TestBase {
public:
    PkTabletSSTWriterTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_tablet_metadata(PRIMARY_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(kTestDirectory));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    Chunk generate_data(int64_t chunk_size, int64_t start_index = 0) {
        std::vector<std::pair<std::string, int>> data(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            data[i] = {fmt::format("key_{:10d}", start_index + i), (start_index + i) * 3};
        }

        auto c0 = BinaryColumn::create();
        auto c1 = Int32Column::create();
        for (const auto& [key, value] : data) {
            c0->append(key);
            c1->append(value);
        }
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    ChunkPtr read(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto ret = ChunkHelper::new_chunk(*_schema, 128);
        while (true) {
            auto tmp = ChunkHelper::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret->append(*tmp);
        }
        return ret;
    }

    int64_t read_rows(int64_t tablet_id, int64_t version) {
        auto chunk = read(tablet_id, version);
        return chunk->num_rows();
    }

    constexpr static const char* const kTestDirectory = "test_pk_tablet_sst_writer";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 456;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
    std::shared_ptr<FileSystem> _fs;
};

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_basic_operations) {
    const int64_t tablet_id = 12345;

    // Create PkTabletSSTWriter
    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Test reset_sst_writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Generate test data
    const int64_t chunk_size = 100;
    auto chunk = generate_data(chunk_size);

    // Test append_sst_record
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));

    // Add more data
    auto chunk2 = generate_data(50, chunk_size);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk2));

    // Test flush_sst_writer
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());

    // Verify file info
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
    ASSERT_TRUE(file_info.path.ends_with(".sst"));
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_multiple_chunks) {
    const int64_t tablet_id = 67890;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Add multiple chunks with different sizes
    std::vector<int64_t> chunk_sizes = {10, 50, 100, 25};
    int64_t start_index = 0;
    for (auto size : chunk_sizes) {
        auto chunk = generate_data(size, start_index);
        ASSERT_OK(pk_sst_writer->append_sst_record(chunk));
        start_index += size;
    }

    // Flush and get result
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());

    // Verify results
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_error_handling) {
    const int64_t tablet_id = 11111;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Test append_sst_record before reset - should fail
    auto chunk = generate_data(10);
    ASSERT_ERROR(pk_sst_writer->append_sst_record(chunk));

    // Test flush_sst_writer before reset - should fail
    ASSERT_ERROR(pk_sst_writer->flush_sst_writer());

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Now operations should work
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info.path.empty());
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_empty_chunk) {
    const int64_t tablet_id = 22222;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Create empty chunk
    auto empty_chunk = generate_data(0);
    ASSERT_OK(pk_sst_writer->append_sst_record(empty_chunk));

    // Add some real data
    auto chunk = generate_data(50);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));

    // Flush
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_reuse) {
    const int64_t tablet_id = 33333;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // First use
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    auto chunk1 = generate_data(30);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk1));

    ASSIGN_OR_ABORT(auto file_info1, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info1.path.empty());

    // Reuse the writer for a second file
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    auto chunk2 = generate_data(40, 30);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk2));

    ASSIGN_OR_ABORT(auto file_info2, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info2.path.empty());

    // The two files should be different
    ASSERT_NE(file_info1.path, file_info2.path);
}

TEST_F(PkTabletSSTWriterTest, test_publish_multi_segments_with_sst) {
    auto chunk0 = generate_data(12);
    auto chunk1 = generate_data(12, 12);
    auto chunk2 = generate_data(12, 24);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<bool> guard2(&config::enable_pk_parallel_execution, true);
    ConfigResetGuard<int64_t> guard3(&config::pk_parallel_execution_threshold_bytes, 1);
    for (int i = 0; i < 5; i++) {
        int64_t txn_id = next_id();
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
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // read txnlog
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_write().ssts_size(), 1);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // Compaction
    {
        ConfigResetGuard<int64_t> guard(&config::lake_pk_compaction_min_input_segments, 1);
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_compaction().input_rowsets_size(), 5);
        EXPECT_EQ(txn_log->op_compaction().ssts_size(), 1);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
}

TEST_F(PkTabletSSTWriterTest, test_parallel_execution_data_import) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1024);
    ConfigResetGuard<bool> guard2(&config::enable_pk_parallel_execution, true);
    ConfigResetGuard<int64_t> guard3(&config::pk_parallel_execution_threshold_bytes, 1);

    auto chunk0 = generate_data(100, 0);
    auto chunk1 = generate_data(100, 100);
    auto chunk2 = generate_data(100, 200);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

    int version = 1;
    int64_t txn_id = next_id();

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
    ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    EXPECT_GT(txn_log->op_write().ssts_size(), 0);

    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));

    // Verify the imported data by reading rows
    int64_t expected_rows = 300; // chunk0(100) + chunk1(100) + chunk2(100)
    int64_t actual_rows = read_rows(tablet_id, version + 1);
    EXPECT_EQ(expected_rows, actual_rows);
}

TEST_F(PkTabletSSTWriterTest, test_no_parallel_execution_with_update_operations) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);

    auto chunk0 = generate_data(50, 0);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

    int version = 1;

    for (int i = 0; i < 3; i++) {
        // flush sst files
        ConfigResetGuard<int64_t> guard2(&config::l0_max_mem_usage, 1);
        int64_t txn_id = next_id();
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

        auto chunk_update = generate_data(50, i * 25);
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_write().ssts_size(), 0);

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
        int64_t txn_id = next_id();
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

        auto chunk_update = generate_data(50, 0);
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_write().ssts_size(), 0);

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify the final result after multiple updates
    // With overlapping keys, the final count should be based on unique primary keys
    int64_t final_rows = read_rows(tablet_id, version);
    EXPECT_EQ(final_rows, 100);
}

TEST_F(PkTabletSSTWriterTest, test_parallel_execution_with_update_operations) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<bool> guard2(&config::enable_pk_parallel_execution, true);
    ConfigResetGuard<int64_t> guard3(&config::pk_parallel_execution_threshold_bytes, 1);

    auto chunk0 = generate_data(50, 0);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

    int version = 1;

    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
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

        auto chunk_update = generate_data(50, i * 25);
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_GT(txn_log->op_write().ssts_size(), 0);

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify the final result after multiple updates
    // With overlapping keys, the final count should be based on unique primary keys
    int64_t final_rows = read_rows(tablet_id, version);
    EXPECT_EQ(final_rows, 100);
}

TEST_F(PkTabletSSTWriterTest, test_parallel_execution_compaction_consistency) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::lake_pk_compaction_min_input_segments, 2);
    ConfigResetGuard<int64_t> guard2(&config::write_buffer_size, 512);
    ConfigResetGuard<bool> guard3(&config::enable_pk_parallel_execution, true);
    ConfigResetGuard<int64_t> guard4(&config::pk_parallel_execution_threshold_bytes, 1);

    auto chunk0 = generate_data(80, 0);
    auto chunk1 = generate_data(80, 80);
    auto chunk2 = generate_data(80, 160);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

    int version = 1;

    for (int i = 0; i < 4; i++) {
        int64_t txn_id = next_id();
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
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_GT(txn_log->op_write().ssts_size(), 0);

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    int64_t compaction_txn_id = next_id();
    auto task_context =
            std::make_unique<CompactionTaskContext>(compaction_txn_id, tablet_id, version, false, false, nullptr);

    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, task_context->progress.value());

    ASSIGN_OR_ABORT(auto compaction_txn_log, _tablet_mgr->get_txn_log(tablet_id, compaction_txn_id));
    EXPECT_EQ(compaction_txn_log->op_compaction().input_rowsets_size(), 4);
    EXPECT_GT(compaction_txn_log->op_compaction().ssts_size(), 0);

    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn_id).status());

    // Verify data consistency after compaction with parallel execution
    int64_t rows_after_compaction = read_rows(tablet_id, version + 1);
    EXPECT_EQ(rows_after_compaction, 240);
}

TEST_F(PkTabletSSTWriterTest, test_parallel_execution_vs_serial_execution_results) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<bool> guard2(&config::enable_pk_parallel_execution, true);
    ConfigResetGuard<int64_t> guard3(&config::pk_parallel_execution_threshold_bytes, 1);

    auto chunk0 = generate_data(60, 0);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

    std::vector<FileMetaPB> parallel_ssts;
    std::vector<FileMetaPB> serial_ssts;

    {
        int64_t txn_id = next_id();
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
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        for (const auto& sst : txn_log->op_write().ssts()) {
            parallel_ssts.push_back(sst);
        }

        ASSERT_OK(publish_single_version(tablet_id, 2, txn_id).status());
    }

    ConfigResetGuard<bool> guard4(&config::enable_pk_parallel_execution, false);
    {
        int64_t txn_id = next_id();
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
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        for (const auto& sst : txn_log->op_write().ssts()) {
            serial_ssts.push_back(sst);
        }

        ASSERT_OK(publish_single_version(tablet_id, 3, txn_id).status());
    }

    EXPECT_GT(parallel_ssts.size(), 0);
    EXPECT_EQ(serial_ssts.size(), 0);

    // Verify both parallel and serial execution produce correct results
    int64_t parallel_rows = read_rows(tablet_id, 2);
    int64_t serial_rows = read_rows(tablet_id, 3);
    EXPECT_EQ(60, parallel_rows);          // Should have 60 unique rows
    EXPECT_EQ(60, serial_rows);            // Should have same 60 unique rows
    EXPECT_EQ(parallel_rows, serial_rows); // Both should produce same result
}

} // namespace starrocks::lake