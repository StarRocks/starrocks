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

#include <map>
#include <random>
#include <tuple>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_compaction_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/logging.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/pk_tablet_unsort_sst_writer.h"
#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

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
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(kTestDirectory));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        StorageEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
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
        auto ret = ChunkFactory::new_chunk(*_schema, 128);
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 128);
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
    ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
    auto [file_info, sst_range] = std::move(sst_ret);

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
    ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
    auto [file_info, sst_range] = std::move(sst_ret);

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
    ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
    auto [file_info, sst_range] = std::move(sst_ret);
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
    ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
    auto [file_info, sst_range] = std::move(sst_ret);
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

    ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
    auto [file_info, sst_range] = std::move(sst_ret);
    ASSERT_FALSE(file_info.path.empty());

    // Reuse the writer for a second file
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    auto chunk2 = generate_data(40, 30);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk2));

    ASSIGN_OR_ABORT(auto sst_ret2, pk_sst_writer->flush_sst_writer());
    auto [file_info2, sst_range2] = std::move(sst_ret2);
    ASSERT_FALSE(file_info2.path.empty());

    // The two files should be different
    ASSERT_NE(file_info.path, file_info2.path);
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
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);
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
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

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
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

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
    ConfigResetGuard<int64_t> guard4(&config::pk_index_eager_build_threshold_bytes, 1);

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
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

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

    // The enable_pk_index_eager_build config was removed (eager build is now always on for a supported
    // schema). Disable it for this block via a data-size threshold no load here will reach, so this
    // exercises the non-eager publish path and confirms it agrees with the eager one above.
    ConfigResetGuard<int64_t> guard4(&config::pk_index_eager_build_threshold_bytes, 1024L * 1024 * 1024);
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

TEST_F(PkTabletSSTWriterTest, test_publish_with_parallel_index_get) {
    int64_t chunk_size = 3 * 4096;
    auto chunk0 = generate_data(chunk_size);
    auto chunk1 = generate_data(chunk_size, 12);
    auto chunk2 = generate_data(chunk_size, 24);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);
    ConfigResetGuard<bool> guard4(&config::enable_pk_index_parallel_execution, true);
    ConfigResetGuard<int64_t> guard5(&config::pk_index_parallel_execution_min_rows, 4096);
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

TEST_F(PkTabletSSTWriterTest, test_publish_early_sst_compact) {
    const int64_t tablet_id = _tablet_metadata->id();
    // Configure to generate SST files during write phase:
    // - Small write_buffer_size to generate multiple segments per write
    // - Enable eager PK index build to generate SST files
    ConfigResetGuard<int64_t> guard1(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);
    // Set a lower threshold to trigger early_sst_compact more easily
    ConfigResetGuard<int32_t> guard4(&config::pk_index_early_sst_compaction_threshold, 3);

    // Generate sufficient data to create multiple SST files
    // Each chunk will generate one SST file when written
    const int64_t chunk_size = 100;
    auto chunk0 = generate_data(chunk_size, 0);
    auto chunk1 = generate_data(chunk_size, chunk_size);
    auto chunk2 = generate_data(chunk_size, chunk_size * 2);
    auto indexes = std::vector<uint32_t>(chunk_size);
    for (uint32_t i = 0; i < chunk_size; i++) {
        indexes[i] = i;
    }

    int version = 1;
    // Import multiple times, each import generates multiple SST files
    // After enough SST files are accumulated, early_sst_compact should be triggered during publish
    const int num_imports = 6;
    for (int i = 0; i < num_imports; i++) {
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
        // Write multiple chunks to generate multiple segments with SST files
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        // Verify that SST files were generated during write phase
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_GT(txn_log->op_write().ssts_size(), 0) << "SST files should be generated during write phase";

        // Publish version - this is where early_sst_compact should be triggered
        // when the number of filesets exceeds pk_index_early_sst_compaction_threshold
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }

    // Verify data correctness after all imports and early_sst_compact
    // Should have 300 unique rows (chunk0 + chunk1 + chunk2 = 100 + 100 + 100)
    int64_t final_rows = read_rows(tablet_id, version);
    EXPECT_EQ(300, final_rows) << "Data should be correct after early_sst_compact";
}

// Helper to generate tablet metadata with a single BIGINT PK column
inline std::shared_ptr<TabletMetadataPB> generate_bigint_pk_tablet_metadata(
        PrimaryKeyEncodingTypePB pk_encoding_type = PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V1) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    //
    //  | column | type   | KEY | NULL |
    //  +--------+--------+-----+------+
    //  |   c0   | BIGINT | YES |  NO  |
    //  |   c1   | INT    | NO  |  NO  |
    auto schema = metadata->mutable_schema();
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);
    schema->set_primary_key_encoding_type(pk_encoding_type);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("BIGINT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");
    }
    return metadata;
}

// Test fixture for BIGINT PK tables with configurable encoding type
class PkTabletSSTWriterBigintKeyTest : public TestBase, public ::testing::WithParamInterface<PrimaryKeyEncodingTypePB> {
public:
    PkTabletSSTWriterBigintKeyTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_bigint_pk_tablet_metadata(GetParam());
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(kTestDirectory));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        StorageEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    Chunk generate_bigint_data(int64_t chunk_size, int64_t start_index = 0) {
        auto c0 = Int64Column::create();
        auto c1 = Int32Column::create();
        for (int64_t i = 0; i < chunk_size; i++) {
            c0->append(start_index + i);
            c1->append(static_cast<int32_t>((start_index + i) * 3));
        }
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    ChunkPtr read(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto ret = ChunkFactory::new_chunk(*_schema, 128);
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 128);
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

    constexpr static const char* const kTestDirectory = "test_pk_tablet_sst_writer_bigint";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 456;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
    std::shared_ptr<FileSystem> _fs;
};

// Test try_enable_pk_index_eager_build: V2 encoding enables for single BIGINT key,
// V1 encoding does not.
TEST_P(PkTabletSSTWriterBigintKeyTest, test_try_enable_pk_index_eager_build_single_bigint_key) {
    auto tablet_id = _tablet_metadata->id();

    auto writer = std::make_unique<HorizontalPkTabletWriter>(_tablet_mgr.get(), tablet_id, _tablet_schema,
                                                             /*txn_id=*/next_id(), /*flush_pool=*/nullptr,
                                                             /*is_compaction=*/false);

    writer->try_enable_pk_index_eager_build();

    if (GetParam() == PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2) {
        // V2: big-endian encoding preserves sort order for all types, so eager build should be enabled
        EXPECT_TRUE(writer->enable_pk_index_eager_build());
    } else {
        // V1: single BIGINT key without big-endian encoding, eager build should NOT be enabled
        EXPECT_FALSE(writer->enable_pk_index_eager_build());
    }
}

// Test that SST construction works with single BIGINT PK under V2 encoding (ascending data),
// and fails under V1 encoding because encoded keys are not in ascending order.
TEST_P(PkTabletSSTWriterBigintKeyTest, test_sst_build_single_bigint_pk_ascending_data) {
    auto tablet_id = _tablet_metadata->id();

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Generate ascending BIGINT data: 200, 201, ..., 299
    // This range crosses the 255/256 byte boundary, where little-endian encoding
    // breaks bytewise ascending order (255=[FF,00,...] > 256=[00,01,...])
    auto chunk = generate_bigint_data(100, 200);
    auto st = pk_sst_writer->append_sst_record(chunk);

    if (GetParam() == PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2) {
        // V2 encoding: big-endian guarantees encoded keys are in ascending order, SST build succeeds
        ASSERT_OK(st);
        ASSIGN_OR_ABORT(auto sst_ret, pk_sst_writer->flush_sst_writer());
        auto [file_info, sst_range] = std::move(sst_ret);
        ASSERT_FALSE(file_info.path.empty());
        ASSERT_GT(file_info.size, 0);
    } else {
        // V1 encoding: native byte order for BIGINT doesn't guarantee ascending encoded order.
        // On little-endian platforms, the encoded bytes are not in ascending order,
        // so SST builder's ascending key check fails.
        // Note: This test validates the V1 limitation. On big-endian platforms this may not fail,
        // but StarRocks primarily targets little-endian (x86_64/aarch64).
        ASSERT_FALSE(st.ok());
    }
}

// Test end-to-end write with eager PK index build for single BIGINT PK under V2 encoding:
// DeltaWriter should successfully produce SST files.
TEST_P(PkTabletSSTWriterBigintKeyTest, test_write_with_eager_build_single_bigint_pk) {
    auto tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

    auto chunk0 = generate_bigint_data(100, 0);
    auto chunk1 = generate_bigint_data(100, 100);
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
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));

    if (GetParam() == PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2) {
        // V2: eager build enabled for single BIGINT key, SST files should be generated
        EXPECT_GT(txn_log->op_write().ssts_size(), 0);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_EQ(200, read_rows(tablet_id, version + 1));
    } else {
        // V1: eager build disabled for single BIGINT key, no SST files
        EXPECT_EQ(txn_log->op_write().ssts_size(), 0);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_EQ(200, read_rows(tablet_id, version + 1));
    }
}

INSTANTIATE_TEST_SUITE_P(PkEncodingType, PkTabletSSTWriterBigintKeyTest,
                         ::testing::Values(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V1,
                                           PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2));

// ---------------------------------------------------------------------------
// PkTabletUnsortSSTWriter (separate-sort-key path) unit tests
// ---------------------------------------------------------------------------

// Builds a chunk from explicit (key, value) rows; keys may repeat.
static Chunk make_kv_chunk(const std::shared_ptr<Schema>& schema,
                           const std::vector<std::pair<std::string, int>>& rows) {
    auto c0 = BinaryColumn::create();
    auto c1 = Int32Column::create();
    for (const auto& [k, v] : rows) {
        c0->append(k);
        c1->append(v);
    }
    return Chunk({std::move(c0), std::move(c1)}, schema);
}

TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_basic_no_dup) {
    const int64_t tablet_id = _tablet_metadata->id();
    auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(w->reset_sst_writer(lp, _fs));

    auto chunk = generate_data(100);
    std::vector<uint64_t> order(chunk.num_rows());
    for (uint32_t i = 0; i < chunk.num_rows(); i++) {
        order[i] = (static_cast<uint64_t>(1) << 32) | i; // slot_idx=1, rowid=i
    }
    ASSERT_OK(w->append_sst_record(chunk, &order));

    ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());
    auto [file_info, sst_range] = std::move(ret);
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
    // Distinct keys -> no dedup losers.
    EXPECT_TRUE(w->take_deleted_rowids().empty());
}

TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_requires_order) {
    const int64_t tablet_id = _tablet_metadata->id();
    auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(w->reset_sst_writer(lp, _fs));

    auto chunk = generate_data(10);
    // Missing per-row order keys must be rejected (the sort-key-ordered merge cannot resolve dup PKs
    // without them).
    ASSERT_ERROR(w->append_sst_record(chunk));
    std::vector<uint64_t> wrong_size(5);
    ASSERT_ERROR(w->append_sst_record(chunk, &wrong_size));
}

TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_dedup_last_flushed_wins) {
    const int64_t tablet_id = _tablet_metadata->id();
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);

    // Two rows share the same primary key; the one with the larger order (later memtable flush) wins,
    // and the loser's rowid is recorded for the delete vector.
    // Case A: row 1 has the larger order -> loser is rowid 0.
    {
        auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
        ASSERT_OK(w->reset_sst_writer(lp, _fs));
        auto chunk = make_kv_chunk(_schema, {{"dup_key", 10}, {"dup_key", 20}});
        std::vector<uint64_t> order = {(static_cast<uint64_t>(1) << 32) | 0, (static_cast<uint64_t>(2) << 32) | 1};
        ASSERT_OK(w->append_sst_record(chunk, &order));
        ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());
        auto dels = w->take_deleted_rowids();
        ASSERT_EQ(dels.size(), 1u);
        EXPECT_EQ(dels[0], 0u);
    }
    // Case B: row 0 has the larger order -> loser is rowid 1.
    {
        auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
        ASSERT_OK(w->reset_sst_writer(lp, _fs));
        auto chunk = make_kv_chunk(_schema, {{"dup_key", 10}, {"dup_key", 20}});
        std::vector<uint64_t> order = {(static_cast<uint64_t>(2) << 32) | 0, (static_cast<uint64_t>(1) << 32) | 1};
        ASSERT_OK(w->append_sst_record(chunk, &order));
        ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());
        auto dels = w->take_deleted_rowids();
        ASSERT_EQ(dels.size(), 1u);
        EXPECT_EQ(dels[0], 1u);
    }
}

TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_overflow_merge_dedup) {
    const int64_t tablet_id = _tablet_metadata->id();
    // Force the map to spill to an intermediate SST on every append, so the duplicate key below is
    // split across two intermediate SSTs and must be resolved by the finish-time k-way merge.
    ConfigResetGuard<int64_t> guard(&config::l0_max_mem_usage, 1);
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);
    auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    ASSERT_OK(w->reset_sst_writer(lp, _fs));

    // append 1: key "A" at segment rowid 0, slot_idx 1 -> spilled to intermediate SST #1.
    auto chunk1 = make_kv_chunk(_schema, {{"A", 10}});
    std::vector<uint64_t> order1 = {(static_cast<uint64_t>(1) << 32) | 0};
    ASSERT_OK(w->append_sst_record(chunk1, &order1));
    // append 2: key "A" again at segment rowid 1, slot_idx 2 (later flush) -> intermediate SST #2.
    auto chunk2 = make_kv_chunk(_schema, {{"A", 20}});
    std::vector<uint64_t> order2 = {(static_cast<uint64_t>(2) << 32) | 1};
    ASSERT_OK(w->append_sst_record(chunk2, &order2));

    ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());
    auto [file_info, sst_range] = std::move(ret);
    ASSERT_FALSE(file_info.path.empty());
    // Cross-SST merge: later-flushed row (rowid 1) wins, rowid 0 loses.
    auto dels = w->take_deleted_rowids();
    ASSERT_EQ(dels.size(), 1u);
    EXPECT_EQ(dels[0], 0u);
}

// Op-aware reconciliation (separate-sort-key spill path): DELETE rows are fed via append_delete_records
// and reconciled against the upserts by rssid_rowid order (latest op per PK wins). An UPSERT that lost
// to a later DELETE (or a later UPSERT) is masked by the delete vector; a DELETE superseded by a later
// UPSERT contributes nothing. A PK whose latest op is a DELETE is collected into the del-file column
// (take_delete_keys) so publish erases it, including a row from an earlier transaction.
TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_op_aware_reconcile) {
    const int64_t tablet_id = _tablet_metadata->id();
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);
    auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    ASSERT_OK(w->reset_sst_writer(lp, _fs));
    auto order = [](uint32_t slot, uint32_t rowid) { return (static_cast<uint64_t>(slot) << 32) | rowid; };

    // Upserts, in segment-rowid order (rowid == append position):
    //   "up_only"  @rowid0 slot1  -> live (no matching delete)
    //   "del_wins" @rowid1 slot1  -> a later DELETE at slot3 supersedes it -> masked (loser)
    //   "up_after" @rowid2 slot4  -> a DELETE at slot2 precedes it -> upsert wins, live
    auto up_chunk = make_kv_chunk(_schema, {{"up_only", 1}, {"del_wins", 2}, {"up_after", 3}});
    std::vector<uint64_t> up_order = {order(1, 0), order(1, 1), order(4, 2)};
    ASSERT_OK(w->append_sst_record(up_chunk, &up_order));

    // Deletes (no segment rowid). Orders decide the winner per PK:
    //   "del_wins" @slot3  > its upsert slot1  -> DELETE wins  -> upsert rowid1 masked
    //   "up_after" @slot2  < its upsert slot4  -> DELETE loses -> dropped, upsert stays live
    //   "del_only" @slot5  (no upsert)         -> DELETE wins  -> del-file key, nothing masked
    auto del_chunk = make_kv_chunk(_schema, {{"del_wins", 0}, {"up_after", 0}, {"del_only", 0}});
    std::vector<uint64_t> del_order = {order(3, 0), order(2, 0), order(5, 0)};
    ASSERT_OK(w->append_delete_records(del_chunk, &del_order));

    ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());

    // The only masked segment row is "del_wins"'s upsert at rowid 1: it lost to a later DELETE.
    // up_only has no delete; up_after's delete is older so the upsert wins; del_only has no segment row.
    auto dels = w->take_deleted_rowids();
    ASSERT_EQ(dels.size(), 1u);
    EXPECT_EQ(dels[0], 1u);

    // Two PKs have DELETE as their latest op -> collected as del-file keys: "del_wins" (supersedes its
    // upsert) and "del_only" (no upsert at all). "up_after"'s DELETE lost to a later upsert, so it is
    // not present. These keys drive publish's erase of pre-existing rows.
    auto del_keys = w->take_delete_keys();
    ASSERT_NE(del_keys, nullptr);
    EXPECT_EQ(del_keys->size(), 2u);
}

// merge_intermediates_into's winning-DELETE branch: with the map forced to spill on every append, a
// DELETE and its earlier UPSERT of the same key land in SEPARATE intermediate SSTs, so the winning
// DELETE is resolved by the finish-time k-way merge (not the in-memory map path in flush_sst_writer).
// Asserts the delete is collected into the del file and the superseded upsert's rowid is masked.
TEST_F(PkTabletSSTWriterTest, test_unsort_sst_writer_overflow_merge_delete_wins) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<int64_t> guard(&config::l0_max_mem_usage, 1);
    auto lp = std::make_shared<FixedLocationProvider>(kTestDirectory);
    auto w = std::make_unique<PkTabletUnsortSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);
    ASSERT_OK(w->reset_sst_writer(lp, _fs));
    auto order = [](uint32_t slot, uint32_t rowid) { return (static_cast<uint64_t>(slot) << 32) | rowid; };

    // UPSERT "A" @rowid0 slot1 -> spilled to intermediate SST #1.
    auto up = make_kv_chunk(_schema, {{"A", 10}});
    std::vector<uint64_t> up_order = {order(1, 0)};
    ASSERT_OK(w->append_sst_record(up, &up_order));
    // DELETE "A" @slot3 (later flush) -> intermediate SST #2. Across SSTs, the DELETE is the latest op.
    auto del = make_kv_chunk(_schema, {{"A", 0}});
    std::vector<uint64_t> del_order = {order(3, 0)};
    ASSERT_OK(w->append_delete_records(del, &del_order));

    ASSIGN_OR_ABORT(auto ret, w->flush_sst_writer());
    // Latest op is the DELETE -> collected into the del file (not written to the SST as a rowid).
    auto del_keys = w->take_delete_keys();
    ASSERT_NE(del_keys, nullptr);
    EXPECT_EQ(del_keys->size(), 1u);
    // The superseded UPSERT's segment rowid 0 is masked by the delete vector.
    auto dels = w->take_deleted_rowids();
    ASSERT_EQ(dels.size(), 1u);
    EXPECT_EQ(dels[0], 0u);
}

// Tablet with a separate sort key (ORDER BY the value column c1, which differs from the PK c0).
inline std::shared_ptr<TabletMetadataPB> generate_separate_sort_key_tablet_metadata() {
    auto metadata = generate_tablet_metadata(PRIMARY_KEYS);
    // PK is c0 (index 0); sort by c1 (index 1) -> has_separate_sort_key() == true.
    metadata->mutable_schema()->add_sort_key_idxes(1);
    return metadata;
}

class PkTabletUnsortWriterE2ETest : public TestBase {
public:
    PkTabletUnsortWriterE2ETest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_separate_sort_key_tablet_metadata();
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(kTestDirectory));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        StorageEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }
    void TearDown() override { remove_test_dir_ignore_error(); }

    // `val_bias` offsets the sort-key/value column so a re-load of the same primary keys carries a
    // DISTINCT value; a dedup test can then assert (via read_key_values) that the later-flushed value
    // wins, which a same-value load could not distinguish.
    Chunk generate_data(int64_t chunk_size, int64_t start_index = 0, int32_t val_bias = 0) {
        auto c0 = BinaryColumn::create();
        auto c1 = Int32Column::create();
        for (int i = 0; i < chunk_size; i++) {
            c0->append(fmt::format("key_{:10d}", start_index + i));
            c1->append(static_cast<int32_t>((start_index + i) * 3) + val_bias);
        }
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }
    static int32_t expected_val(int64_t pk, int32_t val_bias = 0) { return static_cast<int32_t>(pk * 3) + val_bias; }

    int64_t read_rows(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        int64_t rows = 0;
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            rows += tmp->num_rows();
        }
        return rows;
    }

    // Read back the whole table as PK(string) -> sort-key(int32), so a test can verify not just the row
    // count but the surviving value per key (catches a wrong dedup winner, which a count check misses).
    std::map<std::string, int32_t> read_key_values(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        std::map<std::string, int32_t> out;
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            auto cols = tmp->columns();
            for (size_t i = 0; i < tmp->num_rows(); i++) {
                out[cols[0]->get(i).get_slice().to_string()] = cols[1]->get(i).get_int32();
            }
        }
        return out;
    }

    constexpr static const char* const kTestDirectory = "test_pk_tablet_unsort_writer_e2e";
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 456;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
    std::shared_ptr<FileSystem> _fs;
};

// End-to-end: a separate-sort-key PK load with spill + eager build produces the unsort SST and
// publishes the correct number of rows.
TEST_F(PkTabletUnsortWriterE2ETest, test_load_spill_eager_build_distinct_keys) {
    ASSERT_TRUE(_tablet_schema->has_separate_sort_key());
    const int64_t tablet_id = _tablet_metadata->id();
    // Small buffer forces multiple memtable flushes -> spill; eager build forced for separate sort key.
    ConfigResetGuard<bool> guard0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

    auto chunk0 = generate_data(100, 0);
    auto chunk1 = generate_data(100, 100);
    auto chunk2 = generate_data(100, 200);
    std::vector<uint32_t> indexes(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

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
    // The unsort SST writer ran for the separate-sort-key spill path.
    EXPECT_GT(txn_log->op_write().ssts_size(), 0);

    ASSERT_OK(publish_single_version(tablet_id, 2, txn_id).status());
    EXPECT_EQ(300, read_rows(tablet_id, 2));
}

// End-to-end: duplicate primary keys across the load are deduped to the last-written value; the row
// count reflects only distinct primary keys.
TEST_F(PkTabletUnsortWriterE2ETest, test_load_spill_eager_build_duplicate_keys) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<bool> guard0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

    // Overlapping key ranges across chunks: keys [0,100), [50,150) -> 150 distinct primary keys. The
    // later chunk carries a DISTINCT value (val_bias) for the overlap [50,100) so the read-back can
    // confirm the later-flushed row won the dedup (not just that the count is right).
    constexpr int32_t kBias = 1000000;
    auto chunk0 = generate_data(100, 0);
    auto chunk1 = generate_data(100, 50, kBias);
    std::vector<uint32_t> indexes(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }

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
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    ASSERT_OK(publish_single_version(tablet_id, 2, txn_id).status());
    // 150 distinct primary keys after dedup (loser rows masked by the per-segment delete vector).
    auto kv = read_key_values(tablet_id, 2);
    EXPECT_EQ(150u, kv.size());
    for (int64_t k = 0; k < 50; k++) { // only in chunk0
        auto it = kv.find(fmt::format("key_{:10d}", k));
        ASSERT_NE(it, kv.end()) << k;
        EXPECT_EQ(expected_val(k), it->second) << k;
    }
    for (int64_t k = 50; k < 150; k++) { // chunk1 (later flush) wins for the [50,100) overlap
        auto it = kv.find(fmt::format("key_{:10d}", k));
        ASSERT_NE(it, kv.end()) << k;
        EXPECT_EQ(expected_val(k, kBias), it->second) << "key " << k << " kept the wrong (earlier) value";
    }
}

// Regression for the delvec cardinality check: an unsort load whose incoming segment carries
// duplicate primary keys that ALSO exist in a prior version. `_do_update(read_only=true)` probes
// every physical row (including the duplicates), so the same historical rowid is collected multiple
// times into new_deletes; publish must dedupe it before the `cur_old + cur_add == cur_new` check,
// otherwise it fails with "delvec inconsistent".
TEST_F(PkTabletUnsortWriterE2ETest, test_load_spill_dup_keys_over_existing_data) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<bool> guard0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> guard3(&config::pk_index_eager_build_threshold_bytes, 1);

    auto load_chunks = [&](int64_t txn_id, const std::vector<Chunk*>& chunks) {
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_profile(&_dummy_runtime_profile)
                                         .build());
        ASSERT_OK(dw->open());
        for (auto* c : chunks) {
            std::vector<uint32_t> idx(c->num_rows());
            for (uint32_t i = 0, n = c->num_rows(); i < n; i++) idx[i] = i;
            ASSERT_OK(dw->write(*c, idx.data(), idx.size()));
        }
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
    };

    // Txn 1: seed 100 distinct keys [0, 100).
    auto seed = generate_data(100, 0);
    int64_t txn1 = next_id();
    load_chunks(txn1, {&seed});
    ASSERT_OK(publish_single_version(tablet_id, 2, txn1).status());

    // Txn 2: load the same 100 keys TWICE in one txn -> the incoming segment holds duplicate PKs that
    // also exist historically (from txn 1). Publish must not trip the delvec cardinality check. dup_b
    // carries a DISTINCT value (val_bias) and is flushed after dup_a, so the read-back also confirms the
    // later copy won the dedup.
    constexpr int32_t kBias = 1000000;
    auto dup_a = generate_data(100, 0);
    auto dup_b = generate_data(100, 0, kBias);
    int64_t txn2 = next_id();
    load_chunks(txn2, {&dup_a, &dup_b});
    ASSERT_OK(publish_single_version(tablet_id, 3, txn2).status());

    auto kv = read_key_values(tablet_id, 3);
    EXPECT_EQ(100u, kv.size());
    for (int64_t k = 0; k < 100; k++) {
        auto it = kv.find(fmt::format("key_{:10d}", k));
        ASSERT_NE(it, kv.end()) << k;
        EXPECT_EQ(expected_val(k, kBias), it->second) << "key " << k << " kept the wrong (earlier) value";
    }
}

// End-to-end op-aware DELETE through spill + publish on a separate-sort-key table -- the path the
// del-file fix targets. It exercises what a writer-level test cannot:
//   * a DELETE of a row written by an EARLIER transaction must erase it (an SST tombstone shadows the
//     key in the index but never masks the pre-existing segment row; only a del file does);
//   * the sort key (c1) DECREASES as the PK increases, so sort-key order is the reverse of PK order --
//     the "primary keys arrive out of PK order" premise is actually exercised;
//   * updated rows get a distinct new value that is verified on read-back, so choosing the wrong dedup
//     winner is visible (a row-count check alone would miss it).
TEST_F(PkTabletUnsortWriterE2ETest, test_load_spill_op_aware_delete) {
    ASSERT_TRUE(_tablet_schema->has_separate_sort_key());
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<bool> g0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> g1(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> g3(&config::pk_index_eager_build_threshold_bytes, 1);
    ConfigResetGuard<bool> g4(&config::lake_enable_pk_preserve_txn_delete_order, true);

    // __op column values (on-wire convention: 0 == UPSERT, 1 == DELETE).
    constexpr int8_t kUpsert = 0;
    constexpr int8_t kDelete = 1;
    // Sort key decreases as PK increases -> sort-key order is the reverse of PK order.
    auto seed_sk = [](int64_t pk) { return static_cast<int32_t>(1000000 - pk); };
    auto updated_sk = [](int64_t pk) { return static_cast<int32_t>(2000000 - pk); };
    auto key_of = [](int64_t pk) { return fmt::format("key_{:10d}", pk); };

    std::vector<SlotDescriptor> op_slots;
    op_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_VARCHAR});
    op_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
    op_slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
    std::vector<SlotDescriptor*> op_slot_ptrs{&op_slots[0], &op_slots[1], &op_slots[2]};

    // rows: (pk, sort_key, op). The chunk carries c0, c1 and the trailing __op column.
    auto make_op_chunk = [&](const std::vector<std::tuple<int64_t, int32_t, int8_t>>& rows) {
        auto c0 = BinaryColumn::create();
        auto c1 = Int32Column::create();
        auto cop = Int8Column::create();
        for (const auto& [pk, sk, op] : rows) {
            c0->append(key_of(pk));
            c1->append(sk);
            cop->append(op);
        }
        Chunk::SlotHashMap m;
        m[0] = 0;
        m[1] = 1;
        m[2] = 2;
        return std::make_shared<Chunk>(Columns{std::move(c0), std::move(c1), std::move(cop)}, m);
    };
    auto load = [&](int64_t txn_id, const ChunkPtr& chunk) {
        std::vector<uint32_t> idx(chunk->num_rows());
        for (uint32_t i = 0, n = chunk->num_rows(); i < n; i++) idx[i] = i;
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_slot_descriptors(&op_slot_ptrs)
                                         .set_profile(&_dummy_runtime_profile)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(*chunk, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
    };

    // Txn 1: seed keys [0, 100) as upserts.
    std::vector<std::tuple<int64_t, int32_t, int8_t>> seed;
    for (int64_t k = 0; k < 100; k++) seed.emplace_back(k, seed_sk(k), kUpsert);
    int64_t txn1 = next_id();
    load(txn1, make_op_chunk(seed));
    ASSERT_OK(publish_single_version(tablet_id, 2, txn1).status());
    ASSERT_EQ(100, read_rows(tablet_id, 2));

    // Txn 2 (op-aware, spilled): DELETE the first 50 historical keys, UPSERT the next 50 with a NEW
    // sort-key value, and INSERT 50 brand-new keys -- all in one transaction.
    std::vector<std::tuple<int64_t, int32_t, int8_t>> mixed;
    for (int64_t k = 0; k < 50; k++) mixed.emplace_back(k, 0, kDelete);               // historical deletes
    for (int64_t k = 50; k < 100; k++) mixed.emplace_back(k, updated_sk(k), kUpsert); // updates
    for (int64_t k = 100; k < 150; k++) mixed.emplace_back(k, seed_sk(k), kUpsert);   // new inserts
    int64_t txn2 = next_id();
    load(txn2, make_op_chunk(mixed));
    ASSERT_OK(publish_single_version(tablet_id, 3, txn2).status());

    auto kv = read_key_values(tablet_id, 3);
    EXPECT_EQ(100u, kv.size());
    for (int64_t k = 0; k < 50; k++) {
        EXPECT_EQ(0u, kv.count(key_of(k))) << "historical key " << k << " should be deleted";
    }
    for (int64_t k = 50; k < 100; k++) {
        auto it = kv.find(key_of(k));
        ASSERT_NE(it, kv.end()) << "updated key " << k << " missing";
        EXPECT_EQ(updated_sk(k), it->second) << "updated key " << k << " kept the wrong (stale) value";
    }
    for (int64_t k = 100; k < 150; k++) {
        EXPECT_EQ(1u, kv.count(key_of(k))) << "new key " << k << " missing";
    }
}

// VERTICAL compaction of a separate-sort-key table with eager PK-index build. The vertical key
// column group is normally just the SORT-KEY columns (not the PK), so the compaction task moves the
// PK columns into the key group (appended after the sort-key columns) and the vertical PK writer
// builds the eager unsort SST from them. This forces vertical compaction by lowering
// vertical_compaction_max_columns_per_group below the table's column count, then asserts that
// compaction (a) produced the eager SST, (b) kept the row count correct, and (c) built a correct PK
// index -- verified by re-upserting existing keys and confirming they dedup (a wrong-column SST
// would key the index on the sort-key column, so the re-upsert would not find the rows and the count
// would grow).
TEST_F(PkTabletUnsortWriterE2ETest, test_vertical_compaction_orderby_eager_sst) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<bool> g0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> g1(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> g3(&config::pk_index_eager_build_threshold_bytes, 1);
    ConfigResetGuard<int64_t> g4(&config::vertical_compaction_max_columns_per_group, 1); // force vertical
    ConfigResetGuard<int64_t> g5(&config::lake_pk_compaction_min_input_segments, 2);

    auto load_one = [&](const Chunk& chunk, int64_t txn_id) {
        std::vector<uint32_t> idx(chunk.num_rows());
        for (uint32_t j = 0, n = chunk.num_rows(); j < n; j++) idx[j] = j;
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_profile(&_dummy_runtime_profile)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
    };

    int version = 1;
    for (int i = 0; i < 3; i++) {
        auto chunk = generate_data(100, static_cast<int64_t>(i) * 100); // distinct keys per rowset
        int64_t txn_id = next_id();
        load_one(chunk, txn_id);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Vertical compaction of the separate-sort-key table.
    int64_t compaction_txn_id = next_id();
    auto ctx = std::make_unique<CompactionTaskContext>(compaction_txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    // (a) The eager PK-index SST was built during compaction (not rebuilt lazily at publish).
    ASSIGN_OR_ABORT(auto compaction_txn_log, _tablet_mgr->get_txn_log(tablet_id, compaction_txn_id));
    EXPECT_GT(compaction_txn_log->op_compaction().ssts_size(), 0);

    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn_id).status());
    version++;
    // (b) Row count preserved by compaction.
    EXPECT_EQ(300, read_rows(tablet_id, version));

    // (c) Re-upsert 100 already-present keys [0, 100). If the compaction-built index is keyed on the
    // PK column, these dedup against the existing rows and the count stays 300; a wrong-column SST
    // would miss them and the count would grow to 400.
    auto dup = generate_data(100, 0);
    int64_t reupsert_txn_id = next_id();
    load_one(dup, reupsert_txn_id);
    ASSERT_OK(publish_single_version(tablet_id, version + 1, reupsert_txn_id).status());
    version++;
    EXPECT_EQ(300, read_rows(tablet_id, version));
}

// HORIZONTAL compaction of a separate-sort-key table with eager PK-index build. Compaction of a
// separate-sort-key PK table also routes through the unsort SST writer -- here via
// HorizontalPkTabletWriter::write(data, rssid_rowids), with the real rssid|rowid as the ordering key --
// so it needs its own coverage alongside the vertical path. Force horizontal by keeping all columns in
// one group (vertical_compaction_max_columns_per_group above the column count). Asserts the same
// invariants as the vertical case: (a) the eager SST was built, (b) the row count is preserved, and
// (c) the built PK index is keyed on the PK (re-upserting existing keys dedups instead of growing).
TEST_F(PkTabletUnsortWriterE2ETest, test_horizontal_compaction_orderby_eager_sst) {
    const int64_t tablet_id = _tablet_metadata->id();
    ConfigResetGuard<bool> g0(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> g1(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> g3(&config::pk_index_eager_build_threshold_bytes, 1);
    ConfigResetGuard<int64_t> g4(&config::vertical_compaction_max_columns_per_group, 100); // force horizontal
    ConfigResetGuard<int64_t> g5(&config::lake_pk_compaction_min_input_segments, 2);

    auto load_one = [&](const Chunk& chunk, int64_t txn_id) {
        std::vector<uint32_t> idx(chunk.num_rows());
        for (uint32_t j = 0, n = chunk.num_rows(); j < n; j++) idx[j] = j;
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_profile(&_dummy_runtime_profile)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
    };

    int version = 1;
    for (int i = 0; i < 3; i++) {
        auto chunk = generate_data(100, static_cast<int64_t>(i) * 100); // distinct keys per rowset
        int64_t txn_id = next_id();
        load_one(chunk, txn_id);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    int64_t compaction_txn_id = next_id();
    auto ctx = std::make_unique<CompactionTaskContext>(compaction_txn_id, tablet_id, version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    // (a) The eager PK-index SST was built during horizontal compaction.
    ASSIGN_OR_ABORT(auto compaction_txn_log, _tablet_mgr->get_txn_log(tablet_id, compaction_txn_id));
    EXPECT_GT(compaction_txn_log->op_compaction().ssts_size(), 0);

    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn_id).status());
    version++;
    // (b) Row count preserved by compaction.
    EXPECT_EQ(300, read_rows(tablet_id, version));

    // (c) Re-upsert already-present keys [0, 100): a PK-keyed index dedups them (count stays 300); a
    // wrong-column SST would miss them and the count would grow.
    auto dup = generate_data(100, 0);
    int64_t reupsert_txn_id = next_id();
    load_one(dup, reupsert_txn_id);
    ASSERT_OK(publish_single_version(tablet_id, version + 1, reupsert_txn_id).status());
    version++;
    EXPECT_EQ(300, read_rows(tablet_id, version));
}

} // namespace starrocks::lake
