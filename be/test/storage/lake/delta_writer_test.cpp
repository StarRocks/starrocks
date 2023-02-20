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

#include "storage/lake/delta_writer.h"

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class DeltaWriterTest : public testing::Test {
public:
    DeltaWriterTest() {
        _parent_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_tracker = std::make_unique<MemTracker>(-1, "", _parent_mem_tracker.get());
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 1024 * 1024);

        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
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
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

protected:
    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        tablet.delete_txn_log(_txn_id);
        _txn_id++;
        (void)fs::remove_all(kTestGroupPath);
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
        return Chunk({c0, c1}, _schema);
    }

    constexpr static const char* const kTestGroupPath = "test_lake_delta_writer";

    std::unique_ptr<MemTracker> _parent_mem_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _txn_id = 123;
    int64_t _partition_id = 456;
};

TEST_F(DeltaWriterTest, test_open) {
    // Invalid tablet id
    {
        auto tablet_id = -1;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr,
                                                _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        delta_writer->close();
    }
}

TEST_F(DeltaWriterTest, test_write) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // Write and flush
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->flush_async());
    // Write
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // finish
    ASSERT_OK(delta_writer->finish());
    // close
    delta_writer->close();

    // Check TxnLog
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(2, txnlog->op_write().rowset().segments_size());
    ASSERT_TRUE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(2 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);

    // Check segment file
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    auto path0 = _location_provider->segment_location(tablet_id, txnlog->op_write().rowset().segments(0));
    auto path1 = _location_provider->segment_location(tablet_id, txnlog->op_write().rowset().segments(1));

    ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, path0, 0, _tablet_schema.get()));
    ASSIGN_OR_ABORT(auto seg1, Segment::open(fs, path1, 1, _tablet_schema.get()));

    OlapReaderStatistics statistics;
    SegmentReadOptions opts;
    opts.fs = fs;
    opts.tablet_id = tablet_id;
    opts.stats = &statistics;
    opts.chunk_size = 1024;

    auto check_segment = [&](const SegmentSharedPtr& segment) {
        ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(*_schema, opts));
        auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
        ASSERT_OK(seg_iter->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(kChunkSize, read_chunk_ptr->num_rows());
        for (int i = 0; i < kChunkSize; i++) {
            EXPECT_EQ(i, read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(i * 3, read_chunk_ptr->get(i)[1].get_int32());
        }
        read_chunk_ptr->reset();
        ASSERT_TRUE(seg_iter->get_next(read_chunk_ptr.get()).is_end_of_file());
        seg_iter->close();
    };

    check_segment(seg0);
    check_segment(seg1);
}

TEST_F(DeltaWriterTest, test_close) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // write()
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // close() without finish()
    delta_writer->close();

    // TxnLog should not exist
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSERT_TRUE(tablet.get_txn_log(_txn_id).status().is_not_found());

    // Segment file should not exist
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    ASSERT_OK(fs->iterate_dir(join_path(kTestGroupPath, kMetadataDirectoryName), [&](std::string_view name) {
        EXPECT_TRUE(is_tablet_metadata(name)) << name;
        return true;
    }));
}

TEST_F(DeltaWriterTest, test_finish_without_write_txn_log) {
    // Prepare data for writing
    static const int kChunkSize = 1;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // write()
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->finish(DeltaWriter::kDontWriteTxnLog));
    delta_writer->close();

    // TxnLog should not exist
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSERT_TRUE(tablet.get_txn_log(_txn_id).status().is_not_found());

    // Segment file should exist
    int segment_files = 0;
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    ASSERT_OK(fs->iterate_dir(join_path(kTestGroupPath, kSegmentDirectoryName), [&](std::string_view name) {
        segment_files += is_segment(name);
        return true;
    }));
    ASSERT_EQ(1, segment_files) << segment_files;
}

TEST_F(DeltaWriterTest, test_empty_write) {
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->finish());
    delta_writer->close();

    // Check TxnLog
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(0, txnlog->op_write().rowset().segments_size());
    ASSERT_FALSE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(0, txnlog->op_write().rowset().num_rows());
    ASSERT_EQ(0, txnlog->op_write().rowset().data_size());
}

TEST_F(DeltaWriterTest, test_negative_txn_id) {
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, -1, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());
    ASSERT_ERROR(delta_writer->finish());
    delta_writer->close();
}

TEST_F(DeltaWriterTest, test_memory_limit_unreached) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // Write three times
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // finish
    ASSERT_OK(delta_writer->finish());
    // close
    delta_writer->close();

    // Check TxnLog: there should only one segment file
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(1, txnlog->op_write().rowset().segments_size());
    ASSERT_FALSE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(3 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);
}

TEST_F(DeltaWriterTest, test_reached_memory_limit) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Change the memory limit to a very small value
    _mem_tracker->set_limit(1);
    DeferOp defer([&]() { _mem_tracker->set_limit(-1); });

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // Write tree times
    _mem_tracker->consume(10);
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // finish
    ASSERT_OK(delta_writer->finish());
    // close
    delta_writer->close();

    // Check TxnLog: there should be three segment files
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(3, txnlog->op_write().rowset().segments_size());
    ASSERT_TRUE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(3 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);
}

TEST_F(DeltaWriterTest, test_reached_parent_memory_limit) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Change the memory limit to a very small value
    _parent_mem_tracker->set_limit(1);
    DeferOp defer([&]() { _parent_mem_tracker->set_limit(-1); });

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // Write tree times
    _parent_mem_tracker->consume(10);
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // finish
    ASSERT_OK(delta_writer->finish());
    // close
    delta_writer->close();

    // Check TxnLog: there should be three segment files
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(3, txnlog->op_write().rowset().segments_size());
    ASSERT_TRUE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(3 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);
}

TEST_F(DeltaWriterTest, test_memtable_full) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Change the memtable capacity to a very small value
    auto backup = config::write_buffer_size;
    config::write_buffer_size = 1;
    DeferOp defer([&]() { config::write_buffer_size = backup; });

    // Create and open DeltaWriter
    auto tablet_id = _tablet_metadata->id();
    auto delta_writer =
            DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, _mem_tracker.get());
    ASSERT_OK(delta_writer->open());

    // Write three times
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
    // finish
    ASSERT_OK(delta_writer->finish());
    // close
    delta_writer->close();

    // Check TxnLog: there should be three segment files
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(_txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(_txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(3, txnlog->op_write().rowset().segments_size());
    ASSERT_TRUE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(3 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);
}

} // namespace starrocks::lake
