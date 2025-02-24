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

#include "storage/lake/async_delta_writer.h"

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/load_spill_block_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/countdown_latch.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeAsyncDeltaWriterTest : public TestBase {
public:
    LakeAsyncDeltaWriterTest() : TestBase(kTestDirectory), _partition_id(next_id()) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

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

    constexpr static const char* const kTestDirectory = "test_lake_async_delta_writer";

    int64_t _partition_id;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

TEST_F(LakeAsyncDeltaWriterTest, test_open) {
    // Invalid tablet id
    {
        auto txn_id = next_id();
        auto tablet_id = -1;
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        delta_writer->close();
    }
    // Call open() multiple times
    {
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->open());
        delta_writer->close();
    }
    // Call open() concurrently
    {
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        auto t1 = std::thread([&]() {
            for (int i = 0; i < 10000; i++) {
                ASSERT_OK(delta_writer->open());
            }
        });
        auto t2 = std::thread([&]() {
            for (int i = 0; i < 10000; i++) {
                ASSERT_OK(delta_writer->open());
            }
        });
        t1.join();
        t2.join();
        delta_writer->close();
    }
}

TEST_F(LakeAsyncDeltaWriterTest, test_write) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());
    // Call open() again
    ASSERT_OK(delta_writer->open());

    CountDownLatch latch(1);
    // Write
    delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) { ASSERT_OK(st); });
    // Write
    delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) { ASSERT_OK(st); });
    // finish
    delta_writer->finish([&](StatusOr<TxnLogPtr> res) {
        ASSERT_TRUE(res.ok()) << res.ok();
        latch.count_down();
    });

    latch.wait();

    // close
    delta_writer->close();

    // Check TxnLog
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(1, txnlog->op_write().rowset().segments_size());
    ASSERT_FALSE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(2 * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);

    // Check segment file
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    auto path0 = _tablet_mgr->segment_location(tablet_id, txnlog->op_write().rowset().segments(0));

    ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, FileInfo{path0}, 0, _tablet_schema));

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
        ASSERT_EQ(2 * kChunkSize, read_chunk_ptr->num_rows());
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            EXPECT_EQ(i / 2, read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(i / 2 * 3, read_chunk_ptr->get(i)[1].get_int32());
        }
        read_chunk_ptr->reset();
        ASSERT_TRUE(seg_iter->get_next(read_chunk_ptr.get()).is_end_of_file());
        seg_iter->close();
    };

    check_segment(seg0);
}

TEST_F(LakeAsyncDeltaWriterTest, test_write_concurrently) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());

    ASSERT_OK(delta_writer->open());

    constexpr int kNumThreads = 3;
    constexpr int kChunksPerThread = 10;
    constexpr int kDuplicatedRows = kNumThreads * kChunksPerThread;

    std::atomic<int> started{false};
    auto f = [&]() {
        while (!started.load()) {
            std::this_thread::yield();
        }
        for (int i = 0; i < kChunksPerThread; i++) {
            delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) { ASSERT_OK(st); });
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back(f);
    }
    started.store(true);
    for (auto& t : threads) {
        t.join();
    }

    // finish
    CountDownLatch latch(1);
    delta_writer->finish([&](StatusOr<TxnLogPtr> res) {
        ASSERT_TRUE(res.ok()) << res.status();
        latch.count_down();
    });

    latch.wait();

    // close
    delta_writer->close();

    // Check TxnLog
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(txn_id));
    ASSERT_EQ(tablet_id, txnlog->tablet_id());
    ASSERT_EQ(txn_id, txnlog->txn_id());
    ASSERT_TRUE(txnlog->has_op_write());
    ASSERT_FALSE(txnlog->has_op_compaction());
    ASSERT_FALSE(txnlog->has_op_schema_change());
    ASSERT_TRUE(txnlog->op_write().has_rowset());
    ASSERT_EQ(1, txnlog->op_write().rowset().segments_size());
    ASSERT_FALSE(txnlog->op_write().rowset().overlapped());
    ASSERT_EQ(kNumThreads * kChunksPerThread * kChunkSize, txnlog->op_write().rowset().num_rows());
    ASSERT_GT(txnlog->op_write().rowset().data_size(), 0);

    // Check segment file
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    auto path0 = _tablet_mgr->segment_location(tablet_id, txnlog->op_write().rowset().segments(0));

    ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, FileInfo{path0}, 0, _tablet_schema));

    OlapReaderStatistics statistics;
    SegmentReadOptions opts;
    opts.fs = fs;
    opts.tablet_id = tablet_id;
    opts.stats = &statistics;
    opts.chunk_size = kNumThreads * kChunksPerThread * kChunkSize;

    auto check_segment = [&](const SegmentSharedPtr& segment) {
        ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(*_schema, opts));
        auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, opts.chunk_size);
        ASSERT_OK(seg_iter->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(opts.chunk_size, read_chunk_ptr->num_rows());
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            EXPECT_EQ(i / kDuplicatedRows, read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ((i / kDuplicatedRows) * 3, read_chunk_ptr->get(i)[1].get_int32());
        }
        read_chunk_ptr->reset();
        ASSERT_TRUE(seg_iter->get_next(read_chunk_ptr.get()).is_end_of_file());
        seg_iter->close();
    };

    check_segment(seg0);
}

TEST_F(LakeAsyncDeltaWriterTest, test_write_after_close) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Create and open DeltaWriter
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());

    // close()
    delta_writer->close();

    auto tid = std::this_thread::get_id();
    // write()
    delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) {
        ASSERT_ERROR(st);
        ASSERT_EQ(tid, std::this_thread::get_id());
    });

    // TxnLog should not exist
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    ASSERT_TRUE(tablet.get_txn_log(txn_id).status().is_not_found());

    // Segment file should not exist
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    ASSERT_OK(fs->iterate_dir(join_path(kTestDirectory, kMetadataDirectoryName), [&](std::string_view name) {
        EXPECT_TRUE(is_tablet_metadata(name)) << name;
        return true;
    }));
}

TEST_F(LakeAsyncDeltaWriterTest, test_finish_after_close) {
    // Create and open DeltaWriter
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());

    // close()
    delta_writer->close();

    auto tid = std::this_thread::get_id();
    // finish()
    delta_writer->finish([&](StatusOr<TxnLogPtr> res) {
        ASSERT_FALSE(res.ok());
        ASSERT_EQ(tid, std::this_thread::get_id());
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_close) {
    // Call close() multiple times
    {
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        delta_writer->close();
        delta_writer->close();
    }
    // Call close() concurrently
    {
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        auto t1 = std::thread([&]() {
            for (int i = 0; i < 10000; i++) {
                delta_writer->close();
            }
        });
        auto t2 = std::thread([&]() {
            for (int i = 0; i < 10000; i++) {
                delta_writer->close();
            }
        });
        t1.join();
        t2.join();
    }
}

TEST_F(LakeAsyncDeltaWriterTest, test_open_after_close) {
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());
    delta_writer->close();
    auto st = delta_writer->open();
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("AsyncDeltaWriter has been closed", st.message());
}

TEST_F(LakeAsyncDeltaWriterTest, test_concurrent_write_and_close) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    SyncPoint::GetInstance()->EnableProcessing();

    SyncPoint::GetInstance()->LoadDependency({{"AsyncDeltaWriterImpl::close:1", "AsyncDeltaWriterImpl::execute:1"}});

    // Create and open DeltaWriter
    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());

    auto tid = std::this_thread::get_id();
    // write()
    delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) {
        ASSERT_ERROR(st);
        ASSERT_NE(tid, std::this_thread::get_id());
    });

    // close()
    delta_writer->close();

    // TxnLog should not exist
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    ASSERT_TRUE(tablet.get_txn_log(txn_id).status().is_not_found());

    // Segment file should not exist
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    ASSERT_OK(fs->iterate_dir(join_path(kTestDirectory, kMetadataDirectoryName), [&](std::string_view name) {
        EXPECT_TRUE(is_tablet_metadata(name)) << name;
        return true;
    }));

    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(LakeAsyncDeltaWriterTest, test_flush) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    CountDownLatch latch(1);
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_profile(&_dummy_runtime_profile)
                                               .build());
    ASSERT_OK(delta_writer->open());
    delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) { ASSERT_OK(st); });
    delta_writer->flush([&](const Status& st) {
        ASSERT_OK(st);
        latch.count_down();
    });
    latch.wait();
    while (delta_writer->queueing_memtable_num() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(0, delta_writer->queueing_memtable_num());

    // test flush after close
    SyncPoint::GetInstance()->LoadDependency({{"AsyncDeltaWriterImpl::close:2", "AsyncDeltaWriterImpl::execute:1"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() { SyncPoint::GetInstance()->DisableProcessing(); });
    delta_writer->flush([&](const Status& st) { ASSERT_ERROR(st); });
    delta_writer->close();
}

TEST_F(LakeAsyncDeltaWriterTest, test_block_merger) {
    // Prepare data for writing
    static const int kChunkSize = 128;
    auto chunk0 = generate_data(kChunkSize);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto txn_id = next_id();
    auto tablet_id = _tablet_metadata->id();
    StorageEngine::instance()->load_spill_block_merge_executor()->refresh_max_thread_num();
    CountDownLatch latch(10);
    // flush multi times and generate spill blocks
    int64_t old_val = config::write_buffer_size;
    config::write_buffer_size = 1;
    ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_profile(&_dummy_runtime_profile)
                                               .set_immutable_tablet_size(10000000)
                                               .build());
    ASSERT_OK(delta_writer->open());
    for (int i = 0; i < 10; i++) {
        delta_writer->write(&chunk0, indexes.data(), indexes.size(), [&](const Status& st) { ASSERT_OK(st); });
        delta_writer->flush([&](const Status& st) {
            ASSERT_OK(st);
            latch.count_down();
        });
    }
    latch.wait();
    config::write_buffer_size = old_val;
    // finish
    CountDownLatch latch2(1);
    delta_writer->finish([&](StatusOr<TxnLogPtr> res) {
        ASSERT_TRUE(res.ok()) << res.ok();
        latch2.count_down();
    });
    latch2.wait();
    ASSERT_TRUE(_tablet_mgr->in_writing_data_size(tablet_id) > 0);
}

} // namespace starrocks::lake
