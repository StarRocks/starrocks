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
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/bthreads/util.h"
#include "util/countdown_latch.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeAsyncDeltaWriterTest : public TestBase {
    using Options = AsyncDeltaWriter::Options;

public:
    LakeAsyncDeltaWriterTest() : TestBase(kTestDirectory), _partition_id(next_id()) {
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
        std::iota(v0.begin(), v0.end(), 0);
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        std::generate(v1.begin(), v1.end(), [&, n = 0]() mutable { return v0[n++] * 3; });

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({c0, c1}, _schema);
    }

    struct FunctorArg {
        std::function<void()> func;
    };

    static void* bthread_func(void* arg) {
        auto func_arg = static_cast<FunctorArg*>(arg);
        func_arg->func();
        delete func_arg;
        return nullptr;
    }

    // In a real production environment, AsyncDeltaWriter is called in a bthread, so here
    // we let the tests run in a bthread as well.
    bthread_t run_in_bthread(std::function<void()> func) {
        ASSIGN_OR_ABORT(auto bid, bthreads::start_bthread(std::move(func)));
        return bid;
    }

    void run_in_bthread_and_join(std::function<void()> func) {
        auto st = bthreads::start_bthread_and_join(std::move(func));
        CHECK(st.ok()) << st;
    }

    constexpr static const char* const kTestDirectory = "test_lake_async_delta_writer";

    int64_t _partition_id;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeAsyncDeltaWriterTest, test_open) {
    run_in_bthread_and_join([this]() {
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
                                                       .set_index_id(_tablet_schema->id())
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
                                                       .set_index_id(_tablet_schema->id())
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
                                                       .set_index_id(_tablet_schema->id())
                                                       .build());
            auto t1 = run_in_bthread([&]() {
                for (int i = 0; i < 10000; i++) {
                    ASSERT_OK(delta_writer->open());
                }
            });
            auto t2 = run_in_bthread([&]() {
                for (int i = 0; i < 10000; i++) {
                    ASSERT_OK(delta_writer->open());
                }
            });
            bthread_join(t1, nullptr);
            bthread_join(t2, nullptr);
            delta_writer->close();
        }
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_write) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        // Call open() again
        ASSERT_OK(delta_writer->open());

        CountDownLatch latch(1);
        // Write
        delta_writer->write(Options(), &chunk0, indexes.data(), indexes.size(),
                            [&](const Status& st) { ASSERT_OK(st); });
        // Write
        delta_writer->write(Options(), &chunk0, indexes.data(), indexes.size(),
                            [&](const Status& st) { ASSERT_OK(st); });
        // finish
        delta_writer->finish(Options(), [&](const Status& st) {
            ASSERT_OK(st);
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

        ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, path0, 0, _tablet_schema));

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
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_write_concurrently) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
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
                delta_writer->write(Options(), &chunk0, indexes.data(), indexes.size(),
                                    [&](const Status& st) { ASSERT_OK(st); });
            }
        };

        std::vector<bthread_t> threads;
        for (int i = 0; i < kNumThreads; i++) {
            threads.emplace_back(run_in_bthread(f));
        }
        started.store(true);
        for (auto& t : threads) {
            CHECK_EQ(0, bthread_join(t, nullptr));
        }

        // finish
        CountDownLatch latch(1);
        delta_writer->finish(Options(), [&](const Status& st) {
            ASSERT_OK(st);
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

        ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, path0, 0, _tablet_schema));

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
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_write_after_close) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        // close()
        delta_writer->close();

        auto tid = std::this_thread::get_id();
        // write()
        delta_writer->write(Options(), &chunk0, indexes.data(), indexes.size(), [&](const Status& st) {
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
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_finish_after_close) {
    run_in_bthread_and_join([this]() {
        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        // close()
        delta_writer->close();

        auto tid = std::this_thread::get_id();
        // finish()
        delta_writer->finish(Options(), [&](const Status& st) {
            ASSERT_ERROR(st);
            ASSERT_EQ(tid, std::this_thread::get_id());
        });
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_close) {
    run_in_bthread_and_join([this]() {
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
                                                       .set_index_id(_tablet_schema->id())
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
                                                       .set_index_id(_tablet_schema->id())
                                                       .build());
            ASSERT_OK(delta_writer->open());
            auto t1 = run_in_bthread([&]() {
                for (int i = 0; i < 10000; i++) {
                    delta_writer->close();
                }
            });
            auto t2 = run_in_bthread([&]() {
                for (int i = 0; i < 10000; i++) {
                    delta_writer->close();
                }
            });
            bthread_join(t1, nullptr);
            bthread_join(t2, nullptr);
        }
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_open_after_close) {
    run_in_bthread_and_join([this]() {
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        delta_writer->close();
        auto st = delta_writer->open();
        ASSERT_FALSE(st.ok());
        ASSERT_EQ("AsyncDeltaWriter has been closed", st.message());
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_concurrent_write_and_close) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        SyncPoint::GetInstance()->EnableProcessing();

        SyncPoint::GetInstance()->LoadDependency(
                {{"AsyncDeltaWriterImpl::close:1", "AsyncDeltaWriterImpl::execute:1"}});

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        auto tid = std::this_thread::get_id();
        // write()
        delta_writer->write(Options(), &chunk0, indexes.data(), indexes.size(), [&](const Status& st) {
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
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_concurrency_limit) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        SyncPoint::GetInstance()->SetCallBack("AsyncDeltaWriterImpl::execute:2", [&](void* arg) {
            std::cerr << "Sleeping 1 second\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("AsyncDeltaWriterImpl::execute:2");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        std::atomic<bool> resource_busy = false;
        for (int i = 0; i < 100 && !resource_busy.load(std::memory_order_relaxed); i++) {
            delta_writer->write(Options{.timeout_ms = 500}, &chunk0, indexes.data(), indexes.size(),
                                [&](const Status& st) {
                                    if (st.is_resource_busy()) resource_busy.store(true, std::memory_order_relaxed);
                                });
        }
        EXPECT_TRUE(resource_busy.load(std::memory_order_relaxed));
        delta_writer->close();
    });
}

TEST_F(LakeAsyncDeltaWriterTest, test_timeout) {
    run_in_bthread_and_join([this]() {
        // Prepare data for writing
        static const int kChunkSize = 128;
        auto chunk0 = generate_data(kChunkSize);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        SyncPoint::GetInstance()->SetCallBack("AsyncDeltaWriterImpl::execute:2", [&](void* arg) {
            std::cerr << "Sleeping 2 seconds\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("AsyncDeltaWriterImpl::execute:2");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        // Create and open DeltaWriter
        auto txn_id = next_id();
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, AsyncDeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_index_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());

        std::binary_semaphore sem(0);
        Status res;
        delta_writer->write(Options{.timeout_ms = 1000}, &chunk0, indexes.data(), indexes.size(),
                            [&](const Status& st) {
                                res = std::move(st);
                                sem.release(1);
                            });
        sem.acquire();
        EXPECT_TRUE(res.is_time_out()) << res;
        delta_writer->close();
    });
}

} // namespace starrocks::lake
