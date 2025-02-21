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

#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"

#include <gtest/gtest.h>

#include <memory>

#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/workgroup/work_group.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

class MemLimitedChunkQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        TUniqueId dummy_query_id = generate_uuid();
        auto path = config::storage_root_path + "/spill_test_data/" + print_id(dummy_query_id);
        auto fs = FileSystem::Default();
        ASSERT_OK(fs->create_dir_recursive(path));
        LOG(INFO) << "path: " << path;

        dummy_wg = std::make_shared<workgroup::WorkGroup>("default_wg", workgroup::WorkGroup::DEFAULT_WG_ID,
                                                          workgroup::WorkGroup::DEFAULT_VERSION, 4, 100.0, 0, 1.0,
                                                          workgroup::WorkGroupType::WG_DEFAULT);
        dummy_wg->init();
        dummy_wg->set_shared_executors(ExecEnv::GetInstance()->workgroup_manager()->shared_executors());

        dummy_dir_mgr = std::make_unique<spill::DirManager>();
        ASSERT_OK(dummy_dir_mgr->init(path));

        dummy_block_mgr = std::make_unique<spill::LogBlockManager>(dummy_query_id, dummy_dir_mgr.get());

        dummy_fragment_ctx.set_workgroup(dummy_wg);
        dummy_query_ctx = std::make_shared<QueryContext>();

        dummy_runtime_state.set_fragment_ctx(&dummy_fragment_ctx);
        dummy_runtime_state.set_query_ctx(dummy_query_ctx.get());
    }
    void TearDown() override {}

protected:
    std::shared_ptr<QueryContext> dummy_query_ctx;
    FragmentContext dummy_fragment_ctx;
    RuntimeState dummy_runtime_state;
    RuntimeProfile dummy_runtime_profile{"dummy"};
    workgroup::WorkGroupPtr dummy_wg;

    std::unique_ptr<spill::DirManager> dummy_dir_mgr;
    std::unique_ptr<spill::LogBlockManager> dummy_block_mgr;
};

class AutoIncChunkBuilder {
public:
    AutoIncChunkBuilder(size_t chunk_size = 4096) : _chunk_size(chunk_size) {}

    ChunkPtr get_next() {
        ChunkPtr chunk = std::make_shared<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), false);
        for (size_t i = 0; i < _chunk_size; i++) {
            col->append_datum(Datum(_next_value++));
        }
        chunk->append_column(std::move(col), 0);
        return chunk;
    }
    size_t _next_value = 0;
    size_t _chunk_size;
};

void wait_flush_task_done(MemLimitedChunkQueue& queue) {
    while (queue._has_flush_io_task) {
        usleep(1000);
    }
}

TEST_F(MemLimitedChunkQueueTest, test_iterator) {
    MemLimitedChunkQueue::Options options;
    options.block_size = 1024 * 128;
    options.memory_limit = INT64_MAX; // disable spill
    options.block_manager = dummy_block_mgr.get();
    MemLimitedChunkQueue queue(&dummy_runtime_state, 1, options);
    ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));

    // 32k per chunk,
    AutoIncChunkBuilder builder;

    for (size_t i = 0; i < 100; i++) {
        auto chunk = builder.get_next();
        ASSERT_OK(queue.push(chunk));
    }
    MemLimitedChunkQueue::Iterator iter = MemLimitedChunkQueue::Iterator(queue._head, 0);

    size_t acc_rows = 0, acc_bytes = 0;
    for (size_t i = 0; i < 100; i++) {
        ASSERT_TRUE(iter.valid());
        MemLimitedChunkQueue::Cell* cell = iter.get_cell();
        if (i == 0) {
            // dummy cell
            ASSERT_TRUE(cell->chunk == nullptr);
            ASSERT_EQ(cell->accumulated_rows, 0);
            ASSERT_EQ(cell->accumulated_bytes, 0);
        } else {
            ASSERT_EQ(cell->chunk->num_rows(), 4096);
            acc_rows += cell->chunk->num_rows();
            acc_bytes += cell->chunk->memory_usage();
            ASSERT_EQ(cell->accumulated_rows, acc_rows);
            ASSERT_EQ(cell->accumulated_bytes, acc_bytes);
        }
        iter.move_to_next();
    }
    ASSERT_FALSE(iter.has_next());
}

TEST_F(MemLimitedChunkQueueTest, test_push_pop_without_spill) {
    MemLimitedChunkQueue::Options options;
    options.block_size = 1024 * 128;
    options.memory_limit = INT64_MAX; // disable spill
    options.block_manager = dummy_block_mgr.get();
    int32_t consumer_number = 3;
    MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
    ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
    // set opened consumer number
    for (int32_t i = 0; i < consumer_number; i++) {
        queue.open_consumer(i);
    }
    // 32k per chunk,
    AutoIncChunkBuilder builder;

    for (size_t i = 0; i < 100; i++) {
        auto chunk = builder.get_next();
        ASSERT_OK(queue.push(chunk));
    }

    size_t expected_head_acc_rows = 0;
    size_t expected_head_acc_bytes = 0;
    for (size_t i = 0; i < 100; i++) {
        for (int32_t consumer_idx = 0; consumer_idx < consumer_number; consumer_idx++) {
            ASSERT_TRUE(queue.can_pop(consumer_idx));
            ASSIGN_OR_ABORT(auto chunk, queue.pop(consumer_idx));
            ASSERT_EQ(chunk->num_rows(), 4096);
            if (consumer_idx < consumer_number - 1) {
                ASSERT_EQ(queue._head_accumulated_rows, expected_head_acc_rows);
                ASSERT_EQ(queue._head_accumulated_bytes, expected_head_acc_bytes);
            }
            LOG(INFO) << "consumer " << consumer_idx << ": " << chunk->debug_row(0) << ", " << chunk->debug_row(4095);
        }
        expected_head_acc_rows += 4096;
        expected_head_acc_bytes += 4096 * 8;

        ASSERT_EQ(queue._head_accumulated_rows, expected_head_acc_rows);
        ASSERT_EQ(queue._head_accumulated_bytes, expected_head_acc_bytes);
    }
}

TEST_F(MemLimitedChunkQueueTest, test_back_pressure) {
    MemLimitedChunkQueue::Options options;
    options.block_size = 1024 * 128;
    options.memory_limit = 1024 * 256;
    options.max_unconsumed_bytes = 1024 * 64;
    options.block_manager = dummy_block_mgr.get();
    int32_t consumer_number = 1;
    MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
    ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
    // set opened consumer number
    for (int32_t i = 0; i < consumer_number; i++) {
        queue.open_consumer(i);
    }
    // 32k per chunk,
    AutoIncChunkBuilder builder;
    ASSERT_TRUE(queue.can_push());
    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_TRUE(queue.can_push());
    ASSERT_OK(queue.push(builder.get_next()));
    // unconsumed bytes is 16k, can't push before consuming
    ASSERT_FALSE(queue.can_push());

    // consume
    ASSERT_TRUE(queue.can_pop(0));
    ASSERT_OK(queue.pop(0));
    ASSERT_TRUE(queue.can_push());
}

TEST_F(MemLimitedChunkQueueTest, test_push_with_flush) {
    MemLimitedChunkQueue::Options options;
    options.block_size = 1024 * 64;
    options.memory_limit = 1024 * 64;
    options.max_unconsumed_bytes = INT64_MAX;
    options.block_manager = dummy_block_mgr.get();
    // @TODO test different block size
    int32_t consumer_number = 3;
    MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
    ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
    // set opened consumer number
    for (int32_t i = 0; i < consumer_number; i++) {
        queue.open_consumer(i);
    }
    // 32k per chunk,
    AutoIncChunkBuilder builder;

    // push some chunks, trigger spill, submit spill
    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_OK(queue.push(builder.get_next()));

    // only one block, cannot trigger flush
    ASSERT_TRUE(queue.can_push());

    ASSERT_OK(queue.push(builder.get_next()));
    // after push the third chunk, there are 2 blocks and the 1st should be flushed
    std::atomic_int submitted_flush_tasks = 0, finished_flush_task = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::before_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "before execute flush task";
        ASSERT_EQ(queue._has_flush_io_task, true);
        submitted_flush_tasks++;
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "after execute flush task";
        finished_flush_task++;
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSERT_FALSE(queue.can_push());
    // wait flush task finished
    wait_flush_task_done(queue);
    ASSERT_EQ(submitted_flush_tasks, 1);
    ASSERT_EQ(finished_flush_task, 1);

    ASSERT_TRUE(queue.can_push());

    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_TRUE(queue.can_push());
    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_FALSE(queue.can_push());
    wait_flush_task_done(queue);
    ASSERT_EQ(submitted_flush_tasks, 2);
    ASSERT_EQ(finished_flush_task, 2);
}

TEST_F(MemLimitedChunkQueueTest, test_flush_with_pop) {
    // push -> submit flush task-> (consume some data) -> execute flush task but no data to flush
    MemLimitedChunkQueue::Options options;
    options.block_size = 1024 * 64;
    options.memory_limit = 1024 * 64;
    options.max_unconsumed_bytes = INT64_MAX;
    options.block_manager = dummy_block_mgr.get();
    int32_t consumer_number = 1;
    MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
    ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
    // set opened consumer number
    for (int32_t i = 0; i < consumer_number; i++) {
        queue.open_consumer(i);
    }
    // 32k per chunk,
    AutoIncChunkBuilder builder;

    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_OK(queue.push(builder.get_next()));

    // only one block, cannot trigger flush
    ASSERT_TRUE(queue.can_push());

    ASSERT_OK(queue.push(builder.get_next()));

    std::atomic_int submitted_flush_tasks = 0, finished_flush_task = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::before_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "before execute flush task";
        ASSERT_EQ(queue._has_flush_io_task, true);
        submitted_flush_tasks++;
        // before execute flush task, consumer consume some data
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_calculate_max_serialize_size", [&](void* arg) {
        size_t max_serialize_size = *((size_t*)arg);
        ASSERT_EQ(max_serialize_size, 0);
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_flush_block", [&](void* arg) {
        MemLimitedChunkQueue::Block* block = (MemLimitedChunkQueue::Block*)arg;
        ASSERT_TRUE(block->block == nullptr);
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "after execute flush task";
        finished_flush_task++;
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSERT_FALSE(queue.can_push());
    wait_flush_task_done(queue);

    ASSERT_EQ(submitted_flush_tasks, 1);
    ASSERT_EQ(finished_flush_task, 1);

    ASSERT_TRUE(queue.can_push());
    ASSERT_OK(queue.push(builder.get_next()));
    ASSERT_TRUE(queue.can_push());
    ASSERT_OK(queue.push(builder.get_next()));

    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::before_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "before execute flush task";
        ASSERT_EQ(queue._has_flush_io_task, true);
        submitted_flush_tasks++;
        // before execute flush task, consumer consume some data
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_calculate_max_serialize_size", [&](void* arg) {
        size_t max_serialize_size = *((size_t*)arg);
        // only one chunk should be flushed
        ASSERT_EQ(max_serialize_size, 34844);
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_flush_block", [&](void* arg) {
        MemLimitedChunkQueue::Block* block = (MemLimitedChunkQueue::Block*)arg;
        ASSERT_TRUE(block->block != nullptr);
    });
    SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_execute_flush_task", [&](void* arg) {
        LOG(INFO) << "after execute flush task";
        finished_flush_task++;
    });
    ASSERT_FALSE(queue.can_push());
    wait_flush_task_done(queue);
    ASSERT_EQ(submitted_flush_tasks, 2);
    ASSERT_EQ(finished_flush_task, 2);
}

TEST_F(MemLimitedChunkQueueTest, test_load) {
    // 3 consumers, consume progress is different, flush task, some data not in memory, load

    // case 1
    {
        // 1 consumer, some block not in memory, trigger load task
        MemLimitedChunkQueue::Options options;
        options.block_size = 1024 * 64;
        options.memory_limit = 1024 * 64;
        options.max_unconsumed_bytes = INT64_MAX;
        options.block_manager = dummy_block_mgr.get();
        int32_t consumer_number = 1;
        MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
        ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
        // set opened consumer number
        for (int32_t i = 0; i < consumer_number; i++) {
            queue.open_consumer(i);
        }
        queue.open_producer();
        // 32k per chunk,
        AutoIncChunkBuilder builder;

        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_FALSE(queue.can_push());
        wait_flush_task_done(queue);
        int32_t submitted_load_tasks = 0;
        SyncPoint::GetInstance()->EnableProcessing();
        SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::before_execute_load_task",
                                              [&](void* arg) { submitted_load_tasks++; });
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearAllCallBacks();
            SyncPoint::GetInstance()->DisableProcessing();
        });
        // consume to load
        // 1st block is flushed, should trigger a load task
        ASSERT_FALSE(queue.can_pop(0));
        // wait until can pop
        while (!queue.can_pop(0)) {
            bthread_usleep(1000);
        }
        ASSERT_OK(queue.pop(0));
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));

        //all data consumed
        ASSERT_FALSE(queue.can_pop(0));
        ASSERT_EQ(submitted_load_tasks, 1);
    }

    // case 2
    // multi consumers, some block not in memory, trigger load task, some consumer read the same block,
    {
        MemLimitedChunkQueue::Options options;
        options.block_size = 1024 * 64;
        options.memory_limit = 1024 * 64;
        options.max_unconsumed_bytes = INT64_MAX;
        options.block_manager = dummy_block_mgr.get();
        int32_t consumer_number = 2;
        MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
        ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
        // set opened consumer number
        for (int32_t i = 0; i < consumer_number; i++) {
            queue.open_consumer(i);
        }
        queue.open_producer();
        // 32k per chunk,
        AutoIncChunkBuilder builder;

        // push 3 chunks, consumer 0 consume 1 chunk, consumer 2 do nothing
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));

        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));

        // trigger flush
        ASSERT_FALSE(queue.can_push());
        // wait flush done
        wait_flush_task_done(queue);
        int32_t submitted_load_tasks = 0;
        SyncPoint::GetInstance()->EnableProcessing();
        SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::before_execute_load_task",
                                              [&](void* arg) { submitted_load_tasks++; });
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearAllCallBacks();
            SyncPoint::GetInstance()->DisableProcessing();
        });
        // consumer 1 consume
        ASSERT_FALSE(queue.can_pop(1));
        SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::can_pop::before_submit_load_task", [&](void* arg) {
            MemLimitedChunkQueue::Block* block = (MemLimitedChunkQueue::Block*)arg;
            ASSERT_TRUE(block->has_load_task);
        });
        ASSERT_FALSE(queue.can_pop(0));
        while (!queue.can_pop(1)) {
            bthread_usleep(1000);
        }
        // these 2 consumers consume the same block, so consumer 0 can pop too, only 1 load task is triggered
        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_EQ(submitted_load_tasks, 1);
    }

    {
        // case 3, multi consumer, the fast is very fast, loaded block should evcit
        MemLimitedChunkQueue::Options options;
        options.block_size = 1024 * 32;
        options.memory_limit = 1024 * 32;
        options.max_unconsumed_bytes = INT64_MAX;
        options.block_manager = dummy_block_mgr.get();
        int32_t consumer_number = 2;
        MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
        ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
        // set opened consumer number
        for (int32_t i = 0; i < consumer_number; i++) {
            queue.open_consumer(i);
        }
        queue.open_producer();
        // 32k per chunk,
        AutoIncChunkBuilder builder;

        // force push and wait spill
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_TRUE(queue.can_push());
        ASSERT_OK(queue.push(builder.get_next()));
        for (int i = 0; i < 10; i++) {
            ASSERT_FALSE(queue.can_push());
            wait_flush_task_done(queue);
            ASSERT_OK(queue.push(builder.get_next()));
        }
        for (int i = 0; i < 10; i++) {
            while (!queue.can_pop(0)) {
                bthread_usleep(1000);
            }
            ASSERT_OK(queue.pop(0));
        }
        for (int i = 0; i < 10; i++) {
            while (!queue.can_pop(1)) {
                bthread_usleep(1000);
            }
            ASSERT_OK(queue.pop(1));
        }
    }
}

TEST_F(MemLimitedChunkQueueTest, test_concurrent_load_flush) {
    {
        // timeline: can_pop -> flush -> pop
        // in this case, flush won't flush the block that is about to read
        MemLimitedChunkQueue::Options options;
        options.block_size = 1024 * 32;
        options.memory_limit = 1024 * 32;
        options.max_unconsumed_bytes = INT64_MAX;
        options.block_manager = dummy_block_mgr.get();
        int32_t consumer_number = 2;
        MemLimitedChunkQueue queue(&dummy_runtime_state, consumer_number, options);
        ASSERT_OK(queue.init_metrics(&dummy_runtime_profile));
        // set opened consumer number
        for (int32_t i = 0; i < consumer_number; i++) {
            queue.open_consumer(i);
        }
        queue.open_producer();
        // 32k per chunk,
        AutoIncChunkBuilder builder;

        ASSERT_OK(queue.push(builder.get_next()));
        SyncPoint::GetInstance()->EnableProcessing();
        SyncPoint::GetInstance()->LoadDependency(
                {{"MemLimitedChunkQueue::can_pop::return_true::1", "MemLimitedChunkQueue::before_execute_flush_task"}});
        SyncPoint::GetInstance()->LoadDependency(
                {{"MemLimitedChunkQueue::after_execute_flush_task", "MemLimitedChunkQueue::pop::before_pop"}});
        SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::flush::after_find_block_to_flush", [&](void* arg) {
            MemLimitedChunkQueue::Block* block = (MemLimitedChunkQueue::Block*)arg;
            // this block is about to be read, won't be flushed
            ASSERT_TRUE(block->has_pending_reader());
        });
        int32_t flushed_blocks = 0;
        SyncPoint::GetInstance()->SetCallBack("MemLimitedChunkQueue::after_flush_block",
                                              [&](void* arg) { flushed_blocks++; });
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearAllCallBacks();
            SyncPoint::GetInstance()->DisableProcessing();
        });
        // after can pop, trigger a flush task
        ASSERT_OK(queue.push(builder.get_next()));
        ASSERT_FALSE(queue.can_push());

        ASSERT_TRUE(queue.can_pop(0));
        ASSERT_OK(queue.pop(0));
        // no block is flushed
        ASSERT_EQ(flushed_blocks, 0);
    }
}

} // namespace starrocks::pipeline
