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

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <shared_mutex>

#include "util/bthreads/bthread_shared_mutex.h"

namespace starrocks {

namespace {

typedef void* (*bthread_fun)(void*);

static void run_in_bthread(bthread_fun fun) {
    bthread_t btid;
    int ret = bthread_start_background(&btid, nullptr, fun, nullptr);
    EXPECT_EQ(0, ret);
    // wait for the thread done and join
    bthread_join(btid, nullptr);
}

static void run_ut(bthread_fun fun) {
    // running in pthread
    fun(nullptr);
    // running in bthread
    run_in_bthread(fun);
}

} // namespace

// ========== # basic sanity testing
static void* basic_sanity_test(void* args) {
    bthreads::BThreadSharedMutex mutex;
    int num = 0;
    {
        std::unique_lock lk(mutex);
        ++num;
    }
    EXPECT_EQ(1, num);

    {
        std::shared_lock lk(mutex);
        --num;
    }
    EXPECT_EQ(0, num);
    return args;
}

TEST(BThreadSharedMutex, basic_sanity) {
    run_ut(basic_sanity_test);
}

// ========== # shared mutex testing
struct shared_ctx {
    std::atomic<int> num_reads = 0;
    bthreads::BThreadSharedMutex mutex;
    int total_num = 0;
};

static void* inc_and_wait(void* args) {
    shared_ctx* ctx = (shared_ctx*)args;
    std::shared_lock lk(ctx->mutex);
    ++ctx->num_reads;
    while (ctx->num_reads < ctx->total_num) {
        // sleep under the lock
        bthread_usleep(1000); // 1000us
    }
    EXPECT_TRUE(lk.owns_lock());
    EXPECT_EQ(ctx->total_num, ctx->num_reads);
    return args;
}

// 10 bthreads acquire the shared lock and increase the counter, hold the lock until the counter meets the num of threads
static void* shared_mutex_test(void* args) {
    constexpr int num_threads = 10;
    // reset statistics
    shared_ctx ctx;
    ctx.total_num = num_threads;

    bthread_t btid[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        int ret = bthread_start_background(btid + i, nullptr, inc_and_wait, &ctx);
        EXPECT_EQ(0, ret);
    }
    for (int i = 0; i < num_threads; ++i) {
        bthread_join(btid[i], nullptr);
    }
    return args;
}

TEST(BThreadSharedMutex, shared_mutex) {
    run_ut(shared_mutex_test);
}

// ========== # shared and exclusive mutex testing
struct mix_ctx {
    std::atomic<bool> started = false;
    std::atomic<int> num_reads = 0;
    std::atomic<int> num_writes = 0;
    bthreads::BThreadSharedMutex mutex;
};

static void* exclusive_entrypoint(void* args) {
    mix_ctx* ctx = (mix_ctx*)args;
    while (!ctx->started) {
        // busy wait until started
        bthread_usleep(10);
    }
    std::unique_lock lk(ctx->mutex);
    // when I get the exclusive lock, there is no other reads or writes
    EXPECT_EQ(0, ctx->num_reads);
    EXPECT_EQ(0, ctx->num_writes);
    ++ctx->num_writes;
    // sleep 1000us and exit
    bthread_usleep(1000);
    --ctx->num_writes;
    EXPECT_EQ(0, ctx->num_reads);
    EXPECT_EQ(0, ctx->num_writes);
    return args;
}

static void* shared_entrypoint(void* args) {
    mix_ctx* ctx = (mix_ctx*)args;
    while (!ctx->started) {
        // busy wait until started
        bthread_usleep(10);
    }
    std::shared_lock lk(ctx->mutex);
    // when I get the shared lock, there is no other write at all, but possibly has other reads
    EXPECT_EQ(0, ctx->num_writes);
    EXPECT_LE(0, ctx->num_reads);
    ++ctx->num_reads;
    // sleep 1000us and exit
    bthread_usleep(1000);
    --ctx->num_reads;
    EXPECT_EQ(0, ctx->num_writes);
    EXPECT_LE(0, ctx->num_reads);
    return args;
}

static void* mixture_mutex_test(void* args) {
    mix_ctx ctx;
    constexpr int num_threads = 200;
    bthread_t btid[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        int ret;
        if (i & 0x1) {
            ret = bthread_start_background(btid + i, nullptr, shared_entrypoint, &ctx);
        } else {
            ret = bthread_start_background(btid + i, nullptr, exclusive_entrypoint, &ctx);
        }
        EXPECT_EQ(0, ret);
    }
    // all start at once
    ctx.started = true;
    for (int i = 0; i < num_threads; ++i) {
        bthread_join(btid[i], nullptr);
    }
    EXPECT_EQ(0, ctx.num_reads);
    EXPECT_EQ(0, ctx.num_writes);
    return args;
}

TEST(BThreadSharedMutex, rw_mutex) {
    run_ut(mixture_mutex_test);
}

} // namespace starrocks
