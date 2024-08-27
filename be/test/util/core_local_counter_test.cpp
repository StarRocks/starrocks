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

#include <bvar/bvar.h>
#include <gtest/gtest.h>

#include <atomic>

#include "util/metrics.h"

namespace starrocks {

const size_t OPS_PER_THREAD = 500000;

static void* thread_counter(void* arg) {
    bvar::Adder<uint64_t>* reducer = (bvar::Adder<uint64_t>*)arg;
    butil::Timer timer;
    timer.start();
    for (size_t i = 0; i < OPS_PER_THREAD; ++i) {
        (*reducer) << 2;
    }
    timer.stop();
    return (void*)(timer.n_elapsed());
}

void* add_atomic(void* arg) {
    butil::atomic<uint64_t>* counter = (butil::atomic<uint64_t>*)arg;
    butil::Timer timer;
    timer.start();
    for (size_t i = 0; i < OPS_PER_THREAD / 100; ++i) {
        counter->fetch_add(2, butil::memory_order_relaxed);
    }
    timer.stop();
    return (void*)(timer.n_elapsed());
}

static long start_perf_test_with_atomic(size_t num_thread) {
    butil::atomic<uint64_t> counter(0);
    pthread_t threads[num_thread];
    for (size_t i = 0; i < num_thread; ++i) {
        pthread_create(&threads[i], nullptr, &add_atomic, (void*)&counter);
    }
    long totol_time = 0;
    for (size_t i = 0; i < num_thread; ++i) {
        void* ret;
        pthread_join(threads[i], &ret);
        totol_time += (long)ret;
    }
    long avg_time = totol_time / (OPS_PER_THREAD / 100 * num_thread);
    EXPECT_EQ(2ul * num_thread * OPS_PER_THREAD / 100, counter.load());
    return avg_time;
}

static long start_perf_test_with_adder(size_t num_thread) {
    bvar::Adder<uint64_t> reducer;
    EXPECT_TRUE(reducer.valid());
    pthread_t threads[num_thread];
    for (size_t i = 0; i < num_thread; ++i) {
        pthread_create(&threads[i], nullptr, &thread_counter, (void*)&reducer);
    }
    long totol_time = 0;
    for (size_t i = 0; i < num_thread; ++i) {
        void* ret = nullptr;
        pthread_join(threads[i], &ret);
        totol_time += (long)ret;
    }
    long avg_time = totol_time / (OPS_PER_THREAD * num_thread);
    EXPECT_EQ(2ul * num_thread * OPS_PER_THREAD, reducer.get_value());
    return avg_time;
}

static void* core_local_counter(void* arg) {
    CoreLocalCounter<int64_t>* counter = (CoreLocalCounter<int64_t>*)arg;
    butil::Timer timer;
    timer.start();
    for (size_t i = 0; i < OPS_PER_THREAD; ++i) {
        counter->increment(2);
    }
    timer.stop();
    return (void*)(timer.n_elapsed());
}

static long start_perf_test_with_core_local(size_t num_thread) {
    CoreLocalCounter<int64_t> counter(MetricUnit::NOUNIT);
    pthread_t threads[num_thread];
    for (size_t i = 0; i < num_thread; ++i) {
        pthread_create(&threads[i], nullptr, &core_local_counter, (void*)&counter);
    }
    long totol_time = 0;
    for (size_t i = 0; i < num_thread; ++i) {
        void* ret = nullptr;
        pthread_join(threads[i], &ret);
        totol_time += (long)ret;
    }
    long avg_time = totol_time / (OPS_PER_THREAD * num_thread);
    EXPECT_EQ(2ul * num_thread * OPS_PER_THREAD, counter.value());
    return avg_time;
}

// Compare the performance among bvar, atomic and CoreLocalCounter.
// You should build the test with BUILD_TYPE=release. The way to test
// is same as that of brpc https://github.com/apache/brpc/blob/1.3.0/test/bvar_reducer_unittest.cpp#L124
TEST(CoreLocalCounterTest, DISABLED_test_perf) {
    std::ostringstream oss;
    for (size_t i = 1; i <= 24; ++i) {
        oss << i << '\t' << start_perf_test_with_adder(i) << '\n';
    }
    LOG(INFO) << "Adder performance:\n" << oss.str();
    oss.str("");
    for (size_t i = 1; i <= 24; ++i) {
        oss << i << '\t' << start_perf_test_with_core_local(i) << '\n';
    }
    LOG(INFO) << "CoreLocal performance:\n" << oss.str();
    oss.str("");
    for (size_t i = 1; i <= 24; ++i) {
        oss << i << '\t' << start_perf_test_with_atomic(i) << '\n';
    }
    LOG(INFO) << "Atomic performance:\n" << oss.str();
}

} // namespace starrocks
