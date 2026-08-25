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

#include "cache/scan/shared_buffered_input_stream.h"

#include <gtest/gtest.h>

#include <cstring>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "base/utility/defer_op.h"
#include "common/config_scan_io_fwd.h"
#include "io/io_test_base.h"
#include "runtime/current_thread.h"

namespace starrocks {

class SharedBufferedInputStreamTest : public ::testing::Test {};

PARALLEL_TEST(SharedBufferedInputStreamTest, test_release) {
    size_t len = 1 * 1024 * 1024; // 1MB
    const std::string rand_string = io::random_string(len);
    auto in = std::make_shared<io::TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 1024
    std::vector<SharedBufferedInputStream::IORange> ranges;
    // make two ranges one is active and another is lazy to avoid merging together.
    // 150k -> 520k
    auto r_active = SharedBufferedInputStream::IORange(150 * 1024, 370 * 1024, true);
    ranges.push_back(r_active);
    // 550k -> 650k
    auto r_lazy = SharedBufferedInputStream::IORange(550 * 1024, 100 * 1024, false);
    ranges.push_back(r_lazy);
    auto st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_OK(st);
    // for this case, the first range is aligned to 0 -> 768k, the second range is aligned to 512k -> 768k
    // and now the first range is used and want to release
    // if release with aligned offset, both two sharedbuffers are released.
    sb_stream->release_to_offset(520 * 1024);
    auto sb = sb_stream->find_shared_buffer(550 * 1024, 100 * 1024);
    ASSERT_OK(sb.status());
}

TEST_F(SharedBufferedInputStreamTest, test_orc) {
    const bool saved_io_coalesce_adaptive_lazy_active = config::io_coalesce_adaptive_lazy_active;
    config::io_coalesce_adaptive_lazy_active = true;
    DeferOp restore_config(
            [&]() { config::io_coalesce_adaptive_lazy_active = saved_io_coalesce_adaptive_lazy_active; });

    size_t len = 100 * 1024 * 1024; // 1MB
    const std::string rand_string = io::random_string(len);
    auto in = std::make_shared<io::TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 256kb
    std::vector<SharedBufferedInputStream::IORange> ranges;

    {
        // put lazy
        ranges.emplace_back(3, 1746 - 3, false);
        ranges.emplace_back(1978, 4125 - 1978, false);
        ranges.emplace_back(4288, 5235 - 4288, false);
        ranges.emplace_back(5523, 2833805 - 5523, false);
        ranges.emplace_back(2913460, 3261935 - 2913460, false);
        ranges.emplace_back(3295862, 22211037 - 3295862, false);
        ranges.emplace_back(22417540, 22417878 + 35 - 22417540, false);
    }

    {
        // put active
        ranges.emplace_back(1746, 1978 - 1746, true);
        ranges.emplace_back(4125, 4288 - 4125, true);
        ranges.emplace_back(5235, 5523 - 5235, true);
        ranges.emplace_back(2833805, 2913460 - 2833805, true);
        ranges.emplace_back(3261935, 3295862 - 3261935, true);
        ranges.emplace_back(22211037, 22417540 - 22211037, true);
    }

    auto st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_TRUE(st.ok());

    // read active first
    auto sb = sb_stream->find_shared_buffer(1746, 1978 - 1746);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(2833805, 2913460 - 2833805);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(2833805, sb.value()->raw_offset);
    ASSERT_EQ(3295862 - 2833805, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(22211037, 22417540 - 22211037);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22211037, sb.value()->raw_offset);
    ASSERT_EQ(22417540 - 22211037, sb.value()->raw_size);

    // read lazy column
    sb = sb_stream->find_shared_buffer(3, 1746 - 3);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(1978, 4125 - 1978);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(4288, 5235 - 4288);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(5523, 2833805 - 5523);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(5523, sb.value()->raw_offset);
    ASSERT_EQ(2833805 - 5523, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(2913460, 3261935 - 2913460);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(2833805, sb.value()->raw_offset);
    ASSERT_EQ(3295862 - 2833805, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(3295862, 22211037 - 3295862);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(3295862, sb.value()->raw_offset);
    ASSERT_EQ(22211037 - 3295862, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(22417540, 22417878 + 35 - 22417540);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22417540, sb.value()->raw_offset);
    ASSERT_EQ(22417878 + 35 - 22417540, sb.value()->raw_size);

    // clear previous stripe io range
    sb_stream->release_to_offset(22418414);

    ranges.clear();
    {
        // put active
        ranges.emplace_back(22420223, 22420420 - 22420223, true);
    }
    {
        // put lazy
        ranges.emplace_back(22418414, 22420223 - 22418414, false);
    }

    st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_TRUE(st.ok());

    // get active
    sb = sb_stream->find_shared_buffer(22420223, 22420420 - 22420223);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22420223, sb.value()->raw_offset);
    ASSERT_EQ(22420420 - 22420223, sb.value()->raw_size);

    // get lazy
    sb = sb_stream->find_shared_buffer(22418414, 22420223 - 22418414);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22420223, sb.value()->raw_offset);
    ASSERT_EQ(22420420 - 22420223, sb.value()->raw_size);

    // check debug function
    ASSERT_EQ(
            "SharedBuffer raw_offset=22420223, raw_size=197, offset=22282240, size=262144, ref_count=2, "
            "buffer_capacity=0",
            sb.value()->debug_string());
}

namespace {

bool g_sb_prefetch_test_env_initialized = false;
MemTracker* g_sb_prefetch_test_mem_tracker = nullptr;

bool sb_prefetch_test_env_initialized() {
    return g_sb_prefetch_test_env_initialized;
}

MemTracker* sb_prefetch_test_mem_tracker() {
    return g_sb_prefetch_test_mem_tracker;
}

} // namespace

// prefetch_registered() loads buffers through get_bytes(), which consults the thread-local
// mem tracker, so these tests install one the same way cache_input_stream_test.cpp does.
class SharedBufferedInputStreamPrefetchTest : public ::testing::Test {
public:
    void SetUp() override {
        g_sb_prefetch_test_env_initialized = true;
        g_sb_prefetch_test_mem_tracker = &_mem_tracker;
        CurrentThread::set_mem_tracker_source(sb_prefetch_test_env_initialized, sb_prefetch_test_mem_tracker);
        tls_mem_tracker = nullptr;
    }

    void TearDown() override {
        tls_thread_status.set_mem_tracker(nullptr);
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        g_sb_prefetch_test_env_initialized = false;
        g_sb_prefetch_test_mem_tracker = nullptr;
    }

protected:
    // Registers four small ranges over a 1MB backing file that coalesce into three buffers
    // (no alignment, so buffer sizes equal the raw range bytes):
    //   [0k, 8k)      <- two adjacent 4k ranges merged into one buffer
    //   [200k, 204k)
    //   [400k, 404k)
    // max_dist_size=64k keeps the three groups from merging with each other.
    std::shared_ptr<SharedBufferedInputStream> make_registered_stream() {
        auto in = std::make_shared<io::TestInputStream>(_content, kFileSize);
        auto sb_stream = std::make_shared<SharedBufferedInputStream>(in, "test", kFileSize);
        SharedBufferedInputStream::CoalesceOptions options;
        options.max_dist_size = 64 * 1024;
        sb_stream->set_coalesce_options(options);
        std::vector<SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, 4 * 1024);
        ranges.emplace_back(4 * 1024, 4 * 1024);
        ranges.emplace_back(200 * 1024, 4 * 1024);
        ranges.emplace_back(400 * 1024, 4 * 1024);
        CHECK_OK(sb_stream->set_io_ranges(ranges));
        return sb_stream;
    }

    static constexpr int64_t kFileSize = 1 * 1024 * 1024;
    static constexpr int64_t kBufferCount = 3;
    static constexpr int64_t kTotalRegisteredBytes = (8 + 4 + 4) * 1024;

    const std::string _content = io::random_string(kFileSize);

private:
    MemTracker _mem_tracker;
};

TEST_F(SharedBufferedInputStreamPrefetchTest, test_prefetch_registered_ample_budget) {
    auto sb_stream = make_registered_stream();
    std::atomic<int64_t> budget{kFileSize};
    ASSIGN_OR_ABORT(bool all_loaded, sb_stream->prefetch_registered(&budget));
    ASSERT_TRUE(all_loaded);
    ASSERT_EQ(kBufferCount, sb_stream->shared_io_count());
    ASSERT_EQ(kTotalRegisteredBytes, sb_stream->shared_io_bytes());
    ASSERT_EQ(kFileSize - kTotalRegisteredBytes, budget.load());

    // every registered buffer is resident now, so reading a registered range must not
    // trigger another shared IO.
    std::vector<uint8_t> out(4 * 1024);
    ASSERT_OK(sb_stream->read_at_fully(200 * 1024, out.data(), out.size()));
    ASSERT_EQ(kBufferCount, sb_stream->shared_io_count());
    ASSERT_EQ(kTotalRegisteredBytes, sb_stream->shared_io_bytes());
}

TEST_F(SharedBufferedInputStreamPrefetchTest, test_prefetch_registered_zero_budget) {
    auto sb_stream = make_registered_stream();
    std::atomic<int64_t> budget{0};
    ASSIGN_OR_ABORT(bool all_loaded, sb_stream->prefetch_registered(&budget));
    ASSERT_FALSE(all_loaded);
    ASSERT_EQ(0, sb_stream->shared_io_count());
    ASSERT_EQ(0, sb_stream->shared_io_bytes());
    // the failed reservation is refunded.
    ASSERT_EQ(0, budget.load());
}

TEST_F(SharedBufferedInputStreamPrefetchTest, test_prefetch_registered_partial_budget) {
    auto sb_stream = make_registered_stream();
    // enough for the first two buffers (8k + 4k) but not the third (4k).
    const int64_t initial_budget = 8 * 1024 + 4 * 1024 + 100;
    std::atomic<int64_t> budget{initial_budget};
    ASSIGN_OR_ABORT(bool all_loaded, sb_stream->prefetch_registered(&budget));
    ASSERT_FALSE(all_loaded);
    ASSERT_EQ(2, sb_stream->shared_io_count());
    // reserve-before-load: the loaded bytes never exceed the initial budget.
    ASSERT_LE(sb_stream->shared_io_bytes(), initial_budget);
    ASSERT_EQ(12 * 1024, sb_stream->shared_io_bytes());
    // the failed reservation is refunded, leaving the un-spent remainder.
    ASSERT_GE(budget.load(), 0);
    ASSERT_EQ(100, budget.load());
}

TEST_F(SharedBufferedInputStreamPrefetchTest, test_prefetch_registered_idempotent) {
    auto sb_stream = make_registered_stream();
    std::atomic<int64_t> budget{kFileSize};
    ASSIGN_OR_ABORT(bool all_loaded, sb_stream->prefetch_registered(&budget));
    ASSERT_TRUE(all_loaded);
    ASSERT_EQ(kBufferCount, sb_stream->shared_io_count());

    // already-resident buffers consume no budget, so a second call succeeds even with
    // nothing left to spend.
    std::atomic<int64_t> zero_budget{0};
    ASSIGN_OR_ABORT(bool still_loaded, sb_stream->prefetch_registered(&zero_budget));
    ASSERT_TRUE(still_loaded);
    ASSERT_EQ(kBufferCount, sb_stream->shared_io_count());
    ASSERT_EQ(kTotalRegisteredBytes, sb_stream->shared_io_bytes());
    ASSERT_EQ(0, zero_budget.load());
}

TEST_F(SharedBufferedInputStreamPrefetchTest, test_prefetch_registered_read_correctness) {
    auto sb_stream = make_registered_stream();
    std::atomic<int64_t> budget{kFileSize};
    ASSIGN_OR_ABORT(bool all_loaded, sb_stream->prefetch_registered(&budget));
    ASSERT_TRUE(all_loaded);

    const std::vector<std::pair<int64_t, int64_t>> registered_ranges = {
            {0, 4 * 1024}, {4 * 1024, 4 * 1024}, {200 * 1024, 4 * 1024}, {400 * 1024, 4 * 1024}};
    for (const auto& [offset, size] : registered_ranges) {
        std::vector<uint8_t> out(size);
        ASSERT_OK(sb_stream->read_at_fully(offset, out.data(), size));
        ASSERT_EQ(0, memcmp(out.data(), _content.data() + offset, size)) << "offset=" << offset;
    }
    // every read above was served from the prefetched shared buffers.
    ASSERT_EQ(0, sb_stream->direct_io_count());
    ASSERT_EQ(kBufferCount, sb_stream->shared_io_count());
}

} // namespace starrocks
