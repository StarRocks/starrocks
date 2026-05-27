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

#include <atomic>
#include <chrono>
#include <thread>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "base/utility/defer_op.h"
#include "common/config_scan_io_fwd.h"
#include "io/io_test_base.h"

namespace starrocks {

// SharedBufferedInputStream moved to namespace starrocks; its mocks here still name
// io:: stream base classes (SeekableInputStream, SeekableInputStreamWrapper) unqualified.
using namespace io;

// Mock stream with configurable delay for testing parallel read behavior
class MockDelayedInputStream : public SeekableInputStream {
public:
    MockDelayedInputStream(std::string data, int read_delay_ms = 0)
            : _data(std::move(data)), _read_delay_ms(read_delay_ms) {}

    StatusOr<int64_t> read(void* out, int64_t count) override {
        int64_t to_read = std::min(count, static_cast<int64_t>(_data.size()) - _offset);
        if (to_read > 0) {
            memcpy(out, _data.data() + _offset, to_read);
            _offset += to_read;
        }
        return to_read;
    }

    Status read_fully(void* out, int64_t count) override {
        ASSIGN_OR_RETURN(auto nread, read(out, count));
        if (nread < count) {
            return Status::IOError("Unexpected EOF");
        }
        return Status::OK();
    }

    Status seek(int64_t offset) override {
        _offset = offset;
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return static_cast<int64_t>(_data.size()); }

    Status skip(int64_t count) override {
        _offset += count;
        return Status::OK();
    }

    // Thread-safe positional read with configurable delay
    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (_read_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_read_delay_ms));
        }
        int call_idx = _read_count.fetch_add(1, std::memory_order_relaxed);

        // Inject error on the configured call index (1-based; 0 means disabled).
        int fail_at = _fail_at_call.load(std::memory_order_relaxed);
        if (fail_at > 0 && call_idx + 1 == fail_at) {
            return Status::IOError("MockDelayedInputStream injected failure");
        }
        if (offset + count > static_cast<int64_t>(_data.size())) {
            return Status::IOError("Read beyond EOF");
        }
        memcpy(out, _data.data() + offset, count);
        return Status::OK();
    }

    bool is_thread_safe_positional_read() const override { return true; }

    int read_count() const { return _read_count.load(std::memory_order_relaxed); }

    // 1-based index of read_at_fully call that should return IOError. 0 disables injection.
    void set_fail_at_call(int idx) { _fail_at_call.store(idx, std::memory_order_relaxed); }

private:
    std::string _data;
    int64_t _offset{0};
    int _read_delay_ms;
    std::atomic<int> _read_count{0};
    std::atomic<int> _fail_at_call{0};
};

// RAII guard for tests that mutate the global parallel_io_buffer_limit config so
// failures mid-test don't leak the override into subsequent tests.
class ParallelIOBufferLimitGuard {
public:
    explicit ParallelIOBufferLimitGuard(const std::string& new_value) {
        _original = config::parallel_io_buffer_limit;
        config::parallel_io_buffer_limit = new_value;
        // get_parallel_read_buffer_limit() short-circuits to its 1GB fallback when the
        // process memory limit is unset and never consults the config. The service layer
        // injects that limit at startup; unit tests must do the same or the buffer limit
        // would never trip.
        SharedBufferedInputStream::set_process_mem_limit(kProcessMemLimit);
    }
    ~ParallelIOBufferLimitGuard() {
        config::parallel_io_buffer_limit = _original;
        SharedBufferedInputStream::set_process_mem_limit(0);
    }

private:
    static constexpr int64_t kProcessMemLimit = int64_t{1} * 1024 * 1024 * 1024; // 1GB
    std::string _original;
};

// Helper to generate test data
static std::string generate_test_data(size_t size) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>('A' + (i % 26));
    }
    return data;
}

class SharedBufferedInputStreamTest : public ::testing::Test {
protected:
    static constexpr int64_t KB = 1024;
    static constexpr int64_t MB = 1024 * 1024;
};

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

    // check debug function (state==Ready=2 after parallel init, or Filled=6 after consume)
    auto dbg = sb.value()->debug_string();
    ASSERT_TRUE(dbg.find("raw_offset=22420223") != std::string::npos) << dbg;
    ASSERT_TRUE(dbg.find("raw_size=197") != std::string::npos) << dbg;
    ASSERT_TRUE(dbg.find("ref_count=2") != std::string::npos) << dbg;
}

// ==================== Parallel Read Tests ====================

// Test: Parallel read is triggered by set_io_ranges and get_bytes returns correct data
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_data_integrity) {
    const size_t file_size = 8 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Create 4 IO ranges of 2MB each
    std::vector<SharedBufferedInputStream::IORange> ranges;
    for (int i = 0; i < 4; ++i) {
        ranges.emplace_back(i * 2 * MB, 2 * MB, true);
    }

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read all ranges - get_bytes() blocks until parallel read completes
    for (int i = 0; i < 4; ++i) {
        int64_t offset = i * 2 * MB;
        const uint8_t* buffer = nullptr;
        ASSERT_OK(sb_stream->get_bytes(&buffer, offset, 2 * MB, nullptr));

        // Verify data matches
        for (size_t j = 0; j < 2 * MB; ++j) {
            ASSERT_EQ(data[offset + j], static_cast<char>(buffer[j])) << "Mismatch at offset " << (offset + j);
        }
    }
}

// Test: parallel reads each chunk hits read_at_fully exactly once; data is correct.
// Wall-clock comparisons are intentionally NOT asserted — they're flaky under CI load.
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_concurrent) {
    const size_t file_size = 10 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, /*delay_ms=*/0);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // 2MB gap > default max_dist_size (1MB) prevents the two ranges from being coalesced into
    // one SharedBuffer; we want two independent buffers so the two parallel reads are observable.
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ranges.emplace_back(6 * MB, 4 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 4 * MB, nullptr));
    for (size_t i = 0; i < 4 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i])) << "Mismatch at offset " << i;
    }
    ASSERT_OK(sb_stream->get_bytes(&buffer, 6 * MB, 4 * MB, nullptr));
    for (size_t i = 0; i < 4 * MB; ++i) {
        ASSERT_EQ(data[6 * MB + i], static_cast<char>(buffer[i])) << "Mismatch at offset " << (6 * MB + i);
    }
    // Two non-adjacent ranges → two SharedBuffers, each a single chunk (4MB ≤ 8MB * 1.5).
    ASSERT_EQ(2, underlying->read_count());
}

// Test: Direct IO fallback works when not using set_io_ranges
TEST_F(SharedBufferedInputStreamTest, test_direct_io_fallback) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Read without setting IO ranges - should use direct read
    std::string result(file_size, '\0');
    ASSERT_OK(sb_stream->read_at_fully(0, result.data(), file_size));

    ASSERT_EQ(data, result);
    ASSERT_EQ(1, sb_stream->direct_io_count());
}

// Test: Shared buffers are created correctly with coalescing
TEST_F(SharedBufferedInputStreamTest, test_coalesce_io_ranges) {
    const size_t file_size = 16 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set coalesce options
    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_dist_size = 1 * MB;   // Coalesce ranges within 1MB
    opts.max_buffer_size = 8 * MB; // Max buffer 8MB
    sb_stream->set_coalesce_options(opts);

    // Create ranges that should be coalesced (close together)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 1 * MB, true);
    ranges.emplace_back(1 * MB + 100, 1 * MB, true); // 100 bytes gap - should coalesce
    ranges.emplace_back(10 * MB, 1 * MB, true);      // Far away - separate buffer

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // First two ranges should be in same buffer
    auto sb1 = sb_stream->find_shared_buffer(0, 1 * MB);
    ASSERT_OK(sb1);
    auto sb2 = sb_stream->find_shared_buffer(1 * MB + 100, 1 * MB);
    ASSERT_OK(sb2);

    // They should be the same shared buffer (coalesced)
    ASSERT_EQ(sb1.value().get(), sb2.value().get());

    // Third range should be separate
    auto sb3 = sb_stream->find_shared_buffer(10 * MB, 1 * MB);
    ASSERT_OK(sb3);
    ASSERT_NE(sb1.value().get(), sb3.value().get());
}

// Test: 8 parallel ranges all read; each fires read_at_fully once. Spaced with a 2MB gap so
// they exceed the default max_dist_size (1MB) and stay as separate SharedBuffers — this is
// what exercises the parallel_io_workers_per_file sliding window.
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_sliding_window) {
    const size_t stride = 6 * MB;
    const size_t file_size = 8 * stride;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, /*delay_ms=*/0);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    for (int i = 0; i < 8; ++i) {
        ranges.emplace_back(i * stride, 4 * MB, true);
    }

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    for (int i = 0; i < 8; ++i) {
        const uint8_t* buffer = nullptr;
        ASSERT_OK(sb_stream->get_bytes(&buffer, i * stride, 4 * MB, nullptr));
        for (size_t j = 0; j < 4 * MB; ++j) {
            ASSERT_EQ(data[i * stride + j], static_cast<char>(buffer[j]));
        }
    }
    // 8 non-adjacent ranges → 8 SharedBuffers, each a single chunk (4MB ≤ 8MB * 1.5).
    ASSERT_EQ(8, underlying->read_count());
}

// Test: Release clears buffers
TEST_F(SharedBufferedInputStreamTest, test_release_clears_buffers) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set IO ranges
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 2 * MB, true);
    ranges.emplace_back(2 * MB, 2 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read data first (this waits for parallel reads to complete)
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 2 * MB, nullptr));

    // Release everything
    sb_stream->release();

    // Now find_shared_buffer should fail
    auto sb = sb_stream->find_shared_buffer(0, 1 * MB);
    ASSERT_FALSE(sb.ok());
}

// Test: Read statistics are tracked correctly
TEST_F(SharedBufferedInputStreamTest, test_io_statistics) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set max_buffer_size to 2MB to prevent coalescing of adjacent ranges
    // (default 8MB would merge [0,2MB) and [2MB,4MB) into single SharedBuffer)
    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 2 * MB;
    sb_stream->set_coalesce_options(opts);

    // Set IO ranges
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 2 * MB, true);
    ranges.emplace_back(2 * MB, 2 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read both ranges - get_bytes() waits for parallel reads
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 2 * MB, nullptr));
    ASSERT_OK(sb_stream->get_bytes(&buffer, 2 * MB, 2 * MB, nullptr));

    // Verify IO statistics
    ASSERT_EQ(2, sb_stream->shared_io_count());
    ASSERT_EQ(4 * MB, sb_stream->shared_io_bytes());
    ASSERT_EQ(0, sb_stream->direct_io_count());
}

// ==================== Chunked Parallel Read Tests ====================

// Test: Large buffer (>8MB) is split into chunks and read in parallel
TEST_F(SharedBufferedInputStreamTest, test_chunked_parallel_read) {
    const size_t file_size = 32 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, /*delay_ms=*/0);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // 24MB range → 3 chunks of 8MB
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 24 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 24 * MB, nullptr));

    for (size_t i = 0; i < 24 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i])) << "Mismatch at offset " << i;
    }

    // 3 chunks
    ASSERT_EQ(3, underlying->read_count());
}

// Test: Verify chunk boundaries are correct
// Also tests smart chunking: don't create tiny last chunks (< chunk_size/2)
TEST_F(SharedBufferedInputStreamTest, test_chunked_read_boundaries) {
    const size_t file_size = 20 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // 17MB range → 2 chunks: 8MB + 9MB (not 8+8+1, because 1MB < 4MB threshold)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 17 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 17 * MB, nullptr));

    // Verify data at chunk boundary (8MB)
    ASSERT_EQ(data[8 * MB - 1], static_cast<char>(buffer[8 * MB - 1]));   // end of chunk 1
    ASSERT_EQ(data[8 * MB], static_cast<char>(buffer[8 * MB]));           // start of chunk 2
    ASSERT_EQ(data[17 * MB - 1], static_cast<char>(buffer[17 * MB - 1])); // end of chunk 2

    // 2 read_at_fully calls (2 chunks: 8MB + 9MB)
    ASSERT_EQ(2, underlying->read_count());
}

// Test: Small buffer (<= max_buffer_size * 1.5) uses single chunk
TEST_F(SharedBufferedInputStreamTest, test_small_buffer_single_chunk) {
    const size_t file_size = 16 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // 12MB range <= 8MB * 1.5 = 12MB → 1 chunk (smart chunking avoids 8+4 split)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 12 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 12 * MB, nullptr));

    // Verify data
    for (size_t i = 0; i < 12 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i])) << "Mismatch at offset " << i;
    }

    // Only 1 read_at_fully call (1 chunk, not 8+4)
    ASSERT_EQ(1, underlying->read_count());
}

// ==================== Buffer Limit Tests ====================

// Test: Full cycle - hit limit, fallback to sync, release, parallel resumes
TEST_F(SharedBufferedInputStreamTest, test_buffer_limit_full_cycle) {
    // RAII guard restores config even if an assertion below fails mid-test.
    ParallelIOBufferLimitGuard limit_guard("4M");

    const size_t file_size = 16 * MB;
    std::string data = generate_test_data(file_size);

    int64_t initial_used = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();

    // Stream 1: 4MB buffer - should use parallel reads (fits in limit)
    auto underlying1 = std::make_shared<MockDelayedInputStream>(data);
    auto stream1 = std::make_shared<SharedBufferedInputStream>(underlying1, "test1", file_size);
    {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, 4 * MB, true);
        ASSERT_OK(stream1->set_io_ranges(ranges, true));

        const uint8_t* buffer = nullptr;
        ASSERT_OK(stream1->get_bytes(&buffer, 0, 4 * MB, nullptr));
    }

    // Buffer should be tracked (4MB used)
    int64_t after_stream1 = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used + 4 * MB, after_stream1);

    // Stream 2: Try another 4MB - should NOT use parallel reads (would exceed limit)
    // It will fallback to sync read
    auto underlying2 = std::make_shared<MockDelayedInputStream>(data);
    auto stream2 = std::make_shared<SharedBufferedInputStream>(underlying2, "test2", file_size);
    {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, 4 * MB, true);
        ASSERT_OK(stream2->set_io_ranges(ranges, true));

        const uint8_t* buffer = nullptr;
        ASSERT_OK(stream2->get_bytes(&buffer, 0, 4 * MB, nullptr));
    }

    // Buffer used should still be 4MB (stream2 didn't get parallel read allocation)
    int64_t after_stream2 = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used + 4 * MB, after_stream2);

    // Verify stream2 used sync read (only 1 read_at_fully call, not chunked)
    ASSERT_EQ(1, underlying2->read_count());

    // Release stream1 - frees 4MB
    stream1->release();
    int64_t after_release = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used, after_release);

    // Stream 3: Now 4MB should work with parallel reads again
    auto underlying3 = std::make_shared<MockDelayedInputStream>(data);
    auto stream3 = std::make_shared<SharedBufferedInputStream>(underlying3, "test3", file_size);
    {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, 4 * MB, true);
        ASSERT_OK(stream3->set_io_ranges(ranges, true));

        const uint8_t* buffer = nullptr;
        ASSERT_OK(stream3->get_bytes(&buffer, 0, 4 * MB, nullptr));
    }

    // Buffer should be tracked again
    int64_t after_stream3 = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used + 4 * MB, after_stream3);

    // Cleanup
    stream2.reset();
    stream3.reset();

    // Verify all released
    int64_t final_used = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used, final_used);
}

// ==================== State Machine / Error Path Tests ====================

// Test: a chunk read error is propagated through get_bytes AND peek_shared_buffer.
// Subsequent calls on the same sb return the same sticky error.
TEST_F(SharedBufferedInputStreamTest, test_chunk_error_propagation) {
    const size_t file_size = 24 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, /*delay_ms=*/0);
    // Fail on the 2nd chunk of the 24MB range (chunks of 8MB each → 3 chunks).
    underlying->set_fail_at_call(2);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);
    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 24 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // First consumer sees the IO error.
    const uint8_t* buffer = nullptr;
    Status s1 = sb_stream->get_bytes(&buffer, 0, 24 * MB, nullptr);
    ASSERT_FALSE(s1.ok());

    // Second consumer on the same sb sees the same sticky error (state == Failed).
    Status s2 = sb_stream->get_bytes(&buffer, 0, 24 * MB, nullptr);
    ASSERT_FALSE(s2.ok());

    sb_stream->release();
}

// Test: release() while a worker is mid-read leaves no UAF; budget is refunded after
// worker finishes. Consumers waiting on the sb get Aborted.
TEST_F(SharedBufferedInputStreamTest, test_release_during_inflight) {
    const size_t file_size = 16 * MB;
    const int delay_ms = 100;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, delay_ms);

    int64_t initial_used = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Snapshot the sb pointer so we can refer to it after release().
    auto sb = sb_stream->find_shared_buffer(0, 4 * MB).value();

    // Spawn a waiter that will observe Abandoned.
    Status waiter_status;
    std::thread waiter([&]() {
        const uint8_t* buf = nullptr;
        waiter_status = sb_stream->get_bytes(&buf, 0, 4 * MB, sb);
    });

    // Give the worker time to start; release while it's in flight.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    sb_stream->release();

    waiter.join();
    ASSERT_FALSE(waiter_status.ok()) << "consumer should see Aborted from release()";

    // Drop our references; the worker still holds a self/sb capture in its lambda. When the
    // worker finishes its delayed read_at_fully, ActiveReadGuard decrements active_count and
    // then the SBI/SharedBuffer destructors run on the worker thread.
    sb.reset();
    sb_stream.reset();

    // Poll until the worker has refunded its reservation (bounded retry).
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms * 4 + 500);
    while (SharedBufferedInputStream::TEST_get_parallel_read_buffer_used() != initial_used &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    int64_t final_used = SharedBufferedInputStream::TEST_get_parallel_read_buffer_used();
    ASSERT_EQ(initial_used, final_used);
}

// Test: when the underlying mock is wrapped in a SeekableInputStreamWrapper (any of the
// many wrappers like ThrottledSeekableInputStream), the gate's true value should forward
// through and parallel reads should still fire.
TEST_F(SharedBufferedInputStreamTest, test_wrapper_gate_forwarding) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, /*delay_ms=*/0);

    // Simple unique_ptr-owning wrapper forwarding via SeekableInputStreamWrapper.
    class PassthroughWrapper : public SeekableInputStreamWrapper {
    public:
        explicit PassthroughWrapper(std::shared_ptr<SeekableInputStream> impl)
                : SeekableInputStreamWrapper(impl.get(), kDontTakeOwnership), _impl_keep(std::move(impl)) {}

    private:
        std::shared_ptr<SeekableInputStream> _impl_keep;
    };

    auto wrapper = std::make_shared<PassthroughWrapper>(underlying);
    ASSERT_TRUE(wrapper->is_thread_safe_positional_read());

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(wrapper, "test", file_size);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 4 * MB, nullptr));
    for (size_t i = 0; i < 4 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i]));
    }
    // Wrapper passed gate; parallel path ran (1 chunk).
    ASSERT_EQ(1, underlying->read_count());
}

// Test: when the underlying stream reports NOT thread-safe-positional, SBI falls back to
// the SyncOnly path. Consumer still gets correct bytes; just no parallel prefetch.
TEST_F(SharedBufferedInputStreamTest, test_sync_only_demand) {
    class NonPositionalMock : public MockDelayedInputStream {
    public:
        using MockDelayedInputStream::MockDelayedInputStream;
        bool is_thread_safe_positional_read() const override { return false; }
    };

    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<NonPositionalMock>(data, /*delay_ms=*/0);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Consumer drives the fill (state==Cold → init → SyncOnly → SyncFilling).
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 4 * MB, nullptr));
    for (size_t i = 0; i < 4 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i]));
    }
}

// Test: two consumers calling get_bytes on the same sb don't duplicate IO. The SyncFilling
// state ensures exactly one IO; the other waits on cv. Uses NonPositional mock so the path
// goes SyncOnly→SyncFilling (the parallel path doesn't go through SyncFilling).
TEST_F(SharedBufferedInputStreamTest, test_concurrent_consumers_same_sb) {
    class NonPositionalMock : public MockDelayedInputStream {
    public:
        using MockDelayedInputStream::MockDelayedInputStream;
        bool is_thread_safe_positional_read() const override { return false; }
    };

    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<NonPositionalMock>(data, /*delay_ms=*/50);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    auto sb = sb_stream->find_shared_buffer(0, 4 * MB).value();

    std::atomic<int> ok_count{0};
    auto worker = [&]() {
        const uint8_t* buf = nullptr;
        Status s = sb_stream->get_bytes(&buf, 0, 4 * MB, sb);
        if (s.ok()) ok_count.fetch_add(1);
    };
    std::thread t1(worker);
    std::thread t2(worker);
    t1.join();
    t2.join();

    ASSERT_EQ(2, ok_count.load());
    // Exactly one underlying read (the SyncFilling claim), even with two consumers.
    ASSERT_EQ(1, underlying->read_count());
}

} // namespace starrocks
