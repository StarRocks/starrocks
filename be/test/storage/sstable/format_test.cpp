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

#include "storage/sstable/format.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <random>
#include <utility>

#include "base/compression/compression_headers.h"
#include "base/debug/trace.h"
#include "base/hash/crc32c.h"
#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "io/array_input_stream.h"
#include "storage/sstable/coding.h"
#include "storage/sstable/options.h"

namespace starrocks::sstable {
namespace {
class ProbeStream : public io::ArrayInputStream {
public:
    explicit ProbeStream(const std::string& block) : ArrayInputStream(block.data(), block.size()), _block(block) {}
    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (count == _block.size()) ++bulk_reads;
        if (misreport_compression && count == 1 && offset == _block.size() - kBlockTrailerSize) {
            *static_cast<char*>(out) = kNoCompression;
            return Status::OK();
        }
        return ArrayInputStream::read_at_fully(offset, out, count);
    }
    bool misreport_compression = false;
    int bulk_reads = 0;

private:
    const std::string& _block;
};

std::string encode_block(const std::string& raw, bool compressed) {
    std::string block;
    if (compressed)
        snappy::Compress(raw.data(), raw.size(), &block);
    else
        block = raw;
    block.push_back(compressed ? kSnappyCompression : kNoCompression);
    block.append(4, '\0'); // checksums are disabled in these allocation-budget tests
    return block;
}
} // namespace

TEST(SstableFormatTest, reserve_input_and_decompression_output_before_bulk_read) {
    const std::string raw(65536, 'x');
    for (bool compressed : {false, true}) {
        auto block = encode_block(raw, compressed);
        auto stream = std::make_shared<ProbeStream>(block);
        RandomAccessFile file(stream, "budget.sst");
        BlockHandle handle;
        handle.set_offset(0);
        handle.set_size(block.size() - kBlockTrailerSize);
        ReadOptions options;
        size_t requested = 0;
        options.reserve_memory = [&](size_t bytes) {
            requested = bytes;
            return Status::MemoryLimitExceeded("test byte budget");
        };
        BlockContents contents;
        EXPECT_TRUE(ReadBlock(&file, options, handle, &contents).is_mem_limit_exceeded());
        EXPECT_EQ(block.size() + (compressed ? raw.size() : 0), requested);
        EXPECT_EQ(0, stream->bulk_reads);
        EXPECT_TRUE(contents.data.empty());
    }
}

TEST(SstableFormatTest, reserved_snappy_block_decodes_correctly) {
    const std::string raw(65536, 'x');
    auto block = encode_block(raw, true);
    auto stream = std::make_shared<ProbeStream>(block);
    RandomAccessFile file(stream, "decode.sst");
    BlockHandle handle;
    handle.set_offset(0);
    handle.set_size(block.size() - kBlockTrailerSize);
    size_t reserved = 0;
    ReadOptions options;
    options.reserve_memory = [&](size_t bytes) {
        reserved += bytes;
        return Status::OK();
    };
    BlockContents contents;
    ASSERT_OK(ReadBlock(&file, options, handle, &contents));
    std::unique_ptr<const char[]> cleanup(contents.heap_allocated ? contents.data.get_data() : nullptr);
    EXPECT_EQ(block.size() + raw.size(), reserved);
    EXPECT_EQ(raw, contents.data.to_string());
    EXPECT_EQ(1, stream->bulk_reads);
}

TEST(SstableFormatTest, changed_probe_cannot_bypass_decompression_reservation) {
    const std::string raw(65536, 'x');
    auto block = encode_block(raw, true);
    auto stream = std::make_shared<ProbeStream>(block);
    stream->misreport_compression = true;
    RandomAccessFile file(stream, "changed-probe.sst");
    BlockHandle handle;
    handle.set_offset(0);
    handle.set_size(block.size() - kBlockTrailerSize);
    size_t reserved = 0;
    ReadOptions options;
    options.reserve_memory = [&](size_t bytes) {
        if (bytes > block.size() - reserved) return Status::MemoryLimitExceeded("decompression budget");
        reserved += bytes;
        return Status::OK();
    };
    BlockContents contents;
    EXPECT_TRUE(ReadBlock(&file, options, handle, &contents).is_mem_limit_exceeded());
    EXPECT_EQ(block.size(), reserved);
    EXPECT_EQ(1, stream->bulk_reads);
    EXPECT_TRUE(contents.data.empty());
}

TEST(SstableFormatTest, mandatory_index_probe_distinguishes_decoded_size_from_open_peak_before_rejection) {
    const std::string raw(65536, 'x');
    for (bool compressed : {false, true}) {
        auto block = encode_block(raw, compressed);
        auto stream = std::make_shared<ProbeStream>(block);
        RandomAccessFile file(stream, "index-probe.sst");
        BlockHandle handle;
        handle.set_offset(0);
        handle.set_size(block.size() - kBlockTrailerSize);
        ReadOptions options;
        options.is_index_block = true;
        options.reserve_memory = [&](size_t) { return Status::MemoryLimitExceeded("unchanged budget cause"); };
        scoped_refptr<Trace> trace(new Trace);
        BlockContents contents;
        {
            ADOPT_TRACE(trace.get());
            const auto status = ReadBlock(&file, options, handle, &contents);
            ASSERT_TRUE(status.is_mem_limit_exceeded());
            EXPECT_EQ("unchanged budget cause", status.message());
        }
        std::map<std::string, int64_t> counters;
        for (const auto& [name, value] : trace->metrics()->Get()) counters[name] = value;
        EXPECT_EQ(1, counters["sst_index_probe_count"]);
        EXPECT_EQ(compressed ? 1 : 0, counters["sst_index_probe_snappy_count"]);
        EXPECT_EQ(block.size() - kBlockTrailerSize, counters["sst_index_probe_encoded_bytes"]);
        EXPECT_EQ(raw.size(), counters["sst_index_probe_decoded_bytes"]);
        EXPECT_EQ(block.size() + (compressed ? raw.size() : 0), counters["sst_index_probe_open_peak_bytes"]);
        EXPECT_EQ(0, stream->bulk_reads);
        EXPECT_TRUE(contents.data.empty());
    }
}

} // namespace starrocks::sstable

namespace starrocks::sstable {
namespace {
class WindowStream : public io::ArrayInputStream {
public:
    explicit WindowStream(const std::string& data) : ArrayInputStream(data.data(), data.size()) {}
    Status read_at_fully(int64_t offset, void* out, int64_t size) override {
        max_read = std::max(max_read, size);
        if (offset >= fail_from && size > 5) return Status::IOError("injected late encoded IO failure");
        if (lie_prefix && offset == 0 && size == 5) {
            std::memset(out, 0, 5);
            static_cast<char*>(out)[0] = 1;
            return Status::OK();
        }
        return ArrayInputStream::read_at_fully(offset, out, size);
    }
    int64_t max_read = 0, fail_from = INT64_MAX;
    bool lie_prefix = false;
};
std::string raw_data() {
    std::mt19937 rng(290448);
    std::string raw;
    raw.reserve(8 * 1024 * 1024);
    for (size_t i = 0; i < 32768; ++i) {
        std::string record(128, 'x');
        for (char& c : record) c = static_cast<char>(rng());
        raw += record;
        raw += record;
    }
    return raw;
}
std::string encode(const std::string& raw) {
    std::string data;
    snappy::Compress(raw.data(), raw.size(), &data);
    data.push_back(kSnappyCompression);
    PutFixed32(&data, crc32c::Mask(crc32c::Value(data.data(), data.size())));
    return data;
}
Status read_window(const std::string& block, std::shared_ptr<WindowStream> stream, size_t budget, size_t* reserved,
                   BlockContents* result, ReadIOStat* stat = nullptr) {
    RandomAccessFile file(stream, "bounded.sst");
    BlockHandle h;
    h.set_offset(0);
    h.set_size(block.size() - kBlockTrailerSize);
    ReadOptions options;
    options.verify_checksums = true;
    options.stat = stat;
    bool bulk = true;
    options.try_reserve_memory = [&](size_t bytes) {
        if (std::exchange(bulk, false) || bytes > budget - *reserved) return false;
        *reserved += bytes;
        return true;
    };
    options.reserve_memory = [&](size_t bytes) {
        if (bytes > budget - *reserved) return Status::MemoryLimitExceeded("test root headroom");
        *reserved += bytes;
        return Status::OK();
    };
    return ReadBlock(&file, options, h, result);
}
} // namespace
TEST(SstableFormatTest, preserves_large_existing_block_with_smaller_peak) {
    const auto raw = raw_data(), block = encode(raw);
    const size_t budget = 10 * 1024 * 1024;
    ASSERT_GT(block.size() + raw.size(), budget);
    ASSERT_GT(block.size(), 64 * 1024);
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    ReadIOStat stat;
    ASSERT_OK(read_window(block, stream, budget, &reserved, &result, &stat));
    std::unique_ptr<const char[]> free(result.data.get_data());
    EXPECT_EQ(raw, result.data.to_string());
    EXPECT_EQ(raw.size() + 64 * 1024 + kBlockTrailerSize, reserved);
    EXPECT_LE(stream->max_read, 64 * 1024);
    EXPECT_EQ(block.size(), stat.bytes_from_file);
}
TEST(SstableFormatTest, headroom_rejection_precedes_bulk_allocation) {
    const auto raw = raw_data(), block = encode(raw);
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    auto st = read_window(block, stream, raw.size(), &reserved, &result);
    EXPECT_TRUE(st.is_mem_limit_exceeded());
    EXPECT_EQ(64 * 1024 + kBlockTrailerSize, reserved);
    EXPECT_LE(stream->max_read, 64 * 1024);
    EXPECT_TRUE(result.data.empty());
}
TEST(SstableFormatTest, checksum_and_io_causes_survive_decoder_failure) {
    const auto raw = raw_data();
    auto block = encode(raw);
    size_t header = 0;
    while (static_cast<unsigned char>(block[header++]) & 128) {
    }
    block[header] = 3;
    std::fill(block.begin() + header + 1, block.begin() + header + 5, 0);
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    auto st = read_window(block, stream, 16 * 1024 * 1024, &reserved, &result);
    ASSERT_TRUE(st.is_corruption());
    EXPECT_EQ("block checksum mismatch", st.message());
    EXPECT_TRUE(result.data.empty());
    block.resize(block.size() - 4);
    PutFixed32(&block, crc32c::Mask(crc32c::Value(block.data(), block.size())));
    stream = std::make_shared<WindowStream>(block);
    reserved = 0;
    st = read_window(block, stream, 16 * 1024 * 1024, &reserved, &result);
    EXPECT_TRUE(st.is_corruption());
    EXPECT_EQ("corrupted compressed block contents", st.message());
    EXPECT_TRUE(result.data.empty());
    stream = std::make_shared<WindowStream>(block);
    stream->fail_from = 128 * 1024;
    reserved = 0;
    st = read_window(block, stream, 16 * 1024 * 1024, &reserved, &result);
    EXPECT_TRUE(st.is_io_error());
    EXPECT_EQ("injected late encoded IO failure", st.message());
    EXPECT_TRUE(result.data.empty());
}
TEST(SstableFormatTest, changed_prefix_cannot_bypass_reservation_or_checksum_healing) {
    const auto raw = raw_data();
    auto block = encode(raw);
    auto stream = std::make_shared<WindowStream>(block);
    stream->lie_prefix = true;
    size_t reserved = 0;
    BlockContents result;
    auto st = read_window(block, stream, 1024 * 1024, &reserved, &result);
    EXPECT_TRUE(st.is_mem_limit_exceeded());
    EXPECT_EQ(64 * 1024 + kBlockTrailerSize, reserved);
    EXPECT_TRUE(result.data.empty());
    size_t header = 0;
    while (static_cast<unsigned char>(block[header++]) & 128) {
    }
    block[header] = 3;
    std::fill(block.begin() + header + 1, block.begin() + header + 5, 0);
    stream = std::make_shared<WindowStream>(block);
    stream->lie_prefix = true;
    reserved = 0;
    st = read_window(block, stream, 1024 * 1024, &reserved, &result);
    EXPECT_TRUE(st.is_corruption());
    EXPECT_EQ("block checksum mismatch", st.message());
    EXPECT_TRUE(result.data.empty());
}
TEST(SstableFormatTest, highly_compressible_input_also_uses_a_bounded_window) {
    const std::string raw(64 * 1024 * 1024, 'x');
    const auto block = encode(raw);
    ASSERT_GT(block.size(), 64 * 1024);
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    const size_t budget = raw.size() + 64 * 1024 + kBlockTrailerSize;
    ASSERT_GT(raw.size() + block.size(), budget);
    ASSERT_OK(read_window(block, stream, budget, &reserved, &result));
    std::unique_ptr<const char[]> free(result.data.get_data());
    EXPECT_EQ(raw, result.data.to_string());
    EXPECT_EQ(budget, reserved);
    EXPECT_LE(stream->max_read, 64 * 1024);
}
TEST(SstableFormatTest, large_uncompressed_input_keeps_single_buffer_semantics) {
    const auto raw = raw_data();
    auto block = raw;
    block.push_back(kNoCompression);
    PutFixed32(&block, crc32c::Mask(crc32c::Value(block.data(), block.size())));
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    ASSERT_OK(read_window(block, stream, block.size(), &reserved, &result));
    std::unique_ptr<const char[]> free(result.data.get_data());
    EXPECT_EQ(raw, result.data.to_string());
    EXPECT_EQ(block.size(), reserved);
    EXPECT_EQ(block.size(), stream->max_read);
}

TEST(SstableFormatTest, immediate_full_reservation_retains_bulk_read) {
    const auto raw = raw_data(), block = encode(raw);
    auto stream = std::make_shared<WindowStream>(block);
    RandomAccessFile file(stream, "bulk.sst");
    BlockHandle handle;
    handle.set_offset(0);
    handle.set_size(block.size() - kBlockTrailerSize);
    ReadOptions options;
    options.verify_checksums = true;
    size_t reserved = 0, declining = 0;
    options.try_reserve_memory = [&](size_t bytes) {
        reserved += bytes;
        return true;
    };
    options.reserve_memory = [&](size_t) {
        ++declining;
        return Status::InternalError("unnecessary reclaim or wait");
    };
    BlockContents result;
    ASSERT_OK(ReadBlock(&file, options, handle, &result));
    std::unique_ptr<const char[]> free(result.data.get_data());
    EXPECT_EQ(raw, result.data.to_string());
    EXPECT_EQ(block.size() + raw.size(), reserved);
    EXPECT_EQ(0, declining);
    EXPECT_EQ(block.size(), stream->max_read);
}
TEST(SstableFormatTest, cancellation_stops_between_windows_with_exact_attempted_bytes) {
    const auto raw = raw_data(), block = encode(raw);
    auto stream = std::make_shared<WindowStream>(block);
    RandomAccessFile file(stream, "cancel.sst");
    BlockHandle handle;
    handle.set_offset(0);
    handle.set_size(block.size() - kBlockTrailerSize);
    ReadOptions options;
    options.verify_checksums = true;
    size_t checks = 0, reserved = 0;
    ReadIOStat stat;
    options.stat = &stat;
    bool bulk = true;
    options.try_reserve_memory = [&](size_t bytes) {
        if (std::exchange(bulk, false)) return false;
        reserved += bytes;
        return true;
    };
    options.reserve_memory = [&](size_t bytes) {
        reserved += bytes;
        return Status::OK();
    };
    options.check_read_cancelled = [&] {
        return ++checks < 3 ? Status::OK() : Status::Cancelled("bounded test cancellation");
    };
    BlockContents result;
    auto st = ReadBlock(&file, options, handle, &result);
    EXPECT_TRUE(st.is_cancelled());
    EXPECT_EQ("bounded test cancellation", st.message());
    EXPECT_TRUE(result.data.empty());
    EXPECT_EQ(128 * 1024, stat.bytes_from_file);
    EXPECT_LE(stream->max_read, 64 * 1024);
    EXPECT_EQ(raw.size() + 64 * 1024 + kBlockTrailerSize, reserved);
}

TEST(SstableFormatTest, output_budget_decline_still_validates_checksum_before_reporting_memory) {
    const auto raw = raw_data();
    auto block = encode(raw);
    block[block.size() / 2] ^= 1;
    auto stream = std::make_shared<WindowStream>(block);
    size_t reserved = 0;
    BlockContents result;
    auto status = read_window(block, stream, 1024 * 1024, &reserved, &result);
    EXPECT_TRUE(status.is_corruption());
    EXPECT_EQ("block checksum mismatch", status.message());
    EXPECT_EQ(64 * 1024 + kBlockTrailerSize, reserved);
    EXPECT_TRUE(result.data.empty());
}

} // namespace starrocks::sstable
