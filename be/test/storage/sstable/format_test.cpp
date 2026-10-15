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

#include "base/compression/compression_headers.h"
#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "io/array_input_stream.h"
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

} // namespace starrocks::sstable
