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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "base/compression/stream_decompressor.h"
#include "io/compressed_input_stream.h"
#include "io/fd_input_stream.h"

namespace starrocks::io {

class CompressedInputStreamLzoTest : public ::testing::Test {
protected:
    struct ReadContext {
        size_t read_buffer_size = 1024;
        size_t decompressor_buffer_size = 8 * 1024 * 1024;
    };

    void read_compressed_file_ctx(const char* path, std::string& out, const ReadContext& ctx) {
        int fd = ::open(path, O_RDONLY);
        ASSERT_GE(fd, 0) << path << ": " << std::strerror(errno);
        auto file = std::make_shared<FdInputStream>(fd);
        file->set_close_on_delete(true);

        using DecompressorPtr = std::shared_ptr<StreamDecompressor>;
        auto dec = StreamDecompressor::create_decompressor(CompressionTypePB::LZO);
        ASSERT_TRUE(dec.ok()) << dec.status().message();

        auto compressed_input_stream = std::make_shared<io::CompressedInputStream>(
                file, DecompressorPtr(std::move(dec).value().release()), ctx.decompressor_buffer_size);

        std::vector<char> vec_buf(ctx.read_buffer_size + 1);
        char* buf = vec_buf.data();

        for (;;) {
            auto st = compressed_input_stream->read(buf, ctx.read_buffer_size);
            ASSERT_TRUE(st.ok()) << st.status().message();
            uint64_t sz = st.value();
            if (sz == 0) break;
            buf[sz] = 0;
            out += buf;
        }
    }

    void read_compressed_file(const char* path, std::string& out) {
        ReadContext ctx;
        read_compressed_file_ctx(path, out, ctx);
    }
};

TEST_F(CompressedInputStreamLzoTest, test_LZO0) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test0.csv.lzo";
    std::string out;
    read_compressed_file(path, out);
    std::string expected = R"(Alice,1
Bob,2
CharlieX,3
)";
    std::cout << out << "\n";
    ASSERT_EQ(out, expected);
}

TEST_F(CompressedInputStreamLzoTest, test_LZO1) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test1.csv.lzo";

    std::string head = R"(0,1
1,2
2,3
3,4
4,5
5,6
6,)";

    std::string tail = R"(9998
99998,99999
99999,100000
)";

    std::vector<size_t> decompressor_buffer_sizes = {
            1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    std::vector<size_t> read_buffer_sizes = {
            32, 1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    for (size_t decompressor_buffer_size : decompressor_buffer_sizes) {
        for (size_t read_buffer_size : read_buffer_sizes) {
            std::cout << "test lzo1: read_buffer_size: " << read_buffer_size
                      << ", decompressor_buffer_size: " << decompressor_buffer_size << std::endl;
            std::string out;
            ReadContext ctx{.read_buffer_size = read_buffer_size, .decompressor_buffer_size = decompressor_buffer_size};
            read_compressed_file_ctx(path, out, ctx);
            ASSERT_EQ(out.size(), 1177785);
            ASSERT_EQ(out.substr(0, head.size()), head);
            ASSERT_EQ(out.substr(out.size() - tail.size(), tail.size()), tail);
        }
    }
}

} // namespace starrocks::io
