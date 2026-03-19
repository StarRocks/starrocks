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

#include "exec/parquet_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "common/statusor.h"
#include "exec/file_scanner/file_scanner.h"
#include "fs/fs.h"
#include "io/core/seekable_input_stream.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
namespace {

class TestSeekableInputStream final : public io::SeekableInputStream {
public:
    explicit TestSeekableInputStream(std::string data) : _data(std::move(data)) {}

    StatusOr<int64_t> read(void* out, int64_t count) override {
        if (count < 0) {
            return Status::InvalidArgument("count must be non-negative");
        }
        const int64_t available = static_cast<int64_t>(_data.size()) - _position;
        const int64_t to_read = std::min(count, std::max<int64_t>(available, 0));
        if (to_read > 0) {
            memcpy(out, _data.data() + _position, to_read);
            _position += to_read;
        }
        return to_read;
    }

    Status read_fully(void* out, int64_t count) override {
        ASSIGN_OR_RETURN(auto bytes_read, read(out, count));
        if (bytes_read != count) {
            return Status::IOError("unexpected eof");
        }
        return Status::OK();
    }

    Status skip(int64_t count) override {
        if (count < 0) {
            return Status::InvalidArgument("count must be non-negative");
        }
        _position = std::min<int64_t>(_position + count, _data.size());
        return Status::OK();
    }

    Status seek(int64_t position) override {
        if (position < 0) {
            return Status::InvalidArgument("position must be non-negative");
        }
        _position = std::min<int64_t>(position, _data.size());
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _position; }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        if (offset < 0 || count < 0) {
            return Status::InvalidArgument("offset and count must be non-negative");
        }
        const int64_t available = static_cast<int64_t>(_data.size()) - offset;
        const int64_t to_read = std::min(count, std::max<int64_t>(available, 0));
        if (to_read > 0) {
            memcpy(out, _data.data() + offset, to_read);
        }
        return to_read;
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        ASSIGN_OR_RETURN(auto bytes_read, read_at(offset, out, count));
        if (bytes_read != count) {
            return Status::IOError("unexpected eof");
        }
        return Status::OK();
    }

    StatusOr<int64_t> get_size() override { return static_cast<int64_t>(_data.size()); }

private:
    std::string _data;
    int64_t _position = 0;
};

std::shared_ptr<RandomAccessFile> create_random_access_file(std::string content) {
    return std::make_shared<RandomAccessFile>(std::make_shared<TestSeekableInputStream>(std::move(content)),
                                              "test.parquet");
}

TEST(ParquetReaderTest, LateBufferFreeAfterReaderClose) {
    MemTracker* mem_tracker = ExecEnv::GetInstance()->query_pool_mem_tracker();
    ASSERT_NE(mem_tracker, nullptr);

    const int64_t initial_consumption = mem_tracker->consumption();
    auto file = create_random_access_file("hello");
    ScannerCounter counter;
    auto parquet_file = std::make_shared<ParquetChunkFile>(file, 0, &counter);
    auto arrow_file = std::static_pointer_cast<arrow::io::RandomAccessFile>(parquet_file);
    auto reader = std::make_unique<ParquetReaderWrap>(std::move(arrow_file), 1, 0, 0, mem_tracker);

    auto buffer_result = parquet_file->Read(5);
    ASSERT_TRUE(buffer_result.ok()) << buffer_result.status().ToString();
    auto buffer = std::move(buffer_result).MoveValueUnsafe();
    const int64_t allocated_bytes = mem_tracker->consumption() - initial_consumption;

    EXPECT_GT(allocated_bytes, 0);
    EXPECT_GE(allocated_bytes, buffer->size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()), "hello");

    reader->close();
    reader.reset();
    parquet_file.reset();

    EXPECT_EQ(mem_tracker->consumption(), initial_consumption + allocated_bytes);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()), "hello");

    buffer.reset();
    EXPECT_EQ(mem_tracker->consumption(), initial_consumption);
}

} // namespace
} // namespace starrocks
