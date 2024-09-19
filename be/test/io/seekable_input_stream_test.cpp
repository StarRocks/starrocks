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

#include <gtest/gtest.h>

#include "common/logging.h"
#include "fs/encrypt_file.h"
#include "io/array_input_stream.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::io {

class TestInputStream : public io::SeekableInputStream {
public:
    explicit TestInputStream(std::string contents, int64_t block_size)
            : _contents(std::move(contents)), _block_size(block_size) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min(count, _block_size);
        count = std::min(count, (int64_t)_contents.size() - _offset);
        memcpy(data, &_contents[_offset], count);
        _offset += count;
        return count;
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _contents.size());
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _contents.size(); }

private:
    std::string _contents;
    int64_t _block_size;
    int64_t _offset{0};
};

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_skip) {
    TestInputStream in("0123456789", 10);
    char buff[12];
    ASSERT_OK(in.skip(2));
    ASSERT_EQ(2, *in.position());
    ASSERT_EQ(4, *in.read(buff, 4));
    ASSERT_EQ("2345", std::string_view(buff, 4));

    ASSERT_OK(in.skip(6));
    ASSERT_EQ(10, *in.position());
    ASSERT_EQ(0, *in.read(buff, 4));
}

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_read_fully) {
    TestInputStream in("0123456789", 2);
    char buff[12];
    ASSERT_OK(in.read_fully(buff, 6));
    ASSERT_EQ(6, *in.position());
    ASSERT_EQ("012345", std::string_view(buff, 6));
    ASSERT_ERROR(in.read_fully(buff, 6));
    ASSERT_EQ(10, *in.position());

    ASSERT_OK(in.seek(0));
    ASSERT_ERROR(in.read_fully(buff, 12));
}

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_read_at) {
    TestInputStream in("0123456789", 5);
    char buff[10];
    ASSERT_EQ(5, *in.read_at(1, buff, 10));
    ASSERT_EQ(6, *in.position());

    ASSERT_EQ(0, *in.read_at(10, buff, 10));
    ASSERT_EQ(10, *in.position());

    ASSERT_EQ(0, *in.read_at(20, buff, 10));
    ASSERT_EQ(10, *in.position());
}

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_read_at_fully) {
    TestInputStream in("0123456789", 5);
    char buff[10];
    ASSERT_OK(in.read_at_fully(1, buff, 9));
    ASSERT_EQ(10, *in.position());

    ASSERT_ERROR(in.read_at_fully(1, buff, 10));
}

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_encrypted) {
    TestInputStream in("0123456789", 5);
    char buff[10];
    ASSERT_OK(in.read_at_fully(1, buff, 9));
    ASSERT_EQ(10, *in.position());

    ASSERT_ERROR(in.read_at_fully(1, buff, 10));
    ASSERT_FALSE(in.is_encrypted());
    std::unique_ptr<SeekableInputStream> s =
            std::make_unique<SeekableInputStreamWrapper>(&in, Ownership::kDontTakeOwnership);
    ASSERT_FALSE(s->is_encrypted());

    EncryptSeekableInputStream encrypted(std::move(s), {EncryptionAlgorithmPB::AES_128, "0000000000000000"});
    ASSERT_TRUE(encrypted.is_encrypted());
}

} // namespace starrocks::io
