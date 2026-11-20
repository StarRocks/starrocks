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

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "common/logging.h"
#include "io/array_input_stream.h"

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

    Status touch_cache(int64_t offset, size_t length) override { return Status::InvalidArgument("TestInputStream"); }
    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return Status::InvalidArgument("TestInputStream");
    }

private:
    std::string _contents;
    int64_t _block_size;
    int64_t _offset{0};
};

namespace {

class WarmTestHandle final : public CacheWarmHandle {
public:
    size_t submitted_blocks() const override { return 2; }
    size_t pending_blocks() const override { return 1; }
};

// Answers warm_cache_async() and records what it was asked.
class WarmRecordingStream final : public TestInputStream {
public:
    WarmRecordingStream() : TestInputStream("", 1) {}
    StatusOr<std::shared_ptr<CacheWarmHandle>> warm_cache_async(int64_t offset, size_t length, bool urgent) override {
        last_offset = offset;
        last_length = length;
        last_urgent = urgent;
        return std::make_shared<WarmTestHandle>();
    }
    int64_t last_offset = -1;
    size_t last_length = 0;
    bool last_urgent = false;
};

} // namespace

// A stream that cannot warm the cache says so, and a wrapper forwards the request to the stream it wraps.
PARALLEL_TEST(SeekableInputStreamTest, test_warm_cache_async) {
    TestInputStream plain("abc", 1);
    EXPECT_TRUE(plain.warm_cache_async(0, 3).status().is_not_supported());

    auto owned = std::make_unique<WarmRecordingStream>();
    auto* inner = owned.get();
    SeekableInputStreamWrapper wrapper(std::move(owned));
    ASSIGN_OR_ABORT(auto handle, wrapper.warm_cache_async(5, 7, true));
    EXPECT_EQ(2u, handle->submitted_blocks());
    EXPECT_EQ(1u, handle->pending_blocks());
    EXPECT_EQ(5, inner->last_offset);
    EXPECT_EQ(7u, inner->last_length);
    EXPECT_TRUE(inner->last_urgent);
}

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
PARALLEL_TEST(SeekableInputStreamTest, test_touch_cache) {
    TestInputStream in("0123456789", 5);
    SeekableInputStreamWrapper wrapper(&in, Ownership::kDontTakeOwnership);
    ASSERT_TRUE(wrapper.touch_cache(0, 0).is_invalid_argument());
}

// NOLINTNEXTLINE
PARALLEL_TEST(SeekableInputStreamTest, test_get_numeric_statistics) {
    TestInputStream in("0123456789", 5);
    SeekableInputStreamWrapper wrapper(&in, Ownership::kDontTakeOwnership);
    ASSERT_TRUE(wrapper.get_numeric_statistics().status().is_invalid_argument());
}

} // namespace starrocks::io
