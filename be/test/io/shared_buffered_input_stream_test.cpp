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

#include "io/shared_buffered_input_stream.h"

#include <gtest/gtest.h>

#include "io_test_base.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::io {

class SharedBufferedInputStreamTest : public ::testing::Test {};

PARALLEL_TEST(SharedBufferedInputStreamTest, test_release) {
    size_t len = 1 * 1024 * 1024; // 1MB
    const std::string rand_string = random_string(len);
    auto in = std::make_shared<TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 1024
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    // make two ranges one is active and another is lazy to avoid merging together.
    // 150k -> 520k
    auto r_active = io::SharedBufferedInputStream::IORange(150 * 1024, 370 * 1024, true);
    ranges.push_back(r_active);
    // 550k -> 650k
    auto r_lazy = io::SharedBufferedInputStream::IORange(550 * 1024, 100 * 1024, false);
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
    size_t len = 100 * 1024 * 1024; // 1MB
    const std::string rand_string = random_string(len);
    auto in = std::make_shared<TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 256kb
    std::vector<io::SharedBufferedInputStream::IORange> ranges;

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

} // namespace starrocks::io