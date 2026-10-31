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

#include "base/string/volnitsky.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

#include "base/string/tight_buffer.h"

namespace starrocks {

class VolnitskyTest : public ::testing::Test {};

TEST_F(VolnitskyTest, MainAlgorithmPathFindsNeedle) {
    const std::string needle = "pattern";
    std::string haystack(25000, 'x');
    const size_t expected_offset = 12345;
    std::copy(needle.begin(), needle.end(), haystack.begin() + expected_offset);

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + expected_offset, result);
}

TEST_F(VolnitskyTest, FallbackPathWithShortNeedle) {
    const std::string haystack = "abcyd";
    const std::string needle = "y";

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + 3, result);
}

TEST_F(VolnitskyTest, EmptyNeedleReturnsHaystackBegin) {
    const std::string haystack = "abcdef";
    VolnitskyUTF8 searcher("", 0, haystack.size());
    EXPECT_EQ(haystack.data(), searcher.search(haystack.data(), haystack.size()));
}

TEST_F(VolnitskyTest, NotFoundReturnsHaystackEnd) {
    const std::string haystack = "abcdef";
    const std::string needle = "xyz";

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    EXPECT_EQ(haystack.data() + haystack.size(), searcher.search(haystack.data(), haystack.size()));
}

TEST_F(VolnitskyTest, Utf8NeedleFoundAtExpectedOffset) {
    const std::string haystack =
            "StarRocks-"
            "\xE6\x95\xB0\xE6\x8D\xAE\xE5\xBA\x93"
            "-BE";
    const std::string needle = "\xE6\x95\xB0\xE6\x8D\xAE\xE5\xBA\x93";

    const size_t expected_offset = haystack.find(needle);
    ASSERT_NE(std::string::npos, expected_offset);

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + expected_offset, result);
}

TEST_F(VolnitskyTest, BinaryNeedleWithEmbeddedNull) {
    const char haystack_raw[] = {'a', 'b', '\0', 'c', '\0', 'd', 'e', 'f'};
    const char needle_raw[] = {'c', '\0', 'd', 'e'};

    VolnitskyUTF8 searcher(needle_raw, sizeof(needle_raw), 0);
    const char* result = searcher.search(haystack_raw, sizeof(haystack_raw));
    EXPECT_EQ(haystack_raw + 3, result);
}

TEST_F(VolnitskyTest, ShortAndMediumNeedlesUseSimdFallback) {
    EXPECT_TRUE(VolnitskyTraits::isFallbackNeedle(1, 25000));
    EXPECT_TRUE(VolnitskyTraits::isFallbackNeedle(6, 25000));
    EXPECT_TRUE(VolnitskyTraits::isFallbackNeedle(32, 25000));
    EXPECT_FALSE(VolnitskyTraits::isFallbackNeedle(33, 25000));
}

TEST_F(VolnitskyTest, LongNeedleCollisionChainsAndEndOfHaystack) {
    // 40-byte needle with the bigram "BG" at offsets 0 and 38 (a collision chain in the hash table).
    std::string needle(40, 'a');
    needle[0] = 'B';
    needle[1] = 'G';
    needle[38] = 'B';
    needle[39] = 'G';

    // Probes happen at 38 + 39k while pos <= size - 40 (24960): the last in-range probe is 24959 (k = 639).
    // Note: compare() cannot over-read through VolnitskyBase (res = pos - i <= pos <= end - needle_size).
    for (size_t offset : {size_t(0), size_t(1), size_t(14321), size_t(24959), size_t(24960)}) {
        std::string h(25000, 'x');
        h[24959] = 'B';
        h[24960] = 'G'; // decoy bigram on the last in-range probe
        h.replace(offset, needle.size(), needle);
        TightBuffer hay(h);
        VolnitskyUTF8 searcher(needle.data(), needle.size(), h.size());
        EXPECT_EQ(hay.begin() + offset, searcher.search(hay.begin(), h.size())) << "offset=" << offset;
    }

    std::string decoys(25000, 'x');
    decoys[24959] = 'B';
    decoys[24960] = 'G';
    decoys[100] = 'B';
    decoys[101] = 'G';
    TightBuffer miss(decoys);
    VolnitskyUTF8 searcher(needle.data(), needle.size(), decoys.size());
    EXPECT_EQ(miss.end(), searcher.search(miss.begin(), decoys.size()));
}

} // namespace starrocks
