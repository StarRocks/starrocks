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

#include "base/string/string_searcher.h"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "base/string/tight_buffer.h"

namespace starrocks {

class StringSearcherTest : public ::testing::Test {};

// Runs every compiled kernel; all results must be identical.
static std::vector<const uint8_t*> run_all_kernels(const StringSearcher& s, const uint8_t* b, const uint8_t* e,
                                                   size_t needle_len) {
    std::vector<const uint8_t*> r{s.search(b, e), s.search_scalar(b, e)};
    if (needle_len >= 2) {
#if defined(__x86_64__)
        if (__builtin_cpu_supports("sse4.1")) r.push_back(s.search_sse41(b, e));
        if (__builtin_cpu_supports("avx2")) r.push_back(s.search_avx2(b, e));
#elif defined(__ARM_NEON)
        r.push_back(s.search_neon(b, e));
#endif
    }
    return r;
}

TEST_F(StringSearcherTest, FindAndNotFound) {
    const std::string haystack = "abchellodef";
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(haystack.data());
    const uint8_t* end = begin + haystack.size();

    const char needle[] = "hello";
    StringSearcher searcher(needle, 5);
    EXPECT_EQ(begin + 3, searcher.search(begin, end));

    const char missing_needle[] = "xyz";
    StringSearcher missing(missing_needle, 3);
    EXPECT_EQ(end, missing.search(begin, end));
}

TEST_F(StringSearcherTest, EmptyNeedleReturnsHaystackBegin) {
    const std::string haystack = "abcdef";
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(haystack.data());
    const uint8_t* end = begin + haystack.size();

    const char needle[] = "";
    StringSearcher searcher(needle, 0);
    EXPECT_EQ(begin, searcher.search(begin, end));
}

TEST_F(StringSearcherTest, EmbeddedNullNeedle) {
    const uint8_t haystack_raw[] = {'a', 'b', 'c', '\0', 'd', 'e'};
    const uint8_t needle_raw[] = {'c', '\0', 'd'};
    const uint8_t* begin = haystack_raw;
    const uint8_t* end = begin + sizeof(haystack_raw);

    StringSearcher searcher(needle_raw, sizeof(needle_raw));
    EXPECT_EQ(begin + 2, searcher.search(begin, end));
}

TEST_F(StringSearcherTest, LibcSearcherFindAndMiss) {
    const char haystack[] = "aaaaab";
    LibcASCIICaseSensitiveStringSearcher searcher("aab", 3);
    EXPECT_EQ(haystack + 3, searcher.search(haystack, 6));

    LibcASCIICaseSensitiveStringSearcher missing("AAB", 3);
    EXPECT_EQ(nullptr, missing.search(haystack, 6));
}

TEST_F(StringSearcherTest, CompareRespectsHaystackBounds) {
    const char buf[] = "Qxyzhello";
    const char* haystack = buf + 1;
    const char* short_end = buf + 6; // view is "xyzhe"
    StringSearcher searcher("hello", 5);

    EXPECT_FALSE(searcher.compare(haystack, short_end, buf + 4)); // only "he" remains
    EXPECT_FALSE(searcher.compare(haystack, short_end + 3, buf)); // pos before haystack
    EXPECT_TRUE(searcher.compare(haystack, buf + 9, buf + 4));    // full view contains "hello"
}

TEST_F(StringSearcherTest, FirstEqualsLastSaturation) {
    for (size_t len : {size_t(3), size_t(16), size_t(17), size_t(32), size_t(33)}) {
        const std::string needle = "a" + std::string(len - 2, 'X') + "a";
        const std::string hit = std::string(300, 'a') + needle;
        TightBuffer hay(hit);
        TightBuffer ndl(needle);
        StringSearcher searcher(ndl.begin(), ndl.size());
        for (const auto* r : run_all_kernels(searcher, hay.begin(), hay.end(), len)) {
            EXPECT_EQ(hay.begin() + 300, r) << "len=" << len;
        }
        const std::string miss(300, 'a');
        TightBuffer miss_hay(miss);
        for (const auto* r : run_all_kernels(searcher, miss_hay.begin(), miss_hay.end(), len)) {
            EXPECT_EQ(miss_hay.end(), r) << "len=" << len;
        }
    }

    // Space-filled haystack with 32B needle (" " + 30*'X' + " ") and single-interior-diff needle
    for (const std::string& space_needle :
         {std::string(" ") + std::string(30, 'X') + " ", std::string(15, ' ') + "X" + std::string(16, ' ')}) {
        const std::string space_miss(4096, ' ');
        TightBuffer miss_buf(space_miss);
        TightBuffer ndl_buf(space_needle);
        StringSearcher searcher(ndl_buf.begin(), ndl_buf.size());
        for (const auto* r : run_all_kernels(searcher, miss_buf.begin(), miss_buf.end(), space_needle.size())) {
            EXPECT_EQ(miss_buf.end(), r);
        }
        const std::string space_hit = space_miss + space_needle;
        TightBuffer hit_buf(space_hit);
        for (const auto* r : run_all_kernels(searcher, hit_buf.begin(), hit_buf.end(), space_needle.size())) {
            EXPECT_EQ(hit_buf.begin() + 4096, r);
        }
    }
}

TEST_F(StringSearcherTest, EmptyNullAndEmbeddedNul) {
    StringSearcher two("ab", 2);
    TightBuffer empty_hay("");
    EXPECT_EQ(empty_hay.end(), two.search(empty_hay.begin(), empty_hay.end()));
    EXPECT_EQ(nullptr, two.search(static_cast<const uint8_t*>(nullptr), static_cast<const uint8_t*>(nullptr)));
    StringSearcher empty("", 0);
    TightBuffer h("xyz");
    EXPECT_EQ(h.begin(), empty.search(h.begin(), h.end()));

    const std::string nul_needle("a\0b", 3);
    const std::string nul_hay("xx a\0b yy", 9);
    TightBuffer nh(nul_hay);
    TightBuffer nn(nul_needle);
    StringSearcher nul_searcher(nn.begin(), nn.size());
    for (const auto* r : run_all_kernels(nul_searcher, nh.begin(), nh.end(), 3)) {
        EXPECT_EQ(nh.begin() + 3, r);
    }
    const std::string zero_byte("\0", 1);
    TightBuffer zb(zero_byte);
    StringSearcher zero_searcher(zb.begin(), zb.size());
    EXPECT_EQ(nh.begin() + 4, zero_searcher.search(nh.begin(), nh.end()));
}

TEST_F(StringSearcherTest, NearMatchAmplification) {
    const std::string needle = "F" + std::string(30, 'M') + "L";
    const std::string decoy = "F" + std::string(14, 'M') + "X" + std::string(15, 'M') + "L"; // 32 B, differs mid-way
    std::string hay;
    for (int i = 0; i < 2000; ++i) hay += decoy;
    hay += needle;
    TightBuffer h(hay);
    TightBuffer n(needle);
    StringSearcher searcher(n.begin(), n.size());
    for (const auto* r : run_all_kernels(searcher, h.begin(), h.end(), needle.size())) {
        EXPECT_EQ(h.begin() + 2000 * 32, r);
    }
}

TEST_F(StringSearcherTest, RandomisedDifferentialFuzz) {
    constexpr uint32_t kSeed = 0x5EED;
    std::mt19937 rng(kSeed);
    for (int iter = 0; iter < 100000; ++iter) {
        const int alphabet = 2 + static_cast<int>(rng() % 2);
        auto gen = [&](size_t n) {
            std::string s(n, 'a');
            for (auto& c : s) c = static_cast<char>('a' + rng() % alphabet);
            return s;
        };
        const size_t needle_len = 1 + rng() % 40;
        const std::string needle = gen(needle_len);
        const std::string hay = gen(rng() % 301);
        TightBuffer h(hay);
        TightBuffer n(needle);
        StringSearcher searcher(n.begin(), n.size());
        const size_t pos = std::string_view(hay).find(needle);
        const uint8_t* expected = pos == std::string_view::npos ? h.end() : h.begin() + pos;
        for (const auto* r : run_all_kernels(searcher, h.begin(), h.end(), needle_len)) {
            ASSERT_EQ(expected, r) << "seed=" << kSeed << " iter=" << iter << " needle=" << needle << " hay=" << hay;
        }
    }
}

TEST_F(StringSearcherTest, OverlappingDecoysAndMaskSaturation) {
    // 1. Multiple "g....e" decoys within the same 16B/32B SIMD window before the real "google"
    TightBuffer hay("gxxxxeg1234egooXlegoglgoogle_tail");
    const auto* begin = hay.begin();
    const auto* end = hay.end();

    StringSearcher searcher("google", 6);
    for (const auto* r : run_all_kernels(searcher, begin, end, 6)) {
        EXPECT_EQ(begin + 22, r);
    }

    // 2. All-'a' prefix followed by "baaaa"
    const std::string sat_hay = std::string(150, 'a') + "baaaa";
    TightBuffer sat_buf(sat_hay);
    const auto* sat_begin = sat_buf.begin();
    const auto* sat_end = sat_buf.end();

    StringSearcher sat_miss("aaaaac", 6);
    EXPECT_EQ(sat_end, sat_miss.search(sat_begin, sat_end));
    EXPECT_EQ(sat_end, sat_miss.search_scalar(sat_begin, sat_end));

    StringSearcher sat_hit("aaaaab", 6);
    EXPECT_EQ(sat_begin + 145, sat_hit.search(sat_begin, sat_end));
    EXPECT_EQ(sat_begin + 145, sat_hit.search_scalar(sat_begin, sat_end));

    StringSearcher sat_same("aaaaaa", 6);
    EXPECT_EQ(sat_begin, sat_same.search(sat_begin, sat_end));
    EXPECT_EQ(sat_begin, sat_same.search_scalar(sat_begin, sat_end));
}

TEST_F(StringSearcherTest, AllLengthsOffsetsAndKernelEquivalence) {
    for (size_t needle_len : {size_t(1), size_t(2), size_t(3), size_t(6), size_t(15), size_t(16), size_t(17),
                              size_t(31), size_t(32), size_t(33)}) {
        std::string needle(needle_len, 'a');
        needle.front() = 'F';
        needle.back() = 'L';
        if (needle_len > 2) {
            needle[needle_len / 2] = 'M';
        }
        TightBuffer ndl(needle);
        StringSearcher searcher(ndl.begin(), ndl.size());

        for (size_t hay_len : {size_t(0), needle_len - 1, needle_len, needle_len + 1, size_t(48), size_t(96)}) {
            std::string hay(hay_len, 'x');
            TightBuffer miss_buf(hay);
            EXPECT_EQ(miss_buf.end(), searcher.search(miss_buf.begin(), miss_buf.end()));
            EXPECT_EQ(miss_buf.end(), searcher.search_scalar(miss_buf.begin(), miss_buf.end()));

            if (hay_len >= needle_len) {
                for (size_t pos = 0; pos <= hay_len - needle_len; ++pos) {
                    std::string h = hay;
                    h.replace(pos, needle_len, needle);
                    TightBuffer hit_buf(h);
                    const auto* expected = hit_buf.begin() + pos;
                    for (const auto* r : run_all_kernels(searcher, hit_buf.begin(), hit_buf.end(), needle_len)) {
                        EXPECT_EQ(expected, r)
                                << "needle_len=" << needle_len << " hay_len=" << hay_len << " pos=" << pos;
                    }
                }
            }
        }
    }
}

} // namespace starrocks
