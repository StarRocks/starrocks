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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/utf8_check_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/string/utf8_check.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "base/string/utf8.h"
#include "simdutf.h"

namespace starrocks {

struct test {
    const char* data;
    int len;
};

class Utf8CheckTest : public testing::Test {
public:
    Utf8CheckTest() = default;
    ~Utf8CheckTest() override = default;

protected:
    /* positive tests */
    std::vector<test> pos = {{"", 0},
                             {"\x00", 1},
                             {R"(f)", 1},
                             {"\x7F", 1},
                             {"\x00\x7F", 2},
                             {"\x7F\x00", 2},
                             {"\xC2\x80", 2},
                             {"\xDF\xBF", 2},
                             {"\xE0\xA0\x80", 3},
                             {"\xE0\xA0\xBF", 3},
                             {"\xED\x9F\x80", 3},
                             {"\xEF\x80\xBF", 3},
                             {"\xF0\x90\xBF\x80", 4},
                             {"\xF2\x81\xBE\x99", 4},
                             {"\xF4\x8F\x88\xAA", 4}};

    /* negative tests */
    std::vector<test> neg = {{"\x80", 1},
                             {"\xBF", 1},
                             {"\xC0\x80", 2},
                             {"\xC1\x00", 2},
                             {"\xC2\x7F", 2},
                             {"\xDF\xC0", 2},
                             {"\xE0\x9F\x80", 3},
                             {"\xE0\xC2\x80", 3},
                             {"\xED\xA0\x80", 3},
                             {"\xED\x7F\x80", 3},
                             {"\xEF\x80\x00", 3},
                             {"\xF0\x8F\x80\x80", 4},
                             {"\xF0\xEE\x80\x80", 4},
                             {"\xF2\x90\x91\x7F", 4},
                             {"\xF4\x90\x88\xAA", 4},
                             {"\xF4\x00\xBF\xBF", 4},
                             {"\x00\x00\x00\x00\x00\xC2\x80\x00\x00\x00\xE1\x80\x80\x00\x00\xC2"
                              "\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                              32},
                             {"\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00", 16},
                             {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                              "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80",
                              32},
                             {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                              "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1",
                              32},
                             {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                              "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80"
                              "\x80",
                              33},
                             {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                              "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80"
                              "\xC2\x80",
                              34},
                             {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                              "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0"
                              "\x80\x80\x80",
                              35}};
};
TEST_F(Utf8CheckTest, empty) {
    ASSERT_TRUE(validate_utf8(pos[0].data, pos[0].len));
}

TEST_F(Utf8CheckTest, normal) {
    for (size_t i = 0; i < pos.size(); ++i) {
        ASSERT_TRUE(validate_utf8(pos[i].data, pos[i].len)) << "pos index: " << i;
    }
}

TEST_F(Utf8CheckTest, abnormal) {
    for (size_t i = 0; i < neg.size(); ++i) {
        ASSERT_FALSE(validate_utf8(neg[i].data, neg[i].len)) << "neg index: " << i;
    }
}

TEST_F(Utf8CheckTest, naive) {
    for (size_t i = 0; i < pos.size(); ++i) {
        ASSERT_TRUE(validate_utf8_naive(pos[i].data, pos[i].len)) << "pos index: " << i;
    }
    for (size_t i = 0; i < neg.size(); ++i) {
        ASSERT_FALSE(validate_utf8_naive(neg[i].data, neg[i].len)) << "neg index: " << i;
    }
}

TEST_F(Utf8CheckTest, NullAndEmptyPointers) {
    ASSERT_TRUE(validate_utf8(nullptr, 0));
    ASSERT_TRUE(validate_ascii_fast(nullptr, 0));
    ASSERT_EQ(0, utf8_len(nullptr, nullptr));
    ASSERT_EQ(0u, simdutf::binary_length_from_base64(static_cast<const char*>(nullptr), 0));
}

TEST_F(Utf8CheckTest, UnalignedAndTailLengths) {
    std::vector<char> storage(4096 + 128, 'a');
    const std::vector<size_t> lengths = {0,  1,  2,  3,  7,  8,   15,  16,  17,  31,  32,  33,  63,
                                         64, 65, 95, 96, 97, 127, 128, 129, 191, 192, 255, 256, 2100};

    for (size_t align = 0; align < 64; ++align) {
        char* buf = storage.data() + align;
        for (size_t len : lengths) {
            std::memset(buf, 'x', len);
            ASSERT_TRUE(validate_ascii_fast(buf, len)) << "align=" << align << " len=" << len;
            ASSERT_TRUE(validate_utf8(buf, len)) << "align=" << align << " len=" << len;
            ASSERT_EQ(static_cast<int>(len), utf8_len(buf, buf + len)) << "align=" << align << " len=" << len;

            if (len > 0) {
                // Corrupt last byte with invalid UTF-8 / non-ASCII bytes
                for (uint8_t bad : {uint8_t(0x80), uint8_t(0xC2), uint8_t(0xF1), uint8_t(0xFF)}) {
                    buf[len - 1] = static_cast<char>(bad);
                    ASSERT_FALSE(validate_ascii_fast(buf, len))
                            << "align=" << align << " len=" << len << " bad=" << int(bad);
                    ASSERT_FALSE(validate_utf8(buf, len)) << "align=" << align << " len=" << len << " bad=" << int(bad);
                }
                buf[len - 1] = 'x';
            }

            // Valid 2-byte, 3-byte, and 4-byte UTF-8 sequences at the tail
            if (len >= 2) {
                buf[len - 2] = '\xC3';
                buf[len - 1] = '\xA9'; // U+00E9
                ASSERT_FALSE(validate_ascii_fast(buf, len));
                ASSERT_TRUE(validate_utf8(buf, len));
                ASSERT_EQ(static_cast<int>(len - 1), utf8_len(buf, buf + len));
                buf[len - 2] = 'x';
                buf[len - 1] = 'x';
            }
            if (len >= 3) {
                buf[len - 3] = '\xE4';
                buf[len - 2] = '\xB8';
                buf[len - 1] = '\xAD'; // U+4E2D
                ASSERT_FALSE(validate_ascii_fast(buf, len));
                ASSERT_TRUE(validate_utf8(buf, len));
                ASSERT_EQ(static_cast<int>(len - 2), utf8_len(buf, buf + len));
                buf[len - 3] = 'x';
                buf[len - 2] = 'x';
                buf[len - 1] = 'x';
            }
            if (len >= 4) {
                buf[len - 4] = '\xF0';
                buf[len - 3] = '\x9F';
                buf[len - 2] = '\x98';
                buf[len - 1] = '\x80'; // U+1F600
                ASSERT_FALSE(validate_ascii_fast(buf, len));
                ASSERT_TRUE(validate_utf8(buf, len));
                ASSERT_EQ(static_cast<int>(len - 3), utf8_len(buf, buf + len));
                buf[len - 4] = 'x';
                buf[len - 3] = 'x';
                buf[len - 2] = 'x';
                buf[len - 1] = 'x';
            }
        }
    }
}

TEST_F(Utf8CheckTest, SimdutfBase64ExactBufferOverflowRegression) {
    constexpr uint8_t kCanary = 0xA5;

    for (const auto* impl : simdutf::get_available_implementations()) {
        if (!impl->supported_by_runtime_system()) {
            continue;
        }
        SCOPED_TRACE(std::string("simdutf impl=") + std::string(impl->name()));

        auto check_exact = [&](const std::string& input, simdutf::base64_options options, size_t expected_len) {
            size_t need = impl->binary_length_from_base64(input.data(), input.size());
            ASSERT_LE(expected_len, need);
            std::vector<char> out(need + 32, static_cast<char>(kCanary));
            simdutf::result r = impl->base64_to_binary(input.data(), input.size(), out.data(), options);
            ASSERT_EQ(simdutf::error_code::SUCCESS, r.error);
            ASSERT_EQ(expected_len, r.count);
            for (size_t i = expected_len; i < out.size(); ++i) {
                EXPECT_EQ(kCanary, static_cast<uint8_t>(out[i]))
                        << "Buffer overflow at byte offset +" << (i - expected_len) << " past expected_len ("
                        << expected_len << ") on impl=" << impl->name();
            }
        };

        // Case A (simdutf #1044 & 9f87f25c):
        //   - 64 Base64 chars + 64 spaces (no trailing payload -> 48 decoded bytes)
        //   - 64 Base64 chars + 64 spaces + 64 Base64 chars (-> 96 decoded bytes)
        //   - 64 Base64 chars + 1000 spaces + "AAAA" (-> 51 decoded bytes)
        check_exact(std::string(64, 'A') + std::string(64, ' '), simdutf::base64_default, 48u);
        check_exact(std::string(64, 'A') + std::string(64, ' ') + std::string(64, 'A'), simdutf::base64_default, 96u);
        check_exact(std::string(64, 'A') + std::string(1000, ' ') + "AAAA", simdutf::base64_default, 51u);

        // Case B (simdutf #1044): Five 56-'A' runs and one 52-'A'+4-newline run separated by spaces + "AAAAAA=="
        // (338 Base64 chars -> 253 decoded bytes). Triggers the multi-block buffer flush in compress_decode_base64
        // where end_of_safe_64byte_zone was inflated by whitespace.
        std::string runs;
        for (int g = 0; g < 6; ++g) {
            if (g == 2) {
                runs += std::string(52, 'A') + "\n\n\n\n";
            } else {
                runs += std::string(56, 'A');
            }
            runs += ' ';
        }
        runs += "AAAAAA==";
        check_exact(runs, simdutf::base64_default, 253u);

        // Case C (simdutf #1044): 60 'A's + "#" + 32 spaces + 7 'A's under base64_default_accept_garbage (50 decoded bytes).
        std::string with_garbage = std::string(60, 'A') + "#" + std::string(32, ' ') + std::string(7, 'A');
        check_exact(with_garbage, simdutf::base64_default_accept_garbage, 50u);

        // Case D (simdutf #1074): 59 spaces + "AAAAA" (5 Base64 chars) with loose handling -> 3 valid bytes before remainder error.
        {
            std::string input = std::string(59, ' ') + "AAAAA";
            size_t exact_len = 3;
            std::vector<char> out(exact_len + 16, static_cast<char>(kCanary));
            simdutf::result r = impl->base64_to_binary(input.data(), input.size(), out.data(), simdutf::base64_default,
                                                       simdutf::last_chunk_handling_options::loose);
            ASSERT_EQ(simdutf::error_code::BASE64_INPUT_REMAINDER, r.error);
            for (size_t i = exact_len; i < out.size(); ++i) {
                EXPECT_EQ(kCanary, static_cast<uint8_t>(out[i]))
                        << "Case D buffer overflow at byte offset +" << (i - exact_len) << " on impl=" << impl->name();
            }
        }

        // Case E (simdutf 877be5d5): '[' (0x5B) in base64_default_or_url mode must be rejected as INVALID_BASE64_CHARACTER.
        {
            std::string input(128, 'A');
            input[31] = '[';
            std::vector<char> out(96, 0);
            simdutf::result r =
                    impl->base64_to_binary(input.data(), input.size(), out.data(), simdutf::base64_default_or_url);
            EXPECT_EQ(simdutf::error_code::INVALID_BASE64_CHARACTER, r.error) << "impl=" << impl->name();
        }

        // Case F (simdutf #1087 & trailing whitespace): base64_to_binary_details must report full input_count including trailing whitespace.
        {
            std::string input = std::string(64, 'A') + "   ";
            std::vector<char> out(48, 0);
            simdutf::full_result fr = impl->base64_to_binary_details(input.data(), input.size(), out.data());
            EXPECT_EQ(simdutf::error_code::SUCCESS, fr.error);
            EXPECT_EQ(48u, fr.output_count);
            EXPECT_EQ(input.size(), fr.input_count) << "impl=" << impl->name();
        }
    }
}

#if defined(__aarch64__)
TEST_F(Utf8CheckTest, Arm64FlushToZeroSurrogateMaskRegression) {
    // Regression test for simdutf f40cd326: ARM64 any_lane_set used fcmp != 0.0 on a narrowed mask,
    // which flushes subnormal non-zero masks to 0.0 when FPCR.FZ (bit 24) is set.
    uint64_t old_fpcr = 0;
    asm volatile("mrs %0, fpcr" : "=r"(old_fpcr));
    uint64_t fz_fpcr = old_fpcr | (1ULL << 24);
    asm volatile("msr fpcr, %0" : : "r"(fz_fpcr));

    const char32_t bad_utf32[4] = {0xD800, U'a', U'a', U'a'};
    bool valid_utf32 = simdutf::validate_utf32(bad_utf32, 4);

    const char16_t bad_utf16[8] = {0xD800, u'a', u'a', u'a', u'a', u'a', u'a', u'a'};
    char utf8_out[32];
    size_t converted = simdutf::convert_utf16le_to_utf8(bad_utf16, 8, utf8_out);

    asm volatile("msr fpcr, %0" : : "r"(old_fpcr));

    EXPECT_FALSE(valid_utf32);
    EXPECT_EQ(0u, converted);
}
#endif

} // namespace starrocks
