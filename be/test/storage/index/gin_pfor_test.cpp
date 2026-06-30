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

#include "storage/index/inverted/builtin/gin_pfor.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

#include "base/string/faststring.h"

namespace starrocks {

class GinPforTest : public testing::Test {
protected:
    // encode `in`, decode it back, and assert the round-trip is lossless and that decode()
    // reports it consumed exactly the encoded length.
    static void check_roundtrip(const std::vector<uint32_t>& in) {
        faststring buf;
        gin_pfor::encode(in.data(), in.size(), &buf);
        std::vector<uint32_t> out(in.size(), 0xDEADBEEF);
        size_t consumed = gin_pfor::decode(buf.data(), buf.size(), in.size(), out.data());
        EXPECT_EQ(consumed, buf.size()) << "decode must consume the whole encoded buffer";
        EXPECT_EQ(out, in);
    }
};

// Pins the frozen byte layout for a simple no-exception stream: values {3,1,2}, max=3 -> bit_width=2,
// no exceptions, low stream = 3 values * 2 bits = 6 bits -> 1 byte.
// LSB-first packing: v0=3(0b11)@bits0-1, v1=1(0b01)@bits2-3, v2=2(0b10)@bits4-5 => 0b00100111 = 0x27.
TEST_F(GinPforTest, encode_exact_bytes_no_exception) {
    std::vector<uint32_t> in = {3, 1, 2};
    faststring buf;
    gin_pfor::encode(in.data(), in.size(), &buf);
    ASSERT_EQ(buf.size(), 3u);
    EXPECT_EQ(buf.data()[0], 0x02); // bit_width
    EXPECT_EQ(buf.data()[1], 0x00); // num_exceptions
    EXPECT_EQ(buf.data()[2], 0x27); // packed low stream
}

// Pins the frozen byte layout for the exception path: {2,1,3,2,1000,2,1,3}, max=1000 -> the
// size-optimal width is 2 with one exception (1000). Header {bit_width=2, num_exceptions=1},
// exception {pos=4, high=1000>>2=250 -> varint 0xFA 0x01}, then the 8 low-2-bit values packed
// LSB-first into 2 bytes (0xB6, 0xD8). This characterizes the chosen width + exception encoding so a
// refactor of the width-selection heuristic cannot silently change the output.
TEST_F(GinPforTest, encode_exact_bytes_with_exception) {
    std::vector<uint32_t> in = {2, 1, 3, 2, 1000, 2, 1, 3};
    faststring buf;
    gin_pfor::encode(in.data(), in.size(), &buf);
    ASSERT_EQ(buf.size(), 7u);
    const uint8_t expected[] = {0x02, 0x01, 0x04, 0xFA, 0x01, 0xB6, 0xD8};
    for (size_t i = 0; i < sizeof(expected); ++i) {
        EXPECT_EQ(buf.data()[i], expected[i]) << "byte " << i;
    }
}

TEST_F(GinPforTest, roundtrip_basic) {
    check_roundtrip({2, 1, 3, 2, 1, 2, 1, 3});
}

// One large outlier among small values: PFOR must patch it rather than corrupt the round-trip.
TEST_F(GinPforTest, roundtrip_single_outlier) {
    check_roundtrip({2, 1, 3, 2, 1000, 2, 1, 3});
}

TEST_F(GinPforTest, roundtrip_empty) {
    std::vector<uint32_t> in = {};
    faststring buf;
    gin_pfor::encode(in.data(), in.size(), &buf);
    // empty stream still emits the 2-byte header (bit_width=0, num_exceptions=0).
    ASSERT_EQ(buf.size(), 2u);
    EXPECT_EQ(buf.data()[0], 0x00);
    EXPECT_EQ(buf.data()[1], 0x00);
    size_t consumed = gin_pfor::decode(buf.data(), buf.size(), 0, nullptr);
    EXPECT_EQ(consumed, 2u);
}

TEST_F(GinPforTest, roundtrip_single_value) {
    check_roundtrip({5});
}

TEST_F(GinPforTest, roundtrip_all_zeros) {
    check_roundtrip({0, 0, 0, 0});
}

TEST_F(GinPforTest, roundtrip_all_equal) {
    check_roundtrip({7, 7, 7});
}

TEST_F(GinPforTest, roundtrip_max_u32) {
    check_roundtrip({UINT32_MAX, 0, UINT32_MAX, 1});
}

// Several outliers of differing magnitude mixed with small values.
TEST_F(GinPforTest, roundtrip_mixed_outliers) {
    check_roundtrip({1, 2, 100000, 3, 1, 70000, 2, 1, 4, 999999});
}

// Two streams concatenated into one buffer (as a block stores gaps then tfs): decoding the first
// must report the exact byte offset where the second begins.
TEST_F(GinPforTest, decode_framing_two_streams) {
    std::vector<uint32_t> a = {2, 1, 3, 2};       // gaps-like
    std::vector<uint32_t> b = {1, 2, 1, 1, 5000}; // tfs-like with an outlier
    faststring buf;
    gin_pfor::encode(a.data(), a.size(), &buf);
    size_t size_a = buf.size();
    gin_pfor::encode(b.data(), b.size(), &buf);

    std::vector<uint32_t> out_a(a.size(), 0);
    size_t consumed_a = gin_pfor::decode(buf.data(), buf.size(), a.size(), out_a.data());
    EXPECT_EQ(consumed_a, size_a);
    EXPECT_EQ(out_a, a);

    std::vector<uint32_t> out_b(b.size(), 0);
    size_t consumed_b = gin_pfor::decode(buf.data() + consumed_a, buf.size() - consumed_a, b.size(), out_b.data());
    EXPECT_EQ(consumed_a + consumed_b, buf.size());
    EXPECT_EQ(out_b, b);
}

// Deterministic fuzz: many random stream sizes/values (with occasional large outliers) round-trip.
TEST_F(GinPforTest, fuzz_roundtrip) {
    std::mt19937 rng(12345);
    for (int iter = 0; iter < 1000; ++iter) {
        size_t n = rng() % 129; // 0..128 (GIN block size)
        std::vector<uint32_t> in(n);
        for (size_t i = 0; i < n; ++i) {
            uint32_t r = rng();
            if (r % 16 == 0) {
                in[i] = r; // ~6% large outliers
            } else {
                in[i] = r % 8; // mostly small, like real gaps/tfs
            }
        }
        check_roundtrip(in);
    }
}

// Truncated input must be rejected (return 0), never read out of bounds.
TEST_F(GinPforTest, decode_truncated_returns_zero) {
    std::vector<uint32_t> in = {2, 1, 1000, 3};
    faststring buf;
    gin_pfor::encode(in.data(), in.size(), &buf);
    std::vector<uint32_t> out(in.size(), 0);
    // Drop the last byte -> truncated low stream.
    EXPECT_EQ(gin_pfor::decode(buf.data(), buf.size() - 1, in.size(), out.data()), 0u);
    // A 1-byte buffer is too short even for the header.
    EXPECT_EQ(gin_pfor::decode(buf.data(), 1, in.size(), out.data()), 0u);
}

} // namespace starrocks
