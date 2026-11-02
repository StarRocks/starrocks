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

#include "base/string/memcmp.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <vector>

namespace starrocks {

template <class T>
int vsigned(T v) {
    return (v >> (sizeof(T) * 8 - 1)) & 1;
}

#if defined(__SSE4_2__)
TEST(sse_memcmp, Test) {
    ASSERT_EQ(vsigned((int)-1), 1);
    ASSERT_EQ(vsigned((int)1), 0);

    {
        const char c1[32] = "ABC";
        const char c2[32] = "CBA";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }
    {
        const char c1[32] = "AAB";
        const char c2[32] = "ABA";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }
    {
        const char c1[32] = "ABC";
        const char c2[32] = "ABA";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }
    {
        const char c1[32] = "ABC";
        const char c2[32] = "ABC";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(res, res2);
    }

    {
        const char c1[32] = "ABC";
        const char c2[32] = "你好";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }

    {
        const char c1[32] = "";
        const char c2[32] = "你好";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }

    {
        const char c1[32] = "0123456789abcdef";
        const char c2[32] = "0123456789abcdff";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(vsigned(res), vsigned(res2));
    }

    {
        const char c1[32] = "0123456789abcdef";
        const char c2[32] = "0123456789abcdef";

        int res = memcmp(c1, c2, 3);
        int res2 = sse_memcmp2(c1, c2, 3);
        ASSERT_EQ(res, res2);
    }
}

TEST(MemcmpTest, SimdMemcmp2Equal16Bytes) {
    alignas(16) char buf1[32];
    alignas(16) char buf2[32];
    for (int i = 0; i < 32; ++i) {
        buf1[i] = static_cast<char>((i * 29 + 7) & 0xFF);
        buf2[i] = buf1[i];
    }

    // Identical 16-byte buffers produce mask == 0; must return 0 without UB.
    for (size_t sz = 0; sz <= 16; ++sz) {
        ASSERT_EQ(0, sse_memcmp2(buf1, buf2, sz)) << "sz=" << sz;
    }

    // Difference at index >= sz must still return 0.
    for (size_t sz = 0; sz < 16; ++sz) {
        buf2[sz] = static_cast<char>(buf1[sz] ^ 0x55);
        ASSERT_EQ(0, sse_memcmp2(buf1, buf2, sz)) << "sz=" << sz;
        ASSERT_NE(0, sse_memcmp2(buf1, buf2, sz + 1)) << "sz=" << sz;
        buf2[sz] = buf1[sz];
    }

    // Unsigned high-bit byte comparison (0x80 > 0x7F).
    buf1[5] = static_cast<char>(0x80);
    buf2[5] = static_cast<char>(0x7F);
    ASSERT_GT(sse_memcmp2(buf1, buf2, 16), 0);
    ASSERT_LT(sse_memcmp2(buf2, buf1, 16), 0);
}
#endif

TEST(MemcmpTest, MemequalMultiChunkAndUnaligned) {
    std::vector<char> raw1(256, 0);
    std::vector<char> raw2(256, 0);
    for (size_t i = 0; i < raw1.size(); ++i) {
        raw1[i] = static_cast<char>((i * 41 + 19) & 0xFF);
    }

    for (size_t off1 = 0; off1 < 16; ++off1) {
        for (size_t off2 = 0; off2 < 16; ++off2) {
            for (size_t len = 0; len <= 100; ++len) {
                char* p1 = raw1.data() + off1;
                char* p2 = raw2.data() + off2;
                std::memcpy(p2, p1, len);

                ASSERT_TRUE(memequal(p1, len, p2, len)) << "off1=" << off1 << " off2=" << off2 << " len=" << len;
                ASSERT_FALSE(memequal(p1, len, p2, len + 1));

                if (len > 0) {
                    const size_t positions[3] = {0, len / 2, len - 1};
                    for (size_t pos : positions) {
                        p2[pos] = static_cast<char>(p1[pos] ^ 0x01);
                        ASSERT_FALSE(memequal(p1, len, p2, len))
                                << "off1=" << off1 << " off2=" << off2 << " len=" << len << " pos=" << pos;
                        p2[pos] = p1[pos];
                    }
                }
            }
        }
    }
}

TEST(MemcmpTest, MemcompareAndPaddedOracle) {
    auto oracle = [](const char* p1, size_t s1, const char* p2, size_t s2) -> int {
        size_t min_s = std::min(s1, s2);
        int r = std::memcmp(p1, p2, min_s);
        if (r != 0) return r > 0 ? 1 : -1;
        return compare(s1, s2);
    };

    std::vector<char> raw1(256, 0);
    std::vector<char> raw2(256, 0);
    for (size_t i = 0; i < raw1.size(); ++i) {
        raw1[i] = static_cast<char>((i * 73 + 0x80) & 0xFF);
    }

    for (size_t off : {size_t(0), size_t(1), size_t(7), size_t(15)}) {
        for (size_t len = 0; len <= 64; ++len) {
            char* p1 = raw1.data() + off;
            char* p2 = raw2.data() + off;
            std::memcpy(p2, p1, len);

            ASSERT_EQ(0, memcompare(p1, len, p2, len));
            ASSERT_EQ(0, memcompare_padded(p1, len, p2, len));
            ASSERT_EQ(-1, memcompare(p1, len, p2, len + 1));
            ASSERT_EQ(1, memcompare(p1, len + 1, p2, len));
            ASSERT_EQ(-1, memcompare_padded(p1, len, p2, len + 1));
            ASSERT_EQ(1, memcompare_padded(p1, len + 1, p2, len));

            for (size_t pos = 0; pos < len; ++pos) {
                const uint8_t orig = static_cast<uint8_t>(p1[pos]);
                for (uint8_t cand : {uint8_t(0x00), uint8_t(0x7F), uint8_t(0x80), uint8_t(0xFF), uint8_t(orig ^ 1)}) {
                    p2[pos] = static_cast<char>(cand);
                    int exp = oracle(p1, len, p2, len);
                    ASSERT_EQ(exp, memcompare(p1, len, p2, len)) << "len=" << len << " pos=" << pos;
                    ASSERT_EQ(exp, memcompare_padded(p1, len, p2, len)) << "len=" << len << " pos=" << pos;
                }
                p2[pos] = static_cast<char>(orig);
            }
        }
    }
}

TEST(MemcmpTest, PageBoundaryGuard) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    void* map1 = mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    void* map2 = mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(MAP_FAILED, map1);
    ASSERT_NE(MAP_FAILED, map2);
    ASSERT_EQ(0, mprotect(static_cast<char*>(map1) + page_size, page_size, PROT_NONE));
    ASSERT_EQ(0, mprotect(static_cast<char*>(map2) + page_size, page_size, PROT_NONE));

    char* end1 = static_cast<char*>(map1) + page_size;
    char* end2 = static_cast<char*>(map2) + page_size;

    for (size_t len = 0; len <= 64; ++len) {
        char* p1 = end1 - len;
        char* p2 = end2 - len;
        for (size_t i = 0; i < len; ++i) {
            p1[i] = static_cast<char>((i * 37 + 11) & 0xFF);
            p2[i] = p1[i];
        }
        EXPECT_TRUE(memequal(p1, len, p2, len)) << "len=" << len;
        EXPECT_EQ(0, memcompare(p1, len, p2, len)) << "len=" << len;
        EXPECT_EQ(0, memcompare_padded(p1, len, p2, len)) << "len=" << len;

        if (len > 0) {
            p2[len - 1] = static_cast<char>(static_cast<uint8_t>(p1[len - 1]) ^ 0x80);
            EXPECT_FALSE(memequal(p1, len, p2, len)) << "len=" << len;
            int expected = (static_cast<uint8_t>(p1[len - 1]) < static_cast<uint8_t>(p2[len - 1])) ? -1 : 1;
            EXPECT_EQ(expected, memcompare(p1, len, p2, len)) << "len=" << len;
            EXPECT_EQ(expected, memcompare_padded(p1, len, p2, len)) << "len=" << len;
        }
    }

    munmap(map1, 2 * page_size);
    munmap(map2, 2 * page_size);
}

} // namespace starrocks
