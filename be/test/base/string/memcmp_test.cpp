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

} // namespace starrocks
