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

#include "util/memcmp.h"

#include <gtest/gtest.h>

namespace starrocks {

template <class T>
int vsigned(T v) {
    return (v >> (sizeof(T) * 8 - 1)) & 1;
}

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

} // namespace starrocks
