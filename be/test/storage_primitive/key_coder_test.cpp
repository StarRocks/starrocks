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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/key_coder_test.cpp

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

#include "storage_primitive/key_coder.h"

#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <random>

#include "common/util/debug_util.h"
#include "runtime/mem_pool.h"

namespace starrocks {

class KeyCoderTest : public testing::Test {
public:
    KeyCoderTest() = default;
    ~KeyCoderTest() override = default;

private:
    MemPool _pool;
};

template <LogicalType type>
void test_integer_encode() {
    using CppType = StorageCppType<type>;

    auto key_coder = get_key_coder(type);

    {
        std::string buf;
        CppType val = std::numeric_limits<CppType>::lowest();
        key_coder->encode_ascending(&val, sizeof(CppType), &buf);

        std::string result;
        for (int i = 0; i < sizeof(CppType); ++i) {
            result.append("00");
        }

        ASSERT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());

        {
            Slice slice(buf);
            CppType check_val;
            key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val, nullptr);
            ASSERT_EQ(val, check_val);
        }
    }

    {
        std::string buf;
        CppType val = std::numeric_limits<CppType>::max();
        key_coder->encode_ascending(&val, sizeof(CppType), &buf);

        std::string result;
        for (int i = 0; i < sizeof(CppType); ++i) {
            result.append("FF");
        }

        ASSERT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());
        {
            Slice slice(buf);
            CppType check_val;
            key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val, nullptr);
            ASSERT_EQ(val, check_val);
        }
    }

    for (auto i = 0; i < 100; ++i) {
        CppType val1 = random();
        CppType val2 = random();

        std::string buf1;
        std::string buf2;

        key_coder->encode_ascending(&val1, sizeof(CppType), &buf1);
        key_coder->encode_ascending(&val2, sizeof(CppType), &buf2);

        if (val1 < val2) {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
        } else if (val1 > val2) {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
        } else {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) == 0);
        }
    }
}

TEST_F(KeyCoderTest, test_int) {
    test_integer_encode<TYPE_TINYINT>();
    test_integer_encode<TYPE_SMALLINT>();
    test_integer_encode<TYPE_INT>();
    test_integer_encode<TYPE_UNSIGNED_INT>();
    test_integer_encode<TYPE_BIGINT>();
    test_integer_encode<TYPE_UNSIGNED_BIGINT>();
    test_integer_encode<TYPE_LARGEINT>();

    test_integer_encode<TYPE_DATETIME_V1>();
}

TEST_F(KeyCoderTest, test_date) {
    using CppType = uint24_t;
    auto key_coder = get_key_coder(TYPE_DATE_V1);

    {
        std::string buf;
        CppType val = 0;
        key_coder->encode_ascending(&val, 1, &buf);

        std::string result;
        for (int i = 0; i < sizeof(uint24_t); ++i) {
            result.append("00");
        }

        ASSERT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());

        {
            Slice slice(buf);
            CppType check_val;
            key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val, nullptr);
            ASSERT_EQ(val, check_val);
        }
    }

    {
        std::string buf;
        CppType val = 10000;
        key_coder->encode_ascending(&val, sizeof(CppType), &buf);

        std::string result("002710");

        ASSERT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());
        {
            Slice slice(buf);
            CppType check_val;
            key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val, nullptr);
            ASSERT_EQ(val, check_val);
        }
    }

    for (auto i = 0; i < 100; ++i) {
        CppType val1 = random();
        CppType val2 = random();

        std::string buf1;
        std::string buf2;

        key_coder->encode_ascending(&val1, sizeof(CppType), &buf1);
        key_coder->encode_ascending(&val2, sizeof(CppType), &buf2);

        if (val1 < val2) {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
        } else if (val1 > val2) {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
        } else {
            ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) == 0);
        }
    }
}

TEST_F(KeyCoderTest, test_decimal) {
    auto key_coder = get_key_coder(TYPE_DECIMAL);

    decimal12_t val1(1, 100000000);
    std::string buf1;

    key_coder->encode_ascending(&val1, sizeof(decimal12_t), &buf1);

    decimal12_t check_val;
    Slice slice1(buf1);
    key_coder->decode_ascending(&slice1, sizeof(decimal12_t), (uint8_t*)&check_val, nullptr);
    ASSERT_EQ(check_val, val1);

    {
        decimal12_t val2(-1, -100000000);
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
    }
    {
        decimal12_t val2(1, 100000001);
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
    }
    {
        decimal12_t val2(0, 0);
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        ASSERT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);

        std::string result("80");
        for (int i = 0; i < sizeof(int64_t) - 1; ++i) {
            result.append("00");
        }
        result.append("80");
        for (int i = 0; i < sizeof(int32_t) - 1; ++i) {
            result.append("00");
        }

        ASSERT_STREQ(result.c_str(), hexdump(buf2.data(), buf2.size()).c_str());
    }
}

TEST_F(KeyCoderTest, test_char) {
    auto key_coder = get_key_coder(TYPE_CHAR);

    char buf[] = "1234567890";
    Slice slice(buf, 10);

    {
        std::string key;
        key_coder->encode_ascending(&slice, 10, &key);
        Slice encoded_key(key);

        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 10, (uint8_t*)&check_slice, &_pool);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(10, check_slice.size);
        ASSERT_EQ(strncmp("1234567890", check_slice.data, 10), 0);
    }

    {
        std::string key;
        key_coder->encode_ascending(&slice, 5, &key);
        Slice encoded_key(key);

        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 5, (uint8_t*)&check_slice, &_pool);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(5, check_slice.size);
        ASSERT_EQ(strncmp("12345", check_slice.data, 5), 0);
    }
}

TEST_F(KeyCoderTest, test_varchar) {
    auto key_coder = get_key_coder(TYPE_VARCHAR);

    char buf[] = "1234567890";
    Slice slice(buf, 10);

    {
        std::string key;
        key_coder->encode_ascending(&slice, 15, &key);
        Slice encoded_key(key);

        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 15, (uint8_t*)&check_slice, &_pool);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(10, check_slice.size);
        ASSERT_EQ(strncmp("1234567890", check_slice.data, 10), 0);
    }

    {
        std::string key;
        key_coder->encode_ascending(&slice, 5, &key);
        Slice encoded_key(key);

        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 5, (uint8_t*)&check_slice, &_pool);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(5, check_slice.size);
        ASSERT_EQ(strncmp("12345", check_slice.data, 5), 0);
    }
}

TEST_F(KeyCoderTest, test_int256_full_encode_ordering_and_roundtrip) {
    auto key_coder = get_key_coder(TYPE_INT256);

    // most-negative, -1, 0, 1, 2^128, most-positive, in ascending order.
    std::vector<int256_t> values = {
            INT256_MIN,
            int256_t(-1),
            int256_t(0),
            int256_t(1),
            int256_t(static_cast<int128_t>(1), static_cast<uint128_t>(0)), // 2^128
            INT256_MAX,
    };

    std::vector<std::string> encoded;
    for (const auto& v : values) {
        std::string buf;
        key_coder->full_encode_ascending(&v, &buf);
        ASSERT_EQ(32, buf.size());
        encoded.push_back(buf);

        // round-trip
        Slice slice(encoded.back());
        int256_t decoded;
        auto st = key_coder->decode_ascending(&slice, 32, (uint8_t*)&decoded, nullptr);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(v, decoded);
        ASSERT_EQ(0, slice.size); // exactly 32 bytes consumed
    }

    // encoded byte order must match value order (values is already ascending).
    for (size_t i = 0; i + 1 < encoded.size(); ++i) {
        ASSERT_LT(memcmp(encoded[i].data(), encoded[i + 1].data(), 32), 0) << "index " << i << " vs " << (i + 1);
    }

    // full_encode_ascending_datum should agree with full_encode_ascending.
    for (const auto& v : values) {
        std::string buf_datum;
        Datum datum(v);
        key_coder->full_encode_ascending(datum, &buf_datum);

        std::string buf_raw;
        key_coder->full_encode_ascending(&v, &buf_raw);

        ASSERT_EQ(buf_raw, buf_datum);
    }
}

TEST_F(KeyCoderTest, test_int256_full_encode_fuzz) {
    auto key_coder = get_key_coder(TYPE_INT256);

    std::mt19937_64 rng(20260722); // fixed seed for reproducibility
    auto rand_u128 = [&rng]() -> uint128_t {
        uint64_t hi = rng();
        uint64_t lo = rng();
        return (static_cast<uint128_t>(hi) << 64) | lo;
    };

    for (int i = 0; i < 1000; ++i) {
        int256_t val1(static_cast<int128_t>(rand_u128()), rand_u128());
        int256_t val2(static_cast<int128_t>(rand_u128()), rand_u128());

        std::string buf1;
        std::string buf2;
        key_coder->full_encode_ascending(&val1, &buf1);
        key_coder->full_encode_ascending(&val2, &buf2);
        ASSERT_EQ(32, buf1.size());
        ASSERT_EQ(32, buf2.size());

        if (val1 < val2) {
            ASSERT_LT(memcmp(buf1.data(), buf2.data(), 32), 0);
        } else if (val1 > val2) {
            ASSERT_GT(memcmp(buf1.data(), buf2.data(), 32), 0);
        } else {
            ASSERT_EQ(memcmp(buf1.data(), buf2.data(), 32), 0);
        }

        // round-trip decode.
        Slice slice1(buf1);
        int256_t decoded1;
        auto st = key_coder->decode_ascending(&slice1, 32, (uint8_t*)&decoded1, nullptr);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(val1, decoded1);
    }
}

TEST_F(KeyCoderTest, test_boolean_roundtrip) {
    auto key_coder = get_key_coder(TYPE_BOOLEAN);

    for (bool v : {false, true}) {
        std::string buf;
        key_coder->full_encode_ascending(&v, &buf);
        ASSERT_EQ(1, buf.size());

        Slice slice(buf);
        bool decoded;
        auto st = key_coder->decode_ascending(&slice, sizeof(bool), (uint8_t*)&decoded, nullptr);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(v, decoded);
    }
}

} // namespace starrocks
