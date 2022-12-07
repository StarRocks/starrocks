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

#include "util/mysql_row_buffer.h"

#include <optional>

#include "gtest/gtest.h"
#include "gutil/port.h"
#include "runtime/large_int_value.h"
#include "types/constexpr.h"

namespace starrocks {

class MysqlRowBufferTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// the first byte:
// <= 250: length
// = 251: NULL
// = 252: the next two byte is length
// = 253: the next three byte is length
// = 254: the next eighth byte is length
std::optional<Slice> decode_mysql_row(Slice* s) {
    assert(s->size > 0);
    uint8_t x = (*s)[0];
    s->remove_prefix(1);
    if (x < 251) {
        assert(s->size >= x);
        Slice ret(s->data, x);
        s->remove_prefix(x);
        return ret;
    }
    if (x == 251) {
        return {};
    }
    if (x == 252) {
        uint16_t l = UNALIGNED_LOAD16(s->data);
        s->remove_prefix(2);
        assert(s->size >= l);
        Slice ret(s->data, l);
        s->remove_prefix(l);
        return ret;
    }
    if (x == 253) {
        uint32_t l = *reinterpret_cast<const uint24_t*>(s->data);
        s->remove_prefix(3);
        assert(s->size >= l);
        Slice ret(s->data, l);
        s->remove_prefix(l);
        return ret;
    }
    if (x == 254) {
        uint64_t l = UNALIGNED_LOAD64(s->data);
        s->remove_prefix(8);
        assert(s->size >= l);
        Slice ret(s->data, l);
        s->remove_prefix(l);
        return ret;
    }
    assert(false);
    return {};
}

// NOLINTNEXTLINE
TEST_F(MysqlRowBufferTest, test_basic) {
    MysqlRowBuffer row_buffer;

    row_buffer.push_null();
    row_buffer.push_tinyint(-128);
    row_buffer.push_tinyint(+127);
    row_buffer.push_smallint(-32768);
    row_buffer.push_smallint(+32767);
    row_buffer.push_int(-2147483648);
    row_buffer.push_int(+2147483647);
    row_buffer.push_bigint(-9223372036854775808ULL);
    row_buffer.push_bigint(+9223372036854775807ULL);
    row_buffer.push_string("string value");
    row_buffer.push_number((uint8_t)255);
    row_buffer.push_number((uint16_t)65535);
    row_buffer.push_number((uint32_t)4294967295ULL);
    row_buffer.push_number((uint64_t)18446744073709551615ULL);
    row_buffer.push_number(MIN_INT128);
    row_buffer.push_number(MAX_INT128);

    Slice s(row_buffer.data());

    auto data = decode_mysql_row(&s);
    ASSERT_FALSE(data.has_value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("-128", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("127", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("-32768", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("32767", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("-2147483648", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("2147483647", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("-9223372036854775808", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("9223372036854775807", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("string value", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("255", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("65535", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("4294967295", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ("18446744073709551615", data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ(LargeIntValue::to_string(starrocks::MIN_INT128), data.value());

    data = decode_mysql_row(&s);
    ASSERT_EQ(LargeIntValue::to_string(starrocks::MAX_INT128), data.value());

    ASSERT_EQ("", s);
}

// NOLINTNEXTLINE
TEST_F(MysqlRowBufferTest, test_push_string) {
    // strlen < 251
    {
        std::string s(250, 'x');
        MysqlRowBuffer row_buffer;
        row_buffer.push_string(s);

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ(s, data.value());
        ASSERT_EQ("", slice);
    }
    // strlen < 65536
    {
        std::string s(65535, 'x');
        MysqlRowBuffer row_buffer;
        row_buffer.push_string(s);

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ(s, data.value());
        ASSERT_EQ("", slice);
    }
    // strlen < 16777216
    {
        std::string s(16777215, 'x');
        MysqlRowBuffer row_buffer;
        row_buffer.push_string(s);

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ(s, data.value());
        ASSERT_EQ("", slice);
    }
    // strlen >= 16777216
    {
        std::string s(16777217, 'x');
        MysqlRowBuffer row_buffer;
        row_buffer.push_string(s);

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ(s, data.value());
    }
}

// NOLINTNEXTLINE
TEST_F(MysqlRowBufferTest, test_array) {
    // empty array
    {
        MysqlRowBuffer row_buffer;
        row_buffer.begin_push_array();
        row_buffer.finish_push_array();
        ASSERT_EQ(2, row_buffer.data()[0]);
        ASSERT_EQ("[]", Slice(row_buffer.data().data() + 1, 2));
    }
    // 10 elements
    {
        MysqlRowBuffer row_buffer;
        row_buffer.begin_push_array();
        row_buffer.push_int(0);
        for (int i = 1; i < 10; i++) {
            row_buffer.separator(',');
            row_buffer.push_int(i);
        }
        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ("[0,1,2,3,4,5,6,7,8,9]", data.value());
        ASSERT_EQ("", slice);
    }
    // 200 elements
    {
        MysqlRowBuffer row_buffer;

        row_buffer.begin_push_array();
        row_buffer.push_int(1);
        for (int i = 1; i < 200; i++) {
            row_buffer.separator(',');
            row_buffer.push_int(1);
        }
        row_buffer.finish_push_array();

        std::string expect;
        expect.reserve(500);
        expect.push_back('[');
        expect.push_back('1');
        for (int i = 1; i < 200; i++) {
            expect.push_back(',');
            expect.push_back('1');
        }
        expect.push_back(']');

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        ASSERT_EQ(expect, data.value());
        ASSERT_EQ("", slice);
    }
    // 60000 elements
    {
        MysqlRowBuffer row_buffer;

        row_buffer.begin_push_array();
        row_buffer.push_int(1);
        for (int i = 1; i < 60000; i++) {
            row_buffer.separator(',');
            row_buffer.push_int(1);
        }
        row_buffer.finish_push_array();

        std::string expect;
        expect.reserve(60000 * 2 + 3);
        expect.push_back('[');
        expect.push_back('1');
        for (int i = 1; i < 60000; i++) {
            expect.push_back(',');
            expect.push_back('1');
        }
        expect.push_back(']');

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);
        ASSERT_EQ(expect, data.value());
        ASSERT_EQ("", slice);
    }
    {
        MysqlRowBuffer row_buffer;

        row_buffer.begin_push_array();
        row_buffer.push_string("abc");
        row_buffer.separator(',');
        row_buffer.push_string("def");
        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        // ["abc","def"]
        ASSERT_EQ("[\"abc\",\"def\"]", data.value());
        ASSERT_EQ("", slice);
    }
    {
        MysqlRowBuffer row_buffer;

        row_buffer.begin_push_array();
        row_buffer.push_string("I\"m a");
        row_buffer.separator(',');
        row_buffer.push_string("programmer");
        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        // ["I\"m a","programmer"]
        ASSERT_EQ("[\"I\\\"m a\",\"programmer\"]", data.value());
        ASSERT_EQ("", slice);
    }
    {
        MysqlRowBuffer row_buffer;

        row_buffer.begin_push_array();
        row_buffer.push_string("I\\\"m a"); // I\"m a
        row_buffer.separator(',');
        row_buffer.push_string("programmer");
        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        // ["I\\\"m a","programmer"]
        ASSERT_EQ("[\"I\\\\\\\"m a\",\"programmer\"]", data.value());
        ASSERT_EQ("", slice);
    }
    // original string length is 250, but escaped string length is 252 (>251).
    {
        MysqlRowBuffer row_buffer;

        std::string s(248, 'x');

        row_buffer.begin_push_array();
        row_buffer.push_string(s);
        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        s.insert(0, "[\"");
        s.append("\"]");

        ASSERT_EQ(s, data.value());
        ASSERT_EQ("", slice);
    }
    // original string length is 250, but escaped string length is 253 (>251).
    {
        MysqlRowBuffer row_buffer;

        std::string s(248, 'x');
        s[10] = '"';

        row_buffer.begin_push_array(); // '['
        row_buffer.push_string(s);
        row_buffer.finish_push_array(); // ']'

        s.insert(s.begin() + 10, '\\');
        s.insert(0, "[\"");
        s.append("\"]");

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        ASSERT_EQ(s, data.value());
        ASSERT_EQ("", slice);
    }
    // nested array
    {
        MysqlRowBuffer row_buffer;
        row_buffer.begin_push_array();

        row_buffer.begin_push_array();
        row_buffer.push_int(1);
        row_buffer.separator(',');
        row_buffer.push_int(2);

        row_buffer.finish_push_array();
        row_buffer.separator(',');

        row_buffer.begin_push_array();
        row_buffer.finish_push_array();
        row_buffer.separator(',');

        row_buffer.push_null();

        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        ASSERT_EQ("[[1,2],[],null]", data.value());
        ASSERT_EQ("", slice);
    }
    // json array
    {
        MysqlRowBuffer row_buffer;
        row_buffer.begin_push_array();

        Slice json = R"({"k1": "v1"})";
        row_buffer.push_string(json.data, json.size, '\'');

        row_buffer.finish_push_array();

        Slice slice(row_buffer.data());
        auto data = decode_mysql_row(&slice);

        ASSERT_EQ(R"(['{"k1": "v1"}'])", data.value());
        ASSERT_EQ("", slice);
    }
}

} // namespace starrocks
