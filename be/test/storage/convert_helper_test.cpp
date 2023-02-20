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

#include "storage/convert_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "runtime/decimalv3.h"
#include "storage/chunk_helper.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "testutil/parallel_test.h"

namespace starrocks {

static std::string trim_trailing_zeros(const std::string& s) {
    if (s.find('.') == std::string::npos) return s;
    auto l = s.size();
    while (l > 0 && s[l - 1] == '0') l--;
    l = (l > 0 && s[l - 1] == '.') ? l - 1 : l;
    return s.substr(0, l);
}

PARALLEL_TEST(ConvertHelperTest, testVoidPtr) {
    std::vector<std::string> test_cases = {
            "0.000000000", "999999999999999999.999999999", "-999999999999999999.999999999",
            "1.001000000", "123456789.987654321",
    };
    for (auto& tc : test_cases) {
        int128_t decimalv3_value;
        auto fail = DecimalV3Cast::from_string<int128_t>(&decimalv3_value, 27, 9, tc.data(), tc.size());
        ASSERT_FALSE(fail);
        Status status;
        {
            auto conv = get_field_converter(TYPE_DECIMAL128, TYPE_DECIMALV2);
            ASSERT_TRUE(status.ok());
            DecimalV2Value decimalv2_value;
            conv->convert(&decimalv2_value, &decimalv3_value);

            conv = get_field_converter(TYPE_DECIMALV2, TYPE_DECIMAL128);
            ASSERT_TRUE(status.ok());
            int128_t ya_decimalv3_value;
            conv->convert(&ya_decimalv3_value, &decimalv2_value);

            auto actual = DecimalV3Cast::to_string<int128_t>(ya_decimalv3_value, 27, 9);
            ASSERT_EQ(tc, actual);
        }

        {
            auto conv = get_field_converter(TYPE_DECIMAL128, TYPE_DECIMAL);
            ASSERT_TRUE(status.ok());
            decimal12_t decimalv1_value;
            conv->convert(&decimalv1_value, &decimalv3_value);

            conv = get_field_converter(TYPE_DECIMAL, TYPE_DECIMAL128);
            ASSERT_TRUE(status.ok());
            int128_t ya_decimalv3_value;
            conv->convert(&ya_decimalv3_value, &decimalv1_value);

            auto actual = DecimalV3Cast::to_string<int128_t>(ya_decimalv3_value, 27, 9);
            ASSERT_EQ(tc, actual);
        }
    }
}

PARALLEL_TEST(ConvertHelperTest, testDatum) {
    std::vector<std::string> test_cases = {
            "0.000000000", "999999999999999999.999999999", "-999999999999999999.999999999",
            "1.001000000", "123456789.987654321",
    };
    for (auto& tc : test_cases) {
        int128_t decimalv3_value;
        auto fail = DecimalV3Cast::from_string<int128_t>(&decimalv3_value, 27, 9, tc.data(), tc.size());
        ASSERT_FALSE(fail);
        Datum decimalv3_datum(decimalv3_value);
        Status status;
        {
            auto conv = get_field_converter(TYPE_DECIMAL128, TYPE_DECIMALV2);
            ASSERT_TRUE(status.ok());
            Datum decimalv2_datum;
            conv->convert(&decimalv2_datum, decimalv3_datum);

            conv = get_field_converter(TYPE_DECIMALV2, TYPE_DECIMAL128);
            ASSERT_TRUE(status.ok());
            Datum ya_decimalv3_datum;
            conv->convert(&ya_decimalv3_datum, decimalv2_datum);

            auto actual = DecimalV3Cast::to_string<int128_t>(ya_decimalv3_datum.get_int128(), 27, 9);
            ASSERT_EQ(tc, actual);
        }

        {
            auto conv = get_field_converter(TYPE_DECIMAL128, TYPE_DECIMAL);
            ASSERT_TRUE(status.ok());
            Datum decimalv1_datum;
            conv->convert(&decimalv1_datum, decimalv3_datum);

            conv = get_field_converter(TYPE_DECIMAL, TYPE_DECIMAL128);
            ASSERT_TRUE(status.ok());
            Datum ya_decimalv3_datum;
            conv->convert(&ya_decimalv3_datum, decimalv1_datum);

            auto actual = DecimalV3Cast::to_string<int128_t>(ya_decimalv3_datum.get_int128(), 27, 9);
            ASSERT_EQ(tc, actual);
        }
    }
}

PARALLEL_TEST(ConvertHelperTest, testDecimalToDecimalV2Column) {
    auto type_info = get_scalar_type_info(TYPE_DECIMAL);
    auto conv = get_field_converter(TYPE_DECIMAL, TYPE_DECIMALV2);
    decimal12_t values[5];
    ASSERT_TRUE(type_info->from_string(&values[0], "-9999999.999999").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "-0.000001").ok());
    ASSERT_TRUE(type_info->from_string(&values[2], "0").ok());
    ASSERT_TRUE(type_info->from_string(&values[3], "0.0000001").ok());
    ASSERT_TRUE(type_info->from_string(&values[4], "9999999.999999").ok());

    std::string values_string[5] = {
            trim_trailing_zeros(values[0].to_string()), trim_trailing_zeros(values[1].to_string()),
            trim_trailing_zeros(values[2].to_string()), trim_trailing_zeros(values[3].to_string()),
            trim_trailing_zeros(values[4].to_string()),
    };
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DECIMAL, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});
        c0->append_datum({values[2]});
        c0->append_datum({values[3]});
        c0->append_datum({values[4]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values_string[0], c1->get(0).get_decimal().to_string());
        EXPECT_EQ(values_string[1], c1->get(1).get_decimal().to_string());
        EXPECT_EQ(values_string[2], c1->get(2).get_decimal().to_string());
        EXPECT_EQ(values_string[3], c1->get(3).get_decimal().to_string());
        EXPECT_EQ(values_string[4], c1->get(4).get_decimal().to_string());
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DECIMAL, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});
        c0->append_datum({values[2]});
        c0->append_datum({values[3]});
        c0->append_datum({values[4]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values_string[0], c1->get(0).get_decimal().to_string());
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values_string[1], c1->get(2).get_decimal().to_string());
        EXPECT_TRUE(c1->get(3).is_null());
        EXPECT_EQ(values_string[2], c1->get(4).get_decimal().to_string());
        EXPECT_EQ(values_string[3], c1->get(5).get_decimal().to_string());
        EXPECT_EQ(values_string[4], c1->get(6).get_decimal().to_string());
    }
}

PARALLEL_TEST(ConvertHelperTest, testDecimalV2ToDecimalColumn) {
    auto type_info = get_scalar_type_info(TYPE_DECIMALV2);
    auto conv = get_field_converter(TYPE_DECIMALV2, TYPE_DECIMAL);
    DecimalV2Value values[5];
    ASSERT_TRUE(type_info->from_string(&values[0], "-9999999.999999").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "-0.000001").ok());
    ASSERT_TRUE(type_info->from_string(&values[2], "0").ok());
    ASSERT_TRUE(type_info->from_string(&values[3], "0.0000001").ok());
    ASSERT_TRUE(type_info->from_string(&values[4], "9999999.999999").ok());

    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DECIMALV2, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});
        c0->append_datum({values[2]});
        c0->append_datum({values[3]});
        c0->append_datum({values[4]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_string(), trim_trailing_zeros(c1->get(0).get_decimal12().to_string()));
        EXPECT_EQ(values[1].to_string(), trim_trailing_zeros(c1->get(1).get_decimal12().to_string()));
        EXPECT_EQ(values[2].to_string(), trim_trailing_zeros(c1->get(2).get_decimal12().to_string()));
        EXPECT_EQ(values[3].to_string(), trim_trailing_zeros(c1->get(3).get_decimal12().to_string()));
        EXPECT_EQ(values[4].to_string(), trim_trailing_zeros(c1->get(4).get_decimal12().to_string()));
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DECIMALV2, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});
        c0->append_datum({values[2]});
        c0->append_datum({values[3]});
        c0->append_datum({values[4]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_string(), trim_trailing_zeros(c1->get(0).get_decimal12().to_string()));
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values[1].to_string(), trim_trailing_zeros(c1->get(2).get_decimal12().to_string()));
        EXPECT_TRUE(c1->get(3).is_null());
        EXPECT_EQ(values[2].to_string(), trim_trailing_zeros(c1->get(4).get_decimal12().to_string()));
        EXPECT_EQ(values[3].to_string(), trim_trailing_zeros(c1->get(5).get_decimal12().to_string()));
        EXPECT_EQ(values[4].to_string(), trim_trailing_zeros(c1->get(6).get_decimal12().to_string()));
    }
}

PARALLEL_TEST(ConvertHelperTest, testDateToDateV2Column) {
    auto type_info = get_scalar_type_info(TYPE_DATE_V1);
    auto conv = get_field_converter(TYPE_DATE_V1, TYPE_DATE);
    uint24_t values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31").ok());

    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATE_V1, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0], c1->get(0).get_date().to_mysql_date());
        EXPECT_EQ(values[1], c1->get(1).get_date().to_mysql_date());
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATE_V1, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0], c1->get(0).get_date().to_mysql_date());
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values[1], c1->get(2).get_date().to_mysql_date());
        EXPECT_TRUE(c1->get(3).is_null());
    }
}

PARALLEL_TEST(ConvertHelperTest, testDateV2ToDateColumn) {
    auto type_info = get_scalar_type_info(TYPE_DATE);
    auto conv = get_field_converter(TYPE_DATE, TYPE_DATE_V1);
    DateValue values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31").ok());

    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATE, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_mysql_date(), c1->get(0).get_uint24());
        EXPECT_EQ(values[1].to_mysql_date(), c1->get(1).get_uint24());
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATE, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_mysql_date(), c1->get(0).get_uint24());
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values[1].to_mysql_date(), c1->get(2).get_uint24());
        EXPECT_TRUE(c1->get(3).is_null());
    }
}

PARALLEL_TEST(ConvertHelperTest, testDatetimeToTimestampColumn) {
    auto type_info = get_scalar_type_info(TYPE_DATETIME_V1);
    auto conv = get_field_converter(TYPE_DATETIME_V1, TYPE_DATETIME);
    int64_t values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01 05:06:07").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31 08:09:10").ok());

    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATETIME_V1, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0], c1->get(0).get_timestamp().to_timestamp_literal());
        EXPECT_EQ(values[1], c1->get(1).get_timestamp().to_timestamp_literal());
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATETIME_V1, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0], c1->get(0).get_timestamp().to_timestamp_literal());
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values[1], c1->get(2).get_timestamp().to_timestamp_literal());
        EXPECT_TRUE(c1->get(3).is_null());
    }
}

PARALLEL_TEST(ConvertHelperTest, testTimestampToDatetimeColumn) {
    auto type_info = get_scalar_type_info(TYPE_DATETIME);
    auto conv = get_field_converter(TYPE_DATETIME, TYPE_DATETIME_V1);
    TimestampValue values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01 04:05:06").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31 05:06:07").ok());

    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATETIME, false);
        c0->append_datum({values[0]});
        c0->append_datum({values[1]});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_timestamp_literal(), c1->get(0).get_int64());
        EXPECT_EQ(values[1].to_timestamp_literal(), c1->get(1).get_int64());
    }
    {
        auto c0 = ChunkHelper::column_from_field_type(TYPE_DATETIME, true);
        c0->append_datum({values[0]});
        c0->append_datum({});
        c0->append_datum({values[1]});
        c0->append_datum({});

        auto c1 = conv->copy_convert(*c0);
        EXPECT_EQ(values[0].to_timestamp_literal(), c1->get(0).get_int64());
        EXPECT_TRUE(c1->get(1).is_null());
        EXPECT_EQ(values[1].to_timestamp_literal(), c1->get(2).get_int64());
        EXPECT_TRUE(c1->get(3).is_null());
    }
}

template <LogicalType field_type>
static void test_convert_same_numeric_types() {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    auto conv = get_field_converter(field_type, field_type);
    CppType values[5] = {std::numeric_limits<CppType>::lowest(), -123, 0, 123, std::numeric_limits<CppType>::max()};

    auto c0 = ChunkHelper::column_from_field_type(field_type, false);
    c0->append_datum({values[0]});
    c0->append_datum({values[1]});
    c0->append_datum({values[2]});
    c0->append_datum({values[3]});
    c0->append_datum({values[4]});

    auto c1 = conv->copy_convert(*c0);
    EXPECT_EQ(values[0], c1->get(0).get<CppType>());
    EXPECT_EQ(values[1], c1->get(1).get<CppType>());
    EXPECT_EQ(values[2], c1->get(2).get<CppType>());
    EXPECT_EQ(values[3], c1->get(3).get<CppType>());
    EXPECT_EQ(values[4], c1->get(4).get<CppType>());

    c0->reset_column();
    EXPECT_EQ(values[0], c1->get(0).get<CppType>());
    EXPECT_EQ(values[1], c1->get(1).get<CppType>());
    EXPECT_EQ(values[2], c1->get(2).get<CppType>());
    EXPECT_EQ(values[3], c1->get(3).get<CppType>());
    EXPECT_EQ(values[4], c1->get(4).get<CppType>());
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_TINYINT) {
    test_convert_same_numeric_types<TYPE_TINYINT>();
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_SMALLINT) {
    test_convert_same_numeric_types<TYPE_SMALLINT>();
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_INT) {
    test_convert_same_numeric_types<TYPE_INT>();
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_BIGINT) {
    test_convert_same_numeric_types<TYPE_BIGINT>();
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_LARGEINT) {
    test_convert_same_numeric_types<TYPE_LARGEINT>();
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_VARCHAR) {
    auto conv = get_field_converter(TYPE_VARCHAR, TYPE_VARCHAR);
    const Slice values[5] = {"", "xxxx", "yyyyyyy", "aaaaaa", "b"};

    auto c0 = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    c0->append_datum({values[0]});
    c0->append_datum({values[1]});
    c0->append_datum({values[2]});
    c0->append_datum({values[3]});
    c0->append_datum({values[4]});

    auto c1 = conv->copy_convert(*c0);

    auto datum = c1->get(0);
    EXPECT_EQ(values[0], datum.get_slice());

    datum = c1->get(1);
    EXPECT_EQ(values[1], datum.get_slice());

    datum = c1->get(2);
    EXPECT_EQ(values[2], datum.get_slice());

    datum = c1->get(3);
    EXPECT_EQ(values[3], datum.get_slice());

    datum = c1->get(4);
    EXPECT_EQ(values[4], datum.get_slice());
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_DOUBLE) {
    auto conv = get_field_converter(TYPE_DOUBLE, TYPE_DOUBLE);
    double values[4] = {INFINITY, -12345.11, 0.11, 12345.111};

    auto c0 = ChunkHelper::column_from_field_type(TYPE_DOUBLE, false);
    c0->append_datum({values[0]});
    c0->append_datum({values[1]});
    c0->append_datum({values[2]});
    c0->append_datum({values[3]});

    auto c1 = conv->copy_convert(*c0);
    EXPECT_EQ(values[0], c1->get(0).get_double());
    EXPECT_EQ(values[1], c1->get(1).get_double());
    EXPECT_EQ(values[2], c1->get(2).get_double());
    EXPECT_EQ(values[3], c1->get(3).get_double());

    c0->reset_column();
    EXPECT_EQ(values[0], c1->get(0).get_double());
    EXPECT_EQ(values[1], c1->get(1).get_double());
    EXPECT_EQ(values[2], c1->get(2).get_double());
    EXPECT_EQ(values[3], c1->get(3).get_double());
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_DATE_V2) {
    auto type_info = get_scalar_type_info(TYPE_DATE);
    auto conv = get_field_converter(TYPE_DATE, TYPE_DATE);
    DateValue values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31").ok());

    auto c0 = ChunkHelper::column_from_field_type(TYPE_DATE, false);
    c0->append_datum({values[0]});
    c0->append_datum({values[1]});

    auto c1 = conv->copy_convert(*c0);
    EXPECT_EQ(values[0], c1->get(0).get_date());
    EXPECT_EQ(values[1], c1->get(1).get_date());

    c0->reset_column();
    EXPECT_EQ(values[0], c1->get(0).get_date());
    EXPECT_EQ(values[1], c1->get(1).get_date());
}

PARALLEL_TEST(ConvertHelperTest, testSameTypeConvertColumn_TIMESTAMP) {
    auto type_info = get_scalar_type_info(TYPE_DATETIME);
    auto conv = get_field_converter(TYPE_DATETIME, TYPE_DATETIME);
    TimestampValue values[2];
    ASSERT_TRUE(type_info->from_string(&values[0], "1990-01-01 02:03:04").ok());
    ASSERT_TRUE(type_info->from_string(&values[1], "1983-12-31 10:11:12").ok());

    auto c0 = ChunkHelper::column_from_field_type(TYPE_DATETIME, false);
    c0->append_datum({values[0]});
    c0->append_datum({values[1]});

    auto c1 = conv->copy_convert(*c0);
    EXPECT_EQ(values[0], c1->get(0).get_timestamp());
    EXPECT_EQ(values[1], c1->get(1).get_timestamp());

    c0->reset_column();
    EXPECT_EQ(values[0], c1->get(0).get_timestamp());
    EXPECT_EQ(values[1], c1->get(1).get_timestamp());
}

} // namespace starrocks
