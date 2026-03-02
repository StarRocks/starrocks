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

#include <arrow/util/endian.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <string_view>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/variant_converter.h"
#include "gutil/casts.h"
#include "types/date_value.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/variant.h"
#include "types/variant_value.h"

namespace starrocks {
namespace {

uint8_t primitive_header(VariantType type) {
    return static_cast<uint8_t>(type) << 2;
}

template <typename T>
std::string encode_primitive(VariantType type, T value) {
    std::string bytes(1 + sizeof(T), '\0');
    bytes[0] = primitive_header(type);
    T little_endian_value = arrow::bit_util::ToLittleEndian(value);
    memcpy(bytes.data() + 1, &little_endian_value, sizeof(T));
    return bytes;
}

std::string encode_string(std::string_view value) {
    std::string bytes(1 + sizeof(uint32_t) + value.size(), '\0');
    bytes[0] = primitive_header(VariantType::STRING);

    uint32_t length = arrow::bit_util::ToLittleEndian(static_cast<uint32_t>(value.size()));
    memcpy(bytes.data() + 1, &length, sizeof(uint32_t));
    memcpy(bytes.data() + 1 + sizeof(uint32_t), value.data(), value.size());
    return bytes;
}

VariantRowValue make_variant_row(std::string value_bytes) {
    return VariantRowValue(VariantMetadata::kEmptyMetadata, std::string_view(value_bytes));
}

} // namespace

TEST(VariantConverterCoreTest, CastArithmeticAndTemporalValues) {
    VariantRowValue int_row = make_variant_row(encode_primitive<int32_t>(VariantType::INT32, 42));
    ColumnBuilder<TYPE_BIGINT> int_builder(1);
    Status int_status = cast_variant_value_to<TYPE_BIGINT, true>(int_row, cctz::utc_time_zone(), int_builder);
    ASSERT_TRUE(int_status.ok()) << int_status;
    auto int_column = int_builder.build(false);
    auto* int_data = ColumnHelper::cast_to_raw<TYPE_BIGINT>(int_column.get());
    ASSERT_EQ(1, int_data->size());
    EXPECT_EQ(42, int_data->get_data()[0]);

    int32_t days = DateValue::create(2025, 2, 2).to_days_since_unix_epoch();
    VariantRowValue date_row = make_variant_row(encode_primitive<int32_t>(VariantType::DATE, days));
    ColumnBuilder<TYPE_DATE> date_builder(1);
    Status date_status = cast_variant_value_to<TYPE_DATE, true>(date_row, cctz::utc_time_zone(), date_builder);
    ASSERT_TRUE(date_status.ok()) << date_status;
    auto date_column = date_builder.build(false);
    auto* date_data = ColumnHelper::cast_to_raw<TYPE_DATE>(date_column.get());
    ASSERT_EQ(1, date_data->size());
    EXPECT_EQ(DateValue::create(2025, 2, 2), date_data->get_data()[0]);

    int64_t time_micros = (1 * 3600 + 2 * 60 + 3) * USECS_PER_SEC + 456;
    VariantRowValue time_row = make_variant_row(encode_primitive<int64_t>(VariantType::TIME_NTZ, time_micros));
    ColumnBuilder<TYPE_TIME> time_builder(1);
    Status time_status = cast_variant_value_to<TYPE_TIME, true>(time_row, cctz::utc_time_zone(), time_builder);
    ASSERT_TRUE(time_status.ok()) << time_status;
    auto time_column = time_builder.build(false);
    auto* time_data = ColumnHelper::cast_to_raw<TYPE_TIME>(time_column.get());
    ASSERT_EQ(1, time_data->size());
    EXPECT_DOUBLE_EQ(static_cast<double>(time_micros) / USECS_PER_SEC, time_data->get_data()[0]);

    constexpr int64_t kSeconds = 1700000000;
    constexpr int64_t kSubMicros = 123456;
    VariantRowValue datetime_row = make_variant_row(
            encode_primitive<int64_t>(VariantType::TIMESTAMP_NTZ, kSeconds * USECS_PER_SEC + kSubMicros));
    ColumnBuilder<TYPE_DATETIME> datetime_builder(1);
    Status datetime_status =
            cast_variant_value_to<TYPE_DATETIME, true>(datetime_row, cctz::utc_time_zone(), datetime_builder);
    ASSERT_TRUE(datetime_status.ok()) << datetime_status;
    auto datetime_column = datetime_builder.build(false);
    auto* datetime_data = ColumnHelper::cast_to_raw<TYPE_DATETIME>(datetime_column.get());
    ASSERT_EQ(1, datetime_data->size());

    TimestampValue expected;
    expected.from_unix_second(kSeconds, kSubMicros);
    EXPECT_EQ(expected, datetime_data->get_data()[0]);
}

TEST(VariantConverterCoreTest, CastBooleanAndString) {
    VariantRowValue bool_row = make_variant_row(encode_string("TrUe"));
    ColumnBuilder<TYPE_BOOLEAN> bool_builder(1);
    Status bool_status = cast_variant_value_to<TYPE_BOOLEAN, true>(bool_row, cctz::utc_time_zone(), bool_builder);
    ASSERT_TRUE(bool_status.ok()) << bool_status;
    auto bool_column = bool_builder.build(false);
    auto* bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(bool_column.get());
    ASSERT_EQ(1, bool_data->size());
    EXPECT_EQ(1, bool_data->get_data()[0]);

    VariantRowValue int_row = make_variant_row(encode_primitive<int32_t>(VariantType::INT32, 42));
    ColumnBuilder<TYPE_VARCHAR> string_builder(1);
    Status string_status = cast_variant_value_to<TYPE_VARCHAR, true>(int_row, cctz::utc_time_zone(), string_builder);
    ASSERT_TRUE(string_status.ok()) << string_status;
    auto string_column = string_builder.build(false);
    auto* string_data = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(string_column.get());
    ASSERT_EQ(1, string_data->size());
    EXPECT_EQ("42", string_data->get_slice(0).to_string());
}

TEST(VariantConverterCoreTest, ThrowAndNullOnInvalidBooleanString) {
    VariantRowValue invalid_bool_row = make_variant_row(encode_string("not_bool"));

    ColumnBuilder<TYPE_BOOLEAN> throw_builder(1);
    auto throw_status =
            cast_variant_value_to<TYPE_BOOLEAN, true>(invalid_bool_row, cctz::utc_time_zone(), throw_builder);
    EXPECT_FALSE(throw_status.ok());

    ColumnBuilder<TYPE_BOOLEAN> null_builder(1);
    Status null_status =
            cast_variant_value_to<TYPE_BOOLEAN, false>(invalid_bool_row, cctz::utc_time_zone(), null_builder);
    ASSERT_TRUE(null_status.ok()) << null_status;
    auto null_column = null_builder.build(false);
    ASSERT_TRUE(null_column->is_nullable());
    auto* nullable = down_cast<NullableColumn*>(null_column.get());
    EXPECT_TRUE(nullable->is_null(0));
}

} // namespace starrocks
