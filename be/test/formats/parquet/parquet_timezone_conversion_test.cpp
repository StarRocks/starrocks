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

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/types/int96.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_scan_io_fwd.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/schema.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks::parquet {

// parquet_fast_timezone_conversion is deprecated and ignored: a zone-adjusted parquet timestamp always gets the offset
// in force at its own instant, so a batch with values on both sides of a DST change converts the same with it on.
class ParquetTimezoneConversionTest : public testing::Test {
protected:
    void TearDown() override { config::parquet_fast_timezone_conversion = false; }

    template <typename T>
    static ColumnPtr make_source(const std::vector<T>& values) {
        auto data = FixedLengthColumn<T>::create();
        auto null_column = NullColumn::create();
        for (const T& v : values) {
            data->append(v);
            null_column->append(0);
        }
        return NullableColumn::create(std::move(data), std::move(null_column));
    }

    static std::vector<std::string> convert(ColumnConverter* converter, const ColumnPtr& src, bool fast) {
        config::parquet_fast_timezone_conversion = fast;
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        auto dst = ColumnHelper::create_column(col_type, true);
        EXPECT_OK(converter->convert(src.get(), dst.get()));
        std::vector<std::string> result;
        for (size_t i = 0; i < dst->size(); i++) {
            result.push_back(dst->debug_item(i));
        }
        return result;
    }

    static constexpr int64_t kSummer = 1719835200; // 2024-07-01 12:00:00 UTC, 13:00 BST
    static constexpr int64_t kWinter = 1704110400; // 2024-01-01 12:00:00 UTC, 12:00 GMT
    static constexpr int64_t kBstEnd = 1729990800; // 2024-10-27 01:00:00 UTC, BST ends
};

TEST_F(ParquetTimezoneConversionTest, Int64IgnoresFastConversion) {
    ParquetField field;
    field.physical_type = tparquet::Type::INT64;
    field.schema_element.type = tparquet::Type::INT64;
    tparquet::TimestampType ts;
    ts.isAdjustedToUTC = true;
    ts.unit.__set_MICROS(tparquet::MicroSeconds());
    tparquet::LogicalType logical_type;
    logical_type.__set_TIMESTAMP(ts);
    field.schema_element.__set_logicalType(logical_type);
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, "Europe/London", &converter));

    std::vector<int64_t> micros;
    for (int64_t s : {kSummer, kWinter, kBstEnd - 1800, kBstEnd + 600}) {
        micros.push_back(s * 1000000);
    }
    auto src = make_source(micros);
    const std::vector<std::string> want = {"2024-07-01 13:00:00", "2024-01-01 12:00:00", "2024-10-27 01:30:00",
                                           "2024-10-27 01:10:00"};
    EXPECT_EQ(want, convert(converter.get(), src, false));
    EXPECT_EQ(want, convert(converter.get(), src, true));

    // min/max statistics keep bounds that hold every value across the backward shift
    auto min_src = make_source(std::vector<int64_t>{(kBstEnd - 1800) * 1000000});
    auto max_src = make_source(std::vector<int64_t>{(kBstEnd + 600) * 1000000});
    for (bool fast : {false, true}) {
        config::parquet_fast_timezone_conversion = fast;
        auto min_dst = ColumnHelper::create_column(col_type, true);
        auto max_dst = ColumnHelper::create_column(col_type, true);
        ASSERT_OK(converter->convert_min_max(min_src.get(), max_src.get(), min_dst.get(), max_dst.get()));
        EXPECT_EQ("2024-10-27 00:30:00", min_dst->debug_item(0)) << fast;
        EXPECT_EQ("2024-10-27 02:10:00", max_dst->debug_item(0)) << fast;
    }
}

TEST_F(ParquetTimezoneConversionTest, Int96IgnoresFastConversion) {
    ParquetField field;
    field.physical_type = tparquet::Type::INT96;
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, "Europe/London", &converter));

    std::vector<int96_t> values;
    for (const Timestamp t :
         {timestamp::from_datetime(2024, 7, 1, 12, 0, 0, 0), timestamp::from_datetime(2024, 1, 1, 12, 0, 0, 0)}) {
        int96_t raw;
        raw.hi = static_cast<uint32_t>(t >> TIMESTAMP_BITS);
        raw.lo = (t & ((static_cast<uint64_t>(1) << TIMESTAMP_BITS) - 1)) * 1000;
        values.push_back(raw);
    }
    auto src = make_source(values);
    const std::vector<std::string> want = {"2024-07-01 13:00:00", "2024-01-01 12:00:00"};
    EXPECT_EQ(want, convert(converter.get(), src, false));
    EXPECT_EQ(want, convert(converter.get(), src, true));
}

} // namespace starrocks::parquet
