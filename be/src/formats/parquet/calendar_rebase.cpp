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

#include "formats/parquet/calendar_rebase.h"

#include <algorithm>
#include <optional>
#include <string>
#include <utility>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "formats/parquet/column_converter.h"
#include "gutil/casts.h"

namespace starrocks::parquet {

namespace {

// 1582-10-15, the first Gregorian day of the hybrid calendar, as days since the epoch.
constexpr int32_t SWITCH_DAY = -141427;
// Julian Day Number of 1970-01-01.
constexpr int64_t UNIX_EPOCH_JDN = 2440588;

// Proleptic Gregorian days since the epoch of a civil date (H. Hinnant's days_from_civil).
int64_t days_from_civil(int64_t y, int64_t m, int64_t d) {
    y -= m <= 2 ? 1 : 0;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

bool is_int32_date(const tparquet::SchemaElement& schema_element) {
    if (schema_element.__isset.logicalType) {
        return schema_element.logicalType.__isset.DATE;
    }
    return schema_element.__isset.converted_type && schema_element.converted_type == tparquet::ConvertedType::DATE;
}

class LegacyDateConverter final : public ColumnConverter {
public:
    explicit LegacyDateConverter(std::unique_ptr<ColumnConverter> inner) : _inner(std::move(inner)) {
        init_info(true, _inner->parquet_type);
    }

    Status convert(const Column* src, Column* dst) override {
        // Most legacy files hold no date before 1582, so the column is only copied when one needs rebasing.
        const auto* nullable = ColumnHelper::as_raw_column<NullableColumn>(src);
        const auto& days = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(nullable->data_column())->get_data();
        if (std::none_of(days.begin(), days.end(), [](int32_t v) { return v < SWITCH_DAY; })) {
            return _inner->convert(src, dst);
        }
        auto rebased = src->clone();
        auto* data = down_cast<NullableColumn*>(rebased.get())->mutable_data_column();
        for (auto& v : down_cast<FixedLengthColumn<int32_t>*>(data)->get_data()) {
            v = rebase_julian_to_gregorian_days(v);
        }
        return _inner->convert(rebased.get(), dst);
    }

private:
    std::unique_ptr<ColumnConverter> _inner;
};

} // namespace

LegacyCalendar LegacyCalendar::from_key_value_metadata(const std::vector<tparquet::KeyValue>& key_value_metadata) {
    std::optional<std::string> version;
    bool legacy_datetime = false;
    for (const auto& kv : key_value_metadata) {
        if (kv.key == SPARK_VERSION_KEY && kv.__isset.value) {
            version = kv.value;
        } else if (kv.key == SPARK_LEGACY_DATETIME_KEY) {
            legacy_datetime = true;
        }
    }
    LegacyCalendar calendar;
    if (version.has_value()) {
        // Spark compares the version strings lexicographically.
        calendar.dates = *version < "3.0.0" || legacy_datetime;
    }
    return calendar;
}

int32_t rebase_julian_to_gregorian_days(int32_t days) {
    // Days before Julian Day 0 (4713 BC), far outside any StarRocks DATE, are left as they are.
    if (days >= SWITCH_DAY || days < -UNIX_EPOCH_JDN) {
        return days;
    }
    // Julian calendar date of the day (E. G. Richards' algorithm), then the same label in the proleptic calendar.
    const int64_t c = days + UNIX_EPOCH_JDN + 32082;
    const int64_t d = (4 * c + 3) / 1461;
    const int64_t e = c - (1461 * d) / 4;
    const int64_t m = (5 * e + 2) / 153;
    const int64_t day = e - (153 * m + 2) / 5 + 1;
    const int64_t month = m + 3 - 12 * (m / 10);
    const int64_t year = d - 4800 + m / 10;
    // Like Spark, a Julian-only leap day (e.g. 1500-02-29) rolls over to March 1st.
    return static_cast<int32_t>(days_from_civil(year, month, 1) + day - 1);
}

std::unique_ptr<ColumnConverter> wrap_legacy_calendar_converter(std::unique_ptr<ColumnConverter> inner,
                                                                const tparquet::SchemaElement& schema_element,
                                                                LogicalType target_type,
                                                                const LegacyCalendar& calendar) {
    // Only DATE values read as dates are rebased; e.g. a DATE column read as INT keeps its raw days.
    if (inner == nullptr || !inner->need_convert || !calendar.dates || inner->parquet_type != tparquet::Type::INT32 ||
        (target_type != TYPE_DATE && target_type != TYPE_DATETIME) || !is_int32_date(schema_element)) {
        return inner;
    }
    return std::make_unique<LegacyDateConverter>(std::move(inner));
}

} // namespace starrocks::parquet
