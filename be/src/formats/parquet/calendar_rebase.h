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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "gen_cpp/parquet_types.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

class ColumnConverter;

// Spark 2.x, and Spark 3+ in LEGACY rebase mode (Databricks SQL warehouses, for files holding such values), write
// DATE values before 1582-10-15 in the hybrid Julian/Gregorian calendar and mark the file in its footer. StarRocks,
// like the Delta and Parquet specs, uses the proleptic Gregorian calendar, so such dates read shifted, e.g.
// 0001-01-01 as 0000-12-30. These helpers rebase them on read as Spark does (RebaseDateTime / DataSourceUtils).
//
// Only DATE is handled. Databricks writes TIMESTAMP as INT96 without rebasing it and never rebases TIMESTAMP_NTZ;
// INT64 timestamps of other Spark writers in LEGACY mode are read as before.
struct LegacyCalendar {
    static constexpr const char* SPARK_VERSION_KEY = "org.apache.spark.version";
    static constexpr const char* SPARK_LEGACY_DATETIME_KEY = "org.apache.spark.legacyDateTime";

    // DATE values are in the hybrid calendar.
    bool dates = false;

    bool any() const { return dates; }

    // Spark's rule: no writer version means proleptic; Spark < 3.0.0 or the legacyDateTime marker means hybrid.
    static LegacyCalendar from_key_value_metadata(const std::vector<tparquet::KeyValue>& key_value_metadata);
};

// Days since 1970-01-01 in the hybrid calendar -> the same year/month/day in the proleptic Gregorian calendar.
// Non-decreasing, so row-group and page-index min/max can be rebased like the values.
int32_t rebase_julian_to_gregorian_days(int32_t days);

// Wraps a converter that reads a Parquet DATE into a StarRocks DATE / DATETIME so the raw days are rebased before
// the inner conversion. Returns `inner` unchanged when there is nothing to rebase.
std::unique_ptr<ColumnConverter> wrap_legacy_calendar_converter(std::unique_ptr<ColumnConverter> inner,
                                                                const tparquet::SchemaElement& schema_element,
                                                                LogicalType target_type,
                                                                const LegacyCalendar& calendar);

} // namespace starrocks::parquet
