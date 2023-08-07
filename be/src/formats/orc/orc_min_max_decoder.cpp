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

#include "formats/orc/orc_min_max_decoder.h"

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exprs/cast_expr.h"
#include "exprs/literal.h"
#include "formats/orc/utils.h"
#include "types/logical_type.h"
#include "util/timezone_utils.h"

namespace starrocks {

#define DOWN_CAST_ASSIGN_MIN_MAX(TYPE)                         \
    do {                                                       \
        ColumnHelper::cast_to_raw<TYPE>(min_col)->append(min); \
        ColumnHelper::cast_to_raw<TYPE>(max_col)->append(max); \
        return Status::OK();                                   \
    } while (0)

static Status decode_int_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats,
                                 const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_intstatistics() && colStats.intstatistics().has_minimum() &&
        colStats.intstatistics().has_maximum()) {
        const auto& stats = colStats.intstatistics();
        int64_t min = stats.minimum();
        int64_t max = stats.maximum();

        switch (ltype) {
        case LogicalType::TYPE_TINYINT:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_TINYINT);
        case LogicalType::TYPE_SMALLINT:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_SMALLINT);
        case LogicalType::TYPE_INT:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_INT);
        case LogicalType::TYPE_BIGINT:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_BIGINT);
        default:
            break;
        }
    }
    return Status::NotFound("int column stats not found");
}

static Status decode_double_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats,
                                    const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_doublestatistics() && colStats.doublestatistics().has_minimum() &&
        colStats.doublestatistics().has_maximum()) {
        const auto& stats = colStats.doublestatistics();
        double min = stats.minimum();
        double max = stats.maximum();
        switch (ltype) {
        case LogicalType::TYPE_FLOAT:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_FLOAT);
        case LogicalType::TYPE_DOUBLE:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_DOUBLE);
        default:
            break;
        }
    }
    return Status::NotFound("double column stats not found");
}
static Status decode_string_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats,
                                    const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_stringstatistics() && colStats.stringstatistics().has_minimum() &&
        colStats.stringstatistics().has_maximum()) {
        const auto& stats = colStats.stringstatistics();
        const std::string& min_value = stats.minimum();
        const std::string& max_value = stats.maximum();
        size_t min_value_size = min_value.size();
        size_t max_value_size = max_value.size();
        if (ltype == TYPE_CHAR) {
            min_value_size = remove_trailing_spaces(min_value.c_str(), min_value_size);
            max_value_size = remove_trailing_spaces(max_value.c_str(), max_value_size);
        }
        const Slice min(min_value.c_str(), min_value_size);
        const Slice max(max_value.c_str(), max_value_size);
        switch (ltype) {
        case LogicalType::TYPE_VARCHAR:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_VARCHAR);
        case LogicalType::TYPE_CHAR:
            DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_CHAR);
        default:
            break;
        }
    }
    return Status::NotFound("string column stats not found");
}

static Status decode_date_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats,
                                  const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_datestatistics() && colStats.datestatistics().has_minimum() &&
        colStats.datestatistics().has_maximum()) {
        const auto& stats = colStats.datestatistics();
        DateValue min, max;
        OrcDateHelper::orc_date_to_native_date(&min, stats.minimum());
        OrcDateHelper::orc_date_to_native_date(&max, stats.maximum());
        DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_DATE);
    }
    return Status::NotFound("date column stats not found");
}

// It's quite odd that, timestamp statistics stores milliseconds since unix epoch time.
// but timestamp column vector batch stores seconds since unix epoch time.
// https://orc.apache.org/specification/ORCv1/
static Status decode_datetime_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats,
                                      int64_t tz_offset_in_seconds, const ColumnPtr& min_col,
                                      const ColumnPtr& max_col) {
    if (colStats.has_timestampstatistics() && colStats.timestampstatistics().has_minimumutc() &&
        colStats.timestampstatistics().has_maximumutc()) {
        const auto& stats = colStats.timestampstatistics();
        TimestampValue min, max;
        const cctz::time_zone utc_tzinfo = cctz::utc_time_zone();
        {
            int64_t ms = stats.minimumutc();
            int64_t ns = 0;
            if (stats.has_minimumnanos()) {
                ns = stats.minimumnanos();
            }
            int64_t secs = ms / 1000;
            ns += (ms - secs * 1000) * 1000000L;
            OrcTimestampHelper::orc_ts_to_native_ts(&min, utc_tzinfo, tz_offset_in_seconds, secs, ns, true);
        }

        {
            int64_t ms = stats.maximumutc();
            int64_t ns = 0;
            if (stats.has_maximumnanos()) {
                ns = stats.maximumnanos();
            }
            int64_t secs = ms / 1000;
            ns += (ms - secs * 1000) * 1000000L;
            OrcTimestampHelper::orc_ts_to_native_ts(&max, utc_tzinfo, tz_offset_in_seconds, secs, ns, true);
        }

        DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_DATETIME);
    }
    return Status::NotFound("date column stats not found");
}

Status OrcMinMaxDecoder::decode(SlotDescriptor* slot, const orc::proto::ColumnStatistics& stats, ColumnPtr min_col,
                                ColumnPtr max_col, int64_t tz_offset_in_seconds) {
    if (slot->is_nullable()) {
        auto* a = ColumnHelper::as_raw_column<NullableColumn>(min_col);
        auto* b = ColumnHelper::as_raw_column<NullableColumn>(max_col);
        a->mutable_null_column()->append(0);
        b->mutable_null_column()->append(0);
        min_col = a->data_column();
        max_col = b->data_column();
    }
    LogicalType ltype = slot->type().type;
    switch (ltype) {
    case LogicalType::TYPE_TINYINT:
    case LogicalType::TYPE_SMALLINT:
    case LogicalType::TYPE_INT:
    case LogicalType::TYPE_BIGINT:
        // case LogicalType::TYPE_LARGEINT:
        return decode_int_min_max(ltype, stats, min_col, max_col);

    case LogicalType::TYPE_FLOAT:
    case LogicalType::TYPE_DOUBLE:
        return decode_double_min_max(ltype, stats, min_col, max_col);

    case LogicalType::TYPE_VARCHAR:
    case LogicalType::TYPE_CHAR:
        return decode_string_min_max(ltype, stats, min_col, max_col);

    case LogicalType::TYPE_DATE:
        return decode_date_min_max(ltype, stats, min_col, max_col);

    case LogicalType::TYPE_DATETIME:
        return decode_datetime_min_max(ltype, stats, tz_offset_in_seconds, min_col, max_col);

    default:
        return Status::NotSupported("Not support to decode min/max from orc column stats. type = " +
                                    std::to_string(ltype));
    }
}

} // namespace starrocks
