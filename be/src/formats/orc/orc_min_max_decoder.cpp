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

static Status decode_int_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats, Column* min_col,
                                 Column* max_col) {
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
    return Status::NotFound("OrcMinMaxFilter: int column stats not found");
}

static Status decode_double_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats, Column* min_col,
                                    Column* max_col) {
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
    return Status::NotFound("OrcMinMaxFilter: double column stats not found");
}
static Status decode_string_min_max(LogicalType ltype, const orc::proto::ColumnStatistics& colStats, Column* min_col,
                                    Column* max_col) {
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
    return Status::NotFound("OrcMinMaxFilter: string column stats not found");
}

static Status decode_date_min_max(const orc::proto::ColumnStatistics& colStats, Column* min_col, Column* max_col) {
    if (colStats.has_datestatistics() && colStats.datestatistics().has_minimum() &&
        colStats.datestatistics().has_maximum()) {
        const auto& stats = colStats.datestatistics();
        DateValue min, max;
        OrcDateHelper::orc_date_to_native_date(&min, stats.minimum());
        OrcDateHelper::orc_date_to_native_date(&max, stats.maximum());
        DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_DATE);
    }
    return Status::NotFound("OrcMinMaxFilter: date column stats not found");
}

// It's quite odd that, timestamp statistics stores milliseconds since unix epoch time.
// but timestamp column vector batch stores seconds since unix epoch time.
// https://orc.apache.org/specification/ORCv1/
//
// ORC serializes a TIMESTAMP min/max as whole milliseconds since the Unix epoch (minimumUtc /
// maximumUtc) plus a sub-millisecond nanos remainder in [0, 999999] (minimumNanos / maximumNanos).
// The nanos field is stored with a +1 offset and is omitted when equal to its default (0 for the
// minimum, 999999 for the maximum). StarRocks timestamps are microsecond precision, so each bound
// is rounded conservatively -- the minimum floored, the maximum ceiled -- to keep [min, max] a
// superset of the true value range. For a TIMESTAMP_INSTANT the reader timezone offset is folded
// into the seconds and the value is decoded as plain UTC, matching the after-epoch instant path.
//
// Note: the folded offset is the scalar reader-minus-writer offset looked up at the epoch. For a
// pre-1970 instant in a named zone whose historical offset differs from its epoch offset this is
// approximate (the row-load path uses a per-instant cctz conversion), so the bound may still
// under-cover -- a pre-existing limitation of the scalar-offset model, not specific to this path.
static void decode_orc_timestamp_bound(int64_t utc_ms, bool has_nanos, int32_t raw_nanos, bool is_max, bool is_instant,
                                       int64_t tz_offset_in_seconds, TimestampValue* out) {
    // The sub-millisecond nanos remainder is in [0, 999999]; a present field is the value + 1 (so
    // in [1, 1000000]). Undo the offset, falling back to the conservative default when the field is
    // absent or -- for untrusted external files -- malformed.
    constexpr int64_t kMaxSubNanos = 999999;
    int64_t sub_nanos = (has_nanos && raw_nanos >= 1 && raw_nanos <= kMaxSubNanos + 1) ? (raw_nanos - 1)
                                                                                       : (is_max ? kMaxSubNanos : 0);
    // Floor the millisecond split toward -inf so the remainder (and nanos) stay non-negative.
    int64_t secs = utc_ms / 1000;
    int64_t rem_ms = utc_ms - secs * 1000;
    if (rem_ms < 0) {
        secs -= 1;
        rem_ms += 1000;
    }
    int64_t nanos = rem_ms * NANOSECS_PER_MILLIS + sub_nanos; // nanos-of-second in [0, NANOSECS_PER_SEC)
    if (is_max) {
        // Ceil to microsecond so the upper bound never understates the true maximum.
        int64_t us = (nanos + NANOSECS_PER_USEC - 1) / NANOSECS_PER_USEC;
        if (us == USECS_PER_SEC) {
            secs += 1;
            nanos = 0;
        } else {
            nanos = us * NANOSECS_PER_USEC;
        }
    }
    // The minimum keeps nanos: orc_ts_to_native_ts floors nanos/1000 to microseconds.
    if (is_instant) {
        secs += tz_offset_in_seconds;
    }
    // The offset is already folded in, so decode as plain UTC; the tz argument is unused on the
    // is_instant=false path (the before-epoch branch hardcodes UTC, the after-epoch branch ignores it).
    OrcTimestampHelper::orc_ts_to_native_ts(out, cctz::utc_time_zone(), /*tzoffset=*/0, secs, nanos,
                                            /*is_instant=*/false);
}

static Status decode_datetime_min_max(const orc::Type* orc_type, const orc::proto::ColumnStatistics& colStats,
                                      int64_t tz_offset_in_seconds, Column* min_col, Column* max_col) {
    if (orc_type->getKind() != orc::TypeKind::TIMESTAMP && orc_type->getKind() != orc::TypeKind::TIMESTAMP_INSTANT) {
        return Status::InvalidArgument("OrcMinMaxFilter: Invalid ORC timestamp kind");
    }
    bool is_instant = orc_type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT;

    if (colStats.has_timestampstatistics() && colStats.timestampstatistics().has_minimumutc() &&
        colStats.timestampstatistics().has_maximumutc()) {
        const auto& stats = colStats.timestampstatistics();
        TimestampValue min, max;
        decode_orc_timestamp_bound(stats.minimumutc(), stats.has_minimumnanos(), stats.minimumnanos(),
                                   /*is_max=*/false, is_instant, tz_offset_in_seconds, &min);
        decode_orc_timestamp_bound(stats.maximumutc(), stats.has_maximumnanos(), stats.maximumnanos(),
                                   /*is_max=*/true, is_instant, tz_offset_in_seconds, &max);
        DOWN_CAST_ASSIGN_MIN_MAX(LogicalType::TYPE_DATETIME);
    }
    return Status::NotFound("OrcMinMaxFilter: date column stats not found");
}

Status OrcMinMaxDecoder::decode(SlotDescriptor* slot, const orc::Type* type, const orc::proto::ColumnStatistics& stats,
                                Column* min_col, Column* max_col, int64_t tz_offset_in_seconds) {
    if (slot->is_nullable()) {
        auto* a = ColumnHelper::as_raw_column<NullableColumn>(min_col);
        auto* b = ColumnHelper::as_raw_column<NullableColumn>(max_col);
        a->null_column_raw_ptr()->append(0);
        b->null_column_raw_ptr()->append(0);
        min_col = a->data_column_raw_ptr();
        max_col = b->data_column_raw_ptr();
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
        return decode_date_min_max(stats, min_col, max_col);

    case LogicalType::TYPE_DATETIME:
        return decode_datetime_min_max(type, stats, tz_offset_in_seconds, min_col, max_col);
    default:
        return Status::NotSupported("Not support to decode min/max from orc column stats. type = " +
                                    std::to_string(ltype));
    }
}

} // namespace starrocks
