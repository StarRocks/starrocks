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

#include <algorithm>
#include <limits>
#include <orc/OrcFile.hh>
#include <utility>
#include <vector>

#include "base/time/tz_offset_cache.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/vectorized_fwd.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "formats/orc/orc_mapping.h"
#include "gen_cpp/orc_proto.pb.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

class OrcPredicates {
public:
    OrcPredicates(const std::vector<Expr*>* conjuncts, const RuntimeFilterProbeCollector* rf_collector)
            : conjuncts(conjuncts), rf_collector(rf_collector) {}
    const std::vector<Expr*>* conjuncts;
    const RuntimeFilterProbeCollector* rf_collector;
};

// Hive ORC char type will pad trailing spaces.
// https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_char.html
static inline size_t remove_trailing_spaces(const char* s, size_t size) {
    while (size > 0 && s[size - 1] == ' ') size--;
    return size;
}

class OrcDateHelper {
public:
    static int64_t native_date_to_orc_date(const DateValue& dv) { return dv._julian - date::UNIX_EPOCH_JULIAN; }

    // orc date value is days since unix epoch time.
    // so conversion will be very simple.
    static void orc_date_to_native_date(DateValue* dv, int64_t value) { dv->_julian = value + date::UNIX_EPOCH_JULIAN; }
    static void orc_date_to_native_date(JulianDate* jd, int64_t value) { *jd = value + date::UNIX_EPOCH_JULIAN; }
};

// orc timestamp is millseconds since unix epoch time.
// timestamp conversion is quite tricky, because it involves timezone info,
// and it affects how we interpret `value`. according to orc v1 spec
// https://orc.apache.org/specification/ORCv1/ writer timezone is in stripe footer.

// time conversion involves two aspects:
// 1. timezone (UTC/GMT and local timezone)
// 2. timestamp representation. (StarRocks timestampvalue or ORC seconds/nanoseconds)
// so to simplify handling timestamp conversion, we force to read timestamp from orc file in UTC timezone
// which liborc will do timestamp conversion for us efficiently. and we just handle mismatch of timestamp representation.

// in the following code, seconds has already be adjusted according to timezone.
// Timestamp: {Jualian Date}{microsecond in one day, 0 ~ 86400000000}
// JulianDate use high 22 bits, microsecond use low 40 bits
class OrcTimestampHelper {
public:
    const static cctz::time_point<cctz::sys_seconds> CCTZ_UNIX_EPOCH;

    static void orc_ts_to_native_ts_after_unix_epoch(TTimestamp* ts, int64_t seconds, int64_t nanoseconds) {
        int64_t days = seconds / SECS_PER_DAY;
        int64_t microseconds = (seconds % SECS_PER_DAY) * 1000000L + nanoseconds / 1000;
        JulianDate jd;
        OrcDateHelper::orc_date_to_native_date(&jd, days);
        *ts = timestamp::from_julian_and_time(jd, microseconds);
    }
    static void orc_ts_to_native_ts_after_unix_epoch(TimestampValue* tv, int64_t seconds, int64_t nanoseconds) {
        return orc_ts_to_native_ts_after_unix_epoch(&tv->_timestamp, seconds, nanoseconds);
    }
    static void orc_ts_to_native_ts_before_unix_epoch(TimestampValue* tv, const cctz::time_zone& tz, int64_t seconds,
                                                      int64_t nanoseconds) {
        cctz::time_point<cctz::sys_seconds> t = CCTZ_UNIX_EPOCH + cctz::seconds(seconds);
        const auto tp = cctz::convert(t, tz);
        // Carry the sub-second through as microseconds (it used to be dropped to 0). As on the
        // after-epoch path, nanoseconds is the floored sub-second in [0, NANOSECS_PER_SEC).
        tv->from_timestamp(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(),
                           nanoseconds / NANOSECS_PER_USEC);
    }
    // An instant is shown in `tz` with the offset in effect at that instant (daylight saving time included), as the
    // parquet reader does.
    static void orc_ts_to_native_ts(TimestampValue* tv, const cctz::time_zone& tz, int64_t seconds, int64_t nanoseconds,
                                    bool is_instant) {
        if (is_instant) {
            // cctz's offset over its full range, as the batch path (TzOffsetCache) uses: no clamp at year 2262.
            orc_instant_to_native_ts(tv, tz, seconds, nanoseconds,
                                     tz.lookup_offset(CCTZ_UNIX_EPOCH + cctz::seconds(seconds)).offset);
        } else if (seconds >= 0) {
            orc_ts_to_native_ts_after_unix_epoch(tv, seconds, nanoseconds);
        } else {
            orc_ts_to_native_ts_before_unix_epoch(tv, cctz::utc_time_zone(), seconds, nanoseconds);
        }
    }

    // An instant with the offset of `tz` at it already known. The fast split into days and time needs a local time
    // at or after 1970-01-01 00:00; earlier ones (including the first hours after the epoch in zones west of UTC)
    // take the cctz conversion.
    static void orc_instant_to_native_ts(TimestampValue* tv, const cctz::time_zone& tz, int64_t seconds,
                                         int64_t nanoseconds, int64_t offset) {
        if (seconds + offset >= 0) {
            orc_ts_to_native_ts_after_unix_epoch(tv, seconds + offset, nanoseconds);
        } else {
            orc_ts_to_native_ts_before_unix_epoch(tv, tz, seconds, nanoseconds);
        }
    }

    // Batch form of orc_ts_to_native_ts for instants: out[i] from seconds[i] and nanoseconds[i] (nanoseconds may be
    // nullptr for 0), skipping rows with is_null[i] != 0 (is_null may be nullptr). offset_cache and offsets_buf are
    // owned by one reader; when the batch lies within one offset window, as for a zone without daylight saving time,
    // every value gets one constant added.
    static void orc_instants_to_native_ts(TimestampValue* out, const cctz::time_zone& tz, const int64_t* seconds,
                                          const int64_t* nanoseconds, const uint8_t* is_null, size_t n,
                                          TzOffsetCache* offset_cache, std::vector<int64_t>* offsets_buf) {
        int64_t lo = std::numeric_limits<int64_t>::max();
        int64_t hi = std::numeric_limits<int64_t>::min();
        for (size_t i = 0; i < n; i++) {
            if (is_null == nullptr || !is_null[i]) {
                lo = std::min(lo, seconds[i]);
                hi = std::max(hi, seconds[i]);
            }
        }
        if (lo > hi) {
            return;
        }
        int64_t uniform_offset;
        // One offset for the batch, and every value at or after 1970-01-01 00:00 in the zone: one constant added.
        if (offset_cache->uniform_offset_for_unix(lo, hi, tz, &uniform_offset) && lo + uniform_offset >= 0) {
            for (size_t i = 0; i < n; i++) {
                if (is_null == nullptr || !is_null[i]) {
                    orc_ts_to_native_ts_after_unix_epoch(&out[i], seconds[i] + uniform_offset,
                                                         nanoseconds != nullptr ? nanoseconds[i] : 0);
                }
            }
            return;
        }
        offsets_buf->resize(n);
        int64_t* offsets = offsets_buf->data();
        offset_cache->offsets_for_unix(seconds, is_null, n, tz, offsets);
        for (size_t i = 0; i < n; i++) {
            if (is_null != nullptr && is_null[i]) {
                continue;
            }
            orc_instant_to_native_ts(&out[i], tz, seconds[i], nanoseconds != nullptr ? nanoseconds[i] : 0, offsets[i]);
        }
    }

    static void native_ts_to_orc_ts(const TimestampValue& tv, int64_t& seconds, int64_t& nanoseconds) {
        Timestamp time = tv._timestamp & TIMESTAMP_BITS_TIME;
        uint64_t microseconds = time % USECS_PER_SEC;
        seconds = tv.to_unix_second();
        nanoseconds = microseconds * 1000;
    }
};

} // namespace starrocks
