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

#include <exception>
#include <orc/OrcFile.hh>
#include <set>
#include <unordered_map>
#include <utility>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/vectorized_fwd.h"
#include "formats/orc/orc_mapping.h"
#include "formats/orc/utils.h"
#include "gen_cpp/orc_proto.pb.h"
#include "runtime/types.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

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
// https://orc.apache.org/specification/ORCv1/ writer timezoe  is in stripe footer.

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
        tv->from_timestamp(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(), 0);
    }
    static void orc_ts_to_native_ts(TimestampValue* tv, const cctz::time_zone& tz, int64_t tzoffset, int64_t seconds,
                                    int64_t nanoseconds, bool is_instant) {
        if (seconds >= 0) {
            seconds = is_instant ? seconds + tzoffset : seconds;
            orc_ts_to_native_ts_after_unix_epoch(tv, seconds, nanoseconds);
        } else {
            if (is_instant) {
                orc_ts_to_native_ts_before_unix_epoch(tv, tz, seconds, nanoseconds);
            } else {
                orc_ts_to_native_ts_before_unix_epoch(tv, cctz::utc_time_zone(), seconds, nanoseconds);
            }
        }
    }
};

} // namespace starrocks