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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#include "util/timezone_utils.h"

#include <cctz/time_zone.h>

#include <charconv>
#include <string_view>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "util/phmap/phmap.h"

namespace starrocks {

RE2 TimezoneUtils::time_zone_offset_format_reg(R"(^[+-]{1}\d{2}\:\d{2}$)", re2::RE2::Quiet);

const std::string TimezoneUtils::default_time_zone = "+08:00";

static phmap::flat_hash_map<std::string_view, cctz::time_zone> _s_cached_timezone;
static phmap::flat_hash_map<std::pair<std::string_view, std::string_view>, int64_t> _s_cached_offsets;

cctz::time_zone TimezoneUtils::local_time_zone() {
    return cctz::local_time_zone();
}

void TimezoneUtils::init_time_zones() {
    // timezone cache list
    // We cannot add a time zone to the cache that contains both daylight saving time and winter time
    static std::vector<std::string> timezones = {
#include "timezone.dat"
    };
    for (const auto& timezone : timezones) {
        cctz::time_zone ctz;
        if (cctz::load_time_zone(timezone, &ctz)) {
            _s_cached_timezone.emplace(timezone, ctz);
        } else {
            LOG(WARNING) << "not found timezone:" << timezone;
        }
    }
    auto civil = cctz::civil_second(2021, 12, 1, 8, 30, 1);
    for (const auto& [timezone1, _] : _s_cached_timezone) {
        for (const auto& [timezone2, _] : _s_cached_timezone) {
            const auto tp1 = cctz::convert(civil, _s_cached_timezone[timezone1]);
            const auto tp2 = cctz::convert(civil, _s_cached_timezone[timezone2]);
            std::pair<std::string_view, std::string_view> key = {timezone1, timezone2};
            _s_cached_offsets[key] = tp1.time_since_epoch().count() - tp2.time_since_epoch().count();
        }
    }

    // other cached timezone
    // only cache CCTZ, won't caculate offsets
    static std::vector<std::string> other_timezones = {
#include "othertimezone.dat"
    };
    for (const auto& timezone : other_timezones) {
        cctz::time_zone ctz;
        if (cctz::load_time_zone(timezone, &ctz)) {
            _s_cached_timezone.emplace(timezone, ctz);
        } else {
            LOG(WARNING) << "not found timezone:" << timezone;
        }
    }
}

bool TimezoneUtils::find_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz) {
    re2::StringPiece value;
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    } else if (time_zone_offset_format_reg.Match(re2::StringPiece(timezone.data(), timezone.size()), 0, timezone.size(),
                                                 RE2::UNANCHORED, &value, 1)) {
        bool positive = (value[0] != '-');

        //Regular expression guarantees hour and minute mush be int
        int hour = std::stoi(value.substr(1, 2).as_string());
        int minute = std::stoi(value.substr(4, 2).as_string());

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    } else if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

bool TimezoneUtils::timezone_offsets(std::string_view src, std::string_view dst, int64_t* offset) {
    if (const auto iter = _s_cached_offsets.find(std::make_pair(src, dst)); iter != _s_cached_offsets.end()) {
        *offset = iter->second;
        return true;
    }
    return false;
}

bool TimezoneUtils::find_cctz_time_zone(const TimezoneHsScan& timezone_hsscan, std::string_view timezone,
                                        cctz::time_zone& ctz) {
    // find time_zone by cache
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    }

    bool v = false;
    hs_scan(
            timezone_hsscan.database, timezone.data(), timezone.size(), 0, timezone_hsscan.scratch,
            [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags, void* ctx) -> int {
                *((bool*)ctx) = true;
                return 1;
            },
            &v);

    if (v) {
        bool positive = (timezone.substr(0, 1) != "-");

        //Regular expression guarantees hour and minute mush be int
        int hour = 0;
        std::string_view hour_str = timezone.substr(1, 2);
        std::from_chars(hour_str.begin(), hour_str.end(), hour);
        int minute = 0;
        std::string_view minute_str = timezone.substr(4, 5);
        std::from_chars(minute_str.begin(), minute_str.end(), minute);

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    }

    if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

int64_t TimezoneUtils::to_utc_offset(const cctz::time_zone& ctz) {
    cctz::time_zone utc = cctz::utc_time_zone();
    const std::chrono::time_point<std::chrono::system_clock> tp;
    const cctz::time_zone::absolute_lookup a = ctz.lookup(tp);
    const cctz::time_zone::absolute_lookup b = utc.lookup(tp);
    return a.cs - b.cs;
}

} // namespace starrocks
