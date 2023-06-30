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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.h

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

#pragma once

#include <re2/re2.h>

#include <string_view>

#include "cctz/time_zone.h"
#include "types/date_value.h"
#include "util/timezone_hsscan.h"

namespace starrocks {

class TimezoneUtils {
public:
    static bool find_cctz_time_zone(const TimezoneHsScan& timezone_hsscan, std::string_view timezone,
                                    cctz::time_zone& ctz);
    static void init_time_zones();
    static bool find_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz);
    static bool timezone_offsets(std::string_view src, std::string_view dst, int64_t* offset);
    static int64_t to_utc_offset(const cctz::time_zone& ctz); // timezone offset in seconds.
    static cctz::time_zone local_time_zone();

public:
    static const std::string default_time_zone;

private:
    static bool _match_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz);
};
} // namespace starrocks
