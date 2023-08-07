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

package com.starrocks.sql.common;

import com.google.common.collect.ImmutableMap;

public class TimeUnitUtils {

    public static final String SECOND = "second";
    public static final String MINUTE = "minute";
    public static final String HOUR = "hour";
    public static final String DAY = "day";
    public static final String MONTH = "month";
    public static final String QUARTER = "quarter";
    public static final String YEAR = "year";

    // "week" can not exist in timeMap due "month" not sure contains week
    public static final ImmutableMap<String, Integer> TIME_MAP =
            new ImmutableMap.Builder<String, Integer>()
                    .put("second", 1)
                    .put("minute", 2)
                    .put("hour", 3)
                    .put("day", 4)
                    .put("month", 5)
                    .put("quarter", 6)
                    .put("year", 7)
                    .build();
}
