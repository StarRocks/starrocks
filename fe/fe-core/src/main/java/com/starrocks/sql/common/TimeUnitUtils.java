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
    public static final String WEEK = "week";
    public static final String DAY = "day";
    public static final String MONTH = "month";
    public static final String QUARTER = "quarter";
    public static final String YEAR = "year";

    // "week" can not exist in timeMap due "month" not sure contains week
    public static final ImmutableMap<String, Integer> TIME_MAP =
            new ImmutableMap.Builder<String, Integer>()
                    .put(SECOND, 1)
                    .put(MINUTE, 2)
                    .put(HOUR, 3)
                    .put(DAY, 4)
                    .put(MONTH, 5)
                    .put(QUARTER, 6)
                    .put(YEAR, 7)
                    .build();

    // all time units which date_trunc supported
    public static final ImmutableMap<String, Integer> DATE_TRUNC_SUPPORTED_TIME_MAP =
            new ImmutableMap.Builder<String, Integer>()
                    .put(SECOND, 1)
                    .put(MINUTE, 2)
                    .put(HOUR, 3)
                    .put(DAY, 4)
                    .put(WEEK, 5)
                    .put(MONTH, 6)
                    .put(QUARTER, 7)
                    .put(YEAR, 8)
                    .build();
}
