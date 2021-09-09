// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/QueryStatisticsFormatter.java

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

package com.starrocks.common.util;

import com.starrocks.common.Pair;

import java.util.Formatter;

public class QueryStatisticsFormatter {

    public static String getScanBytes(long scanBytes) {
        final Pair<Double, String> pair = DebugUtil.getByteUint(scanBytes);
        final Formatter fmt = new Formatter();
        final StringBuilder builder = new StringBuilder();
        builder.append(fmt.format("%.2f", pair.first)).append(" ").append(pair.second);
        return builder.toString();
    }

    public static String getRowsReturned(long rowsReturned) {
        final StringBuilder builder = new StringBuilder();
        builder.append(rowsReturned).append(" Rows");
        return builder.toString();
    }
}
