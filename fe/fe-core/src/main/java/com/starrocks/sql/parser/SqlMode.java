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

package com.starrocks.sql.parser;

import com.google.common.base.Strings;
import com.starrocks.common.util.PropertyAnalyzer;

import java.util.Map;

public class SqlMode {
    public static final long DEFAULT = 32;

    public static long getSqlMode(Map<String, String> props) {
        if (props == null) {
            return SqlMode.DEFAULT;
        }
        try {
            String val = props.getOrDefault(PropertyAnalyzer.PROPERTIES_MV_SESSION_SQL_MODE, "");
            if (Strings.isNullOrEmpty(val)) {
                return SqlMode.DEFAULT;
            }
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return SqlMode.DEFAULT;
        }
    }
}
