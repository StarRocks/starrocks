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


package com.starrocks.connector.hive.glue.util;

import java.util.Collection;

public class LoggingHelper {

    private static final int MAX_LOG_STRING_LEN = 2000;

    private LoggingHelper() {
    }

    public static String concatCollectionToStringForLogging(Collection<String> collection, String delimiter) {
        if (collection == null) {
            return "";
        }
        if (delimiter == null) {
            delimiter = ",";
        }
        StringBuilder bldr = new StringBuilder();
        int totalLen = 0;
        int delimiterSize = delimiter.length();
        for (String str : collection) {
            if (totalLen > MAX_LOG_STRING_LEN) {
                break;
            }
            if (str.length() + totalLen > MAX_LOG_STRING_LEN) {
                bldr.append(str.subSequence(0, (MAX_LOG_STRING_LEN - totalLen)));
                break;
            } else {
                bldr.append(str);
                bldr.append(delimiter);
                totalLen += str.length() + delimiterSize;
            }
        }
        return bldr.toString();
    }

}
