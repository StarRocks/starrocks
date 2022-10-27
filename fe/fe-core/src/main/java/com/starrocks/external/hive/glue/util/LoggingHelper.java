// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.util;

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
