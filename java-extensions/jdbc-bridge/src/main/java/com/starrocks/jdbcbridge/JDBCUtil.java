// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JDBCUtil {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // format Date to 'YYYY-MM-dd'
    static String formatDate(Date date) {
        return FORMAT.format(date);
    }
    // format LocalDateTime to 'yyyy-MM-dd HH:mm:ss'
    static String formatLocalDatetime(LocalDateTime localDateTime) {
        return DATETIME_FORMATTER.format(localDateTime);
    }
}
