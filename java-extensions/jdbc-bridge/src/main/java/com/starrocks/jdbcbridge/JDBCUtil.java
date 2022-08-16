// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JDBCUtil {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // format Date to 'YYYY-MM-dd'
    static String formatDate(Date date) {
        return format.format(date);
    }
    // format LocalDateTime to 'yyyy-MM-dd HH:mm:ss'
    static String formatLocalDatetime(LocalDateTime localDateTime) {
        return dateTimeFormatter.format(localDateTime);
    }
}
