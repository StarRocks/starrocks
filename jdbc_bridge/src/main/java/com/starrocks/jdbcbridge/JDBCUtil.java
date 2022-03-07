// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class JDBCUtil {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    // format Date to 'YYYY-MM-dd'
    static String formatDate(Date date) {
        return format.format(date);
    }
}
