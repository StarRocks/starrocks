// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class JDBCUtil {
    // format Date to 'YYYY-MM-dd'
    static String formatDate(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(date);
    }
}
