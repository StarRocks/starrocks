// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.common.util;

import com.starrocks.common.AnalysisException;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {

    public static final String DATEKEY_FORMAT = "yyyyMMdd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String WEEK_FORMAT = "yyyy_ww";
    public static final String MONTH_FORMAT = "yyyyMM";
    public static final String YEAR_FORMAT = "yyyy";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT);
    public static final DateTimeFormatter DATEKEY_FORMATTER = DateTimeFormat.forPattern(DATEKEY_FORMAT);
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DATE_TIME_FORMAT);

    public static DateTimeFormatter probeFormat(String dateTimeStr) throws AnalysisException {
        if (dateTimeStr.length() == 8) {
            return DateUtils.DATEKEY_FORMATTER;
        } else if (dateTimeStr.length() == 10) {
            return DateUtils.DATE_FORMATTER;
        } else if (dateTimeStr.length() == 19) {
            return DateUtils.DATE_TIME_FORMATTER;
        } else {
            throw new AnalysisException("can not probe datetime format:" + dateTimeStr);
        }
    }
}