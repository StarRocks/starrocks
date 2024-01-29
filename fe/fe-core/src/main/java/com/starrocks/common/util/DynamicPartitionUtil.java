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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/DynamicPartitionUtil.java

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TimestampArithmeticExpr.TimeUnit;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER;
import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER;

public class DynamicPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionUtil.class);

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void checkTimeUnit(String timeUnit) throws DdlException {
        if (Strings.isNullOrEmpty(timeUnit)
                || !(timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString()))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_TIME_UNIT, timeUnit);
        }
    }

    public static void checkPrefix(String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    private static void checkStart(String start) throws DdlException {
        try {
            if (Integer.parseInt(start) >= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_ZERO, start);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_FORMAT, start);
        }
    }

    private static int checkEnd(String end) throws DdlException {
        if (Strings.isNullOrEmpty(end)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_EMPTY);
        }
        try {
            int endInt = Integer.parseInt(end);
            if (endInt <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_ZERO, end);
            }
            return endInt;
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_FORMAT, end);
        }
        return DynamicPartitionProperty.DEFAULT_END_OFFSET;
    }

    private static void checkBuckets(String buckets) throws DdlException {
        if (Strings.isNullOrEmpty(buckets)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_EMPTY);
        }
        try {
            if (Integer.parseInt(buckets) < 0) {
                buckets = "0";
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT, buckets);
        }
    }

    private static void checkEnable(String enable) throws DdlException {
        if (Strings.isNullOrEmpty(enable)
                || (!Boolean.TRUE.toString().equalsIgnoreCase(enable) &&
                !Boolean.FALSE.toString().equalsIgnoreCase(enable))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_ENABLE, enable);
        }
    }

    public static void checkStartDayOfMonth(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_MONTH);
        }
        try {
            int dayOfMonth = Integer.parseInt(val);
            // only support from 1st to 28th, not allow 29th, 30th and 31th to avoid problems
            // caused by lunar year and lunar month
            if (dayOfMonth < 1 || dayOfMonth > 28) {
                throw new DdlException(DynamicPartitionProperty.START_DAY_OF_MONTH + " should between 1 and 28");
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_MONTH);
        }
    }

    public static void checkStartDayOfWeek(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_WEEK);
        }
        try {
            int dayOfWeek = Integer.parseInt(val);
            if (dayOfWeek < 1 || dayOfWeek > 7) {
                throw new DdlException(DynamicPartitionProperty.START_DAY_OF_WEEK + " should between 1 and 7");
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_WEEK);
        }
    }

    private static void checkReplicationNum(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.REPLICATION_NUM);
        }
        try {
            if (Integer.parseInt(val) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_FORMAT, val);
        }
    }

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        for (String key : properties.keySet()) {
            if (key.startsWith(DynamicPartitionProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkInputDynamicPartitionProperties(Map<String, String> properties,
                                                               PartitionInfo partitionInfo) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }

        Map<String, String> checkProp = new CaseInsensitiveMap<>(properties);

        String timeUnit = checkProp.get(DynamicPartitionProperty.TIME_UNIT);
        String prefix = checkProp.get(DynamicPartitionProperty.PREFIX);
        String start = checkProp.get(DynamicPartitionProperty.START);
        String timeZone = checkProp.get(DynamicPartitionProperty.TIME_ZONE);
        String end = checkProp.get(DynamicPartitionProperty.END);
        String enable = checkProp.get(DynamicPartitionProperty.ENABLE);
        String buckets = checkProp.get(DynamicPartitionProperty.BUCKETS);

        if (!(Strings.isNullOrEmpty(enable) && Strings.isNullOrEmpty(timeUnit) && Strings.isNullOrEmpty(timeZone)
                && Strings.isNullOrEmpty(prefix) && Strings.isNullOrEmpty(start) && Strings.isNullOrEmpty(end)
                && Strings.isNullOrEmpty(buckets))) {

            if (!partitionInfo.isRangePartition() || partitionInfo.isMultiColumnPartition()) {
                throw new DdlException("Dynamic partition only support single-column range partition");
            }

            if (Strings.isNullOrEmpty(enable)) {
                properties.put(DynamicPartitionProperty.ENABLE, "true");
            }
            if (Strings.isNullOrEmpty(timeUnit)) {
                throw new DdlException("Must assign dynamic_partition.time_unit properties");
            }
            if (Strings.isNullOrEmpty(prefix)) {
                properties.put(DynamicPartitionProperty.PREFIX, DynamicPartitionProperty.NOT_SET_PREFIX);
            }
            if (Strings.isNullOrEmpty(start)) {
                properties.put(DynamicPartitionProperty.START, String.valueOf(Integer.MIN_VALUE));
            }
            if (Strings.isNullOrEmpty(end)) {
                throw new DdlException("Must assign dynamic_partition.end properties");
            }
            if (Strings.isNullOrEmpty(timeZone)) {
                properties.put(DynamicPartitionProperty.TIME_ZONE, TimeUtils.getSystemTimeZone().getID());
            }

            if (timeUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                List<Column> partitionColumns = partitionInfo.getPartitionColumns();
                for (Column partitionColumn : partitionColumns) {
                    if (partitionColumn.getPrimitiveType().isDateType()) {
                        throw new SemanticException("Date type partition does not support dynamic partitioning" +
                                " granularity of hour");
                    }
                }
            }

        }
        return true;
    }

    public static void registerOrRemovePartitionScheduleInfo(long dbId, OlapTable olapTable) {
        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(dbId, olapTable);
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(dbId, olapTable);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().createOrUpdateRuntimeInfo(
                olapTable.getName(), DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
    }

    public static void registerOrRemoveDynamicPartitionTable(long dbId, OlapTable olapTable) {
        if (olapTable.getTableProperty() != null
                && olapTable.getTableProperty().getDynamicPartitionProperty() != null) {
            if (olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                        .registerDynamicPartitionTable(dbId, olapTable.getId());
            } else {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                        .removeDynamicPartitionTable(dbId, olapTable.getId());
            }
        }
    }

    public static void registerOrRemovePartitionTTLTable(long dbId, OlapTable olapTable) {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null &&
                (tableProperty.getPartitionTTLNumber() > 0 || !tableProperty.getPartitionTTL().isZero())) {
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                    .registerTtlPartitionTable(dbId, olapTable.getId());
        } else {
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                    .removeTtlPartitionTable(dbId, olapTable.getId());
        }
    }

    private static void checkHistoryPartitionNum(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException(
                    "Invalid properties: " + DynamicPartitionProperty.HISTORY_PARTITION_NUM);
        }
        try {
            int historyPartitionNum = Integer.parseInt(val);
            if (historyPartitionNum < 0) {
                ErrorReport.reportDdlException(
                        ErrorCode.ERROR_DYNAMIC_PARTITION_HISTORY_PARTITION_NUM_ZERO);
            }
        } catch (NumberFormatException e) {
            throw new DdlException(
                    "Invalid properties: " + DynamicPartitionProperty.HISTORY_PARTITION_NUM);
        }
    }

    public static Map<String, String> analyzeDynamicPartition(Map<String, String> properties) throws DdlException {
        // properties should not be empty, check properties before call this function
        Map<String, String> analyzedProperties = new HashMap<>();
        if (properties.containsKey(DynamicPartitionProperty.TIME_UNIT)) {
            String timeUnitValue = properties.get(DynamicPartitionProperty.TIME_UNIT);
            checkTimeUnit(timeUnitValue);
            properties.remove(DynamicPartitionProperty.TIME_UNIT);
            analyzedProperties.put(DynamicPartitionProperty.TIME_UNIT, timeUnitValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.PREFIX)) {
            String prefixValue = properties.get(DynamicPartitionProperty.PREFIX);
            checkPrefix(prefixValue);
            properties.remove(DynamicPartitionProperty.PREFIX);
            analyzedProperties.put(DynamicPartitionProperty.PREFIX, prefixValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.BUCKETS)) {
            String bucketsValue = properties.get(DynamicPartitionProperty.BUCKETS);
            checkBuckets(bucketsValue);
            properties.remove(DynamicPartitionProperty.BUCKETS);
            analyzedProperties.put(DynamicPartitionProperty.BUCKETS, bucketsValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.ENABLE)) {
            String enableValue = properties.get(DynamicPartitionProperty.ENABLE);
            checkEnable(enableValue);
            properties.remove(DynamicPartitionProperty.ENABLE);
            analyzedProperties.put(DynamicPartitionProperty.ENABLE, enableValue);
        }

        // If dynamic property is not specified.Use Integer.MIN_VALUE as default
        if (properties.containsKey(DynamicPartitionProperty.START)) {
            String startValue = properties.get(DynamicPartitionProperty.START);
            checkStart(startValue);
            properties.remove(DynamicPartitionProperty.START);
            analyzedProperties.put(DynamicPartitionProperty.START, startValue);
        }

        int end = DynamicPartitionProperty.DEFAULT_END_OFFSET;
        if (properties.containsKey(DynamicPartitionProperty.END)) {
            String endValue = properties.get(DynamicPartitionProperty.END);
            end = checkEnd(endValue);
            properties.remove(DynamicPartitionProperty.END);
            analyzedProperties.put(DynamicPartitionProperty.END, endValue);
        }

        if (properties.containsKey(DynamicPartitionProperty.HISTORY_PARTITION_NUM)) {
            String val = properties.get(DynamicPartitionProperty.HISTORY_PARTITION_NUM);
            checkHistoryPartitionNum(val);
            properties.remove(DynamicPartitionProperty.HISTORY_PARTITION_NUM);
            analyzedProperties.put(DynamicPartitionProperty.HISTORY_PARTITION_NUM, val);
        }

        int historyPartitionNum = Integer.parseInt(analyzedProperties.getOrDefault(
                DynamicPartitionProperty.HISTORY_PARTITION_NUM,
                String.valueOf(DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM)));

        int expectCreatePartitionNum = end + historyPartitionNum;

        if (expectCreatePartitionNum > Config.max_dynamic_partition_num) {
            throw new DdlException("Too many dynamic partitions: " + expectCreatePartitionNum
                    + ". Limit: " + Config.max_dynamic_partition_num);
        }

        if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH)) {
            String val = properties.get(DynamicPartitionProperty.START_DAY_OF_MONTH);
            checkStartDayOfMonth(val);
            properties.remove(DynamicPartitionProperty.START_DAY_OF_MONTH);
            analyzedProperties.put(DynamicPartitionProperty.START_DAY_OF_MONTH, val);
        }

        if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
            String val = properties.get(DynamicPartitionProperty.START_DAY_OF_WEEK);
            checkStartDayOfWeek(val);
            properties.remove(DynamicPartitionProperty.START_DAY_OF_WEEK);
            analyzedProperties.put(DynamicPartitionProperty.START_DAY_OF_WEEK, val);
        }

        if (properties.containsKey(DynamicPartitionProperty.TIME_ZONE)) {
            String val = properties.get(DynamicPartitionProperty.TIME_ZONE);
            TimeUtils.checkTimeZoneValidAndStandardize(val);
            properties.remove(DynamicPartitionProperty.TIME_ZONE);
            analyzedProperties.put(DynamicPartitionProperty.TIME_ZONE, val);
        }
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM)) {
            String val = properties.get(DynamicPartitionProperty.REPLICATION_NUM);
            checkReplicationNum(val);
            properties.remove(DynamicPartitionProperty.REPLICATION_NUM);
            analyzedProperties.put(DynamicPartitionProperty.REPLICATION_NUM, val);
        }
        return analyzedProperties;
    }

    public static void checkAlterAllowed(OlapTable olapTable) throws DdlException {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null && tableProperty.getDynamicPartitionProperty() != null &&
                tableProperty.getDynamicPartitionProperty().isExist() &&
                tableProperty.getDynamicPartitionProperty().getEnable()) {
            throw new DdlException("Cannot add/drop partition on a Dynamic Partition Table, " +
                    "Use command `ALTER TABLE tbl_name SET (\"dynamic_partition.enable\" = \"false\")` firstly.");
        }
    }

    public static boolean isDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable) ||
                !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        TableProperty tableProperty = ((OlapTable) table).getTableProperty();
        if (tableProperty == null || !tableProperty.getDynamicPartitionProperty().isExist()) {
            return false;
        }

        return rangePartitionInfo.getPartitionColumns().size() == 1 &&
                tableProperty.getDynamicPartitionProperty().getEnable();
    }

    public static boolean isTTLPartitionTable(Table table) {
        if (!(table instanceof OlapTable) ||
                !(((OlapTable) table).getPartitionInfo().isRangePartition())) {
            return false;
        }

        TableProperty tableProperty = ((OlapTable) table).getTableProperty();
        if (tableProperty == null) {
            return false;
        }
        Map<String, String> properties = tableProperty.getProperties();
        if (!properties.containsKey(PROPERTIES_PARTITION_TTL_NUMBER) &&
                !properties.containsKey(PROPERTIES_PARTITION_LIVE_NUMBER)) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        return rangePartitionInfo.getPartitionColumns().size() == 1;
    }


    /**
     * properties should be checked before call this method
     */
    public static void checkAndSetDynamicPartitionProperty(OlapTable olapTable, Map<String, String> properties)
            throws DdlException {
        if (DynamicPartitionUtil.checkInputDynamicPartitionProperties(properties, olapTable.getPartitionInfo())) {
            Map<String, String> dynamicPartitionProperties = DynamicPartitionUtil.analyzeDynamicPartition(properties);
            TableProperty tableProperty = olapTable.getTableProperty();
            if (tableProperty != null) {
                tableProperty.modifyTableProperties(dynamicPartitionProperties);
            } else {
                tableProperty = new TableProperty(dynamicPartitionProperties);
                olapTable.setTableProperty(tableProperty);
            }
            tableProperty.buildDynamicProperty();
        }
    }

    public static void checkIfExpressionPartitionAllowed(Map<String, String> properties, Expr expr) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        if (!Strings.isNullOrEmpty(properties.get(DynamicPartitionProperty.ENABLE))
                && !ExpressionRangePartitionInfoV2.supportedDynamicPartition(expr)) {
            throw new DdlException("Expression partitioning does not support properties of dynamic partitioning");
        }
    }

    public static String getPartitionFormat(Column column) throws DdlException {
        if (column.getPrimitiveType().equals(PrimitiveType.DATE)) {
            return DATE_FORMAT;
        } else if (column.getPrimitiveType().equals(PrimitiveType.DATETIME)) {
            return DATETIME_FORMAT;
        } else if (PrimitiveType.getIntegerTypeList().contains(column.getPrimitiveType())) {
            // TODO: For Integer Type, only support format it as yyyyMMdd now
            return TIMESTAMP_FORMAT;
        } else {
            throw new DdlException("Dynamic Partition Only Support DATE, DATETIME and INTEGER Type Now.");
        }
    }

    public static String getFormattedPartitionName(TimeZone tz, String formattedDateStr, String timeUnit) {
        formattedDateStr = formattedDateStr.replace("-", "").replace(":", "").replace(" ", "");
        if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return formattedDateStr.substring(0, 10);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return formattedDateStr.substring(0, 8);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return formattedDateStr.substring(0, 6);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString())) {
            return formattedDateStr.substring(0, 4);
        } else {
            formattedDateStr = formattedDateStr.substring(0, 8);
            Calendar calendar = Calendar.getInstance(tz);
            try {
                calendar.setTime(new SimpleDateFormat("yyyyMMdd").parse(formattedDateStr));
            } catch (ParseException e) {
                LOG.warn("Format dynamic partition name error. Error={}", e.getMessage());
                return formattedDateStr;
            }
            int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);
            if (weekOfYear <= 1 && calendar.get(Calendar.MONTH) >= 11) {
                // eg: JDK think 2019-12-30 as the first week of year 2020, we need to handle this.
                // to make it as the 53rd week of year 2019.
                weekOfYear += 52;
            }
            return String.format("%s_%02d", calendar.get(Calendar.YEAR), weekOfYear);
        }
    }

    // return the partition range date string formatted as yyyy-MM-dd[ HH:mm::ss]
    public static String getPartitionRangeString(DynamicPartitionProperty property, ZonedDateTime current,
                                                 int offset, String format) {
        String timeUnit = property.getTimeUnit();
        if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return getPartitionRangeOfHour(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return getPartitionRangeOfDay(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            return getPartitionRangeOfWeek(current, offset, property.getStartOfWeek(), format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return getPartitionRangeOfMonth(current, offset, property.getStartOfMonth(), format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString())) {
            return getPartitionRangeOfYear(current, offset, 1, format);
        }
        return null;
    }

    /**
     * return formatted string of partition range in HOUR granularity.
     * offset: The offset from the current hour. 0 means current hour, -1 means pre hour, 1 means next hour.
     * format: the format of the return hour string.
     * <p>
     * Eg:
     * Hour is 2020-05-24 10:00:00, offset = -1
     * It will return 2020-05-24 09:00:00
     */
    private static String getPartitionRangeOfHour(ZonedDateTime current, int offset, String format) {
        return getFormattedTimeWithoutMinuteSecond(current.plusHours(offset), format);
    }

    /**
     * return formatted string of partition range in DAY granularity.
     * offset: The offset from the current day. 0 means current day, 1 means tomorrow, -1 means yesterday.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = -1
     * It will return 2020-05-23
     */
    private static String getPartitionRangeOfDay(ZonedDateTime current, int offset, String format) {
        return getFormattedTimeWithoutHourMinuteSecond(current.plusDays(offset), format);
    }

    /**
     * return formatted string of partition range in WEEK granularity.
     * offset: The offset from the current week. 0 means current week, 1 means next week, -1 means last week.
     * startOf: Define the start day of each week. 1 means MONDAY, 7 means SUNDAY.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = 0, startOf.dayOfWeek = 3
     * It will return 2020-05-20  (Wednesday of last week)
     */
    private static String getPartitionRangeOfWeek(ZonedDateTime current, int offset, StartOfDate startOf,
                                                  String format) {
        Preconditions.checkArgument(startOf.isStartOfWeek());
        // 1. get the offset week
        ZonedDateTime offsetWeek = current.plusWeeks(offset);
        // 2. get the date of `startOf` week
        ZonedDateTime resultTime = offsetWeek.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(startOf.dayOfWeek)));
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    /**
     * return formatted string of partition range in MONTH granularity.
     * offset: The offset from the current month. 0 means current month, 1 means next month, -1 means last month.
     * startOf: Define the start date of each month. 1 means start on the 1st of every month.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = 1, startOf.month = 3
     * It will return 2020-06-03
     */
    private static String getPartitionRangeOfMonth(ZonedDateTime current, int offset, StartOfDate startOf,
                                                   String format) {
        Preconditions.checkArgument(startOf.isStartOfMonth());
        // 1. Get the offset date.
        int realOffset = offset;
        int currentDay = current.getDayOfMonth();
        if (currentDay < startOf.day) {
            // eg: today is 2020-05-20, `startOf.day` is 25, and offset is 0.
            // we should return 2020-04-25, which is the last month.
            realOffset -= 1;
        }
        ZonedDateTime resultTime = current.plusMonths(realOffset).withDayOfMonth(startOf.day);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    private static String getPartitionRangeOfYear(ZonedDateTime current, int offset, int startDayOfYear, String format) {
        // 1. Get the offset date.
        int realOffset = offset;
        int currentDayOfYear = current.getDayOfYear();
        if (currentDayOfYear < startDayOfYear) {
            // eg: today is 2020-05-20, `startDayOfYear` is 180, and offset is 0.
            // we should return 2019-07-01, which is the last year.
            realOffset -= 1;
        }
        ZonedDateTime resultTime = current.plusYears(realOffset).withDayOfYear(startDayOfYear);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    private static String getFormattedTimeWithoutMinuteSecond(ZonedDateTime zonedDateTime, String format) {
        ZonedDateTime timeWithoutMinuteSecond = zonedDateTime.withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutMinuteSecond);
    }

    private static String getFormattedTimeWithoutHourMinuteSecond(ZonedDateTime zonedDateTime, String format) {
        ZonedDateTime timeWithoutHourMinuteSecond = zonedDateTime.withHour(0).withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutHourMinuteSecond);
    }

    /**
     * Used to indicate the start date.
     * Taking the year as the granularity, it can indicate the month and day as the start date.
     * Taking the month as the granularity, it can indicate the date of as the start date.
     * Taking the week as the granularity, it can indicate the day of the week as the starting date.
     */
    public static class StartOfDate {
        public int month;
        public int day;
        public int dayOfWeek;

        public StartOfDate(int month, int day, int dayOfWeek) {
            this.month = month;
            this.day = day;
            this.dayOfWeek = dayOfWeek;
        }

        public boolean isStartOfYear() {
            return this.month != -1 && this.day != -1 && this.dayOfWeek == -1;
        }

        public boolean isStartOfMonth() {
            return this.month == -1 && this.day != -1 && this.dayOfWeek == -1;
        }

        public boolean isStartOfWeek() {
            return this.month == -1 && this.day == -1 && this.dayOfWeek != -1;
        }

        public String toDisplayInfo() {
            if (isStartOfWeek()) {
                return DayOfWeek.of(dayOfWeek).name();
            } else if (isStartOfMonth()) {
                return Util.ordinal(day);
            } else if (isStartOfYear()) {
                return Month.of(month) + " " + Util.ordinal(day);
            } else {
                return FeConstants.NULL_STRING;
            }
        }

        @Override
        public String toString() {
            // TODO Auto-generated method stub
            return super.toString();
        }
    }
}
