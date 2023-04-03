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


package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.parser.NodePosition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class MultiRangePartitionDesc extends PartitionDesc {

    private final String defaultPrefix = "p";
    private final String defaultTempPartitionPrefix = "tp";
    private final String partitionBegin;
    private final String partitionEnd;
    private Long step;
    private final String timeUnit;
    private static final SimpleDateFormat DATEKEY_SDF = new SimpleDateFormat("yyyyMMdd");
    private final ImmutableSet<TimestampArithmeticExpr.TimeUnit> supportedTimeUnitType = ImmutableSet.of(
            TimestampArithmeticExpr.TimeUnit.HOUR,
            TimestampArithmeticExpr.TimeUnit.DAY,
            TimestampArithmeticExpr.TimeUnit.WEEK,
            TimestampArithmeticExpr.TimeUnit.MONTH,
            TimestampArithmeticExpr.TimeUnit.YEAR
    );

    public MultiRangePartitionDesc(String partitionBegin, String partitionEnd, Long step,
                                   String timeUnit, NodePosition pos) {
        super(pos);
        this.partitionBegin = partitionBegin;
        this.partitionEnd = partitionEnd;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public Long getStep() {
        return step;
    }

    public void setStep(Long step) {
        this.step = step;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public List<SingleRangePartitionDesc> convertToSingle(PartitionConvertContext context) throws AnalysisException {

        if (this.getStep() <= 0) {
            throw new AnalysisException("Batch partition every clause mush be larger than zero.");
        }
        Type firstPartitionColumnType = context.getFirstPartitionColumnType();
        if (firstPartitionColumnType.isDateType()) {
            return buildDateTypePartition(context);
        } else if (firstPartitionColumnType.isIntegerType()) {
            return buildNumberTypePartition(context);
        } else {
            throw new AnalysisException("Unsupported batch partition build type:" + firstPartitionColumnType + ".");
        }
    }

    private List<SingleRangePartitionDesc> buildDateTypePartition(PartitionConvertContext context)
            throws AnalysisException {
        // int type does not support datekey int type

        LocalDateTime beginTime;
        LocalDateTime endTime;
        DateTimeFormatter beginDateTimeFormat;
        DateTimeFormatter endDateTimeFormat;
        try {
            beginDateTimeFormat = DateUtils.probeFormat(partitionBegin);
            endDateTimeFormat = DateUtils.probeFormat(partitionEnd);
            beginTime = DateUtils.parseStringWithDefaultHSM(partitionBegin, beginDateTimeFormat);
            endTime = DateUtils.parseStringWithDefaultHSM(partitionEnd, endDateTimeFormat);
        } catch (Exception ex) {
            throw new AnalysisException("Batch build partition EVERY is date type " +
                    "but START or END does not type match.");
        }

        if (!beginTime.isBefore(endTime)) {
            throw new AnalysisException("Batch build partition start date should less than end date.");
        }

        int timeInterval = Integer.parseInt(this.getStep().toString());
        String timeUnit = this.getTimeUnit();

        if (timeUnit == null) {
            throw new AnalysisException("Unknown timeunit for batch build partition.");
        }

        if (context.isAutoPartitionTable() && timeInterval != 1) {
            throw new AnalysisException("Automatically create partition tables and create partitions in advance " +
                    "only supports an interval of 1");
        }

        String partitionName;
        TimestampArithmeticExpr.TimeUnit timeUnitType = TimestampArithmeticExpr.TimeUnit.fromName(timeUnit);
        if (timeUnitType == null) {
            throw new AnalysisException("Batch build partition does not support time interval type.");
        } else if (!supportedTimeUnitType.contains(timeUnitType)) {
            throw new AnalysisException("Batch build partition does not support time interval type: " + timeUnit);
        }
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        long currentLoopNum = 0;
        long maxAllowedLimit = Config.max_partitions_in_one_batch;

        // In China, the Monday is the first day of week. In western country, the Sunday is the first day of week.
        // The semantics it should be consistent between batching partition and dynamic partition.
        // If the option is not set, the Monday will be the first day of week.
        // If user set dynamic_partition.start_day_of_week table properties
        // it will follow this configuration to set day of week
        int dayOfWeek = 1;
        int dayOfMonth = 1;
        TimeZone timeZone = TimeUtils.getSystemTimeZone();
        String partitionPrefix = defaultPrefix;
        if (context.isTempPartition()) {
            partitionPrefix = defaultTempPartitionPrefix;
        }
        Map<String, String> properties = context.getProperties();
        if (properties != null) {
            if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                String dayOfWeekStr = properties.get(DynamicPartitionProperty.START_DAY_OF_WEEK);
                try {
                    DynamicPartitionUtil.checkStartDayOfWeek(dayOfWeekStr);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
                dayOfWeek = Integer.parseInt(dayOfWeekStr);
            }
            if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH)) {
                String dayOfMonthStr = properties.get(DynamicPartitionProperty.START_DAY_OF_MONTH);
                try {
                    DynamicPartitionUtil.checkStartDayOfMonth(dayOfMonthStr);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
                dayOfMonth = Integer.parseInt(dayOfMonthStr);
            }
            if (properties.containsKey(DynamicPartitionProperty.PREFIX)) {
                partitionPrefix = properties.get(DynamicPartitionProperty.PREFIX);
                try {
                    DynamicPartitionUtil.checkPrefix(partitionPrefix);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
            }
        }

        DateTimeFormatter outputDateFormat = DateUtils.DATE_FORMATTER;
        if (context.getFirstPartitionColumnType() == Type.DATETIME) {
            outputDateFormat = DateUtils.DATE_TIME_FORMATTER;
        }

        if (context.isAutoPartitionTable() || !Config.enable_create_partial_partition_in_batch) {
            LocalDateTime standardBeginTime;
            LocalDateTime standardEndTime;
            String extraMsg = "";
            switch (timeUnitType) {
                case HOUR:
                    standardBeginTime = beginTime.withMinute(0).withSecond(0).withNano(0);
                    standardEndTime = endTime.withMinute(0).withSecond(0).withNano(0);
                    if (standardBeginTime.equals(standardEndTime)) {
                        standardEndTime = standardEndTime.plusHours(timeInterval);
                    }
                    break;
                case DAY:
                    standardBeginTime = beginTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                    standardEndTime = endTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                    if (standardBeginTime.equals(standardEndTime)) {
                        standardEndTime = standardEndTime.plusDays(timeInterval);
                    }
                    break;
                case WEEK:
                    standardBeginTime = beginTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
                    standardEndTime = endTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
                    if (standardBeginTime.equals(standardEndTime)) {
                        standardEndTime = standardEndTime.plusWeeks(timeInterval);
                    }
                    extraMsg = "with start day of week " + dayOfWeek;
                    break;
                case MONTH:
                    standardBeginTime = beginTime.withDayOfMonth(dayOfMonth);
                    standardEndTime = endTime.withDayOfMonth(dayOfMonth);
                    if (standardBeginTime.equals(standardEndTime)) {
                        standardEndTime = standardEndTime.plusMonths(timeInterval);
                    }
                    extraMsg = "with start day of month " + dayOfMonth;
                    break;
                case YEAR:
                    standardBeginTime = beginTime.withDayOfYear(1);
                    standardEndTime = endTime.withDayOfYear(1);
                    if (standardBeginTime.equals(standardEndTime)) {
                        standardEndTime = standardEndTime.plusYears(timeInterval);
                    }
                    break;
                default:
                    throw new AnalysisException("Batch build partition does not support time interval type: " +
                            timeUnit);
            }
            if (!(standardBeginTime.equals(beginTime) && standardEndTime.equals(endTime))) {
                String msg = "Batch build partition range [" + partitionBegin + "," + partitionEnd + ")" +
                        " should be a standard unit of time (" + timeUnitType + ") " + extraMsg + ". suggest range ["
                        + standardBeginTime.format(outputDateFormat) + "," + standardEndTime.format(outputDateFormat)
                        + ")";
                if (!context.isAutoPartitionTable()) {
                    msg += "If you want to create partial partitions in batch, you can turn off this check by " +
                            "setting the FE config enable_create_partial_partition_in_batch=true";
                }
                throw new AnalysisException(msg);
            }
        }

        while (beginTime.isBefore(endTime)) {
            PartitionValue lowerPartitionValue = new PartitionValue(beginTime.format(outputDateFormat));

            switch (timeUnitType) {
                case HOUR:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.HOUR_FORMATTER);
                    beginTime = beginTime.withMinute(0).withSecond(0).withNano(0);
                    beginTime = beginTime.plusHours(timeInterval);
                    break;
                case DAY:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.DATEKEY_FORMATTER);
                    beginTime = beginTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                    beginTime = beginTime.plusDays(timeInterval);
                    break;
                case WEEK:
                    // Compatible with dynamic partitioning
                    // First calculate the first day of the week, then calculate the week of the year
                    beginTime = beginTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
                    Calendar calendar = Calendar.getInstance(timeZone);
                    try {
                        calendar.setTime(DATEKEY_SDF.parse(beginTime.format(DateUtils.DATEKEY_FORMATTER)));
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                    int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);
                    if (weekOfYear <= 1 && calendar.get(Calendar.MONTH) >= 11) {
                        // eg: JDK think 2019-12-30 as the first week of year 2020, we need to handle this.
                        // to make it as the 53rd week of year 2019.
                        weekOfYear += 52;
                    }
                    partitionName = partitionPrefix + String.format("%s_%02d", calendar.get(Calendar.YEAR), weekOfYear);
                    beginTime = beginTime.plusWeeks(timeInterval);
                    break;
                case MONTH:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.MONTH_FORMATTER);
                    beginTime = beginTime.withDayOfMonth(dayOfMonth);
                    beginTime = beginTime.plusMonths(timeInterval);
                    break;
                case YEAR:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.YEAR_FORMATTER);
                    beginTime = beginTime.withDayOfYear(1);
                    beginTime = beginTime.plusYears(timeInterval);
                    break;
                default:
                    throw new AnalysisException("Batch build partition does not support time interval type: " +
                            timeUnit);
            }
            if (timeUnitType != TimestampArithmeticExpr.TimeUnit.DAY && beginTime.isAfter(endTime)) {
                beginTime = endTime;
            }

            PartitionValue upperPartitionValue = new PartitionValue(beginTime.format(outputDateFormat));
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue));
            // properties are from table, do not use in new SingleRangePartitionDesc.
            SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false,
                    partitionName, partitionKeyDesc, null);
            singleRangePartitionDescs.add(singleRangePartitionDesc);

            currentLoopNum++;
            if (currentLoopNum > maxAllowedLimit) {
                throw new AnalysisException("The number of batch partitions should not exceed:" + maxAllowedLimit);
            }
        }
        return singleRangePartitionDescs;
    }

    private List<SingleRangePartitionDesc> buildNumberTypePartition(PartitionConvertContext context)
            throws AnalysisException {
        if (this.getTimeUnit() != null) {
            throw new AnalysisException("Batch build partition EVERY is date type " +
                    "but START or END does not type match.");
        }
        long beginNum;
        long endNum;
        try {
            beginNum = Long.parseLong(partitionBegin);
            endNum = Long.parseLong(partitionEnd);
        } catch (NumberFormatException ex) {
            throw new AnalysisException("Batch build partition EVERY is number type " +
                    "but START or END does not type match.");
        }

        if (beginNum >= endNum) {
            throw new AnalysisException("Batch build partition start value should less then end value.");
        }

        String prefix = defaultPrefix;
        if (context.isTempPartition()) {
            prefix = defaultTempPartitionPrefix;
        }
        Long step = this.getStep();
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        long currentLoopNum = 0;
        long maxAllowedLimit = Config.max_partitions_in_one_batch;
        while (beginNum < endNum) {
            String partitionName = prefix + beginNum;
            PartitionValue lowerPartitionValue = new PartitionValue(Long.toString(beginNum));
            beginNum += step;
            PartitionValue upperPartitionValue = new PartitionValue(Long.toString(beginNum));
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue));
            // properties are from table, do not use in new SingleRangePartitionDesc.
            SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false,
                    partitionName, partitionKeyDesc, null);
            singleRangePartitionDescs.add(singleRangePartitionDesc);

            currentLoopNum++;
            if (currentLoopNum > maxAllowedLimit) {
                throw new AnalysisException("The number of batch partitions should not exceed:" + maxAllowedLimit);
            }
        }
        return singleRangePartitionDescs;
    }

}
