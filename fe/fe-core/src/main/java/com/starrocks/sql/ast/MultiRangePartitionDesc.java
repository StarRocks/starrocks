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
    private final String partitionBegin;
    private final String partitionEnd;
    private Long step;
    private String timeUnit;
    private static final SimpleDateFormat DATEKEY_SDF = new SimpleDateFormat("yyyyMMdd");
    private final ImmutableSet<TimestampArithmeticExpr.TimeUnit> supportedTimeUnitType = ImmutableSet.of(
            TimestampArithmeticExpr.TimeUnit.HOUR,
            TimestampArithmeticExpr.TimeUnit.DAY,
            TimestampArithmeticExpr.TimeUnit.WEEK,
            TimestampArithmeticExpr.TimeUnit.MONTH,
            TimestampArithmeticExpr.TimeUnit.YEAR
    );

    public MultiRangePartitionDesc(String partitionBegin, String partitionEnd, Long step,
                                   String timeUnit) {
        this.partitionBegin = partitionBegin;
        this.partitionEnd = partitionEnd;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public MultiRangePartitionDesc(String partitionBegin, String partitionEnd, Long offset) {
        this.partitionBegin = partitionBegin;
        this.partitionEnd = partitionEnd;
        this.step = offset;
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

    public List<SingleRangePartitionDesc> convertToSingle(Type firstPartitionColumnType,
                                                          Map<String, String> properties) throws AnalysisException {

        if (this.getStep() <= 0) {
            throw new AnalysisException("Batch partition every clause mush be larger than zero.");
        }

        if (firstPartitionColumnType.isDateType()) {
            return buildDateTypePartition(properties);
        } else if (firstPartitionColumnType.isIntegerType()) {
            return buildNumberTypePartition(properties);
        } else {
            throw new AnalysisException("Unsupported batch partition build type:" + firstPartitionColumnType + ".");
        }
    }

    private List<SingleRangePartitionDesc> buildDateTypePartition(Map<String, String> properties)
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

        // In china, the Monday is the first day of week. In western country, the Sunday is the first day of week.
        // The semantics is should be consistent between batching partition and dynamic partition.
        // If the option is not set, the Monday will be the first day of week.
        // The 1st January is the first week of every year. Every year have 52 weeks.
        // The last week will end at 31st December.
        // If user set dynamic_partition.start_day_of_week table properties
        // it will follow this configuration to set day of week
        int dayOfWeek = 1;
        int dayOfMonth = 1;
        TimeZone timeZone = TimeUtils.getSystemTimeZone();
        String partitionPrefix = defaultPrefix;
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
        while (beginTime.isBefore(endTime)) {
            PartitionValue lowerPartitionValue = new PartitionValue(beginTime.format(beginDateTimeFormat));

            switch (timeUnitType) {
                case HOUR:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.HOUR_FORMATTER);
                    beginTime = beginTime.plusHours(timeInterval);
                    break;
                case DAY:
                    partitionName = partitionPrefix + beginTime.format(DateUtils.DATEKEY_FORMATTER);
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

            PartitionValue upperPartitionValue = new PartitionValue(beginTime.format(beginDateTimeFormat));
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue));
            SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false,
                    partitionName, partitionKeyDesc, properties);
            singleRangePartitionDescs.add(singleRangePartitionDesc);

            currentLoopNum++;
            if (currentLoopNum > maxAllowedLimit) {
                throw new AnalysisException("The number of batch partitions should not exceed:" + maxAllowedLimit);
            }
        }
        return singleRangePartitionDescs;
    }

    private List<SingleRangePartitionDesc> buildNumberTypePartition(Map<String, String> properties)
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

        Long step = this.getStep();
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        long currentLoopNum = 0;
        long maxAllowedLimit = Config.max_partitions_in_one_batch;
        while (beginNum < endNum) {
            String partitionName = defaultPrefix + beginNum;
            PartitionValue lowerPartitionValue = new PartitionValue(Long.toString(beginNum));
            beginNum += step;
            PartitionValue upperPartitionValue = new PartitionValue(Long.toString(beginNum));
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue));
            SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false,
                    partitionName, partitionKeyDesc, properties);
            singleRangePartitionDescs.add(singleRangePartitionDesc);

            currentLoopNum++;
            if (currentLoopNum > maxAllowedLimit) {
                throw new AnalysisException("The number of batch partitions should not exceed:" + maxAllowedLimit);
            }
        }
        return singleRangePartitionDescs;
    }

}
