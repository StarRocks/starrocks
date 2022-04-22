// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Map;

public class MultiRangePartitionDesc extends PartitionDesc {

    private final String DEFAULT_PREFIX = "p";
    private String partitionBegin;
    private String partitionEnd;
    private Long step;
    private String timeUnit;
    private ImmutableSet<TimestampArithmeticExpr.TimeUnit> supportedTimeUnitType = ImmutableSet.of(
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

    public String getPartitionBegin() {
        return partitionBegin;
    }

    public void setPartitionBegin(String partitionBegin) {
        this.partitionBegin = partitionBegin;
    }

    public String getPartitionEnd() {
        return partitionEnd;
    }

    public void setPartitionEnd(String partitionEnd) {
        this.partitionEnd = partitionEnd;
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

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
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

        DateTime beginTime, endTime;
        DateTimeFormatter beginDateTimeFormat, endDateTimeFormat;
        try {
            beginDateTimeFormat = DateUtils.probeFormat(partitionBegin);
            endDateTimeFormat = DateUtils.probeFormat(partitionEnd);
            beginTime = DateTime.parse(partitionBegin, beginDateTimeFormat);
            endTime = DateTime.parse(partitionEnd, endDateTimeFormat);
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
        }
        WeekFields weekFields = WeekFields.of(DayOfWeek.of(dayOfWeek), 1);
        while (beginTime.isBefore(endTime)) {
            PartitionValue lowerPartitionValue = new PartitionValue(beginTime.toString(beginDateTimeFormat));

            switch (timeUnitType) {
                case DAY:
                    partitionName = DEFAULT_PREFIX + beginTime.toString(DateUtils.DATEKEY_FORMAT);
                    beginTime = beginTime.plusDays(timeInterval);
                    break;
                case WEEK:
                    LocalDate localDate = LocalDate.of(beginTime.getYear(), beginTime.getMonthOfYear(),
                            beginTime.getDayOfMonth());
                    int weekOfYear = localDate.get(weekFields.weekOfYear());
                    partitionName = String.format("%s%s_%02d", DEFAULT_PREFIX,
                            beginTime.toString(DateUtils.YEAR_FORMAT), weekOfYear);
                    beginTime = beginTime.withDayOfWeek(dayOfWeek);
                    beginTime = beginTime.plusWeeks(timeInterval);
                    break;
                case MONTH:
                    partitionName = DEFAULT_PREFIX + beginTime.toString(DateUtils.MONTH_FORMAT);
                    beginTime = beginTime.withDayOfMonth(dayOfMonth);
                    beginTime = beginTime.plusMonths(timeInterval);
                    break;
                case YEAR:
                    partitionName = DEFAULT_PREFIX + beginTime.toString(DateUtils.YEAR_FORMAT);
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

            PartitionValue upperPartitionValue = new PartitionValue(beginTime.toString(beginDateTimeFormat));
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
        long beginNum, endNum;
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
            String partitionName = DEFAULT_PREFIX + beginNum;
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
