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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PartitionTimeUtils;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class MultiRangePartitionDesc extends PartitionDesc {

    private final String defaultPrefix = "p";
    private final String defaultTempPartitionPrefix = "tp";
    private final String partitionBegin;
    private final String partitionEnd;
    private Long step;
    private final String timeUnit;

    public MultiRangePartitionDesc(String partitionBegin, String partitionEnd, Long step,
                                   String timeUnit, NodePosition pos) {
        super(pos);
        this.partitionBegin = partitionBegin;
        this.partitionEnd = partitionEnd;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public String getPartitionBegin() {
        return partitionBegin;
    }

    public String getPartitionEnd() {
        return partitionEnd;
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
        Type firstPartitionColumnType = context.getFirstPartitionColumnType();
        if (firstPartitionColumnType.isDateType()) {
            return buildDateTypePartition(context);
        } else if (firstPartitionColumnType.isIntegerType()) {
            return buildNumberTypePartition(context);
        } else {
            throw new SemanticException("Unsupported batch partition build type:" + firstPartitionColumnType + ".");
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

        DateTimeFormatter outputDateFormat =
                PartitionTimeUtils.getPartitionBoundFormatter(context.getFirstPartitionColumnType());

        TimestampArithmeticExpr.TimeUnit timeUnitType = TimestampArithmeticExpr.TimeUnit.fromName(timeUnit);
        Preconditions.checkNotNull(timeUnitType);
        if (!PartitionTimeUtils.BATCH_PARTITION_TIME_UNITS.contains(timeUnitType)) {
            throw new AnalysisException("Batch build partition does not support time interval type: " + timeUnit);
        }

        if (context.isAutoPartitionTable()) {
            PartitionDescAnalyzer.checkManualAddPartitionDateAligned(
                    beginTime, endTime,
                    timeUnit, timeInterval,
                    dayOfWeek, dayOfMonth,
                    context.getFirstPartitionColumnType());
        }

        while (beginTime.isBefore(endTime)) {
            // The first lower bound is START as the user wrote it; only the name and the
            // following bounds are aligned.
            PartitionValue lowerPartitionValue = new PartitionValue(beginTime.format(outputDateFormat));

            beginTime = PartitionTimeUtils.alignToUnitStart(beginTime, timeUnitType, dayOfWeek, dayOfMonth);
            String partitionName = partitionPrefix
                    + PartitionTimeUtils.formatPartitionNameSuffix(beginTime, timeUnitType);
            beginTime = PartitionTimeUtils.plus(beginTime, timeUnitType, timeInterval);

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
