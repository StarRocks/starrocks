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
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Map;

public class PartitionDescAnalyzer {
    public static void analyze(PartitionDesc partitionDesc) {
        if (partitionDesc instanceof SingleRangePartitionDesc) {
            analyzeSingleRangePartitionDesc((SingleRangePartitionDesc) partitionDesc);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            analyzeMultiRangePartitionDesc((MultiRangePartitionDesc) partitionDesc);
        }
    }

    public static void analyzeSingleRangePartitionDesc(SingleRangePartitionDesc singleRangePartitionDesc) {
    }

    public static void analyzeMultiRangePartitionDesc(MultiRangePartitionDesc multiRangePartitionDesc) {
        if (multiRangePartitionDesc.getStep() <= 0) {
            ErrorReport.report(ErrorCode.ERR_MULTI_PARTITION_STEP_LQ_ZERO);
        }

        if (multiRangePartitionDesc.getTimeUnit() != null) {
            String timeUnit = multiRangePartitionDesc.getTimeUnit();
            TimestampArithmeticExpr.TimeUnit timeUnitType = TimestampArithmeticExpr.TimeUnit.fromName(timeUnit);
            if (timeUnitType == null) {
                throw new SemanticException("Batch build partition does not support time interval type.");
            } else if (!MultiRangePartitionDesc.SUPPORTED_TIME_UNIT_TYPE.contains(timeUnitType)) {
                throw new SemanticException("Batch build partition does not support time interval type: " + timeUnit);
            }
        }
    }

    public static void analyzePartitionDescWithExistsTable(PartitionDesc partitionDesc, OlapTable table) {
        PartitionInfo partitionInfo = table.getPartitionInfo();

        if (!partitionInfo.isRangePartition() && partitionInfo.getType() != PartitionType.LIST) {
            throw new SemanticException("Only support adding partition to range/list partitioned table");
        }

        boolean isAutomaticPartition = partitionInfo.isAutomaticPartition();

        if (partitionDesc instanceof SingleRangePartitionDesc) {
            if (!(partitionInfo instanceof RangePartitionInfo)) {
                throw ErrorReportException.report(ErrorCode.ERR_ADD_PARTITION_WITH_ERROR_PARTITION_TYPE);
            }

            analyzeSingleRangePartitionDescWithExistsTable((SingleRangePartitionDesc) partitionDesc, partitionInfo);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            if (!(partitionInfo instanceof RangePartitionInfo)) {
                throw ErrorReportException.report(ErrorCode.ERR_ADD_PARTITION_WITH_ERROR_PARTITION_TYPE);
            }

            analyzeMultiRangePartitionDescWithExistsTable((MultiRangePartitionDesc) partitionDesc, partitionInfo,
                    table.getIdToColumn());
        } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
            SingleItemListPartitionDesc singleItemListPartitionDesc = (SingleItemListPartitionDesc) partitionDesc;
            if (isAutomaticPartition && singleItemListPartitionDesc.getValues().size() > 1) {
                throw new SemanticException("Automatically partitioned tables does not support " +
                        "multiple values in the same partition");
            }
        } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
            MultiItemListPartitionDesc multiItemListPartitionDesc = (MultiItemListPartitionDesc) partitionDesc;
            if (isAutomaticPartition && multiItemListPartitionDesc.getMultiValues().size() > 1) {
                throw new SemanticException("Automatically partitioned tables does not support " +
                        "multiple values in the same partition");
            }
        }
    }

    public static void analyzeSingleRangePartitionDescWithExistsTable(SingleRangePartitionDesc singleRangePartitionDesc,
                                                                      PartitionInfo partitionInfo) {
        if (partitionInfo.getType() == PartitionType.EXPR_RANGE && partitionInfo.isAutomaticPartition()) {
            throw new SemanticException("Currently, you cannot manually add a single range partition to " +
                    "a table that is automatically partition");
        }
    }

    public static void analyzeMultiRangePartitionDescWithExistsTable(MultiRangePartitionDesc multiRangePartitionDesc,
                                                                     PartitionInfo partitionInfo,
                                                                     Map<ColumnId, Column> idToColumn) {


        List<Column> partitionColumns = partitionInfo.getPartitionColumns(idToColumn);
        if (partitionColumns.size() != 1) {
            ErrorReport.report(ErrorCode.ERR_MULTI_PARTITION_COLUMN_NOT_SUPPORT_ADD_MULTI_RANGE);
        }

        if (partitionInfo.getType() == PartitionType.EXPR_RANGE && partitionInfo.isAutomaticPartition()) {
            // If the current partition type is == EXPR_RANGE, it must be an automatic partition now
            String descGranularity = multiRangePartitionDesc.getTimeUnit();

            if (multiRangePartitionDesc.getStep() > 1) {
                throw new SemanticException("The step of the auto-partitioned table should be 1");
            }
            long descStep = multiRangePartitionDesc.getStep();

            ExpressionRangePartitionInfo exprRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = exprRangePartitionInfo.getPartitionExprs(idToColumn).get(0);
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            String functionName = functionCallExpr.getFnName().getFunction();

            String partitionGranularity;
            long partitionStep;
            if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
                partitionGranularity = ((StringLiteral) functionCallExpr.getChild(0)).getStringValue();
                partitionStep = 1;
            } else if (FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName)) {
                partitionGranularity = ((StringLiteral) functionCallExpr.getChild(2)).getStringValue();
                partitionStep = ((IntLiteral) functionCallExpr.getChild(1)).getLongValue();
            } else {
                throw new SemanticException("Can't support partition function " + functionName);
            }

            if (!descGranularity.equalsIgnoreCase(partitionGranularity)) {
                throw new SemanticException("The granularity of the auto-partitioned table granularity(" +
                        partitionGranularity + ") should be consistent with the increased partition granularity(" +
                        descGranularity + ").");
            }
            if (descStep != partitionStep) {
                ErrorReport.report(ErrorCode.ERR_ADD_PARTITION_WITH_ERROR_STEP_LENGTH, descStep, partitionStep);
            }

            checkManualAddPartitionDateAlignedWithExprRangePartition((ExpressionRangePartitionInfo) partitionInfo,
                    idToColumn, multiRangePartitionDesc.getPartitionBegin(),
                    multiRangePartitionDesc.getPartitionEnd(), 1, 1);
        }
    }

    public static void checkManualAddPartitionDateAlignedWithExprRangePartition(
            ExpressionRangePartitionInfo exprRangePartitionInfo,
            Map<ColumnId, Column> idToColumn,
            String partitionBegin,
            String partitionEnd,
            int dayOfWeek,
            int dayOfMonth) {
        LocalDateTime partitionBeginDateTime = DateUtils.parseStrictDateTime(partitionBegin);
        LocalDateTime partitionEndDateTime = DateUtils.parseStrictDateTime(partitionEnd);

        Expr partitionExpr = exprRangePartitionInfo.getPartitionExprs(idToColumn).get(0);
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
        String functionName = functionCallExpr.getFnName().getFunction();

        String partitionGranularity;
        long partitionStep;

        if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
            partitionGranularity = ((StringLiteral) (functionCallExpr.getParams().exprs().get(0))).getStringValue();
            partitionStep = 1;
        } else if (FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName)) {
            partitionGranularity = ((StringLiteral) functionCallExpr.getChild(2)).getStringValue();

            IntLiteral intervalExpr = (IntLiteral) functionCallExpr.getChild(1);
            partitionStep = intervalExpr.getLongValue();
        } else {
            throw new SemanticException("Not support partition expression " + functionName);
        }

        checkManualAddPartitionDateAligned(partitionBeginDateTime, partitionEndDateTime,
                partitionGranularity, partitionStep,
                dayOfWeek, dayOfMonth,
                exprRangePartitionInfo.getPartitionColumns(idToColumn).get(0).getType());
    }

    public static void checkManualAddPartitionDateAligned(
            LocalDateTime partitionBeginDateTime,
            LocalDateTime partitionEndDateTime,
            String partitionGranularity,
            long partitionStep,
            int dayOfWeek,
            int dayOfMonth,
            Type partitionColumnType) {
        if (Config.enable_create_partial_partition_in_batch) {
            return;
        }

        TimestampArithmeticExpr.TimeUnit timeUnitType = TimestampArithmeticExpr.TimeUnit.fromName(partitionGranularity);
        Preconditions.checkNotNull(timeUnitType);

        LocalDateTime standardBeginTime;
        LocalDateTime standardEndTime;
        String extraMsg = "";
        switch (timeUnitType) {
            case HOUR:
                standardBeginTime = partitionBeginDateTime.withMinute(0).withSecond(0).withNano(0);
                standardEndTime = partitionEndDateTime.withMinute(0).withSecond(0).withNano(0);
                if (standardBeginTime.equals(standardEndTime)) {
                    standardEndTime = standardEndTime.plusHours(partitionStep);
                }
                break;
            case DAY:
                standardBeginTime = partitionBeginDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                standardEndTime = partitionEndDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                if (standardBeginTime.equals(standardEndTime)) {
                    standardEndTime = standardEndTime.plusDays(partitionStep);
                }
                break;
            case WEEK:
                standardBeginTime = partitionBeginDateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
                standardEndTime = partitionEndDateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
                if (standardBeginTime.equals(standardEndTime)) {
                    standardEndTime = standardEndTime.plusWeeks(partitionStep);
                }
                extraMsg = "with start day of week " + dayOfWeek;
                break;
            case MONTH:
                standardBeginTime = partitionBeginDateTime.withDayOfMonth(dayOfMonth);
                standardEndTime = partitionEndDateTime.withDayOfMonth(dayOfMonth);
                if (standardBeginTime.equals(standardEndTime)) {
                    standardEndTime = standardEndTime.plusMonths(partitionStep);
                }
                extraMsg = "with start day of month " + dayOfMonth;
                break;
            case YEAR:
                standardBeginTime = partitionBeginDateTime.withDayOfYear(1);
                standardEndTime = partitionEndDateTime.withDayOfYear(1);
                if (standardBeginTime.equals(standardEndTime)) {
                    standardEndTime = standardEndTime.plusYears(partitionStep);
                }
                break;
            default:
                throw new SemanticException("Batch build partition does not support time interval type: " +
                        partitionGranularity);
        }
        if (!(standardBeginTime.equals(partitionBeginDateTime) && standardEndTime.equals(partitionEndDateTime))) {
            DateTimeFormatter outputDateFormat = partitionColumnType == Type.DATE
                    ? DateUtils.DATE_FORMATTER_UNIX : DateUtils.DATE_TIME_FORMATTER_UNIX;

            String msg = "Batch build partition range [" +
                    partitionBeginDateTime.format(outputDateFormat) + "," +
                    partitionEndDateTime.format(outputDateFormat) + ")" +
                    " should be a standard unit of time (" + timeUnitType + ") " + extraMsg + ". suggest range ["
                    + standardBeginTime.format(outputDateFormat) + "," + standardEndTime.format(outputDateFormat)
                    + ")";
            msg += "If you want to create partial partitions in batch, you can turn off this check by " +
                    "setting the FE config enable_create_partial_partition_in_batch=true";
            throw new SemanticException(msg);
        }
    }
}