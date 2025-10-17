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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.PartitionConvertContext;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SinglePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.util.Strings;
import org.threeten.extra.PeriodDuration;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionDescAnalyzer {
    public static void analyze(PartitionDesc partitionDesc) {
        if (partitionDesc instanceof MultiRangePartitionDesc) {
            analyzeMultiRangePartitionDesc((MultiRangePartitionDesc) partitionDesc);
        }
    }

    /**
     * Main analyze method that handles both column definitions and properties
     */
    public static void analyze(PartitionDesc partitionDesc, List<ColumnDef> columnDefs, Map<String, String> otherProperties)
            throws AnalysisException {
        if (partitionDesc instanceof ListPartitionDesc) {
            analyzeListPartitionDesc((ListPartitionDesc) partitionDesc, columnDefs, otherProperties);
        } else if (partitionDesc instanceof RangePartitionDesc) {
            analyzeRangePartitionDesc((RangePartitionDesc) partitionDesc, columnDefs, otherProperties);
        } else if (partitionDesc instanceof ExpressionPartitionDesc) {
            analyzeExpressionPartitionDesc((ExpressionPartitionDesc) partitionDesc, columnDefs, otherProperties);
        } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
            analyzeSingleItemListPartitionDesc((SingleItemListPartitionDesc) partitionDesc, columnDefs, otherProperties);
        } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
            analyzeMultiItemListPartitionDesc((MultiItemListPartitionDesc) partitionDesc, columnDefs, otherProperties);
        } else {
            throw new AnalysisException("Unsupported partition description type: " + partitionDesc.getClass().getSimpleName());
        }
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

    /**
     * Analyze SingleRangePartitionDesc with partition column number and table properties
     */
    public static void analyzeSingleRangePartitionDesc(SingleRangePartitionDesc desc, int partColNum,
                                                       Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(desc.getPartitionName());
        desc.getPartitionKeyDesc().analyze(partColNum);

        if (partColNum == 1) {
            analyzeSinglePartitionProperties(desc, tableProperties, desc.getPartitionKeyDesc());
        } else {
            analyzeSinglePartitionProperties(desc, tableProperties, null);
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
            DateTimeFormatter outputDateFormat = partitionColumnType.isDate()
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

    // Analyze methods for different partition types

    public static void analyzeListPartitionDesc(ListPartitionDesc desc, List<ColumnDef> columnDefs,
                                                Map<String, String> tableProperties) throws AnalysisException {
        // analyze partition columns
        List<ColumnDef> columnDefList = analyzeListPartitionColumns(desc, columnDefs);
        // analyze partition expr
        analyzeListPartitionExprs(desc, columnDefs);
        // analyze single list property
        analyzeSingleListPartitions(desc, tableProperties, columnDefList);
        // analyze multi list partition
        analyzeMultiListPartitions(desc, tableProperties, columnDefList);
        // list partition values should not contain NULL partition value if this column is not nullable.
        postAnalyzeListPartitionColumns(desc, columnDefList);
    }

    public static void analyzeRangePartitionDesc(RangePartitionDesc desc, List<ColumnDef> columnDefs,
                                                 Map<String, String> otherProperties) throws AnalysisException {
        if (desc.getPartitionColNames() == null || desc.getPartitionColNames().isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        ColumnDef firstPartitionColumn = null;
        for (String partitionCol : desc.getPartitionColNames()) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }

            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equalsIgnoreCase(partitionCol)) {
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE) {
                        throw new AnalysisException("The partition column could not be aggregated column"
                                + " and unique table's partition column must be key column");
                    }
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    found = true;
                    firstPartitionColumn = columnDef;
                    break;
                }
            }

            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }

        // use buildSinglePartitionDesc to build singleRangePartitionDescs
        if (desc.getMultiRangePartitionDescs().size() != 0) {

            if (desc.getPartitionColNames().size() > 1) {
                throw new AnalysisException("Batch build partition only support single range column.");
            }

            for (MultiRangePartitionDesc multiRangePartitionDesc : desc.getMultiRangePartitionDescs()) {
                TimestampArithmeticExpr.TimeUnit timeUnit = TimestampArithmeticExpr.TimeUnit
                        .fromName(multiRangePartitionDesc.getTimeUnit());
                if (timeUnit == TimestampArithmeticExpr.TimeUnit.HOUR && firstPartitionColumn.getType() != Type.DATETIME) {
                    throw new AnalysisException("Batch build partition for hour interval only supports " +
                            "partition column as DATETIME type");
                }
                PartitionConvertContext context = new PartitionConvertContext();
                context.setAutoPartitionTable(desc.isAutoPartitionTable());
                if (desc.getPartitionType() != null) {
                    context.setFirstPartitionColumnType(desc.getPartitionType());
                } else {
                    context.setFirstPartitionColumnType(firstPartitionColumn.getType());
                }
                context.setProperties(otherProperties);

                desc.getSingleRangePartitionDescs().addAll(multiRangePartitionDesc.convertToSingle(context));
            }
        }

        Set<String> nameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        PartitionKeyDesc.PartitionRangeType partitionType = PartitionKeyDesc.PartitionRangeType.INVALID;
        for (SingleRangePartitionDesc singleDesc : desc.getSingleRangePartitionDescs()) {
            if (!nameSet.add(singleDesc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + singleDesc.getPartitionName());
            }
            // in create table stmt, we use given properties
            // copy one. because ProperAnalyzer will remove entry after analyze
            Map<String, String> givenProperties = null;
            if (otherProperties != null) {
                givenProperties = Maps.newHashMap(otherProperties);
            }
            // check partitionType
            if (partitionType == PartitionKeyDesc.PartitionRangeType.INVALID) {
                partitionType = singleDesc.getPartitionKeyDesc().getPartitionType();
            } else if (partitionType != singleDesc.getPartitionKeyDesc().getPartitionType()) {
                throw new AnalysisException("You can only use one of these methods to create partitions");
            }
            analyzeSingleRangePartitionDesc(singleDesc, desc.getPartitionColNames().size(), givenProperties);
        }
    }

    public static void analyzeExpressionPartitionDesc(ExpressionPartitionDesc desc, List<ColumnDef> columnDefs, Map<String,
            String> otherProperties) throws AnalysisException {
        boolean hasExprAnalyze = false;
        SlotRef slotRef;
        RangePartitionDesc rangePartitionDesc = desc.getRangePartitionDesc();
        Expr expr = desc.getExpr();

        if (rangePartitionDesc != null) {
            // for automatic partition table
            if (rangePartitionDesc.isAutoPartitionTable()) {
                rangePartitionDesc.setAutoPartitionTable(true);
                slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
                if (expr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                    List<String> autoPartitionSupportFunctions = Lists.newArrayList(FunctionSet.TIME_SLICE,
                            FunctionSet.DATE_TRUNC);
                    if (!autoPartitionSupportFunctions.contains(functionCallExpr.getFnName().getFunction())) {
                        throw new SemanticException("Only support date_trunc and time_slice as partition expression");
                    }
                }
            } else {
                // for partition by range expr table
                // The type of the partition field may be different from the type after the expression
                if (expr instanceof CastExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromCast(expr);
                    // Set partition type from cast target type
                    if (expr instanceof CastExpr) {
                        CastExpr castExpr = (CastExpr) expr;
                        Type partitionType = castExpr.getTargetTypeDef().getType();
                        desc.setPartitionType(partitionType);
                    }
                } else if (expr instanceof FunctionCallExpr) {
                    slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);

                    Optional<ColumnDef> columnDef = columnDefs.stream()
                            .filter(c -> c.getName().equals(slotRef.getColumnName())).findFirst();
                    Preconditions.checkState(columnDef.isPresent());
                    slotRef.setType(columnDef.get().getType());

                    String functionName = ((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase();
                    if (functionName.equals(FunctionSet.STR2DATE)) {
                        desc.setPartitionType(Type.DATE);
                        if (!PartitionFunctionChecker.checkStr2date(expr)) {
                            throw new SemanticException("partition function check fail, only supports the result " +
                                    "of the function str2date(VARCHAR str, VARCHAR format) as a strict DATE type");
                        }
                    }
                } else {
                    throw new AnalysisException("Unsupported expr:" + expr.toSql());
                }
            }
            rangePartitionDesc.setPartitionType(desc.getPartitionType());
            PartitionDescAnalyzer.analyze(rangePartitionDesc);
            analyzeRangePartitionDesc(rangePartitionDesc, columnDefs, otherProperties);
        } else {
            // for materialized view
            slotRef = AnalyzerUtils.getSlotRefFromFunctionCall(expr);
        }

        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                slotRef.setType(columnDef.getType());
                PartitionExprAnalyzer.analyzePartitionExpr(expr, slotRef);
                desc.setPartitionType(expr.getType());
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }

    public static void analyzeSingleItemListPartitionDesc(SingleItemListPartitionDesc desc, List<ColumnDef> columnDefList,
                                                          Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(desc.getPartitionName());
        analyzeSinglePartitionProperties(desc, tableProperties, null);

        if (columnDefList.size() != 1) {
            throw new AnalysisException("Partition column size should be one when use single list partition ");
        }

        desc.setColumnDefList(columnDefList);
    }

    public static void analyzeMultiItemListPartitionDesc(MultiItemListPartitionDesc desc, List<ColumnDef> columnDefList,
                                                         Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(desc.getPartitionName());
        analyzeMultiItemValues(desc, columnDefList.size());
        analyzeSinglePartitionProperties(desc, tableProperties, null);
        desc.setColumnDefList(columnDefList);
    }

    private static void analyzeMultiItemValues(MultiItemListPartitionDesc desc, int partitionColSize) throws AnalysisException {
        for (List<String> values : desc.getMultiValues()) {
            if (values.size() != partitionColSize) {
                throw new AnalysisException(
                        "(" + String.join(",", values) + ") size should be equal to partition column size ");
            }
        }
    }

    // Helper methods for ListPartitionDesc analysis

    private static List<ColumnDef> analyzeListPartitionColumns(ListPartitionDesc desc, List<ColumnDef> columnDefs)
            throws AnalysisException {
        if (desc.getPartitionColNames() == null || desc.getPartitionColNames().isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }
        List<ColumnDef> partitionColumns = new ArrayList<>(desc.getPartitionColNames().size());
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : desc.getPartitionColNames()) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }
            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()
                            || columnDef.getType().isDecimalOfAnyVersion()) {
                        throw new AnalysisException(String.format("Invalid partition column '%s': %s",
                                columnDef.getName(), "invalid data type " + columnDef.getType()));
                    }
                    if (!columnDef.isKey() && columnDef.getAggregateType() != AggregateType.NONE
                            && !columnDef.isGeneratedColumn()) {
                        throw new AnalysisException("The partition column could not be aggregated column"
                                + " and unique table's partition column must be key column");
                    }
                    found = true;
                    columnDef.setIsPartitionColumn(true);
                    partitionColumns.add(columnDef);
                    break;
                }
            }
            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }
        return partitionColumns;
    }

    private static void analyzeListPartitionExprs(ListPartitionDesc desc, List<ColumnDef> columnDefs) throws AnalysisException {
        if (desc.getPartitionExprs() == null) {
            return;
        }
        List<String> slotRefs = desc.getPartitionExprs().stream()
                .flatMap(e -> e.collectAllSlotRefs().stream())
                .map(SlotRef::getColumnName)
                .collect(Collectors.toList());
        
        // Check that partition expression contains at least one column reference
        if (slotRefs == null || slotRefs.isEmpty()) {
            throw new AnalysisException("The partition expr should base on key column");
        }
        
        for (ColumnDef columnDef : columnDefs) {
            if (slotRefs.contains(columnDef.getName()) && !columnDef.isKey()
                    && columnDef.getAggregateType() != AggregateType.NONE) {
                throw new AnalysisException("The partition expr should base on key column");
            }
        }
    }

    private static void analyzeSingleListPartitions(ListPartitionDesc desc, Map<String, String> tableProperties,
                                                    List<ColumnDef> columnDefList) throws AnalysisException {
        List<LiteralExpr> allLiteralExprValues = Lists.newArrayList();
        Set<String> singListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SingleItemListPartitionDesc singleDesc : desc.getSingleListPartitionDescs()) {
            if (!singListPartitionName.add(singleDesc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + singleDesc.getPartitionName());
            }
            PartitionDescAnalyzer.analyze(singleDesc);
            analyzeSingleItemListPartitionDesc(singleDesc, columnDefList, tableProperties);
            allLiteralExprValues.addAll(singleDesc.getLiteralExprValues());
        }
        analyzeDuplicateValues(allLiteralExprValues);
    }

    private static void analyzeMultiListPartitions(ListPartitionDesc desc, Map<String, String> tableProperties,
                                                   List<ColumnDef> columnDefList) throws AnalysisException {
        Set<String> multiListPartitionName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<List<LiteralExpr>> allMultiLiteralExprValues = Lists.newArrayList();
        for (MultiItemListPartitionDesc multiDesc : desc.getMultiListPartitionDescs()) {
            if (!multiListPartitionName.add(multiDesc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + multiDesc.getPartitionName());
            }
            PartitionDescAnalyzer.analyze(multiDesc);
            analyzeMultiItemListPartitionDesc(multiDesc, columnDefList, tableProperties);
            allMultiLiteralExprValues.addAll(multiDesc.getMultiLiteralExprValues());
        }
        analyzeDuplicateValues(desc.getPartitionColNames().size(), allMultiLiteralExprValues);
    }

    private static void postAnalyzeListPartitionColumns(ListPartitionDesc desc, List<ColumnDef> columnDefs)
            throws AnalysisException {
        // list partition values should not contain NULL partition value if this column is not nullable.
        int partitionColSize = columnDefs.size();
        for (int i = 0; i < columnDefs.size(); i++) {
            ColumnDef columnDef = columnDefs.get(i);
            if (columnDef.isAllowNull()) {
                continue;
            }
            String partitionCol = columnDef.getName();
            for (SingleItemListPartitionDesc singleDesc : desc.getSingleListPartitionDescs()) {
                for (LiteralExpr literalExpr : singleDesc.getLiteralExprValues()) {
                    if (literalExpr.isNullable()) {
                        throw new AnalysisException("Partition column[" + partitionCol + "] could not be null but " +
                                "contains null value in partition[" + singleDesc.getPartitionName() + "]");
                    }
                }
            }
            for (MultiItemListPartitionDesc multiDesc : desc.getMultiListPartitionDescs()) {
                for (List<LiteralExpr> literalExprs : multiDesc.getMultiLiteralExprValues()) {
                    if (literalExprs.size() != partitionColSize) {
                        throw new AnalysisException("Partition column[" + partitionCol + "] size should be equal to " +
                                "partition column size but contains " + literalExprs.size() + " values in partition[" +
                                multiDesc.getPartitionName() + "]");
                    }
                    if (literalExprs.get(i).isNullable()) {
                        throw new AnalysisException("Partition column[" + partitionCol + "] could not be null but " +
                                "contains null value in partition[" + multiDesc.getPartitionName() + "]");
                    }
                }
            }
        }
    }

    private static void analyzeDuplicateValues(int partitionColSize, List<List<LiteralExpr>> allMultiLiteralExprValues)
            throws AnalysisException {
        for (int i = 0; i < allMultiLiteralExprValues.size(); i++) {
            List<LiteralExpr> literalExprValues1 = allMultiLiteralExprValues.get(i);
            for (int j = i + 1; j < allMultiLiteralExprValues.size(); j++) {
                List<LiteralExpr> literalExprValues2 = allMultiLiteralExprValues.get(j);
                int duplicatedSize = 0;
                for (int k = 0; k < literalExprValues1.size(); k++) {
                    String value = literalExprValues1.get(k).getStringValue();
                    String tmpValue = literalExprValues2.get(k).getStringValue();
                    if (value.equals(tmpValue)) {
                        duplicatedSize++;
                    }
                }
                if (duplicatedSize == partitionColSize) {
                    List<String> msg = literalExprValues1.stream()
                            .map(value -> ("\"" + value.getStringValue() + "\""))
                            .collect(Collectors.toList());
                    throw new AnalysisException("Duplicate values " +
                            "(" + String.join(",", msg) + ") not allow");
                }
            }
        }
    }

    private static void analyzeDuplicateValues(List<LiteralExpr> allLiteralExprValues) throws AnalysisException {
        Set<String> hashSet = new HashSet<>();
        for (LiteralExpr value : allLiteralExprValues) {
            if (!hashSet.add(value.getStringValue())) {
                throw new AnalysisException("Duplicated value " + value.getStringValue());
            }
        }
    }

    /**
     * Analyze SinglePartitionDesc properties - moved from SinglePartitionDesc.analyzeProperties
     */
    public static void analyzeSinglePartitionProperties(SinglePartitionDesc desc, Map<String, String> tableProperties,
                                                        PartitionKeyDesc partitionKeyDesc) throws AnalysisException {
        Map<String, String> partitionAndTableProperties = Maps.newHashMap();
        // The priority of the partition attribute is higher than that of the table
        if (tableProperties != null) {
            partitionAndTableProperties.putAll(tableProperties);
        }
        if (desc.getProperties() != null) {
            partitionAndTableProperties.putAll(desc.getProperties());
        }

        // analyze data property
        DataProperty partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(partitionAndTableProperties,
                DataProperty.getInferredDefaultDataProperty(), false);
        Preconditions.checkNotNull(partitionDataProperty);

        if (tableProperties != null && partitionKeyDesc != null
                && tableProperties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = tableProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (Strings.isNotBlank(storageCoolDownTTL)) {
                PeriodDuration periodDuration = TimeUtils.parseHumanReadablePeriodOrDuration(storageCoolDownTTL);
                if (partitionKeyDesc.isMax()) {
                    partitionDataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
                } else {
                    String stringUpperValue = partitionKeyDesc.getUpperValues().get(0).getStringValue();
                    DateTimeFormatter dateTimeFormatter = DateUtils.probeFormat(stringUpperValue);
                    LocalDateTime upperTime = DateUtils.parseStringWithDefaultHSM(stringUpperValue, dateTimeFormatter);
                    LocalDateTime updatedUpperTime = upperTime.plus(periodDuration);
                    DateLiteral dateLiteral = new DateLiteral(updatedUpperTime, Type.DATETIME);
                    long coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
                    partitionDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeStamp);
                }
            }
        }

        // analyze replication num
        Short replicationNum = PropertyAnalyzer
                .analyzeReplicationNum(partitionAndTableProperties, RunMode.defaultReplicationNum());
        if (replicationNum == null) {
            throw new AnalysisException("Invalid replication number: " + replicationNum);
        }

        // analyze version info
        Long versionInfo = PropertyAnalyzer.analyzeVersionInfo(partitionAndTableProperties);

        // analyze in memory
        boolean isInMemory = PropertyAnalyzer
                .analyzeBooleanProp(partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        TTabletType tabletType = PropertyAnalyzer.analyzeTabletType(partitionAndTableProperties);

        DataCacheInfo dataCacheInfo = PropertyAnalyzer.analyzeDataCacheInfo(partitionAndTableProperties);

        // Set the analyzed properties back to the desc
        desc.setPartitionDataProperty(partitionDataProperty);
        desc.setReplicationNum(replicationNum);
        desc.setVersionInfo(versionInfo);
        desc.setInMemory(isInMemory);
        desc.setTabletType(tabletType);
        desc.setDataCacheInfo(dataCacheInfo);

        if (desc.getProperties() != null) {
            // check unknown properties
            Sets.SetView<String> intersection =
                    Sets.intersection(partitionAndTableProperties.keySet(), desc.getProperties().keySet());
            if (!intersection.isEmpty()) {
                Map<String, String> unknownProperties = Maps.newHashMap();
                intersection.stream().forEach(x -> unknownProperties.put(x, desc.getProperties().get(x)));
                throw new AnalysisException("Unknown properties: " + unknownProperties);
            }
        }
    }
}