// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.common.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Process lower border and upper border for Expression Partition,
 * only support SlotRef and FunctionCallExpr
 */
public class ExpressionPartitionUtil {

    public static final Map<String, GetBorderFunction> FN_NAME_TO_GET_BORDER;

    public static final Map<String, CompareBorderFunction> FN_NAME_TO_COMPARE_BORDER;

    @FunctionalInterface
    public interface GetBorderFunction {
        String process(FunctionCallExpr expr, String border, PrimitiveType type, int step);
    }

    @FunctionalInterface
    public interface CompareBorderFunction {
        int process(String border1, String border2, PrimitiveType type);
    }

    static {
        FN_NAME_TO_GET_BORDER = Maps.newHashMap();
        FN_NAME_TO_GET_BORDER.put("date_trunc", ExpressionPartitionUtil::dateTruncGetBorder);

        FN_NAME_TO_COMPARE_BORDER = Maps.newHashMap();
        FN_NAME_TO_COMPARE_BORDER.put("date_trunc", ExpressionPartitionUtil::dateTruncCompareBorder);

    }

    public static Range<PartitionKey> getPartitionKeyRange(
            Expr expr, Column partitionColumn,
            Collection<Range<PartitionKey>> existPartitionKeyRanges,
            Range<PartitionKey> basePartitionRange,
            int basePartitionRangeIndex) {
        String basePartitionLowerValue = basePartitionRange.lowerEndpoint()
                .getKeys().get(basePartitionRangeIndex).getStringValue();
        String basePartitionUpperValue = basePartitionRange.upperEndpoint()
                .getKeys().get(basePartitionRangeIndex).getStringValue();
        if (expr instanceof SlotRef) {
            try {
                PartitionKey lowerBound = PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue(basePartitionLowerValue)),
                        Collections.singletonList(partitionColumn));
                PartitionKey upperBound = PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue(basePartitionUpperValue)),
                        Collections.singletonList(partitionColumn));
                return Range.closedOpen(lowerBound, upperBound);
            } catch (AnalysisException e) {
                throw new SemanticException("Create Partition Range failed:" + e.getMessage());
            }
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            String lowerBorder = ExpressionPartitionUtil.getBorder(
                    functionCallExpr, basePartitionLowerValue, partitionColumn.getPrimitiveType(), 0);
            String upperBorder = ExpressionPartitionUtil.getBorder(
                    functionCallExpr, basePartitionUpperValue, partitionColumn.getPrimitiveType(), 0);
            if (ExpressionPartitionUtil.compareBorder(
                    (functionCallExpr), basePartitionUpperValue, upperBorder, partitionColumn.getPrimitiveType()) > 0) {
                // increase upperBorder
                upperBorder = ExpressionPartitionUtil.getBorder(
                        functionCallExpr, basePartitionUpperValue, partitionColumn.getPrimitiveType(), 1);
            }
            try {
                // compare with existPartitionRange
                for (Range<PartitionKey> existPartitionKeyRange : existPartitionKeyRanges) {
                    PartitionKey lowerPartitionKey = PartitionKey.createPartitionKey(
                            Collections.singletonList(new PartitionValue(lowerBorder)),
                            Collections.singletonList(partitionColumn));
                    PartitionKey upperPartitionKey = PartitionKey.createPartitionKey(
                            Collections.singletonList(new PartitionValue(upperBorder)),
                            Collections.singletonList(partitionColumn));
                    Range<PartitionKey> partitionKeyRange = Range.closedOpen(lowerPartitionKey, upperPartitionKey);
                    try {
                        RangeUtils.checkRangeIntersect(partitionKeyRange, existPartitionKeyRange);
                    } catch (DdlException e) {
                        // todo existPartitionKeyRange.contains e.g. range is [2014-01-01 2015-01-01), 2015-01-01 not in it. can't use here
                        PartitionKey existLowerPartitionKey = existPartitionKeyRange.lowerEndpoint();
                        PartitionKey existUpperPartitionKey = existPartitionKeyRange.upperEndpoint();
                        if (existLowerPartitionKey.compareTo(lowerPartitionKey) <= 0 &&
                                existUpperPartitionKey.compareTo(upperPartitionKey) >= 0) {
                            // range full coverage
                            return null;
                        } else if (existLowerPartitionKey.compareTo(lowerPartitionKey) <= 0) {
                            lowerBorder = ExpressionPartitionUtil.getBorder(
                                    functionCallExpr, lowerBorder, partitionColumn.getPrimitiveType(), 1);
                            if (lowerBorder.equals(upperBorder)) {
                                return null;
                            }
                        } else if (existUpperPartitionKey.compareTo(upperPartitionKey) >= 0) {
                            upperBorder = ExpressionPartitionUtil.getBorder(
                                    functionCallExpr, upperBorder, partitionColumn.getPrimitiveType(), -1);
                            if (upperBorder.equals(lowerBorder)) {
                                return null;
                            }
                        }
                    }
                }
                PartitionKey lowerPartitionKey = PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue(lowerBorder)),
                        Collections.singletonList(partitionColumn));
                PartitionKey upperPartitionKey = PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue(upperBorder)),
                        Collections.singletonList(partitionColumn));
                return Range.closedOpen(lowerPartitionKey, upperPartitionKey);
            } catch (AnalysisException e) {
                throw new SemanticException("Create Partition Range failed:" + e.getMessage());
            }
        } else {
            throw new SemanticException("Do not support expr:" + expr.toSql());
        }
    }

    public static String getBorder(FunctionCallExpr functionCallExpr, String border, PrimitiveType type, int step) {
        GetBorderFunction processPartitionFunction =
                FN_NAME_TO_GET_BORDER.get(functionCallExpr.getFnName().getFunction());
        if (processPartitionFunction == null) {
            throw new SemanticException("Do not support function:" + functionCallExpr.toSql());
        }
        return processPartitionFunction.process(functionCallExpr, border, type, step);
    }

    public static int compareBorder(FunctionCallExpr functionCallExpr, String border1, String border2,
                                    PrimitiveType type) {
        CompareBorderFunction compareBorderFunction =
                FN_NAME_TO_COMPARE_BORDER.get(functionCallExpr.getFnName().getFunction());
        if (compareBorderFunction == null) {
            throw new SemanticException("Do not support function:" + functionCallExpr.toSql());
        }
        return compareBorderFunction.process(border1, border2, type);
    }

    private static String dateTruncGetBorder(FunctionCallExpr expr, String border, PrimitiveType type, int step) {
        String fmtStr = ((StringLiteral) expr.getChild(0)).getValue();
        DateTimeFormatter formatter;
        LocalDateTime localDateTime;
        if (type == PrimitiveType.DATE) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            localDateTime = LocalDate.parse(border, formatter).atTime(0, 0, 0, 0);
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            localDateTime = LocalDateTime.parse(border, formatter);
        }
        switch (fmtStr) {
            case "second":
                localDateTime = localDateTime.plusSeconds(step);
                break;
            case "minute":
                localDateTime = localDateTime.plusMinutes(step);
                break;
            case "hour":
                localDateTime = localDateTime.plusHours(step);
                break;
            case "day":
                localDateTime = localDateTime.plusDays(step);
                break;
            case "month":
                localDateTime = localDateTime.plusMonths(step);
                break;
            case "year":
                localDateTime = localDateTime.plusYears(step);
                break;
            case "week":
                localDateTime = localDateTime.plusWeeks(step);
                break;
            case "quarter":
                localDateTime = localDateTime.plusMonths(3 * step);
                int year = localDateTime.getYear();
                int month = localDateTime.getMonthValue();
                int quarterMonth = (month - 1) / 3 * 3 + 1;
                localDateTime = LocalDateTime.of(year, quarterMonth, 1, 0, 0);
                break;
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + fmtStr);
        }
        ConstantOperator fmt = ConstantOperator.createVarchar(fmtStr);
        ConstantOperator date = ConstantOperator.createDatetime(localDateTime);
        return ScalarOperatorFunctions.dateTrunc(fmt, date).getDatetime().format(formatter);
    }

    private static int dateTruncCompareBorder(String border1, String border2, PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate localDate1 = LocalDate.parse(border1, formatter);
            LocalDate localDate2 = LocalDate.parse(border2, formatter);
            return localDate1.compareTo(localDate2);
        } else {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime1 = LocalDateTime.parse(border1, formatter);
            LocalDateTime localDateTime2 = LocalDateTime.parse(border2, formatter);
            return localDateTime1.compareTo(localDateTime2);
        }
    }
}
