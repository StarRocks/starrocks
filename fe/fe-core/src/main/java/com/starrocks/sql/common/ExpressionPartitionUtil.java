// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Process lower bound and upper bound for Expression Partition,
 * only support SlotRef and FunctionCallExpr
 */
public class ExpressionPartitionUtil {

    public static final Map<String, GetBoundFunction> FN_NAME_TO_GET_BOUND;

    @FunctionalInterface
    public interface GetBoundFunction {
        LiteralExpr process(FunctionCallExpr expr, LiteralExpr literalExpr, PrimitiveType type, int step);
    }

    static {
        FN_NAME_TO_GET_BOUND = Maps.newHashMap();
        FN_NAME_TO_GET_BOUND.put("date_trunc", ExpressionPartitionUtil::dateTruncGetBound);
    }

    public static Range<PartitionKey> getPartitionKeyRange(
            Expr expr, Column partitionColumn,
            Collection<Range<PartitionKey>> existPartitionKeyRanges,
            Range<PartitionKey> basePartitionRange,
            int basePartitionRangeIndex) {
        LiteralExpr baseLowerLiteralExpr = basePartitionRange.lowerEndpoint().getKeys().get(basePartitionRangeIndex);
        LiteralExpr baseUpperLiteralExpr = basePartitionRange.upperEndpoint().getKeys().get(basePartitionRangeIndex);
        if (expr instanceof SlotRef) {
            return generateRange(existPartitionKeyRanges, partitionColumn, baseLowerLiteralExpr, baseUpperLiteralExpr);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;

            LiteralExpr lowerLiteralExpr = ExpressionPartitionUtil.getBound(
                    functionCallExpr, baseLowerLiteralExpr, partitionColumn.getPrimitiveType(), 0);
            LiteralExpr upperLiteralExpr = ExpressionPartitionUtil.getBound(
                    functionCallExpr, baseUpperLiteralExpr, partitionColumn.getPrimitiveType(), 0);
            if (ExpressionPartitionUtil.compareBound(baseUpperLiteralExpr, upperLiteralExpr) > 0) {
                // increase upperBound
                upperLiteralExpr = ExpressionPartitionUtil.getBound(
                        functionCallExpr, upperLiteralExpr, partitionColumn.getPrimitiveType(), 1);
            }
            return generateRange(existPartitionKeyRanges, partitionColumn, lowerLiteralExpr, upperLiteralExpr);
        } else {
            throw new SemanticException("Do not support expr:" + expr.toSql());
        }
    }

    private static Range<PartitionKey> generateRange(Collection<Range<PartitionKey>> existPartitionKeyRanges,
                                              Column partitionColumn, LiteralExpr lowerLiteralExpr,
                                              LiteralExpr upperLiteralExpr) {
        try {
            // compare with existPartitionRange
            for (Range<PartitionKey> existPartitionKeyRange : existPartitionKeyRanges) {
                PartitionKey existLowerPartitionKey = existPartitionKeyRange.lowerEndpoint();
                PartitionKey existUpperPartitionKey = existPartitionKeyRange.upperEndpoint();
                Preconditions.checkState(existLowerPartitionKey.getKeys().size() == 1);
                Preconditions.checkState(existUpperPartitionKey.getKeys().size() == 1);
                LiteralExpr existLowerLiteralExpr = existLowerPartitionKey.getKeys().get(0);
                LiteralExpr existUpperLiteralExpr = existUpperPartitionKey.getKeys().get(0);
                if (existLowerLiteralExpr.compareLiteral(lowerLiteralExpr) <= 0 &&
                        existUpperLiteralExpr.compareLiteral(upperLiteralExpr) >= 0) {
                    // range full coverage
                    return null;
                } else {
                    if (existLowerLiteralExpr.compareTo(lowerLiteralExpr) <= 0) {
                        lowerLiteralExpr = existUpperLiteralExpr;
                    } else {
                        upperLiteralExpr = existLowerLiteralExpr;
                    }
                    if (lowerLiteralExpr.compareLiteral(upperLiteralExpr) >= 0) {
                        // no span
                        return null;
                    }
                }
            }
            PartitionKey lowerPartitionKey = PartitionKey.createPartitionKey(
                    Collections.singletonList(new PartitionValue(lowerLiteralExpr.getStringValue())),
                    Collections.singletonList(partitionColumn));
            PartitionKey upperPartitionKey = PartitionKey.createPartitionKey(
                    Collections.singletonList(new PartitionValue(upperLiteralExpr.getStringValue())),
                    Collections.singletonList(partitionColumn));
            return Range.closedOpen(lowerPartitionKey, upperPartitionKey);
        } catch (AnalysisException e) {
            throw new SemanticException("Create Partition Range failed:" + e.getMessage());
        }
    }

    public static LiteralExpr getBound(FunctionCallExpr functionCallExpr, LiteralExpr bound, PrimitiveType type, int step) {
        GetBoundFunction processPartitionFunction =
                FN_NAME_TO_GET_BOUND.get(functionCallExpr.getFnName().getFunction());
        if (processPartitionFunction == null) {
            throw new SemanticException("Do not support function:" + functionCallExpr.toSql());
        }
        return processPartitionFunction.process(functionCallExpr, bound, type, step);
    }

    public static int compareBound(LiteralExpr literalExpr1, LiteralExpr literalExpr2) {
        return literalExpr1.compareTo(literalExpr2);
    }

    private static LiteralExpr dateTruncGetBound(FunctionCallExpr expr, LiteralExpr boundLiteralExpr,
                                                 PrimitiveType type, int step) {
        String fmtStr = ((StringLiteral) expr.getChild(0)).getValue();
        DateLiteral dateLiteral = (DateLiteral) boundLiteralExpr;
        // if dateLiteral is min value ,return it.
        if (dateLiteral.isMinValue() && step <= 0) {
            return dateLiteral;
        }
        LocalDateTime localDateTime = dateLiteral.toLocalDateTime();
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
        LocalDateTime datetime = ScalarOperatorFunctions.dateTrunc(fmt, date).getDatetime();
        DateLiteral processedDateLiteral;
        try {
            if (type == PrimitiveType.DATE) {
                processedDateLiteral = new DateLiteral(datetime, Type.DATE);
            } else {
                processedDateLiteral = new DateLiteral(datetime, Type.DATETIME);
            }
            return processedDateLiteral;
        } catch (AnalysisException e) {
            throw new SemanticException("Convert to DateLiteral failed:" + e);
        }
    }

    public static String getFormattedPartitionName(String lowerBound, String upperBound, PrimitiveType type) {
        if (type.isDateType()) {
            lowerBound = lowerBound.replace("-", "")
                    .replace(":", "")
                    .replace(" ", "");
            upperBound = upperBound.replace("-", "")
                    .replace(":", "")
                    .replace(" ", "");
        }
        return lowerBound + "_" + upperBound;
    }
}
