// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Convert column predicate to partition column filter
 */
public class ColumnFilterConverter {
    private static final Logger LOG = LogManager.getLogger(ColumnFilterConverter.class);

    private static final ColumnFilterVisitor COLUMN_FILTER_VISITOR = new ColumnFilterVisitor();

    // "week" can not exist in timeMap due "month" not sure contains week
    private static final ImmutableMap<String, Integer> TIME_MAP =
            new ImmutableMap.Builder<String, Integer>()
                    .put("second", 1)
                    .put("minute", 2)
                    .put("hour", 3)
                    .put("day", 4)
                    .put("month", 5)
                    .put("quarter", 6)
                    .put("year", 7)
                    .build();

    public static Map<String, PartitionColumnFilter> convertColumnFilter(List<ScalarOperator> predicates) {
        return convertColumnFilter(predicates, null);
    }

    public static Map<String, PartitionColumnFilter> convertColumnFilter(List<ScalarOperator> predicates, Table table) {
        Map<String, PartitionColumnFilter> result = Maps.newHashMap();
        for (ScalarOperator op : predicates) {
            convertColumnFilter(op, result, table);
        }

        return result;
    }

    public static void convertColumnFilter(ScalarOperator predicate, Map<String, PartitionColumnFilter> result,
                                           Table table) {
        if (predicate.getChildren().size() <= 0) {
            return;
        }

        if (!checkColumnRefCanPartition(predicate.getChild(0), table)) {
            return;
        }

        if (predicate.getChildren().stream().skip(1).anyMatch(d -> !OperatorType.CONSTANT.equals(d.getOpType()))) {
            return;
        }

        predicate.accept(COLUMN_FILTER_VISITOR, result);
    }

    private static boolean checkColumnRefCanPartition(ScalarOperator right, Table table) {
        if (OperatorType.VARIABLE.equals(right.getOpType())) {
            return true;
        }

        if (right instanceof CastOperator && OperatorType.VARIABLE.equals(right.getChild(0).getOpType())) {
            Type type = right.getType();
            Type columnType = right.getChild(0).getType();

            if (type.isFixedPointType() && columnType.isFixedPointType()) {
                // LargeIntLiteral getHashValue method is different with IntLiteral
                return type == columnType || (type != Type.LARGEINT && columnType != Type.LARGEINT);
            }

            return type.equals(columnType);
        }

        if (right instanceof CallOperator) {
            if (!(table instanceof OlapTable)) {
                return false;
            }
            PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
            if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
                return false;
            }
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            return checkPartitionExprsContainsOperator(expressionRangePartitionInfo.getPartitionExprs(),
                    (CallOperator) right);
        }

        return false;
    }

    private static boolean checkPartitionExprsContainsOperator(List<Expr> exprList,
                                                               CallOperator callOperator) {
        // now expr can only support date_trunc,exprList.size() != 1 will remove in the future
        if (CollectionUtils.isEmpty(exprList) || exprList.size() != 1) {
            return false;
        }
        Expr expr = exprList.get(0);
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        return checkPartitionExprEqualsOperator(functionCallExpr, callOperator);
    }

    private static boolean checkPartitionExprEqualsOperator(FunctionCallExpr functionCallExpr,
                                                            CallOperator callOperator) {
        String fnName = functionCallExpr.getFnName().getFunction();
        if (!Objects.equals(fnName, callOperator.getFnName())) {
            return false;
        }
        if (Objects.equals(fnName, FunctionSet.DATE_TRUNC)) {
            return checkDateTruncEquals(functionCallExpr, callOperator);
        }

        return false;
    }

    private static boolean checkDateTruncEquals(FunctionCallExpr functionCallExpr, CallOperator callOperator) {
        if (callOperator.getChildren().size() != 2 || functionCallExpr.getChildren().size() != 2) {
            return false;
        }
        if (!(functionCallExpr.getChild(0) instanceof StringLiteral &&
                functionCallExpr.getChild(1) instanceof SlotRef)) {
            return false;
        }
        if (!(callOperator.getChild(0) instanceof ConstantOperator &&
                callOperator.getChild(1) instanceof ColumnRefOperator)) {
            return false;
        }

        String exprTimeArg = ((StringLiteral) (functionCallExpr.getChild(0))).getStringValue();
        String callTimeArg = callOperator.getChild(0).toString();
        String exprColumnNameArg = ((SlotRef) (functionCallExpr.getChild(1))).getColumnName();
        String callColumnNameArg = ((ColumnRefOperator) (callOperator.getChild(1))).getName();

        return Objects.equals(exprColumnNameArg, callColumnNameArg) &&
                (Objects.equals(exprTimeArg, callTimeArg) ||
                        (TIME_MAP.containsKey(exprTimeArg) && TIME_MAP.containsKey(callTimeArg) &&
                                TIME_MAP.get(exprTimeArg) > TIME_MAP.get(callTimeArg)));
    }

    private static class ColumnFilterVisitor
            extends ScalarOperatorVisitor<ScalarOperator, Map<String, PartitionColumnFilter>> {
        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Map<String, PartitionColumnFilter> context) {
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                                   Map<String, PartitionColumnFilter> context) {
            if (BinaryPredicateOperator.BinaryType.NE == predicate.getBinaryType()
                    || BinaryPredicateOperator.BinaryType.EQ_FOR_NULL == predicate.getBinaryType()) {
                return predicate;
            }

            ColumnRefOperator column = Utils.extractColumnRef(predicate.getChild(0)).get(0);
            ConstantOperator child = (ConstantOperator) predicate.getChild(1);

            PartitionColumnFilter filter = context.getOrDefault(column.getName(), new PartitionColumnFilter());
            try {
                switch (predicate.getBinaryType()) {
                    case EQ:
                        filter.setLowerBound(convertLiteral(child), true);
                        filter.setUpperBound(convertLiteral(child), true);
                        break;
                    case LE:
                        filter.setUpperBound(convertLiteral(child), true);
                        filter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        filter.setUpperBound(convertLiteral(child), false);
                        filter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        filter.setLowerBound(convertLiteral(child), true);
                        break;
                    case GT:
                        filter.setLowerBound(convertLiteral(child), false);
                        break;
                    default:
                        break;
                }

                context.put(column.getName(), filter);
            } catch (AnalysisException e) {
                LOG.warn("build column filter failed.", e);
            }
            return predicate;
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate,
                                               Map<String, PartitionColumnFilter> context) {
            if (predicate.isNotIn()) {
                return predicate;
            }

            ColumnRefOperator column = Utils.extractColumnRef(predicate.getChild(0)).get(0);
            List<LiteralExpr> list = Lists.newArrayList();
            try {
                for (int i = 1; i < predicate.getChildren().size(); i++) {
                    list.add(convertLiteral((ConstantOperator) predicate.getChild(i)));
                }

                PartitionColumnFilter filter = context.getOrDefault(column.getName(), new PartitionColumnFilter());
                if (null != filter.getInPredicateLiterals()) {
                    filter.getInPredicateLiterals().addAll(list);
                } else {
                    filter.setInPredicateLiterals(list);
                }
                context.put(column.getName(), filter);
            } catch (AnalysisException e) {
                LOG.warn("build column filter failed.", e);
            }

            return predicate;
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate,
                                                   Map<String, PartitionColumnFilter> context) {
            if (predicate.isNotNull()) {
                return predicate;
            }

            // Consider that case "fn(x) is null", we can not deduce that bound is [NULL, NULL]
            // It's not safe because some values of x can be converted to null and some can not be
            // It's only safe when we are sure that iff. x is null -> fn(x) is null.
            // The simplest way to fix it is only apply this rule when "x is null"
            ScalarOperator root = predicate.getChild(0);
            if (!OperatorType.VARIABLE.equals(root.getOpType())) {
                return predicate;
            }

            ColumnRefOperator column = (ColumnRefOperator) root;

            PartitionColumnFilter filter = new PartitionColumnFilter();
            NullLiteral nullLiteral = new NullLiteral();
            filter.setLowerBound(nullLiteral, true);
            filter.setUpperBound(nullLiteral, true);
            context.put(column.getName(), filter);

            return predicate;
        }
    }

    public static LiteralExpr convertLiteral(ConstantOperator operator) throws AnalysisException {
        Preconditions.checkArgument(!operator.getType().isInvalid());

        if (operator.isNull()) {
            return new NullLiteral();
        }

        LiteralExpr literalExpr;
        switch (operator.getType().getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(operator.getBoolean());
                break;
            case TINYINT:
                literalExpr = new IntLiteral(operator.getTinyInt(), operator.getType());
                break;
            case SMALLINT:
                literalExpr = new IntLiteral(operator.getSmallint(), operator.getType());
                break;
            case INT:
                literalExpr = new IntLiteral(operator.getInt(), operator.getType());
                break;
            case BIGINT:
                literalExpr = new IntLiteral(operator.getBigint(), operator.getType());
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral(operator.getLargeInt().toString());
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = new FloatLiteral(operator.getDouble());
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                literalExpr = new DecimalLiteral(operator.getDecimal());
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
                literalExpr = new StringLiteral(operator.getVarchar());
                break;
            case DATE:
                LocalDateTime date = operator.getDate();
                literalExpr = new DateLiteral(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
                break;
            case DATETIME:
                LocalDateTime datetime = operator.getDate();
                literalExpr = new DateLiteral(datetime.getYear(), datetime.getMonthValue(), datetime.getDayOfMonth(),
                        datetime.getHour(), datetime.getMinute(), datetime.getSecond());
                break;
            default:
                throw new AnalysisException("Type[" + operator.getType().toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

}
