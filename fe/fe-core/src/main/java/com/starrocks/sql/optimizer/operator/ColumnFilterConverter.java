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


package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CastExpr;
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
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AstVisitor;
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
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.starrocks.sql.common.TimeUnitUtils.TIME_MAP;

/**
 * Convert column predicate to partition column filter
 */
public class ColumnFilterConverter {
    private static final Logger LOG = LogManager.getLogger(ColumnFilterConverter.class);

    private static final ColumnFilterVisitor COLUMN_FILTER_VISITOR = new ColumnFilterVisitor();

    // replaces a field in an expression with a constant
    private static class ExprRewriter extends AstVisitor<Boolean, Void> {

        private final ColumnRefOperator columnRef;
        private final ConstantOperator constant;

        public ExprRewriter(ColumnRefOperator columnRef, ConstantOperator constant) {
            this.columnRef = columnRef;
            this.constant = constant;
        }

        @Override
        public Boolean visitCastExpr(CastExpr node, Void context) {
            ArrayList<Expr> children = node.getChildren();
            boolean success = false;
            for (Expr child : children) {
                if (visit(child)) {
                    success = true;
                }
            }
            return success;
        }

        @Override
        public Boolean visitFunctionCall(FunctionCallExpr node, Void context) {
            if (FunctionSet.SUBSTRING.equalsIgnoreCase(node.getFnName().getFunction()) ||
                    FunctionSet.SUBSTR.equalsIgnoreCase(node.getFnName().getFunction())) {
                Expr firstExpr = node.getChild(0);
                if (firstExpr instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) node.getChild(0);
                    if (columnRef.getName().equals(slotRef.getColumnName())) {
                        node.setChild(0, new StringLiteral(constant.getVarchar()));
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public static boolean rewritePredicate(Expr expr, ColumnRefOperator columnRef, ConstantOperator constant) {
        Boolean success = new ExprRewriter(columnRef, constant).visit(expr);
        if (success == null) {
            return false;
        } else {
            return success;
        }
    }

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

    public static void convertColumnFilterWithoutExpr(ScalarOperator predicate, Map<String,
            PartitionColumnFilter> result, Table table) {
        if (predicate.getChildren().size() <= 0) {
            return;
        }

        if (!checkColumnRefCanPartition(predicate.getChild(0), table)) {
            return;
        }

        // rewrite invalid date cast expr to NullLiteral
        ScalarOperator rewritePredicate = rewriteInvalidDateCast(predicate);

        if (!rewritePredicate.isConstant()) {
            return;
        }

        rewritePredicate.accept(COLUMN_FILTER_VISITOR, result);
    }

    public static void convertColumnFilter(ScalarOperator predicate, Map<String, PartitionColumnFilter> result,
                                           Table table) {
        if (predicate.getChildren().size() <= 0) {
            return;
        }

        if (table != null && table.isExprPartitionTable()) {
            OlapTable olapTable = (OlapTable) table;
            predicate = convertPredicate(predicate, (ExpressionRangePartitionInfoV2) olapTable.getPartitionInfo());
        }

        if (!checkColumnRefCanPartition(predicate.getChild(0), table)) {
            return;
        }

        // rewrite invalid date cast expr to NullLiteral
        ScalarOperator rewritePredicate = rewriteInvalidDateCast(predicate);

        if (rewritePredicate.getChildren().stream().skip(1).anyMatch(d -> !OperatorType.CONSTANT.equals(d.getOpType()))) {
            return;
        }

        rewritePredicate.accept(COLUMN_FILTER_VISITOR, result);
    }

    // Replace the predicate of the query with the predicate of the partition expression and evaluate.
    // If the condition is not met, there will be no change to the predicate.
    public static ScalarOperator convertPredicate(ScalarOperator predicate,
                                                  ExpressionRangePartitionInfoV2 exprRangePartitionInfo) {
        // Currently only one partition column is supported
        if (exprRangePartitionInfo.getPartitionExprs().size() != 1) {
            return predicate;
        }
        Expr firstPartitionExpr = exprRangePartitionInfo.getPartitionExprs().get(0);
        Expr predicateExpr = firstPartitionExpr.clone();

        // only support binary predicate
        if (predicate instanceof BinaryPredicateOperator && predicate.getChildren().size() == 2) {
            List<ScalarOperator> argument = predicate.getChildren();
            ColumnRefOperator columnRef = (ColumnRefOperator) argument.get(0);
            ConstantOperator constant = (ConstantOperator) argument.get(1);
            boolean success = rewritePredicate(predicateExpr, columnRef, constant);
            if (!success) {
                return predicate;
            }
            ScalarOperator translate = SqlToScalarOperatorTranslator.translate(predicateExpr);
            CallOperator callOperator = AnalyzerUtils.getCallOperator(translate);
            if (callOperator == null) {
                return predicate;
            }
            ScalarOperator evaluation = ScalarOperatorEvaluator.INSTANCE.evaluation(callOperator);
            if (!(evaluation instanceof ConstantOperator)) {
                return predicate;
            }
            predicate = predicate.clone();
            ConstantOperator result = (ConstantOperator) evaluation;
            try {
                result = result.castTo(predicateExpr.getType());
            } catch (Exception e) {
                return predicate;
            }
            predicate.setChild(1, result);
        }
        return predicate;
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

    // only rewrite cast invalid date value to null like cast('abc' as date)
    private static ScalarOperator rewriteInvalidDateCast(ScalarOperator scalarOperator) {
        ScalarOperator copy = scalarOperator.clone();
        List<ScalarOperator> children = copy.getChildren();

        for (int i = 1; i < children.size(); i++) {
            ScalarOperator child = children.get(i);
            if (child instanceof CastOperator) {
                CastOperator cast = (CastOperator) child;
                Type toType = cast.getType();
                if (cast.getChildren().size() == 1
                        && cast.getChildren().get(0).isConstantRef()
                        && toType.isDateType()) {
                    ConstantOperator value = (ConstantOperator) cast.getChildren().get(0);
                    try {
                        value.castTo(toType);
                    } catch (Exception e) {
                        children.set(i, ConstantOperator.createNull(toType));
                    }
                }
            }
        }
        return copy;
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
            if (BinaryType.NE == predicate.getBinaryType()
                    || BinaryType.EQ_FOR_NULL == predicate.getBinaryType()) {
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
                        datetime.getHour(), datetime.getMinute(), datetime.getSecond(), datetime.getNano() / 1000);
                break;
            default:
                throw new AnalysisException("Type[" + operator.getType().toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

}
