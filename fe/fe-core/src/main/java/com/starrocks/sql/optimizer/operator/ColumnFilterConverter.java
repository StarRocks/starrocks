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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.spm.SPMFunctions;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.starrocks.sql.common.SyncPartitionUtils.getLowerDateTime;
import static com.starrocks.sql.common.SyncPartitionUtils.nextUpperDateTime;
import static com.starrocks.sql.common.TimeUnitUtils.TIME_MAP;

/**
 * Convert column predicate to partition column filter
 */
public class ColumnFilterConverter {
    private static final Logger LOG = LogManager.getLogger(ColumnFilterConverter.class);

    private static final ColumnFilterVisitor COLUMN_FILTER_VISITOR = new ColumnFilterVisitor();

    // replaces a field in an expression with a constant
    private static class ExprRewriter implements AstVisitorExtendInterface<Boolean, Void> {

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
            String functionName = node.getFunctionName();
            if (FunctionSet.SUBSTRING.equalsIgnoreCase(functionName) ||
                    FunctionSet.SUBSTR.equalsIgnoreCase(functionName)) {
                Expr firstExpr = node.getChild(0);
                if (firstExpr instanceof SlotRef slotRef) {
                    if (columnRef.getName().equals(slotRef.getColumnName())) {
                        node.setChild(0, new StringLiteral(constant.getVarchar()));
                        return true;
                    }
                }
            } else if (FunctionSet.STR2DATE.equalsIgnoreCase(functionName)
                    || FunctionSet.STR_TO_DATE.equalsIgnoreCase(functionName)) {
                // str2date/str_to_date(partition_col, format) -> str2date/str_to_date(constant, format)
                // so that the partition expression can be evaluated to a date literal.
                Expr firstExpr = node.getChild(0);
                if (firstExpr instanceof SlotRef slotRef) {
                    if (columnRef.getName().equals(slotRef.getColumnName())) {
                        node.setChild(0, new StringLiteral(constant.getVarchar()));
                        return true;
                    }
                }
            } else if (FunctionSet.FROM_UNIXTIME.equalsIgnoreCase(functionName) ||
                    FunctionSet.FROM_UNIXTIME_MS.equalsIgnoreCase(functionName)) {
                Expr firstExpr = node.getChild(0);
                if (firstExpr instanceof SlotRef slotRef) {
                    if (columnRef.getName().equals(slotRef.getColumnName())) {
                        // FROM_UNIXTIME supports INT and BIGINT, FROM_UNIXTIME_MS supports only BIGINT
                        long value;
                        if (constant.getType().isInt()) {
                            value = constant.getInt();
                        } else if (constant.getType().isBigint()) {
                            value = constant.getBigint();
                        } else {
                            value = constant.getBigint();
                        }
                        node.setChild(0, new IntLiteral(value, constant.getType()));
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
        if (predicate == null) {
            return;
        }
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

    public static Optional<List<Expr>> getPartitionExprs(Table table) {
        if (!(table instanceof OlapTable)) {
            return Optional.empty();
        }
        PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
        if (partitionInfo instanceof ExpressionRangePartitionInfoV2) {
            return Optional.ofNullable(
                    ((ExpressionRangePartitionInfoV2) partitionInfo).getPartitionExprs(table.getIdToColumn()));
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            return Optional.ofNullable(
                    ((ExpressionRangePartitionInfo) partitionInfo).getPartitionExprs(table.getIdToColumn()));
        } else {
            return Optional.empty();
        }
    }
    public static void convertColumnFilter(ScalarOperator predicate, Map<String, PartitionColumnFilter> result,
                                           Table table) {
        // convert bool_col predicate to bool_col = true
        if (predicate instanceof ColumnRefOperator) {
            predicate = new BinaryPredicateOperator(BinaryType.EQ, predicate, ConstantOperator.TRUE);
        }

        if (CollectionUtils.isEmpty(predicate.getChildren())) {
            return;
        }

        Optional<List<Expr>> optPartitionExprs = getPartitionExprs(table);
        // A mapped filter can only be used to prune. Whether it may also license dropping the
        // predicate depends on the expression plateauing at this constant, so decide that here and
        // mark the filter once it is built.
        boolean plateaus = false;
        if (optPartitionExprs.isPresent()) {
            ScalarOperator mapped = convertPredicate(predicate, optPartitionExprs.get());
            if (mapped != predicate && optPartitionExprs.get().size() == 1
                    && predicate.getChild(0) instanceof ColumnRefOperator columnRef
                    && predicate.getChild(1) instanceof ConstantOperator) {
                plateaus = plateausAt(optPartitionExprs.get().get(0), columnRef,
                        (ConstantOperator) predicate.getChild(1));
            }
            predicate = mapped;
        }

        if (!checkColumnRefCanPartition(predicate.getChild(0), table)) {
            return;
        }

        if (predicate.getChildren().stream().skip(1).anyMatch(SPMFunctions::isSPMFunctions)) {
            if (predicate.getChildren().stream().skip(1).noneMatch(SPMFunctions::canRevert2ScalarOperator)) {
                return;
            }
            ScalarOperator clone = predicate.clone();
            List<ScalarOperator> newChildren = Lists.newArrayList();
            for (ScalarOperator child : clone.getChildren()) {
                if (!SPMFunctions.isSPMFunctions(child)) {
                    newChildren.add(child);
                } else {
                    newChildren.addAll(SPMFunctions.revertSPMFunctions(child));
                }
            }
            clone.getChildren().clear();
            clone.getChildren().addAll(newChildren);
            predicate = clone;
        }

        if (predicate.getChildren().stream().skip(1).anyMatch(d -> !OperatorType.CONSTANT.equals(d.getOpType()))) {
            return;
        }

        // Fast path: build range for date_trunc(...) on expression-partitioned tables; if handled, return.
        if (predicate instanceof BinaryPredicateOperator
                && predicate.getChild(0) instanceof CallOperator
                && buildDateTruncRange((BinaryPredicateOperator) predicate, result, table)) {
            return;
        }

        predicate.accept(COLUMN_FILTER_VISITOR, result);

        if (plateaus) {
            List<ColumnRefOperator> refs = Utils.extractColumnRef(predicate.getChild(0));
            if (!refs.isEmpty()) {
                PartitionColumnFilter filter = result.get(refs.get(0).getName());
                if (filter != null) {
                    filter.setMappedThroughPartitionExpr();
                }
            }
        }
    }

    // Build a PartitionColumnFilter range for date_trunc predicates. Returns true if handled.
    private static boolean buildDateTruncRange(BinaryPredicateOperator predicate,
                                               Map<String, PartitionColumnFilter> result,
                                               Table table) {
        if (!(table instanceof OlapTable)) {
            return false;
        }
        if (!(predicate.getChild(0) instanceof CallOperator call) ||
                !(predicate.getChild(1) instanceof ConstantOperator rhsConst)) {
            return false;
        }
        if (!FunctionSet.DATE_TRUNC.equals(call.getFnName())) {
            return false;
        }
        PartitionInfo pinfo = ((OlapTable) table).getPartitionInfo();
        if (!(pinfo instanceof ExpressionRangePartitionInfo)) {
            return false;
        }
        ExpressionRangePartitionInfo exprInfo = (ExpressionRangePartitionInfo) pinfo;
        if (!checkPartitionExprsContainsOperator(exprInfo.getPartitionExprs(table.getIdToColumn()), call)) {
            return false;
        }

        try {
            // Partition column and RHS constant as literal
            ColumnRefOperator columnRef = Utils.extractColumnRef(predicate.getChild(0)).get(0);
            LiteralExpr rhsLiteral = convertLiteral(columnRef.getType(), rhsConst);
            if (!(rhsLiteral instanceof DateLiteral) || !(call.getChild(0) instanceof ConstantOperator)) {
                return false;
            }
            // Time unit and normalized endpoints
            String granularity = ((ConstantOperator) call.getChild(0)).getVarchar().toLowerCase();
            LocalDateTime rhsDateTime = ((DateLiteral) rhsLiteral).toLocalDateTime();
            LocalDateTime periodStart = getLowerDateTime(rhsDateTime, granularity);
            LocalDateTime nextPeriodStart = nextUpperDateTime(periodStart, granularity);

            DateLiteral startLit = new DateLiteral(periodStart, rhsLiteral.getType());
            DateLiteral nextStartLit = new DateLiteral(nextPeriodStart, rhsLiteral.getType());

            PartitionColumnFilter filter = result.getOrDefault(columnRef.getName(), new PartitionColumnFilter());
            boolean isAligned = rhsDateTime.equals(periodStart);
            switch (predicate.getBinaryType()) {
                case EQ: {
                    // If RHS constant is not aligned to the granularity, equality can never be true.
                    // Build an empty interval: [L, L)
                    if (!isAligned) {
                        filter.setLowerBound(startLit, true);
                        filter.setUpperBound(startLit, false);
                    } else {
                        filter.setLowerBound(startLit, true);
                        filter.setUpperBound(nextStartLit, false);
                    }
                    break;
                }
                case GE:
                    // If not aligned, minimal satisfying dt starts from next period start [U, +inf)
                    filter.setLowerBound(isAligned ? startLit : nextStartLit, true);
                    break;
                case GT:
                    filter.setLowerBound(nextStartLit, true);
                    break;
                case LE:
                    filter.setUpperBound(nextStartLit, false);
                    break;
                case LT:
                    // If not aligned, T(dt) < C allows dt < U; if aligned, dt < L
                    filter.setUpperBound(isAligned ? startLit : nextStartLit, false);
                    break;
                default:
                    return false;
            }
            filter.setFromFunctionCall();
            result.put(columnRef.getName(), filter);
            return true;
        } catch (Exception e) {
            LOG.warn("build date_trunc column filter failed", e);
            return false;
        }
    }

    public static ScalarOperator convertPredicate(ScalarOperator predicate,
                                                  ExpressionRangePartitionInfoV2 exprRangePartitionInfo,
                                                  Map<ColumnId, Column> idToColumn) {
        if (exprRangePartitionInfo.getPartitionExprsSize() != 1) {
            return predicate;
        }
        Expr firstPartitionExpr = exprRangePartitionInfo.getPartitionExprs(idToColumn).get(0);
        return convertPredicate(predicate, List.of(firstPartitionExpr));
    }
    // Replace the predicate of the query with the predicate of the partition expression and evaluate.
    // If the condition is not met, there will be no change to the predicate.
    public static ScalarOperator convertPredicate(ScalarOperator predicate, List<Expr> partitionExprs) {
        if (partitionExprs.size() != 1) {
            return predicate;
        }
        // Currently only one partition column is supported
        Expr firstPartitionExpr = partitionExprs.get(0);

        // only support binary predicate
        if (predicate instanceof BinaryPredicateOperator
                && predicate.getChild(0) instanceof ColumnRefOperator
                && predicate.getChild(1) instanceof ConstantOperator) {
            List<ScalarOperator> argument = predicate.getChildren();
            ColumnRefOperator columnRef = (ColumnRefOperator) argument.get(0);
            ConstantOperator constant = (ConstantOperator) argument.get(1);
            // The rewrite keeps the comparison operator and only maps the constant through the
            // partition expression, so it holds only where that expression preserves order. For a
            // non-monotonic one -- substr(), a varchar-to-integer cast -- a row whose source value
            // satisfies the original predicate can carry a partition value that does not satisfy the
            // rewritten one, and its partition is pruned away, so the row goes silently missing.
            // Equality is exempt: a = c implies f(a) = f(c) for any f. This is the same condition
            // ListPartitionPruner.deduceExtraConjuncts applies to the LIST-partition rewrite.
            BinaryType binaryType = ((BinaryPredicateOperator) predicate).getBinaryType();
            // Equality is exempt: a = c implies f(a) = f(c) whatever the clock did.
            if (!binaryType.isEqual() && constantInsideClockRollback(firstPartitionExpr, constant, binaryType)) {
                return predicate;
            }
            Optional<ConstantOperator> mapped =
                    evaluatePartitionExpr(firstPartitionExpr, columnRef, constant, !binaryType.isEqual());
            if (mapped.isEmpty()) {
                return predicate;
            }
            BinaryPredicateOperator rewritten = (BinaryPredicateOperator) predicate.clone();
            rewritten.setBinaryType(relaxStrictComparison(binaryType, firstPartitionExpr, columnRef, constant,
                    mapped.get()));
            rewritten.setChild(1, mapped.get());
            return rewritten;
        }
        return predicate;
    }

    /**
     * Substitutes the constant into the partition expression and folds it, giving the partition value
     * the row carrying that source value lands on. Empty when the expression cannot take the constant,
     * is not foldable on the FE, or -- when requireMonotonic is set -- does not preserve the order.
     * <p>
     * The monotonicity check looks at the whole rewritten expression, not the CallOperator
     * getCallOperator() digs out of it: that helper strips the enclosing cast, and a cast is exactly
     * what can break the order -- cast(bill as bigint) maps '99845' to 99845, which sorts the other
     * way round.
     */
    private static Optional<ConstantOperator> evaluatePartitionExpr(Expr partitionExpr, ColumnRefOperator columnRef,
                                                                    ConstantOperator constant,
                                                                    boolean requireMonotonic) {
        Expr predicateExpr = partitionExpr.clone();
        if (!rewritePredicate(predicateExpr, columnRef, constant)) {
            return Optional.empty();
        }
        ScalarOperator translate = SqlToScalarOperatorTranslator.translate(predicateExpr);
        CallOperator callOperator = AnalyzerUtils.getCallOperator(translate);
        if (callOperator == null) {
            return Optional.empty();
        }
        if (requireMonotonic && !OperatorFunctionChecker.onlyContainIncreasingFunctions(translate).first) {
            return Optional.empty();
        }
        ScalarOperator evaluation = ScalarOperatorEvaluator.INSTANCE.evaluation(callOperator);
        if (!(evaluation instanceof ConstantOperator result)) {
            return Optional.empty();
        }
        return result.castTo(predicateExpr.getType());
    }

    /**
     * Monotonic is not the same as strictly increasing, and only a strictly increasing partition
     * expression carries a strict comparison. from_unixtime_ms() divides the milliseconds by 1000, so
     * a thousand source values collapse onto one partition value: for `dt < 1609689600500` the rows at
     * 1609689600000 satisfy the predicate yet land on the very partition value the constant maps to,
     * and mapping "<" onto "<" prunes their partition away and loses them. Widening the comparison to
     * "<=" keeps that partition.
     * <p>
     * Only "<" is widened. ">" does not need it: the partition holding f(c) spans [lo, hi) with
     * f(c) &lt; hi, so it always meets (f(c), +inf) and survives a strict lower bound anyway -- it
     * could only be pruned if a partition were as fine-grained as f's own step, which the DATETIME
     * partition column AstBuilder generates never is. Widening it would instead turn the lower bound
     * inclusive, and OptOlapPartitionPruner.prunePartitionPredicates then reads the kept partitions as
     * implying the predicate and drops it -- a partition that merely starts at f(c) also holds rows
     * below c. (That elimination is already unsound for ">=" and "not (&lt; )" on an expression
     * partition, which this change does not address; it just avoids walking into it.)
     * <p>
     * Widening unconditionally -- what ListPartitionPruner.deduceExtraConjuncts does for LIST
     * partitions -- would cost the strictly increasing cases a partition they can legitimately prune,
     * so ask instead whether this particular constant sits inside a plateau: step one value down (for
     * "<") or up (for ">") and see whether the partition value moves. from_unixtime() on seconds never
     * plateaus and keeps its strict bound; from_unixtime_ms() on a whole second does not either, which
     * is why an aligned millisecond constant still prunes as tightly as before.
     * <p>
     * A constant with no nameable neighbour leaves the plateau question unanswered and the comparison
     * is left alone. That is the varchar case, and str2date() does plateau there -- it strips leading
     * and trailing blanks, and a blank sorts below every digit, so " 20210104" is a smaller string
     * landing on the same day as "20210104", and the strict bound prunes the partition those rows live
     * on. Widening it anyway is not the fix: str2date() partitions on a DATE column, where the last
     * selected partition's range ends exactly on the widened bound, and
     * OptOlapPartitionPruner.prunePartitionPredicates then reads the partitions as implying the
     * predicate and drops it -- turning missing rows into extra ones. The from_unixtime() family is
     * unaffected: AstBuilder casts it to DATETIME, whose finer range end never meets the bound.
     * Widening a varchar bound needs that interaction handled first, so it is left as is here.
     * <p>
     * substr() takes a varchar too but is not monotonic, so only its equality rewrite survives the
     * check above, and equality needs no plateau.
     */
    private static BinaryType relaxStrictComparison(BinaryType binaryType, Expr partitionExpr,
                                                    ColumnRefOperator columnRef, ConstantOperator constant,
                                                    ConstantOperator mapped) {
        if (binaryType != BinaryType.LT) {
            return binaryType;
        }
        Optional<ConstantOperator> neighbour = adjacentConstant(constant, -1)
                .flatMap(n -> evaluatePartitionExpr(partitionExpr, columnRef, n, false));
        if (neighbour.isEmpty() || !neighbour.get().equals(mapped)) {
            return binaryType;
        }
        return BinaryType.LE;
    }

    /**
     * The from_unixtime() family named in OperatorFunctionChecker's order-preserving cast rule, or
     * null for anything else. These are the only partition expressions here whose result depends on
     * the session time zone.
     */
    private static FunctionCallExpr unwrapPartitionCall(Expr partitionExpr) {
        Expr expr = partitionExpr;
        while (expr instanceof CastExpr) {
            expr = expr.getChild(0);
        }
        if (!(expr instanceof FunctionCallExpr call)) {
            return null;
        }
        String name = call.getFunctionName().toLowerCase();
        return FunctionSet.FROM_UNIXTIME.equals(name) || FunctionSet.FROM_UNIXTIME_MS.equals(name) ? call : null;
    }

    /**
     * The zone the partition expression renders in: the one written into a three-argument
     * from_unixtime(), otherwise the session's. Empty when a zone is named but cannot be resolved --
     * a non-literal argument, or a name this JVM does not know -- in which case nothing can be
     * concluded about the ordering and the caller declines the rewrite.
     */
    private static Optional<ZoneId> partitionExprZone(FunctionCallExpr call) {
        if (call.getChildren().size() >= 3) {
            if (!(call.getChild(2) instanceof StringLiteral zoneLiteral)) {
                return Optional.empty();
            }
            try {
                return Optional.of(TimeUtils.getOrSystemTimeZone(zoneLiteral.getStringValue()).toZoneId());
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        return Optional.of(TimeUtils.getTimeZone().toZoneId());
    }

    /**
     * Whether the constant sits inside a clock rollback, where from_unixtime() stops preserving the
     * epoch order. Across a rollback of D at instant T, an increasing epoch renders a DECREASING local
     * datetime -- in America/New_York epoch 1636264799 renders as 2021-11-07 01:59:59 and 1636264800,
     * one second later, as 2021-11-07 01:00:00.
     * <p>
     * The unsafe window sits on opposite sides of T for the two bound directions, because each needs a
     * row on the other side of the constant to render the wrong way round:
     * <ul>
     * <li>An upper bound (&lt;, &lt;=) is unsafe for c in [T, T + D). Rows just before T carry the
     * pre-rollback high, and c only climbs back past that high once D has elapsed; until then c maps
     * BELOW them and the rewrite prunes the partition holding them.</li>
     * <li>A lower bound (&gt;, &gt;=) is unsafe for c in [T - D, T). There c still carries the
     * pre-rollback high while rows at or after T render below it, so mapping the bound onto f(c)
     * prunes the partition those later rows live on.</li>
     * </ul>
     * Outside its window the rendering is strictly increasing and the mapping stands, which is why
     * this asks about the constant rather than rejecting every zone that observes DST -- such a zone
     * is only unsafe for an hour a year, and a zone that has stopped observing it (Asia/Shanghai since
     * 1991) is still unsafe for data from back when it did.
     */
    private static boolean constantInsideClockRollback(Expr partitionExpr, ConstantOperator constant,
                                                      BinaryType binaryType) {
        FunctionCallExpr call = unwrapPartitionCall(partitionExpr);
        if (call == null || !constant.getType().isFixedPointType() || constant.isNull()) {
            return false;
        }
        Optional<ZoneId> zone = partitionExprZone(call);
        if (zone.isEmpty()) {
            return true;
        }
        long epochSeconds;
        try {
            long raw = constant.getType().isLargeIntType() ? constant.getLargeInt().longValueExact()
                    : constant.getBigint();
            epochSeconds = FunctionSet.FROM_UNIXTIME_MS.equals(call.getFunctionName().toLowerCase())
                    ? Math.floorDiv(raw, 1000L) : raw;
        } catch (ArithmeticException | ClassCastException e) {
            return true;
        }
        ZoneRules rules = zone.get().getRules();
        boolean upperBound = binaryType == BinaryType.LT || binaryType == BinaryType.LE;
        try {
            if (upperBound) {
                // previousTransition() is strictly before the instant given, so ask from one second
                // later -- otherwise a constant sitting exactly on the transition skips its own
                // rollback.
                return insideRollback(rules.previousTransition(Instant.ofEpochSecond(epochSeconds).plusSeconds(1)),
                        epochSeconds, true);
            }
            // A lower bound looks the other way: the rollback that can strand it is the next one.
            if (insideRollback(rules.nextTransition(Instant.ofEpochSecond(epochSeconds)), epochSeconds, false)) {
                return true;
            }
            // NOT_EQUAL reaches here too and belongs to neither direction, so it is held to both.
            return binaryType != BinaryType.GT && binaryType != BinaryType.GE
                    && insideRollback(rules.previousTransition(Instant.ofEpochSecond(epochSeconds).plusSeconds(1)),
                    epochSeconds, true);
        } catch (DateTimeException | ArithmeticException e) {
            return true;
        }
    }

    private static boolean insideRollback(ZoneOffsetTransition transition, long epochSeconds, boolean after) {
        if (transition == null) {
            return false;
        }
        long rollback = transition.getOffsetBefore().getTotalSeconds() - transition.getOffsetAfter().getTotalSeconds();
        if (rollback <= 0) {
            return false;
        }
        long instant = transition.getInstant().getEpochSecond();
        // [T, T + D) looking back at T, [T - D, T) looking forward to it
        return after ? epochSeconds - instant < rollback : instant - epochSeconds <= rollback;
    }

    /**
     * Whether the partition expression plateaus at this constant -- some neighbouring source value
     * maps to the same partition value. That is exactly when `f(a) OP f(c)` stops implying `a OP c`,
     * so a filter built here cannot answer "do the kept partitions imply the predicate".
     * <p>
     * False when the question cannot be answered (a non-integer constant, or an expression that does
     * not fold): the filter then keeps whatever standing it had, rather than losing an elimination
     * that has always applied.
     */
    private static boolean plateausAt(Expr partitionExpr, ColumnRefOperator columnRef, ConstantOperator constant) {
        Optional<ConstantOperator> here = evaluatePartitionExpr(partitionExpr, columnRef, constant, false);
        if (here.isEmpty()) {
            return false;
        }
        for (long delta : new long[] {-1, 1}) {
            Optional<ConstantOperator> neighbour = adjacentConstant(constant, delta)
                    .flatMap(n -> evaluatePartitionExpr(partitionExpr, columnRef, n, false));
            if (neighbour.isPresent() && neighbour.get().equals(here.get())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The integer one step from the constant, or empty when there is none to name -- a non-integer
     * constant, or one already at the end of its type's range.
     */
    private static Optional<ConstantOperator> adjacentConstant(ConstantOperator constant, long delta) {
        if (constant.isNull()) {
            return Optional.empty();
        }
        Type type = constant.getType();
        try {
            if (type.isTinyint()) {
                long value = Math.addExact(constant.getTinyInt(), delta);
                return value < Byte.MIN_VALUE || value > Byte.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createTinyInt((byte) value));
            } else if (type.isSmallint()) {
                long value = Math.addExact(constant.getSmallint(), delta);
                return value < Short.MIN_VALUE || value > Short.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createSmallInt((short) value));
            } else if (type.isInt()) {
                long value = Math.addExact(constant.getInt(), delta);
                return value < Integer.MIN_VALUE || value > Integer.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createInt((int) value));
            } else if (type.isBigint()) {
                return Optional.of(ConstantOperator.createBigint(Math.addExact(constant.getBigint(), delta)));
            } else if (type.isLargeIntType()) {
                return Optional.of(
                        ConstantOperator.createLargeInt(constant.getLargeInt().add(BigInteger.valueOf(delta))));
            }
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
        return Optional.empty();
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
                return type == columnType || (type != IntegerType.LARGEINT && columnType != IntegerType.LARGEINT);
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
            return checkPartitionExprsContainsOperator(expressionRangePartitionInfo.getPartitionExprs(table.getIdToColumn()),
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
        String fnName = functionCallExpr.getFunctionName();
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
            if (BinaryType.NE == predicate.getBinaryType()
                    || BinaryType.EQ_FOR_NULL == predicate.getBinaryType()) {
                return predicate;
            }

            ColumnRefOperator column = Utils.extractColumnRef(predicate.getChild(0)).get(0);
            ConstantOperator child = (ConstantOperator) predicate.getChild(1);
            PartitionColumnFilter filter = context.getOrDefault(column.getName(), new PartitionColumnFilter());
            if (!predicate.getChild(0).isColumnRef()) {
                filter.setFromFunctionCall();
            }

            try {
                switch (predicate.getBinaryType()) {
                    case EQ:
                        filter.setLowerBound(convertLiteral(column.getType(), child), true);
                        filter.setUpperBound(convertLiteral(column.getType(), child), true);
                        break;
                    case LE:
                        filter.setUpperBound(convertLiteral(column.getType(), child), true);
                        filter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        filter.setUpperBound(convertLiteral(column.getType(), child), false);
                        filter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        filter.setLowerBound(convertLiteral(column.getType(), child), true);
                        break;
                    case GT:
                        filter.setLowerBound(convertLiteral(column.getType(), child), false);
                        break;
                    default:
                        break;
                }

                context.put(column.getName(), filter);
            } catch (SemanticException e) {
                LOG.warn("build column filter failed.", e);
            } catch (AnalysisException e) {
                LOG.warn("build column filter failed.", e);
            }
            return predicate;
        }

        @Override
        public ScalarOperator visitLargeInPredicate(LargeInPredicateOperator predicate,
                                                    Map<String, PartitionColumnFilter> context) {
            throw new UnsupportedOperationException("not support large in predicate in the ColumnFilterConverter");
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
                    list.add(convertLiteral(column.getType(), (ConstantOperator) predicate.getChild(i)));
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
        return convertLiteral(operator.getType(), operator);
    }

    public static LiteralExpr convertLiteral(Type definedType, ConstantOperator operator) throws AnalysisException {
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
            case DECIMAL256:
                literalExpr = new DecimalLiteral(operator.getDecimal());
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
                boolean isConvertToDate = PartitionUtil.isConvertToDate(definedType, operator.getType());
                literalExpr = new StringLiteral(operator.getVarchar());
                if (isConvertToDate) {
                    literalExpr = PartitionUtil.convertToDateLiteral(literalExpr);
                }
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
