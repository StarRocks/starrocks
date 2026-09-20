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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Shared admission checks and helpers for the JDBC push-down rules (aggregate fold / join merge /
 * project push-down). These live in the optimizer layer and decide/assemble what each scan
 * contributes to the remote pushdown SQL.
 */
public class JDBCPushDownRuleUtils {
    private JDBCPushDownRuleUtils() {
    }

    // Read the constrained SR numeric values before evaluating expressions. In particular, a
    // remote aggregate/project could hide an invalid source value or bypass SR decimal rounding.
    public static boolean requiresStrictNumericRead(LogicalJDBCScanOperator scan) {
        JDBCTable table = (JDBCTable) scan.getTable();
        return scan.getColRefToColumnMetaMap().values().stream()
                .anyMatch(column -> table.isUnboundedNumericColumn(column.getName()));
    }

    /** Alias prefix for aggregate-pushdown derived columns; the full alias is {@code jdbc_agg_<refId>}. */
    public static final String JDBC_AGG_ALIAS_PREFIX = "jdbc_agg_";
    /** Alias prefix for projection-pushdown derived columns; the full alias is {@code jdbc_proj_<refId>}. */
    public static final String JDBC_PROJECT_ALIAS_PREFIX = "jdbc_proj_";

    /**
     * Snapshot onto {@code derivedTable} the estimate {@code replacedSubtree} carried, so the
     * derived table the pushdown puts in its place can report that estimate instead of the
     * per-table statistics it can no longer be looked up by.
     *
     * <p>This is the JDBC counterpart of Trino's {@code Rules.deriveTableStatisticsForPushdown}:
     * a pushed-down subtree stops being describable by the source's catalog (no table in the
     * source has the merged join's row count, or the aggregate's group count), so the estimate is
     * computed once here, while the subtree still exists, and travels with the derived table.
     *
     * <p>No column renaming is involved. {@link Statistics} is keyed by {@link ColumnRefOperator},
     * and every pushdown rule reuses the replaced subtree's own column references as the derived
     * scan's output references -- the {@code sr_c<id>} / {@code jdbc_agg_<id>} aliasing happens in
     * the remote SQL text and in the {@link Column} metadata alone. Keying the snapshot by name
     * would be the bug, not the fix.
     *
     * <p>Cost: the caller must invoke this only on the path that actually rewrites the tree. The
     * JDBC pushdown rules run in an iterative rewrite, before the optimizer's own statistics pass,
     * so snapshotting from {@code check()} or from a transform that declines to rewrite would put
     * a full subtree estimate on every visit. Predicate-column collection is suppressed the way
     * {@code JoinReorderDP} suppresses it for its speculative estimates: these statistics exist to
     * describe a subtree that is about to disappear, and must not be recorded as query history.
     *
     * @return the statistics stored, or null when the estimate could not be computed
     */
    public static Statistics snapshotPushDownStatistics(OptExpression replacedSubtree,
                                                        JDBCTable derivedTable,
                                                        OptimizerContext context) {
        if (replacedSubtree == null || derivedTable == null || context == null) {
            return null;
        }
        try (var ignore = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
            deriveMissingLogicalProperty(replacedSubtree);
            Utils.calculateStatistics(replacedSubtree, context);
        } catch (Exception e) {
            // Utils.calculateStatistics already swallows estimation failures; this only guards the
            // scope bookkeeping. A derived table without a snapshot reports no statistics, which is
            // exactly the behaviour that existed before snapshots.
            return null;
        }
        Statistics statistics = replacedSubtree.getStatistics();
        if (statistics == null) {
            return null;
        }
        statistics = Statistics.buildFrom(statistics)
                .setStatsSource(leafStatsSource(replacedSubtree))
                .build();
        derivedTable.setPushDownStatistics(statistics);
        return statistics;
    }

    /**
     * Give every node of a throw-away subtree a logical property, bottom-up, so it can be
     * estimated. A rule that rebuilds the subtree it is replacing (the join merge does) creates
     * operator nodes the framework has not seen, and the join estimator reads its children's
     * output columns off their logical property: without this, estimating anything deeper than a
     * single join level throws, the estimate is silently dropped, and the derived table ends up
     * with no statistics — which is exactly the failure this whole path exists to prevent. Nodes
     * that came from the real tree already have a property and are left alone.
     */
    private static void deriveMissingLogicalProperty(OptExpression expr) {
        for (OptExpression child : expr.getInputs()) {
            deriveMissingLogicalProperty(child);
        }
        if (expr.getLogicalProperty() == null) {
            expr.deriveLogicalPropertyItself();
        }
    }

    /**
     * The statistics source to report for a derived estimate: the weakest one among the leaves it
     * was computed from. Intermediate operators build their statistics with a fresh builder, whose
     * source is always NONE, so the subtree root cannot answer this. Reporting the weakest leaf is
     * what makes {@code EXPLAIN COSTS} truthful on a merged scan -- TABLE_METADATA only when every
     * table underneath really had statistics in the source, NONE as soon as one did not.
     */
    private static Statistics.StatsSource leafStatsSource(OptExpression expr) {
        if (expr.getInputs().isEmpty()) {
            Statistics statistics = expr.getStatistics();
            return statistics == null ? Statistics.StatsSource.NONE : statistics.getStatsSource();
        }
        Statistics.StatsSource weakest = Statistics.StatsSource.ANALYZE;
        for (OptExpression child : expr.getInputs()) {
            Statistics.StatsSource childSource = leafStatsSource(child);
            if (childSource.ordinal() < weakest.ordinal()) {
                weakest = childSource;
            }
        }
        return weakest;
    }

    /**
     * Assemble the output columns a pushed-down JDBC scan exposes, filling the caller-provided
     * collections (kept in lock-step by index, so they suit both the scan operator's colRef maps and
     * the SQL builder's selectItems/selectAliases). Identity passthrough columns reuse their original
     * scan column name; every derived/aggregate column is aliased {@code <derivedAliasPrefix><refId>}
     * (the ColumnRefOperator id is globally unique, so derived aliases never collide with each other).
     * Two passes -- reserve all identity names first, then allocate derived aliases (with a numeric
     * suffix fallback should a remote column happen to be named like a generated alias) -- so the
     * outcome is independent of map iteration order.
     */
    public static void buildPushDownColumns(LogicalJDBCScanOperator scan,
                                            Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr,
                                            String derivedAliasPrefix,
                                            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                            List<Column> outputColumns,
                                            List<ScalarOperator> selectItems,
                                            List<String> selectAliases) {
        Set<String> takenAliases = Sets.newHashSet();

        // Pass 1: reserve every identity passthrough name so a generated alias never collides with one.
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : outputColumnRefToExpr.entrySet()) {
            if (isIdentityColumn(entry.getKey(), entry.getValue(), scan)) {
                String name = scan.getColRefToColumnMetaMap().get(entry.getKey()).getName();
                takenAliases.add(name.toLowerCase(Locale.ROOT));
            }
        }

        // Pass 2: assemble outputs in map order.
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : outputColumnRefToExpr.entrySet()) {
            ColumnRefOperator outputColumnRef = entry.getKey();
            ScalarOperator outputExpr = entry.getValue();
            String alias = isIdentityColumn(outputColumnRef, outputExpr, scan)
                    ? scan.getColRefToColumnMetaMap().get(outputColumnRef).getName()
                    : uniqueDerivedAlias(derivedAliasPrefix, outputColumnRef, takenAliases);
            Column outputColumn = createOutputColumn(outputColumnRef, outputExpr, scan, alias);
            colRefToColumnMetaMap.put(outputColumnRef, outputColumn);
            columnMetaToColRefMap.put(outputColumn, outputColumnRef);
            outputColumns.add(outputColumn);
            selectItems.add(outputExpr);
            selectAliases.add(outputColumn.getName());
            takenAliases.add(outputColumn.getName().toLowerCase(Locale.ROOT));
        }
    }

    private static boolean isIdentityColumn(ColumnRefOperator ref, ScalarOperator expr, LogicalJDBCScanOperator scan) {
        ColumnRefOperator passthrough = passthroughScanColumn(expr, scan);
        return passthrough != null && passthrough.equals(ref);
    }

    /**
     * The bare scan {@link ColumnRefOperator} an expression reduces to once implicit casts are stripped,
     * or null when it is not a passthrough of a scan column. The JDBC SQL renderer drops implicit casts
     * (see {@code ScalarOperatorToJDBCSQLVisitor#visitCastOperator}), so an implicit cast over a scan
     * column carries no remote-evaluable work and renders to that column unchanged. Both "does this
     * projection value need remote evaluation?" (the project rule's push-down gate) and "is this output an
     * identity passthrough that may reuse its scan column?" are decided through this single predicate, so
     * the two judgments can never silently diverge.
     */
    public static ColumnRefOperator passthroughScanColumn(ScalarOperator expr, LogicalJDBCScanOperator scan) {
        ScalarOperator unwrapped = expr;
        while (unwrapped instanceof CastOperator && ((CastOperator) unwrapped).isImplicit()) {
            unwrapped = unwrapped.getChild(0);
        }
        if (unwrapped instanceof ColumnRefOperator && scan.getColRefToColumnMetaMap().containsKey(unwrapped)) {
            return (ColumnRefOperator) unwrapped;
        }
        return null;
    }

    /**
     * {@code <prefix><refId>}; the refId is globally unique so derived columns never collide with one
     * another -- only the identity passthrough names reserved above need a fallback suffix.
     */
    private static String uniqueDerivedAlias(String prefix, ColumnRefOperator ref, Set<String> taken) {
        String base = prefix + ref.getId();
        String alias = base;
        int suffix = 0;
        while (taken.contains(alias.toLowerCase(Locale.ROOT))) {
            alias = base + "_" + suffix++;
        }
        return alias;
    }

    /**
     * True when {@code projection} carries no expression to evaluate downstream: it is either
     * absent (null) or a pure column-pruning identity that maps every output ref to itself, so the
     * scan's projection can be folded into pushdown SQL rather than blocking the merge/push-down.
     */
    public static boolean isColumnPruningOnly(Projection projection) {
        return projection == null || projection.getColumnRefMap().entrySet().stream()
                .allMatch(e -> e.getKey().equals(e.getValue()));
    }

    /**
     * Build the {@link Column} a pushed-down scan exposes for {@code outputColumnRef}. When the
     * output is the scan column itself (identity reference), the original scan column is reused;
     * otherwise a fresh column is synthesized under {@code columnName} — the caller picks the name
     * (e.g. an aggregate alias vs. the ref's own name), since that choice is rule-specific.
     */
    public static Column createOutputColumn(ColumnRefOperator outputColumnRef, ScalarOperator outputExpr,
                                            LogicalJDBCScanOperator scan, String columnName) {
        if (isIdentityColumn(outputColumnRef, outputExpr, scan)) {
            return scan.getColRefToColumnMetaMap().get(outputColumnRef);
        }
        JDBCTable.ProtocolType dialect = ((JDBCTable) scan.getTable()).getProtocolType();
        return new Column(columnName, jdbcReturnType(outputColumnRef.getType(), outputExpr, dialect),
                outputColumnRef.isNullable());
    }

    /**
     * Reconcile the StarRocks type of a remotely-evaluated (derived) push-down column with what the
     * external database's JDBC driver actually returns, since the BE JDBC type checker validates the
     * driver's Java class against this synthesized scan-schema type (see {@code type_checker_config.xml}).
     *
     * <p>Only ClickHouse needs the type widened here: it evaluates a remote expression to a wide UNSIGNED
     * value (e.g. {@code UInt32}, returned as {@code com.clickhouse.data.value.UnsignedInteger}) whose range
     * does not fit an equal-width signed slot, and the type checker deliberately refuses that narrowing
     * (UInt8/16/32 are not mapped into TINYINT/SMALLINT/INT), so widen a narrow ClickHouse scalar/constant
     * column to {@code BIGINT}.
     *
     * <p>Every other dialect -- and a literal constant on any dialect -- returns a signed
     * {@code java.lang.Long}, which the BE JDBC type checker narrows into a TINYINT/SMALLINT/INT slot via
     * its {@code Long} rules (read as bigint, cast down, erroring on overflow), so the StarRocks type is
     * left as-is. A narrow aggregate such as {@code max(int_col)} also keeps its type -- the driver returns
     * it narrow, fitting the narrow slot. Passthrough and wider/other types round-trip unchanged.
     */
    private static Type jdbcReturnType(Type type, ScalarOperator expr, JDBCTable.ProtocolType dialect) {
        if (!type.isScalarType()) {
            return type;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
                if (dialect == JDBCTable.ProtocolType.CLICKHOUSE && !isAggregate(expr)) {
                    return IntegerType.BIGINT;
                }
                return type;
            default:
                return type;
        }
    }

    private static boolean isAggregate(ScalarOperator expr) {
        return expr instanceof CallOperator && ((CallOperator) expr).isAggregate();
    }
}
