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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

/**
 * Fold a projection sitting on a single JDBC scan into the scan's pushed-down SQL, so derived
 * expressions (e.g. {@code a + b}) are evaluated by the external database instead of locally.
 *
 * <p>Each projection value is gated by {@link CanPushDownPredicateVisitor} for the table dialect --
 * the same gate used by predicate and aggregate pushdown -- and predicate-valued SELECT items are
 * only pushed to dialects that support boolean results in the SELECT list. An expression the
 * {@link com.starrocks.sql.optimizer.rewrite.ScalarOperatorToJDBCSQLVisitor} cannot render (or that
 * the dialect evaluates with diverging semantics) leaves the scan untouched and the projection is
 * computed locally. A projection of bare column refs is left to ordinary column pruning.
 */
public class PushDownProjectToJDBCScanRule extends TransformationRule {

    public PushDownProjectToJDBCScanRule() {
        super(RuleType.TF_PUSH_DOWN_PROJECT_TO_JDBC_SCAN, Pattern.create(OperatorType.LOGICAL_JDBC_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableJdbcProjectPushDown()) {
            return false;
        }
        LogicalJDBCScanOperator scan = input.getOp().cast();
        // Cheap structural gate only; the full feasibility analysis runs once in transform().
        Projection projection = scan.getProjection();
        return projection != null && projection.getCommonSubOperatorMap().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJDBCScanOperator scan = input.getOp().cast();
        Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr = buildProjectPushDown(scan);
        if (outputColumnRefToExpr == null) {
            // An empty result means "not applied" -- returning the input would count as a change
            // and keep an iterative rewrite pass spinning.
            return Lists.newArrayList();
        }

        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = Maps.newLinkedHashMap();
        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = Maps.newLinkedHashMap();
        List<Column> outputColumns = Lists.newArrayList();
        List<ScalarOperator> selectItems = Lists.newArrayList();
        List<String> selectAliases = Lists.newArrayList();
        JDBCPushDownRuleUtils.buildPushDownColumns(scan, outputColumnRefToExpr,
                JDBCPushDownRuleUtils.JDBC_PROJECT_ALIAS_PREFIX,
                newColRefToColumnMetaMap, newColumnMetaToColRefMap, outputColumns, selectItems, selectAliases);

        JDBCTable jdbcTable = (JDBCTable) scan.getTable();
        String pushDownQuery = JDBCPushDownSQLBuilder.buildScalarSelectQuery(
                scan, selectItems, selectAliases, List.of(), List.of());
        JDBCTable queryTable = new JDBCTable(jdbcTable);
        queryTable.setNewFullSchema(outputColumns);
        queryTable.setPushDownQuery(pushDownQuery);

        LogicalJDBCScanOperator newScanOperator = new LogicalJDBCScanOperator.Builder()
                .withOperator(scan)
                .setTable(queryTable)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setPredicate(null)
                .setProjection(null)
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();
        return Lists.newArrayList(OptExpression.create(newScanOperator));
    }

    /**
     * The projection's colRef->expression map when it can be folded into the scan's pushed SQL, or
     * null otherwise. Foldable requires: no common sub-expressions, every value convertible to the
     * dialect (gated by {@link CanPushDownPredicateVisitor}), predicate-valued projections only for
     * dialects with boolean SELECT-list support, and at least one non-trivial expression -- a
     * projection of bare column refs is left to ordinary column pruning.
     */
    private Map<ColumnRefOperator, ScalarOperator> buildProjectPushDown(LogicalJDBCScanOperator scan) {
        Projection projection = scan.getProjection();
        if (projection == null || !projection.getCommonSubOperatorMap().isEmpty()) {
            return null;
        }
        JDBCTable.ProtocolType dialect = ((JDBCTable) scan.getTable()).getProtocolType();
        Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr = Maps.newLinkedHashMap();
        boolean hasProjectExpression = false;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
            ColumnRefOperator outputRef = entry.getKey();
            ScalarOperator outputExpr = entry.getValue();
            if (!CanPushDownPredicateVisitor.canPushDown(outputExpr, dialect)) {
                return null;
            }
            // Never push a boolean comparison as a SELECT item, even when nested under another pushable
            // expression (e.g. CAST(a > 1 AS CHAR), CONCAT(a > 1, 'x'), (a > 1) + 0). A comparison rarely
            // maps cleanly into a scalar SELECT item -- MySQL/MariaDB return a wide Long, ClickHouse an
            // UnsignedByte, and Oracle has no boolean scalar type, so a comparison rendered into the
            // SELECT list fails remotely there with ORA-00907. It is a rare pattern, so evaluate any
            // projection that contains a predicate anywhere locally rather than carrying per-dialect
            // type-reconciliation rules for it.
            if (containsPredicate(outputExpr)) {
                return null;
            }
            // The SQL renderer drops implicit casts (ScalarOperatorToJDBCSQLVisitor#visitCastOperator), so
            // an output expression whose top-level operator is an implicit cast reaches the remote WITHOUT
            // it: the driver returns the uncast operand's type while the synthesized column is declared as
            // the cast target. The BE JDBC type checker frequently cannot bridge that gap -- whether the
            // cast wraps a scan column (an INT column implicitly widened to BIGINT) or a derived expression
            // that set-operation coercion widened (a + 1 promoted to DOUBLE/DECIMAL, which MySQL returns as
            // a java.lang.Long that no DOUBLE/DECIMAL slot accepts). Keep any such projection local rather
            // than pushing a column whose remote type cannot match its declaration.
            if (outputExpr instanceof CastOperator && ((CastOperator) outputExpr).isImplicit()) {
                return null;
            }
            ColumnRefOperator passthrough = JDBCPushDownRuleUtils.passthroughScanColumn(outputExpr, scan);
            if (passthrough == null) {
                // A derived (remotely-evaluated) SELECT item. Oracle evaluates numeric literals/arithmetic
                // as NUMBER, returned as java.math.BigDecimal, which the BE JDBC type checker accepts only
                // into DECIMAL/VARCHAR/DOUBLE slots -- never an integer slot -- so do not push an
                // integer-typed derived item there.
                if (dialect == JDBCTable.ProtocolType.ORACLE && outputRef.getType().isFixedPointType()) {
                    return null;
                }
                // A DATE/DATETIME constant is rendered as a bare quoted string by the non-Oracle SQL
                // renderer, but the synthesized scan column keeps its temporal type, so the driver
                // returns a java.lang.String the BE JDBC type checker cannot map into the temporal slot
                // and the scan fails. Oracle wraps it as a typed DATE/TIMESTAMP literal, so only
                // non-Oracle dialects are affected; pushing a constant gains nothing, so evaluate
                // temporal constants locally.
                if (dialect != JDBCTable.ProtocolType.ORACLE
                        && outputExpr instanceof ConstantOperator && outputExpr.getType().isDateType()) {
                    return null;
                }
                // A boolean SELECT item is declared BOOLEAN, but MySQL/MariaDB return a boolean result
                // as a numeric java.lang.Long (ClickHouse a UnsignedByte) the BE JDBC type checker does
                // not map into a BOOLEAN slot, so the scan fails. PostgreSQL returns a native
                // java.lang.Boolean the slot accepts. Only a boolean constant reaches here (comparisons
                // are rejected above), and pushing a constant gains nothing, so keep boolean items local
                // on the dialects whose driver does not return a native boolean.
                if (dialect != JDBCTable.ProtocolType.POSTGRES && outputRef.getType().isBoolean()) {
                    return null;
                }
                // A NULL/untyped constant synthesizes a NULL_TYPE scan column, but type_checker_config.xml
                // maps no JDBC result class into a TYPE_NULL slot (MySQL returns it as java.lang.Object),
                // so the scan fails at initialization. Pushing a constant gains nothing, so keep a
                // NULL-typed projection local on every dialect.
                if (outputRef.getType().isNull()) {
                    return null;
                }
                hasProjectExpression = true;
            }
            outputColumnRefToExpr.put(outputRef, outputExpr);
        }
        if (outputColumnRefToExpr.isEmpty() || !hasProjectExpression) {
            return null;
        }
        return outputColumnRefToExpr;
    }

    /**
     * Returns true when {@code operator} is a {@link PredicateOperator} or contains one anywhere in its
     * expression tree. A nested comparison renders as a bare scalar boolean inside the pushed SELECT
     * list, which some dialects cannot evaluate (Oracle has no boolean scalar type and fails with
     * ORA-00907), so any projection containing a predicate is kept local rather than pushed down.
     */
    private static boolean containsPredicate(ScalarOperator operator) {
        if (operator instanceof PredicateOperator) {
            return true;
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (containsPredicate(child)) {
                return true;
            }
        }
        return false;
    }

}
