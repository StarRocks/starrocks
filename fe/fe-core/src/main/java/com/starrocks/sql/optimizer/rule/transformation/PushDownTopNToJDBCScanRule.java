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

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Locale;

/**
 * Reduce the rows a PostgreSQL JDBC scan transfers by sorting and limiting them remotely, and
 * drop the local TopN that would otherwise re-sort the result the remote ORDER BY already
 * produced; an OFFSET travels down with the LIMIT. The scan then carries
 * {@code preserve_remote_order}, which stops the BE from round-robining its chunks across drivers;
 * without it the rows arrive reordered and nothing is left to repair them. Matching only a direct
 * scan child also prevents LIMIT from crossing a local filter or aggregate.
 */
public class PushDownTopNToJDBCScanRule extends TransformationRule {
    public PushDownTopNToJDBCScanRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_TO_JDBC_SCAN, Pattern.create(OperatorType.LOGICAL_TOPN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JDBC_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableJdbcTopNPushDown()) {
            return false;
        }
        // RewriteTreeTask matches the pattern before calling this, so the operator and its only
        // input are already the ones the casts below assume.
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalJDBCScanOperator scan = input.inputAt(0).getOp().cast();
        // Pushing these down would change the result. A RANK/DENSE_RANK TopN keeps every tied row, so
        // its output is not "the first N of this order". A partition TopN takes N rows per partition,
        // and a pre-aggregating one even adds output columns, so neither is a plain sort plus limit.
        // A TopN predicate filters what the sort already selected, and a partial TopN is one half of a
        // split pair whose other half stays behind; a split or per-pipeline TopN is likewise not one
        // the scan can absorb on its own. A projection carrying common sub-expressions defines columns
        // its own values reference, and the pushdown SQL is built from the scan's columns alone.
        if (topN.getTopNType() != TopNType.ROW_NUMBER || topN.getPredicate() != null
                || (topN.getPartitionByColumns() != null && !topN.getPartitionByColumns().isEmpty())
                || (topN.getPartitionPreAggCall() != null && !topN.getPartitionPreAggCall().isEmpty())
                || topN.getSortPhase() != SortPhase.FINAL || topN.isSplit() || topN.isPerPipeline()
                || !JDBCPushDownRuleUtils.isColumnPruningOnly(scan.getProjection())
                || (scan.getProjection() != null && !scan.getProjection().getCommonSubOperatorMap().isEmpty())
                || (scan.getPredicateCommonOperators() != null && !scan.getPredicateCommonOperators().isEmpty())) {
            return false;
        }
        // Pushing these down would gain nothing, or has nothing to push. A sort with no limit reduces
        // no rows, and would only move the whole sort to a database that is not the one being relieved.
        // A scan carrying its own LIMIT cannot take the remote ORDER BY, which would have to follow
        // that LIMIT in the SQL text. The scans that arrive here with one are this rule's own output,
        // carrying the limit the planner costs the pushed query by; a second TopN over one of those
        // sorts at most that many rows, which is cheap to do locally.
        if (topN.getLimit() <= 0 || topN.getOrderByElements().isEmpty() || scan.hasLimit()) {
            return false;
        }
        JDBCTable table = (JDBCTable) scan.getTable();
        // Legacy resource-backed schemas are user-declared and lack the catalog connection and
        // source-type metadata required by this rule. Keep their TopN local for all sort types.
        if (!Strings.isNullOrEmpty(table.getResourceName())) {
            return false;
        }
        if (table.getProtocolType() != JDBCTable.ProtocolType.POSTGRES
                || (scan.getPredicate() != null && !CanPushDownPredicateVisitor.canPushDown(
                        scan.getPredicate(), table.getProtocolType()))) {
            return false;
        }
        for (Ordering ordering : topN.getOrderByElements()) {
            ColumnRefOperator ref = ordering.getColumnRef();
            Column column = scan.getColRefToColumnMetaMap().get(ref);
            if (column == null || !ref.getType().equals(column.getType()) || !hasCompatibleOrdering(column, table)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalJDBCScanOperator scan = input.inputAt(0).getOp().cast();
        JDBCTable queryTable = new JDBCTable((JDBCTable) scan.getTable());
        queryTable.setPushDownQuery(JDBCPushDownSQLBuilder.buildTopNQuery(
                scan, topN.getOrderByElements(), topN.getLimit(), topN.getOffset()));
        // The remote ORDER BY has already sorted the result, so no local TopN is kept to restore
        // the order. That only holds if the scan delivers its rows in the order it received them:
        // the BE would otherwise round-robin its chunks across drivers to gain parallelism, which
        // the dropped TopN used to repair. This flag asks it not to.
        queryTable.setPreserveRemoteOrder(true);
        LogicalJDBCScanOperator newScan = new LogicalJDBCScanOperator.Builder()
                .withOperator(scan)
                .setTable(queryTable)
                .setLimit(topN.getLimit())
                .setPredicate(null)
                // The dropped TopN may carry a projection that later operators reference; the scan
                // produces the same input it was computed over, so it evaluates the same there.
                .setProjection(topN.getProjection() != null ? topN.getProjection() : scan.getProjection())
                .build();
        return List.of(OptExpression.create(newScan));
    }

    private boolean hasCompatibleOrdering(Column column, JDBCTable table) {
        String typeName = table.getOriginalJdbcColumnTypeNames().get(column.getName());
        switch (column.getType().getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                // Includes the aliases produced by pushed COUNT/SUM/MIN/MAX over integer columns.
                return true;
            case BOOLEAN:
                // PostgreSQL bool and StarRocks BOOLEAN both order false before true.
                return isSourceType(typeName, "bool", "boolean");
            case DECIMAL64:
            case DECIMAL128:
                // A declared numeric(p,s) with p <= 38 maps to these without losing a digit, so the
                // two sides compare the same values. Wider declarations are excluded because
                // TypeFactory clamps the precision, or degrades to DOUBLE, once it exceeds the
                // DECIMAL256 maximum, and neither preserves PostgreSQL's ordering. Requiring the
                // source type name also keeps out pushed aggregate results, whose precision and
                // overflow behaviour are the aggregate's rather than the column's.
                return isSourceType(typeName, "numeric", "decimal");
            case VARCHAR:
                // PostgreSQL compares text under the column's collation while StarRocks compares
                // bytes, so buildTopNQuery sorts these keys under COLLATE "C", which is PostgreSQL's
                // byte order. bpchar is deliberately absent: it maps to CHAR and ignores trailing
                // spaces when comparing. The source type name is required because an unconstrained
                // numeric also maps to VARCHAR until it is read as DECIMAL(38,18), and ordering
                // that column as a string would push down a different order than StarRocks applies.
                return isSourceType(typeName, "text", "varchar", "character varying");
            case DATE:
                return usesPostgresTemporalReader(table) && isSourceType(typeName, "date");
            case DATETIME:
                // Requires the JDBC bridge's lossless LocalDateTime path for plain PG timestamps.
                // JDBC's TIMESTAMP code also covers timestamptz, whose wall-clock conversion can
                // change ordering around DST. Missing/derived type metadata therefore fails closed.
                return usesPostgresTemporalReader(table)
                        && isSourceType(typeName, "timestamp", "timestamp without time zone");
            default:
                // Floating NaN/Infinity ordering and lossy timestamptz mapping need their own
                // contracts; an enum's order is its declaration order, not its string order.
                return false;
        }
    }

    private static boolean isSourceType(String typeName, String... accepted) {
        if (typeName == null) {
            return false;
        }
        for (String candidate : accepted) {
            if (candidate.equalsIgnoreCase(typeName)) {
                return true;
            }
        }
        return false;
    }

    private boolean usesPostgresTemporalReader(JDBCTable table) {
        // Match JDBCScanner's driver gate. A custom driver/schema resolver can expose PG-shaped
        // metadata without using the bridge's lossless PostgreSQL temporal reader.
        String driverClass = table.getConnectInfo(JDBCResource.DRIVER_CLASS);
        return driverClass != null && driverClass.toLowerCase(Locale.ROOT).contains("postgresql");
    }
}
