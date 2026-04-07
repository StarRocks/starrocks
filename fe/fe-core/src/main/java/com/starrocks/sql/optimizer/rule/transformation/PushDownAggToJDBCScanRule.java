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
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/**
 * Pushes a simple aggregation (GROUP BY + aggregate functions) down into the remote ClickHouse
 * table accessed via the JDBC connector.  When the rule fires, the {@link LogicalAggregationOperator}
 * is removed and its information is stored in the {@link LogicalJDBCScanOperator}, which will later
 * generate a SQL query with GROUP BY and the requested aggregate functions.
 *
 * <p>Prerequisites for the rule to fire:
 * <ul>
 *   <li>The scan target must be a ClickHouse table (identified via the JDBC URL prefix).</li>
 *   <li>Each aggregate function argument must be a simple column reference (no nested expressions).</li>
 *   <li>No DISTINCT aggregation.</li>
 *   <li>The aggregation must not have already been pushed (prevents repeated firing).</li>
 * </ul>
 *
 * <p>Plan transformation:
 * <pre>
 *   Before:
 *     LogicalAggregationOperator(groupBy=[...], aggs={colRef → CallOperator})
 *       └─ LogicalJDBCScanOperator(table=ck_table)
 *
 *   After:
 *     LogicalJDBCScanOperator(table=ck_table, groupingKeys=[...])
 * </pre>
 */
public class PushDownAggToJDBCScanRule extends TransformationRule {


    public PushDownAggToJDBCScanRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_JDBC_SCAN,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_JDBC_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {

        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) input.inputAt(0).getOp();

        // Only push down to ClickHouse tables.
        JDBCTable table = (JDBCTable) scan.getTable();
        if (!isClickHouseTable(table)) {
            return false;
        }

        // Must be a single-phase (GLOBAL) aggregation that has not been split yet.
        if (!agg.getType().isGlobal() || agg.isSplit()) {
            return false;
        }

        // Do not fire if aggregation has already been pushed down.
        if (scan.getGroupingKeys() != null) {
            return false;
        }

        // Validate every aggregate call against ClickHouse aggregation model metadata.
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            CallOperator call = entry.getValue();
            if (call.getArguments().size() != 1) {
                return false;
            }

            ScalarOperator arg = call.getArguments().get(0);
            if (arg instanceof CastOperator) {
                CastOperator cast = (CastOperator) arg;
                if (cast.isImplicit()) {
                    arg = cast.getChild(0);
                }
            }

            if (!(arg instanceof ColumnRefOperator)) {
                return false;
            }

            Column column = scan.getColRefToColumnMetaMap().get(arg);
            if (column == null) {
                return false;
            }

            // Precisely match: only push down if it's an AGG_STATE_UNION column and the function/type matches.
            if (column.getAggregationType() != AggregateType.AGG_STATE_UNION || column.getAggStateDesc() == null) {
                return false;
            }
            
            AggStateDesc desc = column.getAggStateDesc();
            String ckFuncName = desc.getFunctionName();
            String baseFuncName = ckFuncName;
            if (ckFuncName.toLowerCase().endsWith("merge")) {
                baseFuncName = ckFuncName.substring(0, ckFuncName.length() - 5);
            }

            if (!baseFuncName.equalsIgnoreCase(call.getFnName())) {
                return false;
            }

            // Check argument type compatibility.
            List<Type> descArgTypes = desc.getArgTypes();
            if (descArgTypes == null || descArgTypes.isEmpty() || !descArgTypes.get(0).equals(arg.getType())) {
                return false;
            }
        }

        // TODO: Currently, we do not support pushing down aggregations that have predicates (WHERE clauses)
        // on columns not present in the GROUP BY clause.
        if (scan.getPredicate() != null) {
            for (int colId : scan.getPredicate().getUsedColumns().getColumnIds()) {
                ColumnRefOperator col = context.getColumnRefFactory().getColumnRef(colId);
                if (!agg.getGroupingKeys().contains(col)) {
                    throw new com.starrocks.sql.common.StarRocksPlannerException(
                            "Unsupported JDBC aggregation pushdown: the WHERE clause contains a column ('" + 
                            col.getName() + "') that is not part of the GROUP BY list. " +
                            "Please include it in the GROUP BY or disable aggregation pushdown.",
                            com.starrocks.sql.common.ErrorType.UNSUPPORTED);
                }
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) input.inputAt(0).getOp();
        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = Maps.newHashMap();

        for (ColumnRefOperator groupingKey : agg.getGroupingKeys()) {
            if (scan.getColRefToColumnMetaMap().containsKey(groupingKey)) {
                newColRefToColumnMetaMap.put(groupingKey, scan.getColRefToColumnMetaMap().get(groupingKey));
            }
        }

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            ColumnRefOperator key = entry.getKey();
            CallOperator call = entry.getValue();

            ScalarOperator arg = call.getArguments().get(0);
            if (arg instanceof CastOperator && ((CastOperator) arg).isImplicit()) {
                arg = arg.getChild(0);
            }
            ColumnRefOperator argRef = (ColumnRefOperator) arg;

            Column column = scan.getColRefToColumnMetaMap().get(argRef);
            String physicalColumnName = column.getName();
            AggStateDesc desc = column.getAggStateDesc();
            String clickhouseFuncName = desc.getFunctionName();

            // Generate native ClickHouse cast wrapped column name
            String baseAgg = clickhouseFuncName + "(`" + physicalColumnName + "`)";
            String ckType = getClickhouseType(call.getType());
            String columnName = "CAST(" + baseAgg + " AS " + ckType + ")";
            
            newColRefToColumnMetaMap.put(key, new Column(columnName, call.getType()));

            CallOperator newCall = new CallOperator(clickhouseFuncName, call.getType(),
                    call.getArguments(), call.getFunction());

            newAggregations.put(entry.getKey(), newCall);
        }

        LogicalJDBCScanOperator newScan = new LogicalJDBCScanOperator.Builder()
                .withOperator(scan)
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setProjection(null)
                .build();
        newScan.setGroupingKeys(agg.getGroupingKeys());

        return Lists.newArrayList(OptExpression.create(newScan, input.inputAt(0).getInputs()));
    }

    // -------------------------------------------------- helpers --------------------------------------------------

    /**
     * Returns {@code true} when the given {@link JDBCTable} is backed by a ClickHouse JDBC source.
     * Detection is based on the JDBC URL prefix, which is the same heuristic used in
     * {@code JDBCScanNode#getIdentifierSymbol()}.
     */
    private boolean isClickHouseTable(JDBCTable table) {
        String uri = table.getJdbcUri();
        if (uri != null && uri.startsWith("jdbc:clickhouse")) {
            return true;
        }

        String driver = table.getConnectInfo(com.starrocks.catalog.JDBCResource.DRIVER_CLASS);
        return driver != null && driver.toLowerCase().contains("clickhouse");
    }

    private String getClickhouseType(Type type) {
        if (type.isDecimalV3()) {
            return "Decimal(" + type.getPrecision() + ", " + 
                   ((com.starrocks.type.ScalarType) type).getScalarScale() + ")";
        }

        switch (type.getPrimitiveType()) {
            case DOUBLE:
                return "Float64";
            case FLOAT:
                return "Float32";
            case BIGINT:
                return "Int64";
            case LARGEINT:
                return "Int128";
            case INT:
                return "Int32";
            case SMALLINT:
                return "Int16";
            case TINYINT:
                return "Int8";
            case VARCHAR:
            case CHAR:
                return "String";
            case BOOLEAN:
                return "UInt8";
            case DATE:
                return "Date";
            case DATETIME:
                return "DateTime";
            default:
                if (type.isStringType()) {
                    return "String";
                }
                return type.toSql();
        }
    }
}
