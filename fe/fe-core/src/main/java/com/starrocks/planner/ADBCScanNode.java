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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.formatter.AST2StringVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.thrift.TADBCScanNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scan node for ADBC (Arrow Database Connectivity) external tables.
 * Generates pushed-down SQL with column pruning, predicate pushdown, and LIMIT pushdown.
 * Mirrors JDBCScanNode patterns.
 */
public class ADBCScanNode extends ScanNode {

    private final ADBCTable adbcTable;
    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private final String tableName;

    public ADBCScanNode(PlanNodeId id, TupleDescriptor desc, ADBCTable adbcTable) {
        super(id, desc, "SCAN ADBC");
        this.adbcTable = adbcTable;
        this.tableName = quoteIdentifier(adbcTable.getDbName()) + "." + quoteIdentifier(adbcTable.getName());
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public void computeColumnsAndFilters() {
        columns.clear();
        filters.clear();
        createADBCTableColumns();
        createADBCTableFilters();
    }

    private void createADBCTableColumns() {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            columns.add(quoteIdentifier(slot.getColumn().getName()));
        }
        // Handle count(*) case
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private void createADBCTableFilters() {
        ADBCSqlBuilder builder = new ADBCSqlBuilder();
        for (Expr predicate : conjuncts) {
            filters.add(builder.visit(predicate));
        }
    }

    private static class ADBCSqlBuilder extends AST2StringVisitor {
        ADBCSqlBuilder() {
            options = FormatOptions.allEnable().setEnableDigest(false);
        }

        @Override
        public String visitSlot(SlotRef node, Void context) {
            return quoteIdentifier(node.getColumnName() == null ? node.getLabel() : node.getColumnName());
        }

        @Override
        public String visitLiteral(LiteralExpr node, Void context) {
            if (node instanceof StringLiteral) {
                // Flight SQL uses SQL quote doubling, not StarRocks backslash escapes.
                return "'" + node.getStringValue().replace("'", "''") + "'";
            }
            return super.visitLiteral(node, context);
        }
    }

    public String getADBCQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tableName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        if (getLimit() != -1) {
            sql.append(" LIMIT ").append(getLimit());
        }
        return sql.toString();
    }

    private String getPropertyValue(String... keys) {
        Map<String, String> props = adbcTable.getProperties();
        if (props == null) {
            return null;
        }
        for (String key : keys) {
            String val = props.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        output.append(prefix).append("QUERY: ").append(getADBCQueryStr()).append("\n");

        String driver = getPropertyValue("driver");
        if (driver != null) {
            output.append(prefix).append("DRIVER: ").append(driver).append("\n");
        }

        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ADBC_SCAN_NODE;
        TADBCScanNode adbcScanNode = new TADBCScanNode();
        adbcScanNode.setTuple_id(desc.getId().asInt());
        adbcScanNode.setTable_name(tableName);
        adbcScanNode.setColumns(columns);
        adbcScanNode.setFilters(filters);
        adbcScanNode.setLimit(limit);
        msg.adbc_scan_node = adbcScanNode;
        setConnectorCatalogType(msg);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public void computeStats() {
        super.computeStats();
    }

    // Visible for testing
    List<String> getColumns() {
        return columns;
    }

    // Visible for testing
    List<String> getFilters() {
        return filters;
    }
}
