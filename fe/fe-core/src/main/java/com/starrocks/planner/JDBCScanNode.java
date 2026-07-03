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

import com.google.common.base.MoreObjects;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;

/**
 * full scan on JDBC table.
 */
public class JDBCScanNode extends ScanNode {
    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    // The table expression used in the FROM clause.
    // For a base-table scan, this is the quoted remote table name (e.g., `tbl0`).
    // For an inline table (table.isInlineTable() == true), this is a wrapped subquery
    // "(<body>) sr_inline" produced by JDBCTable.getInlineTableExpr(). The body comes either from
    // an optimizer pushdown (JDBCTable.setPushDownQuery, e.g. "SELECT ... FROM t0 JOIN t1 ON ...")
    // or from a JDBC query-table function pass-through (JDBCTable.setPassThroughQuery, e.g.
    // "select ..."); column/filter generation must still run for it.
    private String tableName;
    private JDBCTable table;

    public JDBCScanNode(PlanNodeId id, TupleDescriptor desc, JDBCTable tbl) {
        super(id, desc, "SCAN JDBC");
        table = tbl;
        if (tbl.isInlineTable()) {
            tableName = tbl.getInlineTableExpr();
        } else {
            String objectIdentifier = getIdentifierSymbol(getJdbcUri());
            tableName = wrapWithIdentifier(tbl.getCatalogTableName(), objectIdentifier);
        }
    }

    /**
     * Wrap a dot-separated identifier (e.g., {@code db.tbl} or a single {@code tbl}) by quoting
     * each segment with {@code identifier} (e.g., {@code `}, {@code "}). Already-quoted segments
     * are left alone.
     */
    public static String wrapWithIdentifier(String name, String identifier) {
        if (name == null) {
            return "";
        }
        if (identifier.isEmpty()) {
            return name;
        }
        // If name already have identifier wrapped, just return
        if (name.length() > 2 && name.startsWith(identifier) && name.endsWith(identifier)) {
            return name;
        }

        String[] parts = name.split("\\.", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append(".");
            }
            String part = parts[i];
            if (part.length() > 2 && part.startsWith(identifier) && part.endsWith(identifier)) {
                sb.append(part);
            } else {
                sb.append(identifier).append(part).append(identifier);
            }
        }
        return sb.toString();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    /**
     * Set the dialect-aware remote SQL predicate strings for this scan, already rendered from the
     * scan's pushed-down ScalarOperator predicates by
     * {@link JDBCPushDownSQLBuilder#renderScanFilters}. Both the explain {@code QUERY:} preview and
     * the BE remote SQL wrap each entry in parentheses and join with {@code AND}.
     */
    public void setFilters(List<String> renderedFilters) {
        filters.clear();
        filters.addAll(renderedFilters);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        output.append(prefix).append("QUERY: ").append(getJDBCQueryStr()).append("\n");
        return output.toString();
    }

    // Explain-only preview ("QUERY:" line). Keep it on the same JDBC SQL builder path as
    // optimizer-generated pushdown SQL so FE limit rendering stays dialect-aware.
    private String getJDBCQueryStr() {
        return JDBCPushDownSQLBuilder.buildSelectQuery(getJdbcUri(), columns, tableName, filters, limit);
    }

    private static String wrapColumnWithIdentifier(String name, String identifier) {
        if (name == null || identifier.isEmpty() ||
                (name.startsWith(identifier) && name.endsWith(identifier))) {
            return name;
        }
        return identifier + name + identifier;
    }

    public void createJDBCTableColumns() {
        String objectIdentifier = getIdentifierSymbol(getJdbcUri());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            String colName = slot.getColumn().getName();
            columns.add(wrapColumnWithIdentifier(colName, objectIdentifier));
        }
        // this happens when count(*)
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private String getJdbcUri() {
        JDBCResource resource = (JDBCResource) GlobalStateMgr.getCurrentState().getResourceMgr()
                .getResource(table.getResourceName());
        // Compatible with jdbc catalog
        return resource != null ? resource.getProperty(JDBCResource.URI) : table.getConnectInfo(JDBCResource.URI);
    }

    /**
     * Return the SQL identifier quote character for the given JDBC URI's dialect.
     */
    public static String getIdentifierSymbol(String jdbcUri) {
        if (jdbcUri == null) {
            return "";
        }
        if (jdbcUri.startsWith("jdbc:mysql") ||
                jdbcUri.startsWith("jdbc:mariadb") ||
                jdbcUri.startsWith("jdbc:clickhouse")) {
            return "`";
        }
        if (jdbcUri.startsWith("jdbc:postgresql") ||
                jdbcUri.startsWith("jdbc:postgres")) {
            return "\"";
        }
        return "";
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.JDBC_SCAN_NODE;
        msg.jdbc_scan_node = new TJDBCScanNode();
        msg.jdbc_scan_node.setTuple_id(desc.getId().asInt());
        msg.jdbc_scan_node.setTable_name(tableName);
        msg.jdbc_scan_node.setColumns(columns);
        msg.jdbc_scan_node.setFilters(filters);
        msg.jdbc_scan_node.setLimit(limit);

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

}
