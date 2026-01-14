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
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
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
    private String tableName;
    private JDBCTable table;

    public JDBCScanNode(PlanNodeId id, TupleDescriptor desc, JDBCTable tbl) {
        super(id, desc, "SCAN JDBC");
        table = tbl;
        String objectIdentifier = getIdentifierSymbol();
        tableName = wrapWithIdentifier(tbl.getCatalogTableName(), objectIdentifier);
    }

    private String wrapWithIdentifier(String name, String identifier) {
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

    @Override
    public void finalizeStats(Analyzer analyzer) throws StarRocksException {
        createJDBCTableColumns();
        createJDBCTableFilters();
        computeStats(analyzer);
    }

    public void computeColumnsAndFilters() {
        createJDBCTableColumns();
        createJDBCTableFilters();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        output.append(prefix).append("QUERY: ").append(getJDBCQueryStr()).append("\n");
        return output.toString();
    }

    private String getJDBCQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tableName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }
        return sql.toString();
    }

    private void createJDBCTableColumns() {
        String objectIdentifier = getIdentifierSymbol();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            String colName = slot.getColumn().getName();
            if (objectIdentifier.isEmpty() || (
                    colName.startsWith(objectIdentifier) && colName.endsWith(objectIdentifier))) {
                columns.add(colName);
            } else {
                columns.add(objectIdentifier + colName + objectIdentifier);
            }
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

    private String getIdentifierSymbol() {
        String jdbcUri = getJdbcUri();
        if (jdbcUri == null) {
            return "";
        }
        if (jdbcUri.startsWith("jdbc:mysql") ||
                jdbcUri.startsWith("jdbc:mariadb") ||
                jdbcUri.startsWith("jdbc:clickhouse")) {
            return "`";
        }
        if (jdbcUri.startsWith("jdbc:postgresql") ||
                jdbcUri.startsWith("jdbc:postgres") ||
                jdbcUri.startsWith("jdbc:oracle") ||
                jdbcUri.startsWith("jdbc:sqlserver")) {
            return "\"";
        }
        return "";
    }

    private void createJDBCTableFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        String identifier = getIdentifierSymbol();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(identifier + tmpRef.getLabel() + identifier);
            sMap.put(slotRef, tmpRef);
        }

        ArrayList<Expr> jdbcConjuncts = Expr.cloneList(conjuncts, sMap);
        for (Expr p : jdbcConjuncts) {
            p = p.replaceLargeStringLiteral();
            filters.add(AstToStringBuilder.toString(p));
        }
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
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
    }

}
