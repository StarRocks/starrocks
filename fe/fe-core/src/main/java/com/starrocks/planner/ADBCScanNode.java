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
import com.starrocks.catalog.ADBCTable;
import com.starrocks.connector.adbc.ADBCConnector;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.SlotRef;
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
    private String tableName;

    public ADBCScanNode(PlanNodeId id, TupleDescriptor desc, ADBCTable adbcTable) {
        super(id, desc, "SCAN ADBC");
        this.adbcTable = adbcTable;
        String identifier = getIdentifierSymbol();
        this.tableName = buildTableName(identifier);
    }

    private String buildTableName(String identifier) {
        String schema = adbcTable.getDbName();
        String table = adbcTable.getName();
        if (identifier.isEmpty()) {
            return schema + "." + table;
        }
        return identifier + schema + identifier + "." + identifier + table + identifier;
    }

    private String getIdentifierSymbol() {
        Map<String, String> props = adbcTable.getProperties();
        if (props != null && props.containsKey("adbc.identifier.quote")) {
            return props.get("adbc.identifier.quote");
        }
        // Default to double-quote (standard SQL)
        return "\"";
    }

    public void computeColumnsAndFilters() {
        createADBCTableColumns();
        createADBCTableFilters();
    }

    private void createADBCTableColumns() {
        String identifier = getIdentifierSymbol();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            String colName = slot.getColumn().getName();
            if (identifier.isEmpty() || (colName.startsWith(identifier) && colName.endsWith(identifier))) {
                columns.add(colName);
            } else {
                columns.add(identifier + colName + identifier);
            }
        }
        // Handle count(*) case
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private void createADBCTableFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        ExprUtils.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        String identifier = getIdentifierSymbol();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(identifier + tmpRef.getLabel() + identifier);
            sMap.put(slotRef, tmpRef);
        }

        ArrayList<Expr> adbcConjuncts = ExprUtils.cloneList(conjuncts, sMap);
        for (Expr p : adbcConjuncts) {
            p = ExprUtils.replaceLargeStringLiteral(p);
            filters.add(AstToStringBuilder.toString(p));
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

    /**
     * Maps the FE property adbc.driver value to the C library driver name.
     */
    public String getADBCDriverName() {
        Map<String, String> props = adbcTable.getProperties();
        String driver = props != null ? props.get("adbc.driver") : null;
        if ("flight_sql".equals(driver)) {
            return "adbc_driver_flightsql";
        }
        return driver != null ? driver : "";
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

        String driver = getPropertyValue("adbc.driver");
        if (driver != null) {
            output.append(prefix).append("DRIVER: ").append(driver).append("\n");
        }

        String uri = getPropertyValue("adbc.url", "uri");
        if (uri != null) {
            output.append(prefix).append("URI: ").append(uri).append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            // EXPLAIN ANALYZE stats from BE runtime profile
            output.append(prefix).append("ConnectTime: ").append("{connect_time_ms}").append("ms\n");
            output.append(prefix).append("RowsRead: ").append("{rows_read}").append("\n");
            output.append(prefix).append("BytesRead: ").append("{bytes_read}").append("\n");
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
        adbcScanNode.setAdbc_driver(getADBCDriverName());

        String uri = getPropertyValue("adbc.url", "uri");
        if (uri != null) {
            adbcScanNode.setAdbc_uri(uri);
        }

        String username = getPropertyValue("adbc.username", "username");
        if (username != null) {
            adbcScanNode.setAdbc_username(username);
        }

        String password = getPropertyValue("adbc.password", "password");
        if (password != null) {
            adbcScanNode.setAdbc_password(password);
        }

        String token = getPropertyValue("adbc.token", "token");
        if (token != null) {
            adbcScanNode.setAdbc_token(token);
        }

        // TLS fields
        String caCertFile = getPropertyValue(ADBCConnector.PROP_TLS_CA_CERT_FILE);
        if (caCertFile != null) {
            adbcScanNode.setAdbc_tls_ca_cert_file(caCertFile);
        }
        String clientCertFile = getPropertyValue(ADBCConnector.PROP_TLS_CLIENT_CERT_FILE);
        if (clientCertFile != null) {
            adbcScanNode.setAdbc_tls_client_cert_file(clientCertFile);
        }
        String clientKeyFile = getPropertyValue(ADBCConnector.PROP_TLS_CLIENT_KEY_FILE);
        if (clientKeyFile != null) {
            adbcScanNode.setAdbc_tls_client_key_file(clientKeyFile);
        }
        String tlsVerify = getPropertyValue(ADBCConnector.PROP_TLS_VERIFY);
        adbcScanNode.setAdbc_tls_verify(!"false".equalsIgnoreCase(tlsVerify));

        msg.adbc_scan_node = adbcScanNode;
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
