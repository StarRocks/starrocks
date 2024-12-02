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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * full scan on JDBC table.
 */
public class JDBCScanNode extends ScanNode {

    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private final List<String> sessionVariableHints = new ArrayList<>();
    private String tableName;
    private JDBCTable table;

    public JDBCScanNode(PlanNodeId id, TupleDescriptor desc, JDBCTable tbl) {
        super(id, desc, "SCAN JDBC");
        table = tbl;
        String objectIdentifier = getIdentifierSymbol();
        tableName = objectIdentifier + tbl.getCatalogTableName() + objectIdentifier;
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
        createJDBCTableSessionVariableHints();
        computeStats(analyzer);
    }

    public void computeColumnsAndFiltersAndSessionVariables() {
        createJDBCTableColumns();
        createJDBCTableFilters();
        createJDBCTableSessionVariableHints();
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
            Column col = slot.getColumn();
            columns.add(objectIdentifier + col.getName() + objectIdentifier);
        }
        // this happends when count(*)
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    private String getJDBCResourceProperty(String propertyKey) {
        JDBCResource resource = (JDBCResource) GlobalStateMgr.getCurrentState().getResourceMgr()
                .getResource(table.getResourceName());
        // Compatible with jdbc catalog
        return resource != null ? resource.getProperty(propertyKey) : table.getProperty(propertyKey);
    }

    private boolean isMysql() {
        String jdbcURI = getJDBCResourceProperty(JDBCResource.URI);
        return jdbcURI.startsWith("jdbc:mysql");
    }

    private boolean isStarRocksTable() {
        String databaseType = getJDBCResourceProperty(JDBCResource.DATABASE_TYPE);
        return databaseType != null && databaseType.equalsIgnoreCase("starrocks");
    }

    private String getIdentifierSymbol() {
        //TODO: for other jdbc table we need different objectIdentifier to support reserved key words
        return isMysql() ? "`" : "";
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
            filters.add(AstToStringBuilder.toString(p));
        }
    }

    private static final Pattern SESSION_VARIABLE_PATTERN = Pattern.compile(
            "(@'[^']+'|@\"[^\"]+\"|@[A-Za-z0-9_]+|[A-Za-z0-9_]+)\\s*=\\s*('[^']*'|\"[^\"]*\"|\\d+)(?:,|$)"
    );

    private void createJDBCTableSessionVariableHints() {
        String jdbcExternalTableSessionVariables =
                ConnectContext.get().getSessionVariable().getJdbcExternalTableSessionVariables();
        if (StringUtils.isEmpty(jdbcExternalTableSessionVariables)) {
            return;
        }
        if (!isMysql()) {
            throw new UnsupportedOperationException(
                    String.format("Sending session variable to JDBC external table is only supported for MYSQL protocol")
            );
        }
        boolean isStarRocksTable = isStarRocksTable();

        // Split the input into 'variableName=variableValue' string assignments and iterate over each assignment
        String[] jdbcExternalTableVariableAssignments =
                jdbcExternalTableSessionVariables.split(",(?=(?:[^']*'[^']*')*[^']*$)");

        // Collect session variables and user defined variables
        List<String> userVariableAssignments = new ArrayList<>();
        List<String> sessionVariableAssignments = new ArrayList<>();
        for (String assignment : jdbcExternalTableVariableAssignments) {
            String trimmedAssignment = assignment.trim();
            validateSessionVariableAssignmentSyntax(trimmedAssignment);
            // Check if user defined variable: starts with '@' e.g. @var1
            if (trimmedAssignment.startsWith("@")) {
                if (!isStarRocksTable) {
                    // StarRocks table supports User Defined variable in query hint but not plain MySQL
                    throw new UnsupportedOperationException("Sending user defined variables to JDBC external table is " +
                            "only supported for \"database_type\" = 'starrocks'. " + "Invalid assignment: " + assignment +
                            ". Please add \"database_type\" = \"starrocks\" to the JDBC resource properties on creation " +
                            "if you intend to propagate user defined variables to an external StarRocks cluster.");
                }
                userVariableAssignments.add(trimmedAssignment);
            } else {
                sessionVariableAssignments.add(trimmedAssignment);
            }
        }

        // Construct SET_VAR hint for session variables and add to sessionVariableHints (get sent to jdbc_connector.cpp)
        if (!sessionVariableAssignments.isEmpty()) {
            StringBuilder setVarHintClause = new StringBuilder();
            if (isStarRocksTable) {
                // To set more than 1 session variable for StarRocks cluster, use 1x SET_VAR hint and use
                // comma separated variable assignments
                // https://docs.starrocks.io/docs/sql-reference/System_variable/#set-variables-in-a-single-query-statement
                setVarHintClause.append("/*+ SET_VAR\n  (\n  ");
                setVarHintClause.append(String.join(",\n  ", sessionVariableAssignments));
                setVarHintClause.append("\n  ) */\n");
            } else {
                // For standard MySQL, set each session variable with its own SET_VAR hint
                // https://dev.mysql.com/doc/refman/8.4/en/optimizer-hints.html#optimizer-hints-set-var
                for (String assignment : sessionVariableAssignments) {
                    setVarHintClause.append("/*+ SET_VAR(").append(assignment).append(") */\n");
                }
            }
            sessionVariableHints.add(setVarHintClause.toString());
        }

        // Construct SET_USER_VARIABLE hint and add to sessionVariableHints to get sent to jdbc_connector.cpp
        if (!userVariableAssignments.isEmpty()) {
            sessionVariableHints.add("/*+ SET_USER_VARIABLE(" + String.join(", ", userVariableAssignments) + ") */\n");
        }
    }

    private void validateSessionVariableAssignmentSyntax(String sessionVariableAssignment) {
        int equalIndex = sessionVariableAssignment.indexOf('=');
        if (equalIndex == -1) {
            throw new IllegalArgumentException("Malformed session variable assignment: " + sessionVariableAssignment);
        }
        if (!SESSION_VARIABLE_PATTERN.matcher(sessionVariableAssignment).matches()) {
            throw new IllegalArgumentException("Invalid session variable format for " +
                    "jdbc_external_table_session_variables. Invalid assignment: " + sessionVariableAssignment +
                    ". Supports MySQL system variables or StarRocks user defined variables. " +
                    "Values can be a quoted string or a numeric value. For example: " +
                    "@my_var='some_value' or my_var=123. The entire string should be a comma separated string " +
                    "of variables e.g. \"@my_var='some_value',my_var2=123\"");
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
        msg.jdbc_scan_node.setSession_variable_hints(sessionVariableHints);
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
