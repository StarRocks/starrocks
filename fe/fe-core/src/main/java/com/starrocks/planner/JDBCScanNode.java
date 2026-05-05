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
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.VarcharType;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * full scan on JDBC table.
 */
public class JDBCScanNode extends ScanNode {
    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private String tableName;
    private JDBCTable table;

    private static final class OracleTemporalLiteralExpr extends LiteralExpr {
        private final String sqlLiteral;

        private OracleTemporalLiteralExpr(String sqlLiteral) {
            super();
            this.sqlLiteral = sqlLiteral;
            this.type = VarcharType.VARCHAR;
            analysisDone();
        }

        private OracleTemporalLiteralExpr(OracleTemporalLiteralExpr other) {
            super(other);
            this.sqlLiteral = other.sqlLiteral;
        }

        @Override
        public Expr clone() {
            return new OracleTemporalLiteralExpr(this);
        }

        @Override
        public boolean isMinValue() {
            return false;
        }

        @Override
        public int compareLiteral(LiteralExpr expr) {
            return getStringValue().compareTo(expr.getStringValue());
        }

        @Override
        public Object getRealObjectValue() {
            return sqlLiteral;
        }

        @Override
        public String getStringValue() {
            return sqlLiteral;
        }
    }

    public JDBCScanNode(PlanNodeId id, TupleDescriptor desc, JDBCTable tbl) {
        super(id, desc, "SCAN JDBC");
        table = tbl;
        if (tbl.isQueryTable()) {
            tableName = tbl.getCatalogTableName();
        } else {
            String objectIdentifier = getIdentifierSymbol();
            tableName = wrapWithIdentifier(tbl.getCatalogTableName(), objectIdentifier);
        }
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
    public void finalizeStats() throws StarRocksException {
        createJDBCTableColumns();
        createJDBCTableFilters();
        computeStats();
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
                jdbcUri.startsWith("jdbc:postgres")) {
            return "\"";
        }
        return "";
    }

    private boolean isOracleJdbcUri() {
        String jdbcUri = getJdbcUri();
        return jdbcUri != null && jdbcUri.toLowerCase(Locale.ROOT).startsWith("jdbc:oracle");
    }

    private static String normalizeColumnName(String columnName) {
        if (columnName == null) {
            return "";
        }
        if (columnName.length() >= 2) {
            char first = columnName.charAt(0);
            char last = columnName.charAt(columnName.length() - 1);
            if ((first == '"' && last == '"') || (first == '`' && last == '`')) {
                columnName = columnName.substring(1, columnName.length() - 1);
            }
        }
        return columnName.toLowerCase(Locale.ROOT);
    }

    private static final int ORACLE_TIMESTAMP_WITH_LOCAL_TZ = -102;
    private static final int ORACLE_TIMESTAMP_WITH_TZ = -101;

    private static PrimitiveType mapJdbcTypeToTemporalType(int jdbcType) {
        switch (jdbcType) {
            case Types.DATE:
                return PrimitiveType.DATE;
            case Types.TIMESTAMP:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TZ:
            case ORACLE_TIMESTAMP_WITH_TZ:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return PrimitiveType.DATETIME;
            default:
                return null;
        }
    }

    private Map<String, PrimitiveType> collectOracleTemporalColumns() {
        Map<String, PrimitiveType> temporalColumns = new HashMap<>();

        // Prefer original JDBC types from the cached column schema (covers TIMESTAMP -> VARCHAR mapping)
        Map<String, Integer> originalJdbcTypes = table.getOriginalJdbcColumnTypes();
        if (originalJdbcTypes != null && !originalJdbcTypes.isEmpty()) {
            for (Map.Entry<String, Integer> entry : originalJdbcTypes.entrySet()) {
                PrimitiveType temporalType = mapJdbcTypeToTemporalType(entry.getValue());
                if (temporalType != null) {
                    temporalColumns.put(normalizeColumnName(entry.getKey()), temporalType);
                }
            }
            return temporalColumns;
        }
        return temporalColumns;
    }

    private Expr rewriteOracleTemporalPredicateExpr(Expr expr, Map<String, PrimitiveType> temporalColumns) {
        for (int i = 0; i < expr.getChildren().size(); i++) {
            expr.setChild(i, rewriteOracleTemporalPredicateExpr(expr.getChild(i), temporalColumns));
        }

        if (expr instanceof BetweenPredicate) {
            Expr left = expr.getChild(0);
            Expr low = expr.getChild(1);
            Expr high = expr.getChild(2);
            if (!(left instanceof SlotRef) || low == null || high == null || !low.isConstant() || !high.isConstant()) {
                return expr;
            }

            SlotRef slotRef = (SlotRef) left;
            String slotColumnName = slotRef.getColumnName() != null ? slotRef.getColumnName() : slotRef.getLabel();
            PrimitiveType slotType = temporalColumns.get(normalizeColumnName(slotColumnName));
            if (slotType == null) {
                return expr;
            } else if (slotType != PrimitiveType.DATE && slotType != PrimitiveType.DATETIME) {
                return expr;
            }

            Expr lowLiteralExpr = buildOracleTemporalLiteralExpr(low);
            Expr highLiteralExpr = buildOracleTemporalLiteralExpr(high);
            if (lowLiteralExpr == null || highLiteralExpr == null) {
                return expr;
            }
            expr.setChild(1, lowLiteralExpr);
            expr.setChild(2, highLiteralExpr);
            return expr;
        }

        if (expr instanceof InPredicate) {
            Expr left = expr.getChild(0);
            if (!(left instanceof SlotRef)) {
                return expr;
            }

            SlotRef slotRef = (SlotRef) left;
            String slotColumnName = slotRef.getColumnName() != null ? slotRef.getColumnName() : slotRef.getLabel();
            PrimitiveType slotType = temporalColumns.get(normalizeColumnName(slotColumnName));
            if (slotType == null) {
                return expr;
            } else if (slotType != PrimitiveType.DATE && slotType != PrimitiveType.DATETIME) {
                return expr;
            }

            List<Expr> rewrittenItems = new ArrayList<>(expr.getChildren().size() - 1);
            for (int i = 1; i < expr.getChildren().size(); i++) {
                Expr item = expr.getChild(i);
                if (item == null || !item.isConstant()) {
                    return expr;
                }
                Expr rewrittenItem = buildOracleTemporalLiteralExpr(item);
                if (rewrittenItem == null) {
                    return expr;
                }
                rewrittenItems.add(rewrittenItem);
            }
            for (int i = 0; i < rewrittenItems.size(); i++) {
                expr.setChild(i + 1, rewrittenItems.get(i));
            }
            return expr;
        }

        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }

        Expr left = expr.getChild(0);
        Expr right = expr.getChild(1);
        if (!(left instanceof SlotRef) || right == null || !right.isConstant()) {
            return expr;
        }

        SlotRef slotRef = (SlotRef) left;
        String slotColumnName = slotRef.getColumnName() != null ? slotRef.getColumnName() : slotRef.getLabel();
        PrimitiveType slotType = temporalColumns.get(normalizeColumnName(slotColumnName));
        if (slotType == null) {
            return expr;
        } else if (slotType != PrimitiveType.DATE && slotType != PrimitiveType.DATETIME) {
            return expr;
        }

        Expr literalExpr = buildOracleTemporalLiteralExpr(right);
        if (literalExpr == null) {
            return expr;
        }
        expr.setChild(1, literalExpr);
        return expr;
    }

    private static Expr buildOracleTemporalLiteralExpr(Expr constantExpr) {
        String literalValue;
        if (constantExpr instanceof StringLiteral) {
            literalValue = ((StringLiteral) constantExpr).getStringValue();
        } else if (constantExpr instanceof DateLiteral) {
            literalValue = ((DateLiteral) constantExpr).getStringValue();
        } else {
            return null;
        }
        if (literalValue == null) {
            return null;
        }
        Pattern p = Pattern.compile("^(\\d{4})-(\\d{1,2})-(\\d{1,2})$");
        Matcher m = p.matcher(literalValue);
        String keyword = 
                (literalValue.length() <= ("0000-00-00").length()) ? (m.matches() ? "date" : "") : "timestamp";
        String escapedValue = literalValue.replace("'", "''");
        return new OracleTemporalLiteralExpr(keyword + " '" + escapedValue + "'");
    }

    private void createJDBCTableFilters() {
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

        ArrayList<Expr> jdbcConjuncts = ExprUtils.cloneList(conjuncts, sMap);
        // Filters instead of conjuncts are used in BE to filter rows, the types of conjuncts' children
        // would be unmatched after remove cast operator in PushDownPredicateTOExternalTableScanRule, which
        // would cause BE report error "VectorizedInPredicate type not same";
        conjuncts.clear();
        Map<String, PrimitiveType> oracleTemporalColumns = Collections.emptyMap();
        if (isOracleJdbcUri()) {
            oracleTemporalColumns = collectOracleTemporalColumns();
        }
        List<String> originalFilters = new ArrayList<>(jdbcConjuncts.size());
        for (Expr p : jdbcConjuncts) {
            p = ExprUtils.replaceLargeStringLiteral(p);
            if (!oracleTemporalColumns.isEmpty()) {
                p = rewriteOracleTemporalPredicateExpr(p, oracleTemporalColumns);
            }
            originalFilters.add(AstToStringBuilder.toString(p));
        }
        filters.addAll(originalFilters);
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
