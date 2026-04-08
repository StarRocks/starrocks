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
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.PrimitiveType;

import java.sql.Types;
import java.util.ArrayList;
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
    private static final Pattern ORACLE_DATETIME_LITERAL_PATTERN = Pattern.compile(
            "^(\\d{4})-(\\d{2})-(\\d{2})(?: (\\d{2}):(\\d{2}):(\\d{2})(?:\\.(\\d{1,9}))?)?$");
    private static final Pattern ORACLE_COLUMN_LITERAL_PREDICATE_PATTERN = Pattern.compile(
            "(^|[^A-Za-z0-9_$#])(\"?[A-Za-z_][A-Za-z0-9_$#]*\"?)\\s*(=|!=|<>|<=|>=|<|>)\\s*'([^']*)'",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_LITERAL_COLUMN_PREDICATE_PATTERN = Pattern.compile(
            "(^|[^A-Za-z0-9_$#])'([^']*)'\\s*(=|!=|<>|<=|>=|<|>)\\s*(\"?[A-Za-z_][A-Za-z0-9_$#]*\"?)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_BETWEEN_PREDICATE_PATTERN = Pattern.compile(
            "(^|[^A-Za-z0-9_$#])(\"?[A-Za-z_][A-Za-z0-9_$#]*\"?)\\s+BETWEEN\\s+'([^']*)'\\s+AND\\s+'([^']*)'",
            Pattern.CASE_INSENSITIVE);

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
                    temporalColumns.put(entry.getKey(), temporalType);
                }
            }
            return temporalColumns;
        }

        // Fallback: use slot descriptor types (for JDBC resource tables without cached JDBC types)
        for (SlotDescriptor slotDesc : desc.getSlots()) {
            if (slotDesc == null || slotDesc.getType() == null) {
                continue;
            }
            String columnName;
            if (slotDesc.getColumn() != null) {
                columnName = slotDesc.getColumn().getName();
            } else {
                columnName = slotDesc.getLabel();
            }
            String normalizedColumnName = normalizeColumnName(columnName);
            PrimitiveType slotType = slotDesc.getType().getPrimitiveType();
            if (slotType == PrimitiveType.DATETIME || slotType == PrimitiveType.DATE) {
                temporalColumns.put(normalizedColumnName, slotType);
            }
        }
        return temporalColumns;
    }

    private static boolean literalContainsTimeComponent(String literal) {
        return literal.indexOf(':') >= 0 || literal.indexOf(' ') >= 0 || literal.indexOf('.') >= 0;
    }

    private static String buildOracleTemporalConversionExpr(String literal, PrimitiveType slotType) {
        Matcher matcher = ORACLE_DATETIME_LITERAL_PATTERN.matcher(literal);
        if (!matcher.matches()) {
            // Fallback to Oracle NLS-based conversion for non-canonical literals, such as "2022-01-1".
            if (slotType == PrimitiveType.DATE || !literalContainsTimeComponent(literal)) {
                return String.format("TO_DATE('%s')", literal);
            }
            return String.format("TO_TIMESTAMP('%s')", literal);
        }

        if (matcher.group(4) == null) {
            if (slotType == PrimitiveType.DATE) {
                return String.format("TO_DATE('%s', 'YYYY-MM-DD')", literal);
            }
            return String.format("TO_TIMESTAMP('%s', 'YYYY-MM-DD')", literal);
        }

        if (matcher.group(7) == null) {
            if (slotType == PrimitiveType.DATE) {
                return String.format("TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')", literal);
            }
            return String.format("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')", literal);
        }

        return String.format("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF%d')", literal, matcher.group(7).length());
    }

    private static String rewriteOracleColumnLiteralPredicates(String filter, Map<String, PrimitiveType> temporalColumns) {
        Matcher matcher = ORACLE_COLUMN_LITERAL_PREDICATE_PATTERN.matcher(filter);
        StringBuffer rewritten = new StringBuffer();
        while (matcher.find()) {
            String replacement = matcher.group(0);
            PrimitiveType slotType = temporalColumns.get(normalizeColumnName(matcher.group(2)));
            if (slotType != null) {
                String datetimeExpr = buildOracleTemporalConversionExpr(matcher.group(4), slotType);
                replacement = matcher.group(1) + matcher.group(2) + " " + matcher.group(3) + " " + datetimeExpr;
            }
            matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rewritten);
        return rewritten.toString();
    }

    private static String rewriteOracleLiteralColumnPredicates(String filter, Map<String, PrimitiveType> temporalColumns) {
        Matcher matcher = ORACLE_LITERAL_COLUMN_PREDICATE_PATTERN.matcher(filter);
        StringBuffer rewritten = new StringBuffer();
        while (matcher.find()) {
            String replacement = matcher.group(0);
            PrimitiveType slotType = temporalColumns.get(normalizeColumnName(matcher.group(4)));
            if (slotType != null) {
                String datetimeExpr = buildOracleTemporalConversionExpr(matcher.group(2), slotType);
                replacement = matcher.group(1) + datetimeExpr + " " + matcher.group(3) + " " + matcher.group(4);
            }
            matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rewritten);
        return rewritten.toString();
    }

    private static String rewriteOracleBetweenPredicates(String filter, Map<String, PrimitiveType> temporalColumns) {
        Matcher matcher = ORACLE_BETWEEN_PREDICATE_PATTERN.matcher(filter);
        StringBuffer rewritten = new StringBuffer();
        while (matcher.find()) {
            String replacement = matcher.group(0);
            PrimitiveType slotType = temporalColumns.get(normalizeColumnName(matcher.group(2)));
            if (slotType != null) {
                String lowExpr = buildOracleTemporalConversionExpr(matcher.group(3), slotType);
                String highExpr = buildOracleTemporalConversionExpr(matcher.group(4), slotType);
                replacement = matcher.group(1) + matcher.group(2) + " BETWEEN " + lowExpr + " AND " + highExpr;
            }
            matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rewritten);
        return rewritten.toString();
    }

    private List<String> rewriteOracleDatetimeFilters(List<String> originalFilters) {
        Map<String, PrimitiveType> temporalColumns = collectOracleTemporalColumns();
        if (temporalColumns.isEmpty()) {
            return originalFilters;
        }

        List<String> rewrittenFilters = new ArrayList<>(originalFilters.size());
        for (String filter : originalFilters) {
            String rewritten = rewriteOracleBetweenPredicates(filter, temporalColumns);
            rewritten = rewriteOracleColumnLiteralPredicates(rewritten, temporalColumns);
            rewritten = rewriteOracleLiteralColumnPredicates(rewritten, temporalColumns);
            rewrittenFilters.add(rewritten);
        }
        return rewrittenFilters;
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
        List<String> originalFilters = new ArrayList<>(jdbcConjuncts.size());
        for (Expr p : jdbcConjuncts) {
            p = ExprUtils.replaceLargeStringLiteral(p);
            originalFilters.add(AstToStringBuilder.toString(p));
        }

        if (isOracleJdbcUri()) {
            filters.addAll(rewriteOracleDatetimeFilters(originalFilters));
        } else {
            filters.addAll(originalFilters);
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
    public void computeStats() {
        super.computeStats();
    }

}
