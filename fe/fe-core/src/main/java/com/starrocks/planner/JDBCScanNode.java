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
import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * full scan on JDBC table.
 */
public class JDBCScanNode extends ScanNode {
    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    // Remote column reference for every materialized slot, keyed by slot id and already quoted for
    // this dialect. Filled by the very loop that fills `columns` (see createJDBCTableColumns), which
    // is the point: for an inline table the remote column is a generated alias (jdbc_proj_N /
    // jdbc_agg_N / sr_cN), not the base table's column name, so a second place deriving the same
    // reference would drift from the SELECT list the moment a pushdown shape changes.
    // Sent to the BE only when this scan is allowed to render join runtime filters into its remote
    // SQL; see allowsRuntimeFilterPushDown().
    private final Map<Integer, String> runtimeFilterColumns = new LinkedHashMap<>();
    // Zero-based positions in `columns` of the PostgreSQL unconstrained-numeric columns the bridge
    // must read strictly as DECIMAL(38,18). A position, not a name, because the bridge addresses the
    // remote result set by index -- which is why this is filled by the same loop that appends to
    // `columns` rather than by a second walk of the slots: the two walks would be two chances to
    // disagree about which slots are materialized, and the BE's cross-check would then reject a
    // plan that is merely mis-indexed rather than wrong.
    private final List<Integer> strictNumericColumns = new ArrayList<>();
    // Remote type names a VARCHAR runtime filter may be compared against. Consulted for every
    // dialect except PostgreSQL (exempt, see canCarryRuntimeFilter) -- but the PostgreSQL spellings
    // stay, because a catalog whose dialect cannot be determined lands here too. bpchar/char is
    // absent on purpose: it maps to CHAR, which the BE refuses. equalsIgnoreCase, not
    // toLowerCase, which is locale-dependent.
    private static final String[] TEXT_SOURCE_TYPE_NAMES =
            {"text", "varchar", "character varying", "tinytext", "mediumtext", "longtext", "string"};
    // PostgreSQL spellings of the only remote type a DOUBLE runtime filter may be bound against.
    // Required even on PostgreSQL, unlike VARCHAR: {@code money} also arrives as Types.DOUBLE and
    // therefore also maps to a DOUBLE column -- see canCarryRuntimeFilter.
    private static final String[] DOUBLE_SOURCE_TYPE_NAMES = {"float8", "double precision"};
    // Likewise for DATETIME, where the other source type is the red line: PostgresSchemaResolver
    // maps both `timestamp` and `timestamptz` to DATETIME.
    private static final String[] TIMESTAMP_SOURCE_TYPE_NAMES = {"timestamp", "timestamp without time zone"};
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
        if (detailLevel == TExplainLevel.VERBOSE && allowsRuntimeFilterPushDown()) {
            // The QUERY: line above is a plan-time preview and can never show a runtime filter, so
            // without this there is no FE-side signal distinguishing "FE authorized the push down"
            // from "FE vetoed it". The BE profile reports what actually made it into the remote SQL
            // and why it did not; this is the other half of that pair.
            output.append(prefix).append("RUNTIME FILTER PUSH DOWN: allowed on ")
                    .append(runtimeFilterColumns.size()).append(" column(s)\n");
        }
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
            String remoteColumn = wrapColumnWithIdentifier(colName, objectIdentifier);
            // Taken before the add, so it is literally the index this column will occupy in
            // `columns` -- and therefore the position it takes in the remote result the bridge
            // reads. Derived here rather than recomputed in toThrift() so there is exactly one
            // definition of "which slots are materialized, in what order".
            if (table.isUnboundedNumericColumn(colName)) {
                strictNumericColumns.add(columns.size());
            }
            columns.add(remoteColumn);
            // Same string, same loop, same quoting as the SELECT list: a runtime filter the BE
            // renders into the remote WHERE has to address the column exactly the way this scan's
            // projection does.
            if (canCarryRuntimeFilter(slot.getColumn())) {
                runtimeFilterColumns.put(slot.getId().asInt(), remoteColumn);
            }
        }
        // this happens when count(*)
        if (columns.isEmpty()) {
            columns.add("*");
            // No materialized slot means no slot a probe-side runtime filter could ever be bound
            // to, so runtimeFilterColumns stays empty and toThrift() sends nothing.
        }
    }

    /**
     * Whether a runtime filter on this column can be rendered against the remote column without
     * changing what the comparison means -- the half of the decision the BE cannot make, because
     * it only sees the StarRocks type the column was mapped to.
     *
     * <p>Integers need no proof: every remote type StarRocks maps to TINYINT/SMALLINT/INT/BIGINT is
     * a remote integer, and so are the aliases a pushed COUNT/SUM/MIN/MAX produces.
     *
     * <p>VARCHAR does, because a resolver maps non-text remote types to VARCHAR when it cannot
     * express them -- Oracle surfaces {@code TIMESTAMP} as VARCHAR(64), MySQL sends every
     * unrecognised type including {@code json} through a {@code default:} branch. Comparing a text
     * literal against such a column either fails outright or silently compares different text, and
     * only when a join happened to build a filter.
     *
     * <p><b>PostgreSQL is exempt, and only PostgreSQL.</b> That is a property of one resolver, not
     * a judgement about the dialect: since #62848 mapped unconstrained {@code numeric} to
     * DECIMAL128, {@code PostgresSchemaResolver}'s only two VARCHAR branches are both gated on the
     * declared name being {@code varchar} or {@code text}, so a VARCHAR slot *is* the proof. This
     * buys back the derived tables a pushdown builds, where {@code setPushDownQuery} clears the
     * name map. <b>Do not copy it to another dialect without redoing that audit.</b>
     *
     * <p><b>FLOAT, DOUBLE, DATE and DATETIME are PostgreSQL-only</b> -- not because the others were
     * found unsafe, but because what makes these safe is a property of {@code
     * PostgresSchemaResolver} plus pgJDBC. Measured counterexample: Connector/J 8.4.0 defaults to
     * {@code useServerPrepStmts=false}, so on MySQL a FLOAT comparison is resolved as a double and
     * matches no row -- silently, and invisible from here since the flag lives in {@code jdbc_uri}.
     *
     * <p>FLOAT and DATE need no name on PostgreSQL ({@code float4} and {@code date} are the only
     * types reaching them), so they keep pushing down through a derived table. DOUBLE does, because
     * {@code money} also reports {@code Types.DOUBLE}. DATETIME does, and this one is a red line:
     * {@code timestamp} and {@code timestamptz} share a case, and reading a {@code timestamptz} as
     * a wall clock is not reversible -- under a DST repetition two instants collapse onto one, so
     * matching back returns fewer rows, which R3 forbids. Measured: three probe rows, local filter
     * keeps two, the remote IN returned one.
     *
     * <p>The JDBC type constants cannot separate them instead: pgJDBC 42.7.12 reports 93 for a
     * {@code timestamptz} too, from both {@code getColumns()} and {@code ResultSetMetaData} -- 2014
     * never appears. So DATETIME simply does not push down through a pushed projection, aggregate
     * or join. TIME is excluded outright for a different reason: the bridge reads a PostgreSQL
     * {@code time} as {@code java.sql.Time}, already lossy before a filter could be built.
     */
    private boolean canCarryRuntimeFilter(Column column) {
        // The exemptions below are properties of PostgresSchemaResolver, so they hold only where
        // it produced the schema. A legacy resource-backed table's schema is hand-written while
        // getProtocolType() still reads POSTGRES, so a timestamptz declared VARCHAR(n) would sail
        // through the VARCHAR branch. Integers are unaffected and stay pushable.
        boolean isPostgres = table.getProtocolType() == JDBCTable.ProtocolType.POSTGRES
                && Strings.isNullOrEmpty(table.getResourceName());
        switch (column.getType().getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return true;
            case VARCHAR:
                if (isPostgres) {
                    return true;
                }
                return isTextSourceType(table.getOriginalJdbcColumnTypeNames().get(column.getName()));
            case FLOAT:
            case DATE:
                return isPostgres;
            case DOUBLE:
                return isPostgres && isSourceType(table.getOriginalJdbcColumnTypeNames().get(column.getName()),
                        DOUBLE_SOURCE_TYPE_NAMES);
            case DATETIME:
                return isPostgres && isSourceType(table.getOriginalJdbcColumnTypeNames().get(column.getName()),
                        TIMESTAMP_SOURCE_TYPE_NAMES);
            default:
                // Everything else is refused by the BE's own whitelist as well; withholding it here
                // keeps a type the BE would drop out of the map instead of shipping it per query.
                return false;
        }
    }

    private static boolean isTextSourceType(String typeName) {
        if (typeName == null) {
            return false;
        }
        for (String candidate : TEXT_SOURCE_TYPE_NAMES) {
            if (candidate.equalsIgnoreCase(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the remote catalog called this column one of {@code candidates}, ignoring case and a
     * <em>trailing</em> precision suffix. Only trailing: cutting at the first {@code (} instead
     * would turn {@code timestamp(6) with time zone} into {@code timestamp} and admit exactly the
     * column the DATETIME whitelist exists to refuse.
     */
    private static boolean isSourceType(String typeName, String[] candidates) {
        if (typeName == null) {
            return false;
        }
        String normalized = typeName.trim();
        if (normalized.endsWith(")")) {
            int open = normalized.indexOf('(');
            if (open > 0) {
                normalized = normalized.substring(0, open).trim();
            }
        }
        for (String candidate : candidates) {
            if (candidate.equalsIgnoreCase(normalized)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the BE may render this scan's join runtime filters into the outermost WHERE of its
     * remote SQL. Sending {@code runtime_filter_columns} is what grants that permission; withholding
     * it is how FE vetoes the push down, so every gate below is expressed as "do not send the map".
     *
     * <p><b>R1 -- the limit gate.</b> {@code get_jdbc_sql()} puts the WHERE before the row limit,
     * and so do the two dialects that cannot spell {@code LIMIT} (Oracle's {@code ROWNUM}, SQL
     * Server's {@code TOP(n)}) -- all three are filter-then-limit, while the local plan is
     * limit-then-filter. Appending an IN to a scan that carries a limit therefore returns
     * <em>different</em> rows, not fewer: measured, ids 1..10 came back as 150, 160, 170. The BE
     * re-checks {@code _read_limit}; both halves are deliberate, because a single-sided failure
     * would be a wrong answer that does not reproduce on demand.
     *
     * <p>A limit <i>inside</i> the derived table is safe -- the BE appends outside
     * {@code (<body>) sr_inline}. But TopN pushdown also sets the scan's own limit, so it is gated
     * here regardless.
     */
    private boolean allowsRuntimeFilterPushDown() {
        if (runtimeFilterColumns.isEmpty()) {
            return false;
        }
        // R1, FE half.
        if (hasLimit()) {
            return false;
        }
        ConnectContext context = ConnectContext.get();
        return context != null && context.getSessionVariable().isEnableJdbcRuntimeFilterPushDown();
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
        if (allowsRuntimeFilterPushDown()) {
            msg.jdbc_scan_node.setRuntime_filter_columns(runtimeFilterColumns);
        }
        if (table.isPreserveRemoteOrder()) {
            msg.jdbc_scan_node.setPreserve_remote_order(true);
        }
        // Positions into `columns`, collected by createJDBCTableColumns() as it built that list.
        // Left unset when empty so a plan that selects no unconstrained numeric stays byte-identical
        // to one produced before this field existed.
        if (!strictNumericColumns.isEmpty()) {
            msg.jdbc_scan_node.setStrict_numeric_columns(strictNumericColumns);
        }

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
