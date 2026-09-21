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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TJDBCScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FE half of pushing a join runtime filter into a JDBC scan's remote SQL: which scans are allowed
 * to carry one ({@code TJDBCScanNode.runtime_filter_columns} present) and how each slot's remote
 * column reference is spelled. The BE reads this map to render {@code col IN (?, ?)} into the
 * outermost WHERE, so both halves of the contract are asserted here -- the gating, and the fact
 * that a map entry is byte-for-byte the string the SELECT list uses for the same slot.
 */
public class JDBCScanNodeRuntimeFilterTest {

    private static final String PG_URI = "jdbc:postgresql://localhost:5432/testdb";
    private static final String MYSQL_URI = "jdbc:mysql://localhost:3306/testdb";
    private static final String ORACLE_URI = "jdbc:oracle:thin:@localhost:1521/testdb";

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private ConnectContext enableRuntimeFilterPushDown(boolean enabled) {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableJdbcRuntimeFilterPushDown(enabled);
        context.setThreadLocalInfo();
        return context;
    }

    private JDBCTable createTable(String jdbcUri, List<Column> columns) throws DdlException {
        Map<String, String> sourceTypeNames = Maps.newHashMap();
        for (Column column : columns) {
            sourceTypeNames.put(column.getName(), "varchar");
        }
        return createTable(jdbcUri, columns, sourceTypeNames);
    }

    /**
     * @param sourceTypeNames what the remote catalog called each column, as
     *         {@link JDBCTable#getOriginalJdbcColumnTypeNames()} records it. A VARCHAR slot is only
     *         offered for runtime filters when this says the remote column really is a text type;
     *         pass an empty map to model a scan whose source types were lost.
     */
    private JDBCTable createTable(String jdbcUri, List<Column> columns, Map<String, String> sourceTypeNames)
            throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "u");
        properties.put("password", "p");
        properties.put("jdbc_uri", jdbcUri);
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "driver_class");
        JDBCTable table = new JDBCTable(1, "orders", columns, properties);
        table.setOriginalJdbcColumnTypeNames(sourceTypeNames);
        return table;
    }

    private SlotDescriptor materializedSlot(int slotId, Column column) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), column.getName(), column.getType(), true);
        slot.setColumn(column);
        slot.setIsMaterialized(true);
        return slot;
    }

    private JDBCScanNode createScanNode(JDBCTable table, List<SlotDescriptor> slots) {
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(table);
        for (SlotDescriptor slot : slots) {
            tupleDesc.addSlot(slot);
        }
        return new JDBCScanNode(new PlanNodeId(1), tupleDesc, table);
    }

    /** Build a two-column scan whose slot ids are 7 and 9, so a positional bug cannot pass. */
    private JDBCScanNode twoColumnScanNode(String jdbcUri) throws DdlException {
        Column id = new Column("c_id", VarcharType.VARCHAR);
        Column name = new Column("c_name", VarcharType.VARCHAR);
        JDBCTable table = createTable(jdbcUri, Arrays.asList(id, name));
        return createScanNode(table, Arrays.asList(materializedSlot(7, id), materializedSlot(9, name)));
    }

    private TJDBCScanNode toThrift(JDBCScanNode scanNode) {
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        Assertions.assertTrue(planNode.isSetJdbc_scan_node());
        return planNode.getJdbc_scan_node();
    }

    @Test
    public void testPostgresRuntimeFilterColumnsAreSentAndMatchTheSelectList() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode scanNode = twoColumnScanNode(PG_URI);
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Map<Integer, String> runtimeFilterColumns = thrift.getRuntime_filter_columns();
        Assertions.assertNotNull(runtimeFilterColumns);
        Assertions.assertEquals(2, runtimeFilterColumns.size());
        // PostgreSQL quotes identifiers with double quotes.
        Assertions.assertEquals("\"c_id\"", runtimeFilterColumns.get(7));
        Assertions.assertEquals("\"c_name\"", runtimeFilterColumns.get(9));

        // The whole point of filling the map in the loop that fills `columns`: for a given slot the
        // two must be the same string, quoting included. `columns` is emitted in slot order.
        List<String> selectList = thrift.getColumns();
        Assertions.assertEquals(Arrays.asList("\"c_id\"", "\"c_name\""), selectList);
        Assertions.assertEquals(selectList.get(0), runtimeFilterColumns.get(7));
        Assertions.assertEquals(selectList.get(1), runtimeFilterColumns.get(9));
    }

    @Test
    public void testMySqlRuntimeFilterColumnsUseBacktickQuoting() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode scanNode = twoColumnScanNode(MYSQL_URI);
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Map<Integer, String> runtimeFilterColumns = thrift.getRuntime_filter_columns();
        Assertions.assertEquals("`c_id`", runtimeFilterColumns.get(7));
        Assertions.assertEquals("`c_name`", runtimeFilterColumns.get(9));
        Assertions.assertEquals(thrift.getColumns().get(0), runtimeFilterColumns.get(7));
        Assertions.assertEquals(thrift.getColumns().get(1), runtimeFilterColumns.get(9));
    }

    /**
     * An inline table addresses its columns by generated alias, not by the base table's column name.
     * Nothing here re-derives that alias, which is why the map has to come from the same loop as the
     * SELECT list. (A VARCHAR alias is offered too on PostgreSQL -- see
     * testPushedDownVarcharAliasIsOfferedOnPostgres -- so this one uses an integer key only to keep
     * the alias-spelling assertion independent of the dialect exemption.)
     */
    @Test
    public void testInlineTableRuntimeFilterColumnsUseTheDerivedAlias() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column alias = new Column("jdbc_agg_1", IntegerType.BIGINT);
        JDBCTable table = createTable(PG_URI, Collections.singletonList(alias));
        table.setPushDownQuery("SELECT \"c_id\" AS jdbc_agg_1 FROM \"orders\" GROUP BY \"c_id\"");
        JDBCScanNode scanNode = createScanNode(table, Collections.singletonList(materializedSlot(3, alias)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals("\"jdbc_agg_1\"", thrift.getRuntime_filter_columns().get(3));
        Assertions.assertEquals(thrift.getColumns().get(0), thrift.getRuntime_filter_columns().get(3));
    }

    /**
     * The column that used to make the source-type proof necessary on PostgreSQL: a {@code numeric}
     * declared with no precision. Since PR #62848 it is not a VARCHAR slot at all -- it maps to
     * DECIMAL128(38,18) and is read strictly through {@code strict_numeric_columns} -- so it is
     * refused by {@code default} rather than by any name check, and stays refused now that VARCHAR
     * is unconditional on PostgreSQL. That matters beyond the type: remote filtering has to stay off
     * these columns so a value too large for the mapping cannot be filtered away before the reader
     * sees it and raises 22003.
     */
    @Test
    public void testUnconstrainedNumericIsDecimalAndStillWithheld() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column text = new Column("c_name", VarcharType.VARCHAR);
        Column numeric = new Column("c_amount",
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18));
        JDBCTable table = createTable(PG_URI, Arrays.asList(text, numeric),
                Map.of("c_name", "text", "c_amount", "numeric"));
        table.setUnboundedNumericColumns(Set.of("c_amount"));
        JDBCScanNode scanNode = createScanNode(table,
                Arrays.asList(materializedSlot(4, text), materializedSlot(5, numeric)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        // Both columns are still selected -- and the numeric one is still read strictly; only the
        // runtime filter offer is withheld.
        Assertions.assertEquals(2, thrift.getColumnsSize());
        Assertions.assertEquals(Collections.singletonList(1), thrift.getStrict_numeric_columns());
        Assertions.assertEquals(1, thrift.getRuntime_filter_columnsSize());
        Assertions.assertEquals("\"c_name\"", thrift.getRuntime_filter_columns().get(4));
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(5));
    }

    /**
     * The case this exemption exists for. An optimizer pushdown clears the source type names, so
     * before #62848 a VARCHAR alias on a derived table could not be told apart from an unconstrained
     * numeric and had to be refused -- which cost the string-key push down on exactly the shapes
     * (folded projection, pushed GROUP BY, merged join) where it saves the most. PostgreSQL has no
     * VARCHAR-producing branch left that is not text, so the alias is offered.
     */
    @Test
    public void testPushedDownVarcharAliasIsOfferedOnPostgres() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column alias = new Column("jdbc_proj_1", VarcharType.VARCHAR);
        JDBCTable table = createTable(PG_URI, Collections.singletonList(alias));
        table.setPushDownQuery("SELECT \"c_name\" AS jdbc_proj_1 FROM \"orders\"");
        Assertions.assertTrue(table.getOriginalJdbcColumnTypeNames().isEmpty(),
                "setPushDownQuery is expected to have dropped the source type names");
        JDBCScanNode scanNode = createScanNode(table, Collections.singletonList(materializedSlot(3, alias)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(1, thrift.getColumnsSize());
        Assertions.assertEquals(1, thrift.getRuntime_filter_columnsSize());
        // The alias, not the base column name, and quoted the way the SELECT list quotes it.
        Assertions.assertEquals("\"jdbc_proj_1\"", thrift.getRuntime_filter_columns().get(3));
        Assertions.assertEquals(thrift.getColumns().get(0), thrift.getRuntime_filter_columns().get(3));
    }

    /**
     * The exemption is per-dialect, not a global relaxation. MySQL maps every type it does not
     * recognise -- json included -- to VARCHAR through a {@code default:} branch, so a MySQL VARCHAR
     * slot proves nothing and a derived table that lost its source type names still fails closed.
     */
    @Test
    public void testPushedDownVarcharAliasIsStillWithheldOnMySql() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column alias = new Column("jdbc_proj_1", VarcharType.VARCHAR);
        JDBCTable table = createTable(MYSQL_URI, Collections.singletonList(alias));
        table.setPushDownQuery("SELECT `c_name` AS jdbc_proj_1 FROM `orders`");
        JDBCScanNode scanNode = createScanNode(table, Collections.singletonList(materializedSlot(3, alias)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(1, thrift.getColumnsSize());
        Assertions.assertEquals(0, thrift.getRuntime_filter_columnsSize());
    }

    /** Same for Oracle, whose TIMESTAMP arrives as VARCHAR(64) unless the catalog promotes it. */
    @Test
    public void testPushedDownVarcharAliasIsStillWithheldOnOracle() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column alias = new Column("JDBC_PROJ_1", VarcharType.VARCHAR);
        JDBCTable table = createTable(ORACLE_URI, Collections.singletonList(alias));
        table.setPushDownQuery("SELECT C_NAME AS JDBC_PROJ_1 FROM ORDERS");
        JDBCScanNode scanNode = createScanNode(table, Collections.singletonList(materializedSlot(3, alias)));
        scanNode.createJDBCTableColumns();

        Assertions.assertEquals(0, toThrift(scanNode).getRuntime_filter_columnsSize());
    }

    /**
     * And the proof is still applied, not merely still present: a base MySQL table whose VARCHAR
     * column is really a json column keeps its offer withheld even though the type names survive.
     */
    @Test
    public void testMySqlVarcharBackedByJsonIsWithheld() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column text = new Column("c_name", VarcharType.VARCHAR);
        Column doc = new Column("c_doc", VarcharType.VARCHAR);
        JDBCTable table = createTable(MYSQL_URI, Arrays.asList(text, doc),
                Map.of("c_name", "varchar", "c_doc", "json"));
        JDBCScanNode scanNode = createScanNode(table,
                Arrays.asList(materializedSlot(4, text), materializedSlot(5, doc)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(1, thrift.getRuntime_filter_columnsSize());
        Assertions.assertEquals("`c_name`", thrift.getRuntime_filter_columns().get(4));
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(5));
    }

    /**
     * A type the BE would refuse anyway never reaches it. TIME is the live example: its FE gate
     * would look exactly like DATETIME's -- {@code time} and {@code timetz} collapse into one
     * StarRocks type the same way -- but the bridge reads a PostgreSQL {@code time} as
     * {@code java.sql.Time}, whose millisecond resolution has already dropped the microseconds
     * before any filter could be built, so binding the value back matches nothing.
     */
    @Test
    public void testNonFilterableTypeIsNotOffered() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column key = new Column("c_id", IntegerType.BIGINT);
        Column clock = new Column("c_clock", DateType.TIME);
        JDBCTable table = createTable(PG_URI, Arrays.asList(key, clock),
                Map.of("c_id", "int8", "c_clock", "time"));
        JDBCScanNode scanNode = createScanNode(table,
                Arrays.asList(materializedSlot(11, key), materializedSlot(12, clock)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(2, thrift.getColumnsSize());
        Assertions.assertEquals(1, thrift.getRuntime_filter_columnsSize());
        Assertions.assertEquals("\"c_id\"", thrift.getRuntime_filter_columns().get(11));
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(12));
    }

    // ================== FLOAT / DOUBLE / DATE / DATETIME ==================

    /** A base PostgreSQL table carrying one column of each newly offered type. */
    private JDBCScanNode temporalAndFloatScanNode(String jdbcUri, Map<String, String> sourceTypeNames)
            throws DdlException {
        Column real = new Column("c_real", FloatType.FLOAT);
        Column doublePrecision = new Column("c_double", FloatType.DOUBLE);
        Column date = new Column("c_date", DateType.DATE);
        Column timestamp = new Column("c_ts", DateType.DATETIME);
        JDBCTable table = createTable(jdbcUri, Arrays.asList(real, doublePrecision, date, timestamp),
                sourceTypeNames);
        return createScanNode(table, Arrays.asList(materializedSlot(21, real), materializedSlot(22, doublePrecision),
                materializedSlot(23, date), materializedSlot(24, timestamp)));
    }

    private static final Map<String, String> PG_SAFE_SOURCE_TYPES =
            Map.of("c_real", "float4", "c_double", "float8", "c_date", "date", "c_ts", "timestamp");

    /**
     * All four push down on PostgreSQL when the catalog's own type names say what the remote
     * columns are. The BE binds them as java.sql.Types REAL(7), DOUBLE(8), DATE(91) and
     * TIMESTAMP(93) respectively; this side only decides whether it may.
     */
    @Test
    public void testFloatDoubleDateAndTimestampAreOfferedOnPostgres() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode scanNode = temporalAndFloatScanNode(PG_URI, PG_SAFE_SOURCE_TYPES);
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Map<Integer, String> columns = thrift.getRuntime_filter_columns();
        Assertions.assertEquals(4, columns.size());
        Assertions.assertEquals("\"c_real\"", columns.get(21));
        Assertions.assertEquals("\"c_double\"", columns.get(22));
        Assertions.assertEquals("\"c_date\"", columns.get(23));
        Assertions.assertEquals("\"c_ts\"", columns.get(24));
        // Same string as the SELECT list, for each of them.
        Assertions.assertEquals(thrift.getColumns(),
                Arrays.asList(columns.get(21), columns.get(22), columns.get(23), columns.get(24)));
    }

    /**
     * The red line. {@code PostgresSchemaResolver} maps {@code timestamp} and {@code timestamptz}
     * to the same StarRocks DATETIME, and a {@code timestamptz} read as a wall clock cannot be
     * bound back: under America/New_York the two distinct instants 2024-11-03 05:30:00+00 and
     * 06:30:00+00 are both the wall clock 2024-11-03 01:30:00, so the remote IN returns one row
     * where the local filter keeps two. It has to be refused on PostgreSQL too -- this is the case
     * the VARCHAR exemption must never be generalised to.
     */
    @Test
    public void testPostgresTimestamptzIsWithheld() throws DdlException {
        enableRuntimeFilterPushDown(true);
        for (String timezoneSpelling : new String[] {"timestamptz", "timestamp with time zone",
                "timestamptz(6)", "timestamp(6) with time zone"}) {
            Map<String, String> sourceTypeNames = new HashMap<>(PG_SAFE_SOURCE_TYPES);
            sourceTypeNames.put("c_ts", timezoneSpelling);
            JDBCScanNode scanNode = temporalAndFloatScanNode(PG_URI, sourceTypeNames);
            scanNode.createJDBCTableColumns();

            TJDBCScanNode thrift = toThrift(scanNode);
            Assertions.assertNull(thrift.getRuntime_filter_columns().get(24),
                    "a " + timezoneSpelling + " column must not carry a runtime filter");
            // Only that column: the gate is per column, not per scan.
            Assertions.assertEquals(3, thrift.getRuntime_filter_columnsSize(), timezoneSpelling);
        }
    }

    /**
     * {@code timestamp(6)} is the same type as {@code timestamp} and still pushes down: the
     * whitelist compares the name with any trailing precision suffix removed. Removing everything
     * from the first {@code (} instead would also turn {@code timestamp(6) with time zone} into
     * {@code timestamp}, which is why only a trailing suffix is dropped -- the case above covers
     * that direction.
     */
    @Test
    public void testPostgresTimestampWithPrecisionSuffixIsStillOffered() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Map<String, String> sourceTypeNames = new HashMap<>(PG_SAFE_SOURCE_TYPES);
        sourceTypeNames.put("c_ts", "timestamp(6)");
        JDBCScanNode scanNode = temporalAndFloatScanNode(PG_URI, sourceTypeNames);
        scanNode.createJDBCTableColumns();

        Assertions.assertEquals("\"c_ts\"", toThrift(scanNode).getRuntime_filter_columns().get(24));
    }

    /**
     * {@code money} also reports {@code java.sql.Types.DOUBLE} and therefore also maps to a
     * StarRocks DOUBLE, so a DOUBLE slot is not by itself proof of a {@code float8} column the way
     * a FLOAT slot is proof of a {@code real} one. Binding a double parameter against it is not a
     * wrong answer but {@code operator does not exist: money = double precision}, which would fail
     * the query only on the executions where a filter was built.
     */
    @Test
    public void testPostgresMoneyBackedDoubleIsWithheld() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Map<String, String> sourceTypeNames = new HashMap<>(PG_SAFE_SOURCE_TYPES);
        sourceTypeNames.put("c_double", "money");
        JDBCScanNode scanNode = temporalAndFloatScanNode(PG_URI, sourceTypeNames);
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(22));
        Assertions.assertEquals(3, thrift.getRuntime_filter_columnsSize());
        // FLOAT is genuinely one-to-one and keeps its offer.
        Assertions.assertEquals("\"c_real\"", thrift.getRuntime_filter_columns().get(21));
    }

    /** A base table whose source type names were never recorded fails closed for both gated types. */
    @Test
    public void testDoubleAndTimestampAreWithheldWithoutSourceTypeNames() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode scanNode = temporalAndFloatScanNode(PG_URI, Collections.emptyMap());
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(2, thrift.getRuntime_filter_columnsSize());
        Assertions.assertEquals("\"c_real\"", thrift.getRuntime_filter_columns().get(21));
        Assertions.assertEquals("\"c_date\"", thrift.getRuntime_filter_columns().get(23));
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(22));
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(24));
    }

    /**
     * What makes the four safe is a property of PostgresSchemaResolver and of pgJDBC, not a
     * judgement about parameter binding in general. MySQL's Connector/J renders parameters as
     * literal text unless the catalog's URI opts into server-side prepares, and a literal 0.1
     * compared against a 4-byte FLOAT column matches nothing; Oracle folds two different remote
     * float types into one StarRocks FLOAT. Neither is detectable from here, so none of the four
     * is offered outside PostgreSQL -- including when the source type names are present and say
     * the obvious thing.
     */
    @Test
    public void testNewTypesAreWithheldOutsidePostgres() throws DdlException {
        enableRuntimeFilterPushDown(true);
        for (String uri : new String[] {MYSQL_URI, ORACLE_URI}) {
            JDBCScanNode scanNode = temporalAndFloatScanNode(uri,
                    Map.of("c_real", "float", "c_double", "double", "c_date", "date", "c_ts", "datetime"));
            scanNode.createJDBCTableColumns();
            Assertions.assertEquals(0, toThrift(scanNode).getRuntime_filter_columnsSize(), uri);
        }
    }

    /**
     * A derived table -- the shape every optimizer pushdown produces -- addresses its columns by
     * generated alias and {@code setPushDownQuery} clears the source type names with it. FLOAT and
     * DATE need no name and keep pushing down there; DOUBLE and DATETIME fail closed.
     *
     * <p>DATETIME failing closed here is the answer to a tempting shortcut: the other map,
     * {@code getOriginalJdbcColumnTypes()}, is *not* cleared, and Types.TIMESTAMP (93) and
     * Types.TIMESTAMP_WITH_TIMEZONE (2014) are different constants. pgJDBC reports 93 for both
     * kinds of column, so that map cannot separate them either -- and the aliases would be looked
     * up in a map keyed by the base table's column names, which is the next test.
     *
     * @param aliasPrefix the spelling each pushdown rule gives its derived columns:
     *         {@code jdbc_proj_} for a folded projection, {@code jdbc_agg_} for a pushed GROUP BY,
     *         {@code sr_c} for a merged join, which renames every column including passthroughs.
     */
    private void assertDerivedTableOffersFloatAndDateOnly(String aliasPrefix) throws DdlException {
        Column real = new Column(aliasPrefix + "1", FloatType.FLOAT);
        Column doublePrecision = new Column(aliasPrefix + "2", FloatType.DOUBLE);
        Column date = new Column(aliasPrefix + "3", DateType.DATE);
        Column timestamp = new Column(aliasPrefix + "4", DateType.DATETIME);
        JDBCTable table = createTable(PG_URI, Arrays.asList(real, doublePrecision, date, timestamp),
                PG_SAFE_SOURCE_TYPES);
        table.setPushDownQuery("SELECT \"c_real\" AS " + aliasPrefix + "1 FROM \"orders\"");
        Assertions.assertTrue(table.getOriginalJdbcColumnTypeNames().isEmpty(),
                "setPushDownQuery is expected to have dropped the source type names");
        JDBCScanNode scanNode = createScanNode(table,
                Arrays.asList(materializedSlot(31, real), materializedSlot(32, doublePrecision),
                        materializedSlot(33, date), materializedSlot(34, timestamp)));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(2, thrift.getRuntime_filter_columnsSize(), aliasPrefix);
        Assertions.assertEquals("\"" + aliasPrefix + "1\"", thrift.getRuntime_filter_columns().get(31));
        Assertions.assertEquals("\"" + aliasPrefix + "3\"", thrift.getRuntime_filter_columns().get(33));
        // The two gated types, on a derived column that could be either source type.
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(32), aliasPrefix);
        Assertions.assertNull(thrift.getRuntime_filter_columns().get(34), aliasPrefix);
    }

    @Test
    public void testPushedDownProjectionOffersFloatAndDateOnly() throws DdlException {
        enableRuntimeFilterPushDown(true);
        assertDerivedTableOffersFloatAndDateOnly("jdbc_proj_");
    }

    @Test
    public void testPushedDownAggregateOffersFloatAndDateOnly() throws DdlException {
        enableRuntimeFilterPushDown(true);
        assertDerivedTableOffersFloatAndDateOnly("jdbc_agg_");
    }

    @Test
    public void testPushedDownJoinOffersFloatAndDateOnly() throws DdlException {
        enableRuntimeFilterPushDown(true);
        assertDerivedTableOffersFloatAndDateOnly("sr_c");
    }

    /**
     * The one case that would make reading {@code getOriginalJdbcColumnTypes()} on a derived table
     * actively wrong rather than merely useless, kept here as the reason the shortcut was not
     * taken. The remote table has a column literally named {@code jdbc_proj_7}; a folded projection
     * whose own derived column takes that same alias would look itself up in the base table's map
     * and find that column's type. The name map is cleared, so nothing is consulted at all and the
     * DATETIME alias is refused -- which is the behaviour this pins.
     */
    @Test
    public void testDerivedAliasCollidingWithARemoteColumnNameIsStillRefused() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column collidingAlias = new Column("jdbc_proj_7", DateType.DATETIME);
        JDBCTable table = createTable(PG_URI, Collections.singletonList(collidingAlias),
                // The base table's own jdbc_proj_7 really is a plain timestamp, so a lookup by name
                // would come back "safe" for a column that is not it.
                Map.of("jdbc_proj_7", "timestamp"));
        table.setPushDownQuery("SELECT date_trunc('day', \"c_tstz\") AS jdbc_proj_7 FROM \"orders\"");
        JDBCScanNode scanNode = createScanNode(table,
                Collections.singletonList(materializedSlot(41, collidingAlias)));
        scanNode.createJDBCTableColumns();

        Assertions.assertEquals(0, toThrift(scanNode).getRuntime_filter_columnsSize());
    }

    /**
     * R1: the remote WHERE and the remote row limit land in the same SELECT, WHERE first, so adding
     * a predicate to a limited scan changes which rows come back rather than how many. FE refuses to
     * authorize the push down at all in that case.
     */
    @Test
    public void testScanWithLimitWithholdsRuntimeFilterColumns() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode scanNode = twoColumnScanNode(PG_URI);
        scanNode.setLimit(10);
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(0, thrift.getRuntime_filter_columnsSize());
        // The scan itself is unchanged otherwise.
        Assertions.assertEquals(10, thrift.getLimit());
        Assertions.assertEquals(2, thrift.getColumnsSize());
    }

    @Test
    public void testDisabledSessionVariableWithholdsRuntimeFilterColumns() throws DdlException {
        enableRuntimeFilterPushDown(false);
        JDBCScanNode scanNode = twoColumnScanNode(PG_URI);
        scanNode.createJDBCTableColumns();

        Assertions.assertEquals(0, toThrift(scanNode).getRuntime_filter_columnsSize());
    }

    /** The variable is off unless someone turns it on: a fresh session must not push down. */
    @Test
    public void testRuntimeFilterPushDownIsOffByDefault() {
        Assertions.assertFalse(new ConnectContext().getSessionVariable().isEnableJdbcRuntimeFilterPushDown());
    }

    /** No session at all (e.g. an internal replay path) must not be read as permission. */
    @Test
    public void testMissingConnectContextWithholdsRuntimeFilterColumns() throws DdlException {
        ConnectContext.remove();
        JDBCScanNode scanNode = twoColumnScanNode(PG_URI);
        scanNode.createJDBCTableColumns();

        Assertions.assertEquals(0, toThrift(scanNode).getRuntime_filter_columnsSize());
    }

    /**
     * count(*) materializes no slot, so the scan selects "*" and there is no slot a probe-side
     * runtime filter could be bound to.
     */
    @Test
    public void testCountStarScanCarriesNoRuntimeFilterColumns() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCTable table = createTable(PG_URI, Collections.singletonList(new Column("c_id", VarcharType.VARCHAR)));
        JDBCScanNode scanNode = createScanNode(table, new ArrayList<>());
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(Collections.singletonList("*"), thrift.getColumns());
        Assertions.assertEquals(0, thrift.getRuntime_filter_columnsSize());
    }

    /**
     * An unmaterialized slot is not in the SELECT list, so it must not be in the map either -- a
     * remote WHERE referencing a column the derived table does not project would fail the query.
     */
    @Test
    public void testUnmaterializedSlotIsNotOfferedForRuntimeFilters() throws DdlException {
        enableRuntimeFilterPushDown(true);
        Column id = new Column("c_id", VarcharType.VARCHAR);
        Column hidden = new Column("c_hidden", VarcharType.VARCHAR);
        JDBCTable table = createTable(PG_URI, Arrays.asList(id, hidden));
        SlotDescriptor hiddenSlot = materializedSlot(9, hidden);
        hiddenSlot.setIsMaterialized(false);
        JDBCScanNode scanNode = createScanNode(table, Arrays.asList(materializedSlot(7, id), hiddenSlot));
        scanNode.createJDBCTableColumns();

        TJDBCScanNode thrift = toThrift(scanNode);
        Assertions.assertEquals(1, thrift.getRuntime_filter_columnsSize());
        Assertions.assertEquals("\"c_id\"", thrift.getRuntime_filter_columns().get(7));
        Assertions.assertFalse(thrift.getRuntime_filter_columns().containsKey(9));
    }

    /**
     * Observability: neither the explain QUERY: preview nor any existing profile counter says
     * whether FE authorized this scan to carry a runtime filter, so verbose explain says it.
     */
    @Test
    public void testVerboseExplainReportsAuthorizedRuntimeFilterPushDown() throws DdlException {
        enableRuntimeFilterPushDown(true);
        JDBCScanNode allowed = twoColumnScanNode(PG_URI);
        allowed.createJDBCTableColumns();
        Assertions.assertTrue(allowed.getExplainString().contains("RUNTIME FILTER PUSH DOWN: allowed on 2 column(s)"),
                allowed.getExplainString());

        JDBCScanNode limited = twoColumnScanNode(PG_URI);
        limited.setLimit(10);
        limited.createJDBCTableColumns();
        Assertions.assertFalse(limited.getExplainString().contains("RUNTIME FILTER PUSH DOWN"),
                limited.getExplainString());
    }
}
