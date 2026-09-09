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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for OlapTableFactory property handling during CREATE TABLE: table_query_timeout
 * (covers OlapTableFactory.java lines 775-781) and the flat_json.* properties, which must be
 * validated with the same analyzers ALTER TABLE uses.
 */
public class OlapTableFactoryTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final String DB_NAME = "test_olap_table_factory_db";

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * Test CREATE TABLE with valid table_query_timeout property.
     * This test covers OlapTableFactory.java lines 773-778 (success path).
     */
    @Test
    public void testCreateTableWithValidTableQueryTimeout() throws Exception {
        String createTableSql = "CREATE TABLE `test_timeout_valid` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"200\"\n" +
                ");";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        // Call OlapTableFactory directly to ensure we hit OlapTableFactory.java's table_query_timeout branch.
        Table table = OlapTableFactory.INSTANCE.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table instanceof OlapTable);
        Assertions.assertEquals(200, ((OlapTable) table).getTableQueryTimeout());
    }

    /**
     * Test CREATE TABLE with table_query_timeout = 0 (invalid, should throw DdlException).
     * This test covers OlapTableFactory.java lines 779-781 (exception path).
     */
    @Test
    public void testCreateTableWithZeroTableQueryTimeout() throws Exception {
        String createTableSql = "CREATE TABLE `test_timeout_zero` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"0\"\n" +
                ");";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> OlapTableFactory.INSTANCE.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt));
        Assertions.assertTrue(e.getMessage().contains("must be greater than 0"),
                "Expected error message about value must be greater than 0, but got: " + e.getMessage());
    }

    private static Table createFlatJsonTable(String tableName, String flatJsonProperties) throws Exception {
        String createTableSql = "CREATE TABLE `" + tableName + "` (\n" +
                "  `k1` int NULL,\n" +
                "  `j1` json NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                flatJsonProperties +
                ");";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        return OlapTableFactory.INSTANCE.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt);
    }

    private static String flatJsonCreateFailure(String tableName, String flatJsonProperties) {
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> createFlatJsonTable(tableName, flatJsonProperties));
        return e.getMessage();
    }

    /**
     * CREATE TABLE must reject the same out-of-range flat_json values ALTER TABLE rejects, and with
     * the same message: both go through PropertyAnalyzer.analyzeFlatJson*. Before the fix CREATE
     * used the generic analyzerDoubleProp/analyzeIntProp helpers, which only parse, so all of these
     * were stored verbatim and shipped to the BE.
     */
    @Test
    public void testCreateTableRejectsOutOfRangeFlatJsonNullFactor() {
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "5.0");
        String alterMessage = Assertions.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeFlatJsonNullFactor(alterProperties)).getMessage();

        String createMessage = flatJsonCreateFailure("test_flat_json_null_factor",
                "\"flat_json.enable\" = \"true\",\n\"flat_json.null.factor\" = \"5.0\"\n");
        Assertions.assertTrue(createMessage.contains("Illegal flat json null factor: 5.0"),
                "Expected the analyzer's message, but got: " + createMessage);
        Assertions.assertEquals(alterMessage, createMessage,
                "CREATE and ALTER must report flat_json.null.factor identically");
    }

    @Test
    public void testCreateTableRejectsNegativeFlatJsonSparsityFactor() {
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "-2.0");
        String alterMessage = Assertions.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeFlatJsonSparsityFactor(alterProperties)).getMessage();

        String createMessage = flatJsonCreateFailure("test_flat_json_sparsity_factor",
                "\"flat_json.enable\" = \"true\",\n\"flat_json.sparsity.factor\" = \"-2.0\"\n");
        Assertions.assertTrue(createMessage.contains("Illegal flat json sparsity factor: -2.0"),
                "Expected the analyzer's message, but got: " + createMessage);
        Assertions.assertEquals(alterMessage, createMessage,
                "CREATE and ALTER must report flat_json.sparsity.factor identically");
    }

    /**
     * A negative flat_json.column.max is the worst of the three: JsonPathDeriver::_finalize() reads
     * it as {@code _max_column > 0 ? _max_column : SIZE_MAX}, i.e. "no limit" -- the opposite of the
     * tighter limit the user asked for.
     */
    @Test
    public void testCreateTableRejectsNegativeFlatJsonColumnMax() {
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "-7");
        String alterMessage = Assertions.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeFlatJsonColumnMax(alterProperties)).getMessage();

        String createMessage = flatJsonCreateFailure("test_flat_json_column_max",
                "\"flat_json.enable\" = \"true\",\n\"flat_json.column.max\" = \"-7\"\n");
        Assertions.assertTrue(createMessage.contains("Illegal flat json column max: -7"),
                "Expected the analyzer's message, but got: " + createMessage);
        Assertions.assertEquals(alterMessage, createMessage,
                "CREATE and ALTER must report flat_json.column.max identically");
    }

    /**
     * A non-numeric value was already rejected before the fix, but with the generic helper's
     * message ("Invalid flat_json.sparsity.factor format: abc"). It must now read like ALTER's.
     */
    @Test
    public void testCreateTableRejectsNonNumericFlatJsonSparsityFactor() {
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "abc");
        String alterMessage = Assertions.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeFlatJsonSparsityFactor(alterProperties)).getMessage();

        String createMessage = flatJsonCreateFailure("test_flat_json_sparsity_not_a_number",
                "\"flat_json.enable\" = \"true\",\n\"flat_json.sparsity.factor\" = \"abc\"\n");
        Assertions.assertEquals(alterMessage, createMessage,
                "CREATE and ALTER must report a malformed flat_json.sparsity.factor identically");
    }

    /**
     * In-range values must still be accepted, and land on the table unchanged.
     */
    @Test
    public void testCreateTableAcceptsLegalFlatJsonProperties() throws Exception {
        Table table = createFlatJsonTable("test_flat_json_legal",
                "\"flat_json.enable\" = \"true\",\n" +
                        "\"flat_json.column.max\" = \"50\",\n" +
                        "\"flat_json.null.factor\" = \"0.2\",\n" +
                        "\"flat_json.sparsity.factor\" = \"0.4\"\n");
        Assertions.assertTrue(table instanceof OlapTable);
        FlatJsonConfig config = ((OlapTable) table).getFlatJsonConfig();
        Assertions.assertNotNull(config);
        Assertions.assertTrue(config.getFlatJsonEnable());
        Assertions.assertEquals(0.2, config.getFlatJsonNullFactor(), 0.0001);
        Assertions.assertEquals(0.4, config.getFlatJsonSparsityFactor(), 0.0001);
        Assertions.assertEquals(50, config.getFlatJsonColumnMax());
    }

    /**
     * The boundary values the analyzers allow must stay allowed, and a table with no flat_json
     * property at all must keep getting no flat_json config.
     */
    @Test
    public void testCreateTableAcceptsFlatJsonBoundariesAndNoConfig() throws Exception {
        Table boundaries = createFlatJsonTable("test_flat_json_boundaries",
                "\"flat_json.enable\" = \"true\",\n" +
                        "\"flat_json.column.max\" = \"0\",\n" +
                        "\"flat_json.null.factor\" = \"0\",\n" +
                        "\"flat_json.sparsity.factor\" = \"1\"\n");
        FlatJsonConfig config = ((OlapTable) boundaries).getFlatJsonConfig();
        Assertions.assertNotNull(config);
        Assertions.assertEquals(0, config.getFlatJsonNullFactor(), 0.0001);
        Assertions.assertEquals(1, config.getFlatJsonSparsityFactor(), 0.0001);
        Assertions.assertEquals(0, config.getFlatJsonColumnMax());

        Table plain = createFlatJsonTable("test_flat_json_absent", "\"compression\" = \"LZ4\"\n");
        Assertions.assertFalse(((OlapTable) plain).containsFlatJsonConfig());
    }
}

