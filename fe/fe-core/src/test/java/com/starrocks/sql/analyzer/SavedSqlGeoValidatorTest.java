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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SavedSqlGeoValidatorTest {
    private static final String ERROR = "Nested GEOGRAPHY/GEOMETRY types are not supported in saved SQL definitions";
    private static final String BASE = " FROM geo_src";
    private static final String TABLE_OPTIONS =
            " DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')";
    private static final String MV_OPTIONS =
            " DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH MANUAL PROPERTIES('replication_num'='1') AS ";
    private static final String PIPE =
            "CREATE PIPE bad_pipe AS INSERT INTO geo_src SELECT col_int,col_string "
                    + "FROM FILES('path'='fake://somewhere/1.parquet','format'='parquet') WHERE ";
    private static final String CREATE_TABLE = "CREATE TABLE %s(id INT NOT NULL,wkt VARCHAR(80),%s)";
    private static StarRocksAssert sql;
    private static ConnectContext context;
    private static boolean oldEnableUdf;

    @BeforeAll
    static void setup() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        sql = new StarRocksAssert(context).withDatabase("saved_geo").useDatabase("saved_geo");
        sql.withTable("CREATE TABLE geo_src(id INT NOT NULL,wkt VARCHAR(80))" + TABLE_OPTIONS);
        oldEnableUdf = Config.enable_udf;
        Config.enable_udf = true;
    }

    @AfterAll
    static void restoreConfig() {
        Config.enable_udf = oldEnableUdf;
    }

    @ParameterizedTest
    @ValueSource(strings = {"ST_GeogFromText(wkt)", "ST_GeomFromText(wkt,'EPSG:3857')"})
    void checksWriteBoundaries(String geo) throws Exception {
        new MockUp<ExpressionAnalyzer.Visitor>() {
            @Mock
            public Void visit(Invocation invocation, ParseNode node, Scope scope) {
                ExpressionAnalyzer.Visitor visitor = invocation.getInvokedInstance();
                return node.accept(visitor, scope);
            }
        };

        String suffix = geo.startsWith("ST_Geog") ? "gg" : "gm";
        String view = "v_" + suffix;
        String mvName = "am_" + suffix;
        String table = "gc_" + suffix;
        String scalar = "ST_AsText(" + geo + ")";
        String query = "SELECT id," + scalar + " AS v" + BASE;
        String column = "v VARCHAR(128) AS (" + scalar + ")";
        sql.ddl("CREATE VIEW " + view + " AS " + query);
        sql.ddl("CREATE OR REPLACE VIEW " + view + " AS " + query);
        sql.ddl("ALTER VIEW " + view + " AS " + query);
        sql.ddl("CREATE MATERIALIZED VIEW " + mvName + MV_OPTIONS
                + "SELECT id," + scalar + " AS keep_value,wkt AS removable" + BASE);
        analyze("CREATE MATERIALIZED VIEW sync_scalar AS " + query);
        analyze("CREATE FUNCTION f_scalar(wkt STRING) RETURNS " + scalar);
        sql.ddl(CREATE_TABLE.formatted(table, column) + TABLE_OPTIONS);
        analyze("ALTER TABLE " + table + " MODIFY COLUMN " + column);
        analyze("ALTER TABLE " + table + " ADD COLUMN " + column.replace("v VARCHAR", "v1 VARCHAR"));
        analyze("ALTER TABLE " + table + " ADD COLUMN (" + column.replace("v VARCHAR", "v2 VARCHAR") + ")");
        sql.ddl("CREATE BASELINE ON " + query + " USING " + query);
        analyze(PIPE + scalar.replace("wkt", "col_string") + " IS NOT NULL");

        MaterializedView mv = (MaterializedView) context.getGlobalStateMgr().getLocalMetastore()
                .getTable("saved_geo", mvName);
        String savedMv = mv.getViewDefineSql();
        String originalMv = mv.getOriginalViewDefineSql();
        String savedView = sql.show("SHOW CREATE VIEW " + view).get(0).get(1);
        String savedTable = sql.showCreateTable("SHOW CREATE TABLE " + table);

        for (String expr : List.of("cardinality([" + geo + "])", "ST_AsText(map{1:" + geo + "}[1])",
                "ST_AsText((named_struct('geo'," + geo + ")).geo)")) {
            String nested = "SELECT id," + expr + " AS v" + BASE;
            String generated = (expr.startsWith("cardinality") ? "INT" : "VARCHAR(128)") + " AS (" + expr + ")";
            reject("CREATE VIEW bad_v AS " + nested);
            reject("CREATE OR REPLACE VIEW " + view + " AS " + nested);
            reject("ALTER VIEW " + view + " AS " + nested);
            reject("CREATE MATERIALIZED VIEW bad_async" + MV_OPTIONS + nested);
            reject("CREATE MATERIALIZED VIEW bad_sync AS " + nested);
            reject("CREATE FUNCTION bad_f(wkt STRING) RETURNS " + expr);
            reject(CREATE_TABLE.formatted("bad_gc", "v " + generated) + TABLE_OPTIONS);
            reject("ALTER TABLE " + table + " ADD COLUMN added " + generated);
            reject("ALTER TABLE " + table + " ADD COLUMN (added " + generated + ")");
            reject("ALTER TABLE " + table + " MODIFY COLUMN v " + generated);
            reject("CREATE BASELINE USING " + nested);
            reject("CREATE BASELINE ON " + nested + " USING " + query);
            reject(PIPE + expr.replace("wkt", "col_string") + " IS NOT NULL");
            reject("ALTER MATERIALIZED VIEW " + mvName + " ADD COLUMN added AS " + expr);
            assertEquals(originalMv, mv.getOriginalViewDefineSql());

            String legacy = "SELECT id," + expr + " AS keep_value,wkt AS removable" + BASE;
            mv.setOriginalViewDefineSql(legacy);
            try {
                reject("ALTER MATERIALIZED VIEW " + mvName + " DROP COLUMN removable");
                assertEquals(legacy, mv.getOriginalViewDefineSql());
                assertNotNull(mv.getColumn("removable"));
            } finally {
                mv.setOriginalViewDefineSql(originalMv);
                mv.resetDefinedQueryParseNode();
            }
            assertEquals(savedMv, mv.getViewDefineSql());
            assertEquals(savedView, sql.show("SHOW CREATE VIEW " + view).get(0).get(1));
            assertEquals(savedTable, sql.showCreateTable("SHOW CREATE TABLE " + table));
        }

        sql.ddl("ALTER MATERIALIZED VIEW " + mvName + " ADD COLUMN added AS " + scalar);
        sql.ddl("ALTER MATERIALIZED VIEW " + mvName + " DROP COLUMN removable");
        checkQueryTraversal(geo, view);
    }

    private static void checkQueryTraversal(String geo, String view) throws Exception {
        String card = "cardinality([" + geo + "])";
        String leaf = geo.replace("wkt", "'POINT (1 2)'");
        String constant = "cardinality([" + leaf + "])";
        for (String query : List.of("SELECT id" + BASE + " WHERE " + card + " > 0",
                "SELECT a.id FROM geo_src a JOIN geo_src b ON a.id=b.id AND "
                        + card.replace("wkt", "a.wkt") + " > 0",
                "WITH c AS (SELECT " + card + " AS n" + BASE + ") SELECT 1 FROM c",
                "SELECT (SELECT " + constant + ")",
                "SELECT 1 FROM (VALUES (" + constant + ")) t(n)",
                "SELECT id FROM geo_src, UNNEST([" + geo + "])",
                "SELECT 1 AS n UNION ALL SELECT " + card + BASE)) {
            reject("CREATE VIEW bad_tree AS " + query);
        }
        String ordered = "(SELECT 1 AS id) ORDER BY " + constant;
        String cte = "WITH c AS (SELECT [" + leaf + "] AS a) ";
        assertAll(
                () -> reject("CREATE VIEW " + view + "_wrapper_order AS " + ordered),
                () -> reject("CREATE VIEW " + view + "_inner_cte AS (" + cte + "SELECT 1 AS id FROM c) ORDER BY 1"),
                () -> reject("CREATE VIEW " + view + "_wrapper_cte AS " + cte + "(SELECT 1 AS id FROM c) ORDER BY 1"),
                () -> reject("CREATE VIEW " + view + "_derived AS SELECT id FROM (" + ordered + " LIMIT 1) t"),
                () -> reject("CREATE MATERIALIZED VIEW mv_" + view + "_wrapper" + MV_OPTIONS
                        + "SELECT id FROM ((SELECT id FROM geo_src) ORDER BY " + constant + " LIMIT 1) t"));

        analyze("CREATE VIEW tree_control AS SELECT id FROM " + view + " UNION ALL SELECT id" + BASE + " ORDER BY id");
        analyze("CREATE VIEW tree_control AS WITH c AS (SELECT n FROM TABLE(UNNEST([1])) t(n)) SELECT n FROM c");
        analyze("CREATE VIEW tree_control AS SELECT a.id FROM geo_src a JOIN [skew|b.id(1)] geo_src b ON a.id=b.id");
        analyze("CREATE VIEW tree_control AS SELECT * FROM geo_src PIVOT (SUM(id) FOR wkt IN ('test'))");
    }

    private static void analyze(String statement) throws Exception {
        UtFrameUtils.parseStmtWithNewParser(statement, context);
    }

    private static void reject(String statement) {
        Exception error = assertThrows(Exception.class, () -> sql.ddl(statement), statement);
        Throwable cause = error instanceof AnalysisException ? error.getCause() : error;
        assertInstanceOf(SemanticException.class, cause, statement);
        assertTrue(cause.getMessage().contains(ERROR), () -> statement + ": " + cause.getMessage());
    }
}
