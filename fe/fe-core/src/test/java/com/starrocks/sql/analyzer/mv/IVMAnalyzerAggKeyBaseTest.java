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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the IVMAnalyzer rejects materialized views whose semantics would be silently broken
 * by CDC delta on cloud-native AGGREGATE KEY base tables.
 *
 * <p>Background: cloud-native CDC scans do not run AggregateIterator, so the delta stream is
 * raw pre-merge rowset rows; normal reads return the post-merge view. IVM consumers compute
 * state from raw events, so only consumers whose state-union is invariant to base merging
 * produce correct results. The constraints enforced here keep the supported envelope to
 * "strict rollup of the base" — GROUP BY ⊆ AGG_KEY columns plus per-column aggregate type
 * compatibility — so any accepted CREATE-MV is guaranteed to compute the same result the
 * equivalent SELECT-from-base would.
 */
public class IVMAnalyzerAggKeyBaseTest extends BookmarkTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        // BookmarkTestBase's @BeforeAll has already booted the SHARED_DATA cluster and
        // created DB_NAME via JUnit 5 lifecycle inheritance; we just register the fixture
        // tables on top.
        // Single-column key, single SUM value column — the baseline AGG_KEYS table.
        createTableStatic("CREATE TABLE base_agg ("
                + "    k INT NOT NULL,"
                + "    v BIGINT SUM"
                + ") AGGREGATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        // Multi-column key, mixed agg types to exercise R2 (subset GROUP BY) and R4 (per-
        // column aggregate matching) at once.
        createTableStatic("CREATE TABLE base_agg_multi ("
                + "    region VARCHAR(8) NOT NULL,"
                + "    user_id BIGINT NOT NULL,"
                + "    amount BIGINT SUM,"
                + "    max_score INT MAX,"
                + "    min_score INT MIN,"
                + "    last_action VARCHAR(32) REPLACE"
                + ") AGGREGATE KEY(region, user_id) "
                + "DISTRIBUTED BY HASH(region) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        // Plain DUP table on the same cluster so JOIN-with-AGG cases have a non-AGG side.
        createTableStatic("CREATE TABLE base_dup ("
                + "    k INT NOT NULL,"
                + "    payload VARCHAR(32)"
                + ") DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        // BITMAP_UNION value column: delta-mergeable, but BITMAP_UNION is not in IVM's supported
        // aggregate set, so the AGG-base whitelist must not advertise it.
        createTableStatic("CREATE TABLE base_agg_bitmap ("
                + "    k INT NOT NULL,"
                + "    tags BITMAP BITMAP_UNION"
                + ") AGGREGATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
    }

    /** Static peer of {@link BookmarkTestBase#createTable} so @BeforeAll can register tables. */
    private static void createTableStatic(String ddl) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }

    /**
     * Bug A (projection MV on AGG base): MV stores the raw delta rows verbatim, so the same
     * AGG key produces duplicate rows in the MV while base merges them to one row.
     * IVMAnalyzer must reject the CREATE because no GROUP BY means no rollup, and there is
     * no append-delta semantics for AGG base.
     */
    @Test
    public void testRejectProjectionMvOnAggBase() {
        String ddl = "CREATE MATERIALIZED VIEW mv "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k, v FROM " + DB_NAME + ".base_agg";

        SemanticException ex = assertThrows(SemanticException.class,
                () -> runIvmAnalyzer(ddl),
                "projection MV on AGGREGATE KEY base must be rejected");
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * Column classification must follow the resolved base column, not the written name: a table
     * alias on a value-column predicate (`t.v`) must still be rejected.
     */
    @Test
    public void testRejectWhereOnValueColumnViaTableAlias() {
        String ddl = "CREATE MATERIALIZED VIEW mv_alias_where "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT t.k, SUM(t.v) FROM " + DB_NAME + ".base_agg t WHERE t.v > 10 GROUP BY t.k";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * A GROUP BY that references a SELECT-list alias of a key column is a valid rollup; the
     * validator must resolve it to the underlying key column and accept it.
     */
    @Test
    public void testAcceptGroupByKeyColumnAlias() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_alias_groupby "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k AS g, SUM(v) AS s FROM " + DB_NAME + ".base_agg GROUP BY g";
        runIvmAnalyzer(ddl);
    }

    /**
     * GROUP BY on key columns with no aggregate is a DISTINCT-keys rollup: every group is one
     * AGG key, so the MV is one row per distinct key. The analyzer must encode the group keys
     * into __ROW_ID__ (QUERY_COMPUTED) so the refresh plan and MV schema agree on the row-id
     * type — accepting the MV rather than rejecting it.
     */
    @Test
    public void testAcceptDistinctKeyRollup() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_distinct_key "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k FROM " + DB_NAME + ".base_agg GROUP BY k";
        runIvmAnalyzer(ddl);
    }

    /**
     * Strict rollup is the supported envelope: GROUP BY on a key-column subset, MV aggregate
     * matches base column aggregation. This case must analyze successfully.
     */
    @Test
    public void testAcceptStrictRollup() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_strict_rollup "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k, SUM(v) AS s FROM " + DB_NAME + ".base_agg GROUP BY k";
        runIvmAnalyzer(ddl);
    }

    /**
     * SUM on a MAX-typed value column changes semantics: base merges on read with MAX, so
     * delta events carry raw values whose post-merge MAX may differ from their SUM.
     */
    @Test
    public void testRejectSumOnMaxTypedColumn() {
        String ddl = "CREATE MATERIALIZED VIEW mv_sum_on_max "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, user_id, SUM(max_score) FROM " + DB_NAME + ".base_agg_multi "
                + "GROUP BY region, user_id";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().toLowerCase().contains("aggregate"),
                "error should mention aggregate mismatch: " + ex.getMessage());
    }

    /**
     * COUNT(*) on AGG base counts raw delta events, not post-merge logical rows, so the MV
     * value diverges from the equivalent SELECT-from-base count.
     */
    @Test
    public void testRejectCountStarOnAggBase() {
        String ddl = "CREATE MATERIALIZED VIEW mv_count_star "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, COUNT(*) AS c FROM " + DB_NAME + ".base_agg_multi GROUP BY region";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * AVG = SUM / COUNT(*), so AVG inherits COUNT(*)'s incompatibility with AGG-base CDC.
     */
    @Test
    public void testRejectAvgOnAggBase() {
        String ddl = "CREATE MATERIALIZED VIEW mv_avg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, AVG(amount) FROM " + DB_NAME + ".base_agg_multi GROUP BY region";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * GROUP BY value columns lets each (key tuple, value) combination form its own MV group,
     * but the base merges per-key first — IVM would see far more groups than the base view.
     */
    @Test
    public void testRejectGroupByValueColumn() {
        String ddl = "CREATE MATERIALIZED VIEW mv_groupby_value "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, amount, SUM(amount) FROM " + DB_NAME + ".base_agg_multi "
                + "GROUP BY region, amount";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * REPLACE/REPLACE_IF_NOT_NULL is order-dependent in base merging, so delta replay can
     * pick the wrong "latest" value depending on rowset visit order.
     */
    @Test
    public void testRejectAggOnReplaceColumn() {
        String ddl = "CREATE MATERIALIZED VIEW mv_replace "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, MAX(last_action) FROM " + DB_NAME + ".base_agg_multi GROUP BY region";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * Aggregating a key column: base sees one logical row per AGG key, delta sees N events
     * per key, so SUM(key) over delta differs from SUM(key) over base.
     */
    @Test
    public void testRejectAggOnKeyColumn() {
        String ddl = "CREATE MATERIALIZED VIEW mv_agg_on_key "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, SUM(user_id) FROM " + DB_NAME + ".base_agg_multi GROUP BY region";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * A WHERE predicate on an AGG value column is evaluated on raw pre-merge delta rows, while
     * the base read applies it to the post-merge value. e.g. two raw rows v=6 are each dropped
     * by v>10, but the base merges them to v=12 and keeps the row — so the MV silently loses it.
     */
    @Test
    public void testRejectWhereOnValueColumn() {
        String ddl = "CREATE MATERIALIZED VIEW mv_where_value "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k, SUM(v) FROM " + DB_NAME + ".base_agg WHERE v > 10 GROUP BY k";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * A WHERE predicate on AGG key columns is safe: every raw row for a key carries that key
     * verbatim, so filtering raw rows by key equals filtering the post-merge view by key.
     */
    @Test
    public void testAcceptWhereOnKeyColumn() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_where_key "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT region, SUM(amount) FROM " + DB_NAME + ".base_agg_multi "
                + "WHERE region = 'CN' GROUP BY region";
        runIvmAnalyzer(ddl);
    }

    /**
     * BITMAP_UNION is delta-mergeable but not in IVM's supported aggregate set, so the AGG-base
     * whitelist must reject it rather than advertise support that checkAggregate later refuses.
     */
    @Test
    public void testRejectBitmapUnionNotSupportedByIvm() {
        String ddl = "CREATE MATERIALIZED VIEW mv_bitmap "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT k, BITMAP_UNION(tags) FROM " + DB_NAME + ".base_agg_bitmap GROUP BY k";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * Two AGG bases joined: the wholesale JOIN rejection must fire regardless of which side
     * holds an AGG base, so name-based column classification never runs in a multi-table FROM
     * (where two tables could share column names).
     */
    @Test
    public void testRejectJoinTwoAggBases() {
        String ddl = "CREATE MATERIALIZED VIEW mv_join_two_agg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT a.k, SUM(a.v) FROM " + DB_NAME + ".base_agg a "
                + "INNER JOIN " + DB_NAME + ".base_agg_multi b ON a.k = b.user_id "
                + "GROUP BY a.k";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("sole FROM source"),
                "error should reject the multi-table FROM: " + ex.getMessage());
    }

    /**
     * AGG base inside a JOIN is rejected wholesale in v1: the rollup invariant only holds
     * when the AGG base is the sole FROM source. Joining changes the row multiplicity.
     */
    @Test
    public void testRejectJoinWithAggBase() {
        String ddl = "CREATE MATERIALIZED VIEW mv_join_agg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT a.k, SUM(a.v) FROM " + DB_NAME + ".base_agg a "
                + "INNER JOIN " + DB_NAME + ".base_dup d ON a.k = d.k "
                + "GROUP BY a.k";
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("AGGREGATE KEY"),
                "error should mention AGGREGATE KEY: " + ex.getMessage());
    }

    /**
     * Runs IVMAnalyzer end-to-end on the given CREATE MV DDL, returning the analyzer result
     * (or throwing the SemanticException the production code throws).
     */
    private static void runIvmAnalyzer(String ddl) throws Exception {
        StatementBase parsed = SqlParser.parse(ddl,
                connectContext.getSessionVariable().getSqlMode()).get(0);
        assertTrue(parsed instanceof CreateMaterializedViewStatement,
                "expected CreateMaterializedViewStatement but got " + parsed.getClass().getSimpleName());
        CreateMaterializedViewStatement stmt = (CreateMaterializedViewStatement) parsed;
        QueryStatement qs = stmt.getQueryStatement();
        Analyzer.analyze(qs, connectContext);

        IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
        analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL);
    }
}
