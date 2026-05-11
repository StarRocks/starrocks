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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pinning tests that lock the contract between query shape (aggregate vs non-aggregate)
 * and the {@link RowIdStrategy} returned by {@link IVMAnalyzer#rewrite}.
 *
 * <p>These tests extend {@code MVIVMIcebergTestBase} so the Iceberg catalog
 * ({@code iceberg0}) with {@code unpartitioned_db.t0} and {@code unpartitioned_db.t_numeric}
 * is already registered before any test runs.
 */
public class IVMAnalyzerTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    // advanceTableVersionTo is abstract in MVIVMTestBase; these tests never refresh an MV so
    // the method body is intentionally empty.
    @Override
    public void advanceTableVersionTo(long toVersion) {
        // not needed for IVMAnalyzer unit tests
    }

    /**
     * An aggregate MV query must yield {@link RowIdStrategy#QUERY_COMPUTED}.
     *
     * <p>The analyzer rewrites the SELECT list to include {@code encode(group_by_keys)}
     * as {@code __ROW_ID__}; therefore the row-id is produced by the query itself.
     */
    @Test
    public void testAggregateQueryYieldsQueryComputedStrategy() throws Exception {
        // t_numeric has columns: id INT, c1 INT, c2 INT — good for numeric aggregation.
        String ddl = "CREATE MATERIALIZED VIEW mv_agg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, SUM(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id";

        CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
        QueryStatement qs = stmt.getQueryStatement();
        // Analyze the query so that table references and aggregate expressions are resolved.
        Analyzer.analyze(qs, connectContext);

        IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
        Optional<IVMAnalyzer.IVMAnalyzeResult> result =
                analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL);

        assertTrue(result.isPresent(), "aggregate MV query must produce an IVM rewrite result");
        assertEquals(RowIdStrategy.QUERY_COMPUTED, result.get().rowIdStrategy(),
                "aggregate MV must yield QUERY_COMPUTED: the query encodes group-by keys as __ROW_ID__");
    }

    /**
     * Distinct aggregates must be rejected at IVM analysis time; otherwise incremental
     * refresh would silently produce wrong data (the rewrite drops the DISTINCT flag).
     * MIN/MAX(DISTINCT) is not covered: the analyzer normalizes their DISTINCT away
     * earlier, so isDistinct() is already false here.
     */
    @Test
    public void testRejectDistinctAggregate() throws Exception {
        String[] ddls = {
                "CREATE MATERIALIZED VIEW mv_count_distinct "
                        + "REFRESH DEFERRED MANUAL "
                        + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                        + "AS SELECT id, COUNT(DISTINCT c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                "CREATE MATERIALIZED VIEW mv_sum_distinct "
                        + "REFRESH DEFERRED MANUAL "
                        + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                        + "AS SELECT id, SUM(DISTINCT c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
        };

        for (String ddl : ddls) {
            CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
            QueryStatement qs = stmt.getQueryStatement();
            Analyzer.analyze(qs, connectContext);

            IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
            SemanticException ex = assertThrows(SemanticException.class,
                    () -> analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL),
                    "INCREMENTAL refresh must reject distinct aggregates: " + ddl);
            assertTrue(ex.getMessage().contains("does not support distinct aggregate"),
                    "error message must mention distinct rejection, got: " + ex.getMessage());
        }
    }

    /**
     * {@code COUNT(*)} in an aggregate MV query must be accepted by IVMAnalyzer.
     *
     * <p>Regression: until the fix that allows 0-arg count combinators, {@code count(*)}
     * caused IVM rewrite to fail with {@code No matching function with signature: count_combine()}
     * because {@code AggStateUtils.isSupportedAggStateFunction} excluded the 0-arg count form
     * to protect AGG_STATE column DDL — but that DDL path is already blocked by the parser,
     * so the exclusion was redundant and only blocked IVM.
     */
    @Test
    public void testAggregateQueryWithCountStar() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_count_star "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, COUNT(*) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id";

        CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
        QueryStatement qs = stmt.getQueryStatement();
        Analyzer.analyze(qs, connectContext);

        IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
        Optional<IVMAnalyzer.IVMAnalyzeResult> result =
                analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL);

        assertTrue(result.isPresent(),
                "COUNT(*) aggregate MV must produce an IVM rewrite result");
        assertEquals(RowIdStrategy.QUERY_COMPUTED, result.get().rowIdStrategy(),
                "COUNT(*) aggregate MV must yield QUERY_COMPUTED");
    }

    /**
     * Aggregate-function whitelist: each (function, argument-type) combination listed in
     * {@code IVM_SUPPORTED_AGG_FUNCTIONS} must be accepted by the analyzer.
     */
    @Test
    public void testWhitelistAcceptsSupportedAggregates() throws Exception {
        // t_numeric: id INT, c1 INT, c2 INT — all numeric.
        String[] ddls = {
                // sum / avg over numeric
                "SELECT id, SUM(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                "SELECT id, AVG(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                // min / max over numeric
                "SELECT id, MIN(c1), MAX(c2) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                // count(col) — count(*) is exercised by testAggregateQueryWithCountStar above.
                "SELECT id, COUNT(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                // approx_count_distinct / ndv
                "SELECT id, APPROX_COUNT_DISTINCT(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                "SELECT id, NDV(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
        };

        for (String selectSql : ddls) {
            String ddl = "CREATE MATERIALIZED VIEW mv_wl "
                    + "REFRESH DEFERRED MANUAL "
                    + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                    + "AS " + selectSql;
            CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
            QueryStatement qs = stmt.getQueryStatement();
            Analyzer.analyze(qs, connectContext);

            IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
            Optional<IVMAnalyzer.IVMAnalyzeResult> result =
                    analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL);
            assertTrue(result.isPresent(), "whitelist must accept: " + selectSql);
        }
    }

    /**
     * Aggregate-function whitelist: combinations not on the list must be rejected at
     * CREATE time so the user sees a clear error instead of silently wrong data or a
     * refresh-time crash.
     */
    @Test
    public void testWhitelistRejectsUnsupportedAggregates() throws Exception {
        record Case(String selectSql, String expectedFragment) { }
        Case[] cases = {
                // Unknown function: array_agg is not on the whitelist.
                new Case("SELECT id, ARRAY_AGG(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id",
                        "does not support aggregate function: array_agg"),
                // Function on the whitelist but argument type not allowed: MIN(varchar).
                // t0 has columns: id INT, data STRING, date STRING.
                new Case("SELECT id, MIN(data) FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id",
                        "does not support min with argument types"),
        };

        for (Case c : cases) {
            String ddl = "CREATE MATERIALIZED VIEW mv_wl_neg "
                    + "REFRESH DEFERRED MANUAL "
                    + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                    + "AS " + c.selectSql;
            CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
            QueryStatement qs = stmt.getQueryStatement();
            Analyzer.analyze(qs, connectContext);

            IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
            SemanticException ex = assertThrows(SemanticException.class,
                    () -> analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL),
                    "whitelist must reject: " + c.selectSql);
            assertTrue(ex.getMessage().toLowerCase().contains(c.expectedFragment.toLowerCase()),
                    "error message must contain '" + c.expectedFragment + "', got: " + ex.getMessage());
        }
    }

    /**
     * A non-aggregate (scan-only) MV query over an append-only Iceberg table must yield
     * {@link RowIdStrategy#AUTO_INCREMENT}.
     *
     * <p>No {@code __ROW_ID__} expression is injected; the storage engine generates the
     * row-id via AUTO_INCREMENT at insert time.
     */
    @Test
    public void testNonAggregateQueryYieldsAutoIncrementStrategy() throws Exception {
        // t0 has columns: id INT, data STRING, date STRING.
        String ddl = "CREATE MATERIALIZED VIEW mv_scan "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`";

        CreateMaterializedViewStatement stmt = parseMvDdl(ddl);
        QueryStatement qs = stmt.getQueryStatement();
        // Analyze the query so that table references are resolved.
        Analyzer.analyze(qs, connectContext);

        IVMAnalyzer analyzer = new IVMAnalyzer(connectContext, stmt, qs);
        Optional<IVMAnalyzer.IVMAnalyzeResult> result =
                analyzer.rewrite(MaterializedView.RefreshMode.INCREMENTAL);

        assertTrue(result.isPresent(), "non-aggregate MV query must produce an IVM rewrite result");
        assertEquals(RowIdStrategy.AUTO_INCREMENT, result.get().rowIdStrategy(),
                "non-aggregate MV over append-only Iceberg must yield AUTO_INCREMENT");
    }

    /**
     * A non-aggregate incremental MV must be created as a PK table with an
     * AUTO_INCREMENT {@code __ROW_ID__} column (strategy = AUTO_INCREMENT).
     */
    @Test
    public void testNonAggregateIncrementalMvIsPrimaryKeyWithAutoIncrementRowId() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_nonagg_pk "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_nonagg_pk");

            // All incremental MVs are PK tables.
            assertEquals(KeysType.PRIMARY_KEYS, mv.getKeysType(),
                    "non-aggregate incremental MV must be a PRIMARY_KEYS table");

            // __ROW_ID__ column: BIGINT, AUTO_INCREMENT, hidden, key, not null
            Column rowIdCol = mv.getColumn(IvmOpUtils.COLUMN_ROW_ID);
            assertNotNull(rowIdCol, "__ROW_ID__ column must exist on non-agg incremental MV");
            assertEquals(IntegerType.BIGINT, rowIdCol.getType(),
                    "__ROW_ID__ must be BIGINT for AUTO_INCREMENT strategy");
            assertTrue(rowIdCol.isAutoIncrement(), "__ROW_ID__ must be AUTO_INCREMENT");
            assertTrue(rowIdCol.isKey(), "__ROW_ID__ must be a PK column");
            assertFalse(rowIdCol.isAllowNull(), "__ROW_ID__ must be NOT NULL");

            // Strategy persisted on MV
            assertEquals(RowIdStrategy.AUTO_INCREMENT, mv.getRowIdStrategy(),
                    "non-aggregate MV must persist AUTO_INCREMENT strategy");
        });
    }

    /**
     * An aggregate incremental MV must also be a PK table but with QUERY_COMPUTED
     * strategy — {@code __ROW_ID__} comes from the query's encode(group_by_keys)
     * expression and is NOT AUTO_INCREMENT.
     */
    @Test
    public void testAggregateIncrementalMvStillUsesQueryComputedRowId() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_agg_pk "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, SUM(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_agg_pk");

            assertEquals(KeysType.PRIMARY_KEYS, mv.getKeysType(),
                    "aggregate incremental MV must still be a PRIMARY_KEYS table");

            Column rowIdCol = mv.getColumn(IvmOpUtils.COLUMN_ROW_ID);
            assertNotNull(rowIdCol, "__ROW_ID__ must exist on aggregate incremental MV");
            assertFalse(rowIdCol.isAutoIncrement(),
                    "aggregate MV's __ROW_ID__ is QUERY_COMPUTED (encode), not AUTO_INCREMENT");

            assertEquals(RowIdStrategy.QUERY_COMPUTED, mv.getRowIdStrategy(),
                    "aggregate MV must persist QUERY_COMPUTED strategy");
        });
    }

    /**
     * Non-aggregate incremental MV (AUTO_INCREMENT): refresh SQL must use an explicit
     * column list that omits {@code __ROW_ID__} so the storage engine auto-fills it.
     */
    @Test
    public void testGetIVMTaskDefinitionOmitsAutoIncrementColumn() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_taskdef_nonagg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_taskdef_nonagg");
            String sql = mv.getIVMTaskDefinition();

            assertTrue(sql.startsWith("INSERT INTO `mv_taskdef_nonagg` ("),
                    "must use explicit column list form, got: " + sql);
            assertFalse(sql.contains("`" + IvmOpUtils.COLUMN_ROW_ID + "`"),
                    "AUTO_INCREMENT __ROW_ID__ must NOT be in the column list, got: " + sql);
            assertTrue(sql.contains("`id`") && sql.contains("`data`") && sql.contains("`date`"),
                    "visible columns must all be in the column list, got: " + sql);
        });
    }

    /**
     * Aggregate incremental MV (QUERY_COMPUTED): refresh SQL uses positional form since
     * the schema has no AUTO_INCREMENT columns; the query itself produces {@code __ROW_ID__}
     * via encode(group_by_keys).
     */
    @Test
    public void testGetIVMTaskDefinitionForQueryComputedUsesPositionalForm() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_taskdef_agg "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, SUM(c1) FROM `iceberg0`.`unpartitioned_db`.`t_numeric` GROUP BY id";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_taskdef_agg");
            String sql = mv.getIVMTaskDefinition();

            assertTrue(sql.startsWith("INSERT INTO `mv_taskdef_agg` "),
                    "aggregate MV uses positional INSERT, got: " + sql);
            assertFalse(sql.startsWith("INSERT INTO `mv_taskdef_agg` ("),
                    "no explicit column list expected (no AUTO_INCREMENT columns), got: " + sql);
            assertTrue(sql.contains(IvmOpUtils.COLUMN_ROW_ID),
                    "__ROW_ID__ must appear in the SELECT alias (produced by encode), got: " + sql);
        });
    }

    /**
     * When user ORDER BY reorders the MV schema, the INSERT column list must still follow
     * the query SELECT order (not the physical sort-key order). Otherwise refresh writes
     * values into the wrong target columns (e.g. id's value into data's slot).
     */
    @Test
    public void testGetIVMTaskDefinitionPreservesQueryOrderWhenReordered() throws Exception {
        // Schema after PR-B + ORDER BY (data):
        //   physical:          [__ROW_ID__ (PK), data (sort key), id, date]
        //   query projection:  [id, data, date]
        // Correct INSERT column list:  (id, data, date)  ← query SELECT order
        // Buggy pre-fix would emit:    (data, id, date)  ← schema sort-key order
        String ddl = "CREATE MATERIALIZED VIEW mv_reordered_nonagg "
                + "ORDER BY (`data`) "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_reordered_nonagg");
            String sql = mv.getIVMTaskDefinition();

            assertTrue(sql.startsWith("INSERT INTO `mv_reordered_nonagg` ("),
                    "must use explicit column list form, got: " + sql);
            assertFalse(sql.contains("`" + IvmOpUtils.COLUMN_ROW_ID + "`"),
                    "AUTO_INCREMENT __ROW_ID__ must NOT be in the column list, got: " + sql);

            // All three user columns must appear; their relative order must match the
            // SELECT list (id, then data, then date), not the physical schema order
            // (which would put the sort key `data` first).
            int idPos = sql.indexOf("`id`");
            int dataPos = sql.indexOf("`data`");
            int datePos = sql.indexOf("`date`");
            assertTrue(idPos > 0 && dataPos > 0 && datePos > 0,
                    "all three user columns must be in the list, got: " + sql);
            assertTrue(idPos < dataPos && dataPos < datePos,
                    "INSERT column list must be in SELECT order (id, data, date), got: " + sql);
        });
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /**
     * Parse a {@code CREATE MATERIALIZED VIEW} DDL string and return the AST node
     * without running full semantic analysis (which would invoke MaterializedViewAnalyzer
     * and trigger the full MV-creation pipeline).
     */
    private static CreateMaterializedViewStatement parseMvDdl(String ddl) {
        StatementBase stmt = SqlParser.parse(ddl,
                connectContext.getSessionVariable().getSqlMode()).get(0);
        assertTrue(stmt instanceof CreateMaterializedViewStatement,
                "expected CreateMaterializedViewStatement but got " + stmt.getClass().getSimpleName());
        return (CreateMaterializedViewStatement) stmt;
    }
}
