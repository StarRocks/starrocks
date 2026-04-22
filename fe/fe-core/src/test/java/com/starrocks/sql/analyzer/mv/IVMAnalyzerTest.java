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
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
