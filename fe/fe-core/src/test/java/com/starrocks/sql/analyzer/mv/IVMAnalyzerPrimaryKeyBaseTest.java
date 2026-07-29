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
 * Verifies IVMAnalyzer rejects an incremental materialized view over a cloud-native PRIMARY KEY base
 * that has not enabled change data capture.
 *
 * <p>Incremental refresh over a PRIMARY KEY base reads the base's committed changes through CHANGES.
 * Those changes are recorded only while the base table property {@code enable_change_data_capture} is
 * on; at a version where it was off, CHANGES return nothing and the refresh silently produces the
 * wrong result. The analyzer therefore requires the property at CREATE time, so the user gets a clear
 * error instead of an opaque refresh-time failure. A DUPLICATE / AGGREGATE base is ungated (its
 * changes come from existing metadata) and is covered elsewhere.
 */
public class IVMAnalyzerPrimaryKeyBaseTest extends BookmarkTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        // BookmarkTestBase's @BeforeAll has already booted the SHARED_DATA cluster and created
        // DB_NAME; register the PRIMARY KEY fixtures on top.
        // Change data capture left at its default (off): every incremental MV over it must be rejected.
        createTableStatic("CREATE TABLE base_pk_no_cdc ("
                + "    id INT NOT NULL,"
                + "    val INT"
                + ") PRIMARY KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        // Change data capture enabled: the same MV shape must be accepted.
        createTableStatic("CREATE TABLE base_pk_cdc ("
                + "    id INT NOT NULL,"
                + "    val INT"
                + ") PRIMARY KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'enable_change_data_capture' = 'true');");
    }

    /** Static peer of {@link BookmarkTestBase#createTable} so @BeforeAll can register tables. */
    private static void createTableStatic(String ddl) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }

    /**
     * A PRIMARY KEY base without change data capture must be rejected at CREATE, and the error must
     * name both the offending table and the property so the user knows how to fix it.
     */
    @Test
    public void testRejectIncrementalMvOnPrimaryKeyBaseWithoutChangeDataCapture() {
        String ddl = "CREATE MATERIALIZED VIEW mv_pk_no_cdc "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, val FROM " + DB_NAME + ".base_pk_no_cdc";

        SemanticException ex = assertThrows(SemanticException.class,
                () -> runIvmAnalyzer(ddl),
                "incremental MV over a PRIMARY KEY base without change data capture must be rejected");
        assertTrue(ex.getMessage().contains("change data capture")
                        && ex.getMessage().contains("enable_change_data_capture"),
                "error should point at change data capture: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("base_pk_no_cdc"),
                "error should name the offending base table: " + ex.getMessage());
    }

    /**
     * The same MV shape over a PRIMARY KEY base that has change data capture enabled must analyze
     * successfully -- the gate keys on the property, not on the PRIMARY KEY type itself.
     */
    @Test
    public void testAcceptIncrementalMvWhenChangeDataCaptureEnabled() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_pk_cdc "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, val FROM " + DB_NAME + ".base_pk_cdc";
        runIvmAnalyzer(ddl);
    }

    /**
     * The gate checks every PRIMARY KEY base in the tree, not just the first: a join whose first base
     * has change data capture but whose second base does not must still be rejected, naming the second.
     */
    @Test
    public void testRejectWhenAJoinedPrimaryKeyBaseMissesChangeDataCapture() {
        String ddl = "CREATE MATERIALIZED VIEW mv_pk_join "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT a.id, a.val FROM " + DB_NAME + ".base_pk_cdc a "
                + "INNER JOIN " + DB_NAME + ".base_pk_no_cdc b ON a.id = b.id";

        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(ddl));
        assertTrue(ex.getMessage().contains("change data capture")
                        && ex.getMessage().contains("base_pk_no_cdc"),
                "error should name the joined base that missed change data capture: " + ex.getMessage());
    }

    /**
     * Runs IVMAnalyzer end-to-end on the given CREATE MV DDL: parse, analyze the query, then rewrite
     * for incremental maintenance (throwing the SemanticException the production code throws).
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
