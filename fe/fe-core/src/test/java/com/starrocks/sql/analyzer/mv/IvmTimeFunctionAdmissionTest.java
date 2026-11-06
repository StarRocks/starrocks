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
import com.starrocks.common.Config;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.CreateFunctionAnalyzer;
import com.starrocks.sql.analyzer.DropStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IvmTimeFunctionAdmissionTest extends MVIVMIcebergTestBase {
    private static final String T0 = "`iceberg0`.`unpartitioned_db`.`t0`";
    private static final String T_NUMERIC = "`iceberg0`.`unpartitioned_db`.`t_numeric`";
    private static final String REJECTED = "Incremental refresh does not support";

    private static boolean timeFunctionCheckDisabled;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
        // no refresh runs in these tests
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT id, data FROM " + T0 + " WHERE CAST(date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY)",
            "SELECT id, now() AS seen_at FROM " + T0,
            "SELECT id, MAX(timestampdiff(SECOND, CAST(date AS DATETIME), now())) AS age FROM " + T0
                    + " GROUP BY id",
            "SELECT t0.id, t_numeric.c1 FROM " + T0 + " t0 JOIN " + T_NUMERIC + " t_numeric"
                    + " ON t0.id = t_numeric.id AND CAST(t0.date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY)",
            "SELECT x.id, COUNT(*) AS cnt FROM (SELECT id FROM " + T0
                    + " WHERE CAST(date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY)) x GROUP BY x.id",
            "SELECT id, data FROM " + T0 + " WHERE CAST(date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY)"
                    + " UNION ALL SELECT id, data FROM " + T0 + " WHERE id > 100",
            "SELECT id, date(now()) AS d, COUNT(*) AS cnt FROM " + T0 + " GROUP BY id, date(now())",
            "SELECT id, COUNT(*) AS cnt FROM " + T0 + " GROUP BY id HAVING id < unix_timestamp(now())",
    })
    public void testIncrementalRejectsCurrentTimeInAnyPosition(String query) {
        SemanticException e = assertThrows(SemanticException.class,
                () -> analyzeMvDdl(mvDdl("mv_time_position", "incremental", query)));
        assertTrue(e.getMessage().contains(REJECTED + " now()"), e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"now()", "now(3)", "current_timestamp()", "CURRENT_TIMESTAMP", "localtime()",
            "localtimestamp()", "current_date()", "CURRENT_DATE", "curdate()", "current_time()", "curtime()",
            "unix_timestamp()", "utc_timestamp()", "utc_time()"})
    public void testIncrementalRejectsEveryClockFunction(String function) {
        String query = "SELECT id, " + function + " AS t FROM " + T0;
        SemanticException e = assertThrows(SemanticException.class,
                () -> analyzeMvDdl(mvDdl("mv_time_function", "incremental", query)));
        assertTrue(e.getMessage().contains(REJECTED), e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT id, unix_timestamp(CAST(date AS DATETIME)) AS ts FROM " + T0,
            "SELECT id, from_unixtime(id) AS ts FROM " + T0,
            "SELECT id, date_trunc('day', CAST(date AS DATETIME)) AS d FROM " + T0,
    })
    public void testIncrementalAcceptsTimeConversionOfColumns(String query) throws Exception {
        CreateMaterializedViewStatement stmt = analyzeMvDdl(mvDdl("mv_time_conversion", "incremental", query));
        assertEquals(MaterializedView.RefreshMode.INCREMENTAL, stmt.getAnalyzedRefreshMode());
    }

    @Test
    public void testIncrementalRejectsSqlFunctionThatReadsTheClock() throws Exception {
        boolean enableUdf = Config.enable_udf;
        Config.enable_udf = true;
        try {
            createFunction("CREATE FUNCTION clock_fn() RETURNS now()");
            createFunction("CREATE FUNCTION nested_clock_fn() RETURNS clock_fn()");
            for (String function : new String[] {"clock_fn()", "nested_clock_fn()"}) {
                String query = "SELECT id, " + function + " AS t FROM " + T0;
                SemanticException e = assertThrows(SemanticException.class,
                        () -> analyzeMvDdl(mvDdl("mv_time_sql_udf", "incremental", query)));
                assertTrue(e.getMessage().contains(REJECTED + " " + function), e.getMessage());
            }
        } finally {
            dropFunction("DROP FUNCTION IF EXISTS nested_clock_fn()");
            dropFunction("DROP FUNCTION IF EXISTS clock_fn()");
            Config.enable_udf = enableUdf;
        }
    }

    @Test
    public void testIncrementalAcceptsUdfNamedLikeTimeFunction() throws Exception {
        boolean enableUdf = Config.enable_udf;
        Config.enable_udf = true;
        try {
            createFunction("CREATE FUNCTION curdate(x INT) RETURNS x + 1");
            String query = "SELECT id, curdate(id) AS d FROM " + T0;
            CreateMaterializedViewStatement stmt = analyzeMvDdl(mvDdl("mv_time_named_udf", "incremental", query));
            assertEquals(MaterializedView.RefreshMode.INCREMENTAL, stmt.getAnalyzedRefreshMode());
        } finally {
            dropFunction("DROP FUNCTION IF EXISTS curdate(INT)");
            Config.enable_udf = enableUdf;
        }
    }

    @Test
    public void testAutoFallsBackToPctWithTheTimeFunctionAsReason() throws Exception {
        String query = "SELECT id, COUNT(*) AS cnt FROM " + T0
                + " WHERE CAST(date AS DATETIME) >= date_sub(current_timestamp(), INTERVAL 1 DAY) GROUP BY id";
        starRocksAssert.withMaterializedView(mvDdl("mv_time_auto", "auto", query), () -> {
            MaterializedView mv = getMv("test", "mv_time_auto");
            assertEquals(MaterializedView.RefreshMode.AUTO, mv.getRefreshMode());
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
            assertNotNull(mv.getCurrentRefreshModeReason());
            assertTrue(mv.getCurrentRefreshModeReason().toLowerCase().contains(
                    REJECTED.toLowerCase() + " current_timestamp()"), mv.getCurrentRefreshModeReason());
        });
    }

    @Test
    public void testPctAcceptsCurrentTime() throws Exception {
        String query = "SELECT id, COUNT(*) AS cnt FROM " + T0
                + " WHERE CAST(date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY) GROUP BY id";
        CreateMaterializedViewStatement stmt = analyzeMvDdl(mvDdl("mv_time_pct", "pct", query));
        assertEquals(MaterializedView.RefreshMode.PCT, stmt.getAnalyzedRefreshMode());
    }

    @Test
    public void testExistingViewWithCurrentTimeStillActivatesAndRefreshes() throws Exception {
        new MockUp<AnalyzerUtils>() {
            @Mock
            public Optional<FunctionCallExpr> findCurrentTimeFunction(Invocation invocation, ParseNode node) {
                if (timeFunctionCheckDisabled) {
                    return Optional.empty();
                }
                return invocation.proceed(node);
            }
        };
        String query = "SELECT id, COUNT(*) AS cnt FROM " + T0
                + " WHERE CAST(date AS DATETIME) >= date_sub(now(), INTERVAL 1 DAY) GROUP BY id";
        timeFunctionCheckDisabled = true;
        try {
            starRocksAssert.withMaterializedView(mvDdl("mv_time_existing", "incremental", query), () -> {
                timeFunctionCheckDisabled = false;
                MaterializedView mv = getMv("test", "mv_time_existing");
                assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());

                assertThrows(SemanticException.class,
                        () -> analyzeMvDdl(mvDdl("mv_time_new", "incremental", query)));
                assertThrows(SemanticException.class,
                        () -> analyzeMvDdl(mvDdl("mv_time_existing", "incremental", query)));

                starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_time_existing INACTIVE");
                assertFalse(mv.isActive());
                starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_time_existing ACTIVE");
                assertTrue(mv.isActive(), "inactive reason: " + mv.getInactiveReason());
                assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());

                assertDoesNotThrow(() -> IvmRefreshDefinition.derive(connectContext, mv));
                assertThrows(SemanticException.class,
                        () -> analyzeMvDdl(mvDdl("mv_time_after_activate", "incremental", query)));
            });
        } finally {
            timeFunctionCheckDisabled = false;
        }
    }

    private static void createFunction(String sql) throws Exception {
        CreateFunctionStmt stmt = (CreateFunctionStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                sql, connectContext);
        new CreateFunctionAnalyzer().analyze(stmt, connectContext);
        DDLStmtExecutor.execute(stmt, connectContext);
    }

    private static void dropFunction(String sql) throws Exception {
        DropFunctionStmt stmt = (DropFunctionStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                sql, connectContext);
        DropStmtAnalyzer.analyze(stmt, connectContext);
        DDLStmtExecutor.execute(stmt, connectContext);
    }

    private static String mvDdl(String name, String refreshMode, String query) {
        return "CREATE MATERIALIZED VIEW " + name + " REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"" + refreshMode + "\") AS " + query;
    }

    private static CreateMaterializedViewStatement analyzeMvDdl(String ddl) {
        StatementBase stmt = SqlParser.parse(ddl, connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(stmt, connectContext);
        return (CreateMaterializedViewStatement) stmt;
    }
}
