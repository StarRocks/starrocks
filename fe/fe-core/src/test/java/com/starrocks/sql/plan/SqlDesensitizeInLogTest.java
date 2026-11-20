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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlan;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * With enable_sql_desensitize_in_log on, literal values must not reach the audit log, query detail or profile.
 */
public class SqlDesensitizeInLogTest extends PlanTestBase {
    private boolean prevDesensitize;

    @BeforeEach
    public void setUp() {
        prevDesensitize = Config.enable_sql_desensitize_in_log;
        connectContext.getSessionVariable().setEnableDesensitizeExplain(false);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_sql_desensitize_in_log = prevDesensitize;
        connectContext.getSessionVariable().setEnableDesensitizeExplain(false);
        connectContext.setQueryDetail(null);
    }

    private StatementBase analyze(String sql) throws Exception {
        return UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    private StatementBase parseOnly(String sql) throws Exception {
        return UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
    }

    private String auditStmt(String sql, StatementBase stmt) {
        return Deencapsulation.invoke(new ConnectProcessor(connectContext), "formatStmt", sql, stmt);
    }

    private static void assertNoLeak(String text, String... secrets) {
        for (String secret : secrets) {
            Assertions.assertFalse(text.contains(secret), "leaked '" + secret + "' in: " + text);
        }
    }

    // ---------------- A: generated column names ----------------

    @Test
    public void testGeneratedSelectAliasIsNotLogged() throws Exception {
        String[] sqls = {
                "SELECT 'SECRET_A1'",
                "SELECT concat(v1, 'SECRET_A2') FROM t0",
                "SELECT v1 = 'SECRET_A3' FROM t0",
                "SELECT v1 + 987654321 FROM t0",
        };
        for (String sql : sqls) {
            String text = AstToSQLBuilder.toDesensitizedSQL(analyze(sql));
            assertNoLeak(text, "SECRET_A", "987654321");
            Assertions.assertTrue(text.contains("?"), text);
        }
    }

    @Test
    public void testExplicitSelectAliasIsKept() throws Exception {
        String text = AstToSQLBuilder.toDesensitizedSQL(analyze("SELECT concat(v1, 'SECRET_A4') AS my_alias FROM t0"));
        assertNoLeak(text, "SECRET_A4");
        Assertions.assertTrue(text.contains("my_alias"), text);
    }

    @Test
    public void testCteColumnNamesAreNotLogged() throws Exception {
        String[] sqls = {
                "WITH c AS (SELECT 'SECRET_C1') SELECT * FROM c",
                "WITH c(x) AS (SELECT 'SECRET_C2') SELECT x FROM c",
                "SELECT * FROM (SELECT 'SECRET_C3') s",
        };
        for (String sql : sqls) {
            // query detail formats before analysis, audit and profile after it; neither may fall back to a placeholder
            for (StatementBase stmt : new StatementBase[] {parseOnly(sql), analyze(sql)}) {
                String text = AstToSQLBuilder.toDesensitizedSQL(stmt);
                assertNoLeak(text, "SECRET_C");
                Assertions.assertTrue(text.startsWith("WITH") || text.startsWith("SELECT"), text);
            }
        }
    }

    @Test
    public void testAuditAndProfileUseDesensitizedSql() throws Exception {
        Config.enable_sql_desensitize_in_log = true;
        String sql = "SELECT concat(v1, 'SECRET_A5') FROM t0";
        StatementBase stmt = analyze(sql);
        assertNoLeak(auditStmt(sql, stmt), "SECRET_A5");

        StmtExecutor executor = new StmtExecutor(connectContext, stmt);
        RuntimeProfile profile = Deencapsulation.invoke(executor, "buildTopLevelProfile");
        assertNoLeak(profile.getChild("Summary").getInfoString(ProfileManager.SQL_STATEMENT), "SECRET_A5");
    }

    @Test
    public void testDigestIsUnchanged() throws Exception {
        // toDigest() feeds the audit Digest MD5; its text must not change.
        Assertions.assertEquals("SELECT concat(`v1`, ?) FROM `test`.`t0`",
                AstToSQLBuilder.toDigest(analyze("SELECT concat(v1, 'x') FROM t0")));
        Assertions.assertEquals("WITH `c` (`'x'`) AS (SELECT ? FROM VALUES(NULL)) SELECT * FROM `c`",
                AstToSQLBuilder.toDigest(analyze("WITH c AS (SELECT 'x') SELECT * FROM c")));
    }

    // ---------------- D: credential statements still digest plain literals ----------------

    @Test
    public void testCredentialStatementStillDigestsLiterals() throws Exception {
        Config.enable_sql_desensitize_in_log = true;
        String sql = "SELECT 'SECRET_D1' AS x FROM FILES(\"path\"=\"s3://bucket/data.parquet\", "
                + "\"aws.s3.secret_key\"=\"SECRET_KEY_D\")";
        String text = auditStmt(sql, parseOnly(sql));
        assertNoLeak(text, "SECRET_D1", "SECRET_KEY_D");
    }

    @Test
    public void testUnformattableStatementGetsTypedPlaceholder() throws Exception {
        Config.enable_sql_desensitize_in_log = true;
        // no formatter handles CREATE REPOSITORY; its location and properties are literals too
        String sql = "CREATE REPOSITORY repo1 WITH BROKER ON LOCATION \"s3://bucket/SECRET_PATH\" "
                + "PROPERTIES (\"aws.s3.secret_key\" = \"SECRET_KEY\")";
        Assertions.assertEquals("this is a desensitized sql (CreateRepositoryStmt)", auditStmt(sql, parseOnly(sql)));
    }

    // ---------------- B: plan text ----------------

    @Test
    public void testDesensitizeExplainCoversVerboseAndCosts() throws Exception {
        connectContext.getSessionVariable().setEnableDesensitizeExplain(true);
        String sql = "SELECT v1 + 987654321, concat(v2, 'SECRET_B1') FROM t0";
        for (TExplainLevel level : new TExplainLevel[] {TExplainLevel.VERBOSE, TExplainLevel.COSTS}) {
            String plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(level);
            assertNoLeak(plan, "987654321", "SECRET_B1");
        }
    }

    @Test
    public void testExplainUnchangedWhenDesensitizeOff() throws Exception {
        Config.enable_sql_desensitize_in_log = false;
        String sql = "SELECT v1 + 987654321 FROM t0";
        String plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.VERBOSE);
        Assertions.assertTrue(plan.contains("987654321"), plan);
    }

    @Test
    public void testQueryDetailExplainIsDesensitized() throws Exception {
        Config.enable_sql_desensitize_in_log = true;
        connectContext.setQueryDetail(new QueryDetail());
        UtFrameUtils.getPlanAndFragment(connectContext, "SELECT concat(v2, 'SECRET_B2') FROM t0");
        String explain = connectContext.getQueryDetail().getExplain();
        Assertions.assertNotNull(explain);
        Assertions.assertTrue(explain.contains("PLAN FRAGMENT"), explain);
        assertNoLeak(explain, "SECRET_B2");
        Assertions.assertFalse(connectContext.getSessionVariable().isEnableDesensitizeExplain());
    }

    private String explainIntoQueryDetail(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        connectContext.setQueryDetail(new QueryDetail());
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        new StmtExecutor(connectContext, stmt).execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType(),
                connectContext.getState().getErrorMessage());
        return connectContext.getQueryDetail().getExplain();
    }

    @Test
    public void testExplainDmlKeepsDesensitizedPlanInQueryDetail() throws Exception {
        Config.enable_sql_desensitize_in_log = true;
        String[] sqls = {
                "EXPLAIN UPDATE tprimary SET v1 = 'SECRET_U1' WHERE pk = 987654321",
                "EXPLAIN DELETE FROM tprimary WHERE pk = 987654321",
        };
        for (String sql : sqls) {
            String explain = explainIntoQueryDetail(sql);
            Assertions.assertNotNull(explain, sql);
            Assertions.assertTrue(explain.contains("PLAN FRAGMENT"), explain);
            assertNoLeak(explain, "SECRET_U1", "987654321");
        }
        // DELETE on a non-primary-key table is run by DeleteHandler and has no plan
        Assertions.assertEquals("PLAN NOT AVAILABLE\n", explainIntoQueryDetail("EXPLAIN DELETE FROM t0 WHERE v1 = 987654321"));
    }

    @Test
    public void testExplainDmlQueryDetailUnchangedWhenOff() throws Exception {
        Config.enable_sql_desensitize_in_log = false;
        // the client's plan is written back as before
        String explain = explainIntoQueryDetail("EXPLAIN UPDATE tprimary SET v1 = 'SECRET_U2' WHERE pk = 1");
        Assertions.assertTrue(explain.contains("SECRET_U2"), explain);
    }

    @Test
    public void testExplainWithoutPlanningGetsPlaceholderInQueryDetail() throws Exception {
        // EXPLAIN on a recursive CTE returns before planning, so no desensitized plan is recorded
        String sql = "EXPLAIN WITH RECURSIVE c AS (SELECT 1 AS n, 'SECRET_R1' UNION ALL "
                + "SELECT n + 1, 'x' FROM c WHERE n < 3) SELECT * FROM c";
        boolean prevRecursiveCte = connectContext.getSessionVariable().isEnableRecursiveCTE();
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        try {
            Config.enable_sql_desensitize_in_log = true;
            Assertions.assertEquals("PLAN NOT AVAILABLE\n", explainIntoQueryDetail(sql));
            Config.enable_sql_desensitize_in_log = false;
            // the client's explain is written back as before
            String explain = explainIntoQueryDetail(sql);
            Assertions.assertTrue(explain.contains("SECRET_R1"), explain);
        } finally {
            connectContext.getSessionVariable().setEnableRecursiveCTE(prevRecursiveCte);
        }
    }

    // ---------------- B: display strings sent to BE for the operator profile ----------------

    private static List<TPlan> toThrift(ExecPlan plan) {
        List<TPlan> plans = new ArrayList<>();
        for (PlanFragment fragment : plan.getFragments()) {
            plans.add(fragment.getPlanRoot().treeToThrift());
        }
        return plans;
    }

    // sql_* string fields of each node's sub-struct are display-only (BE puts them into the operator profile);
    // collects them and, when strip is set, clears them.
    private static List<String> displayStrings(List<TPlan> plans, boolean strip) throws Exception {
        List<String> values = new ArrayList<>();
        for (TPlan plan : plans) {
            for (TPlanNode node : plan.getNodes()) {
                for (Field nodeField : TPlanNode.class.getFields()) {
                    Object sub = Modifier.isStatic(nodeField.getModifiers()) ? null : nodeField.get(node);
                    if (sub == null || !sub.getClass().getName().startsWith("com.starrocks.thrift.")) {
                        continue;
                    }
                    for (Field f : sub.getClass().getFields()) {
                        if (f.getType() == String.class && !Modifier.isStatic(f.getModifiers())
                                && f.getName().contains("sql_") && f.get(sub) != null) {
                            values.add((String) f.get(sub));
                            if (strip) {
                                f.set(sub, null);
                            }
                        }
                    }
                }
            }
        }
        return values;
    }

    @Test
    public void testPlanThriftDisplayStringsAreDesensitized() throws Exception {
        String sql = "SELECT t0.v2, count(*) FROM t0 JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 + t1.v5 > 987654321 "
                + "WHERE t0.v3 = 123456789 GROUP BY t0.v2 ORDER BY t0.v2";
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        List<TPlan> off = toThrift(plan);
        Assertions.assertTrue(String.join("|", displayStrings(off, false)).contains("123456789"));

        Config.enable_sql_desensitize_in_log = true;
        List<TPlan> on = toThrift(plan);
        String shown = String.join("|", displayStrings(on, false));
        assertNoLeak(shown, "123456789", "987654321");
        Assertions.assertTrue(shown.contains("?"), shown);

        // everything BE executes is unchanged
        displayStrings(off, true);
        displayStrings(on, true);
        Assertions.assertEquals(off, on);
    }

    @Test
    public void testFormatterFailureLogIsDesensitized() throws Exception {
        StatementBase stmt = parseOnly("SELECT 1");
        new MockUp<AST2SQLVisitor>() {
            @Mock
            public AST2SQLVisitor withOptions(FormatOptions options) {
                throw new IllegalStateException("SECRET_FMT");
            }
        };
        List<String> lines = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("desensitize-capture-" + UUID.randomUUID(), null, null) {
            @Override
            public void append(LogEvent event) {
                StringBuilder text = new StringBuilder(event.getMessage().getFormattedMessage());
                for (Throwable t = event.getThrown(); t != null; t = t.getCause()) {
                    text.append(' ').append(t.getMessage());
                }
                lines.add(text.toString());
            }
        };
        appender.start();
        Logger logger = (Logger) LogManager.getLogger(AstToSQLBuilder.class);
        Level prevLevel = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);
        try {
            Config.enable_sql_desensitize_in_log = true;
            Assertions.assertEquals("this is a desensitized sql", AstToSQLBuilder.toDesensitizedSQL(stmt));
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(prevLevel);
        }
        Assertions.assertFalse(lines.isEmpty());
        for (String line : lines) {
            assertNoLeak(line, "SECRET_FMT");
        }
    }

    // ---------------- E: planning and execution failure logs ----------------

    private static Exception planningFailure;

    // a timer name, a var and a reason in the planner trace can each quote the query, e.g. Iceberg split timers
    private static void traceSecret() {
        Tracers.record("failureValue", "SECRET_TRACE");
        Tracers.reasoning(Tracers.Module.OPTIMIZER, "SECRET_TRACE");
        try (Timer ignored = Tracers.watchScope("SECRET_TRACE")) {
            // only the timer's name matters
        }
    }

    // Executes the statement and returns the StmtExecutor log lines, each with its throwable rendered as log4j does.
    private List<String> executeAndCaptureLogs(String sql) throws Exception {
        List<String> lines = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("stmt-executor-capture-" + UUID.randomUUID(), null, null) {
            @Override
            public void append(LogEvent event) {
                String line = event.getMessage().getFormattedMessage();
                if (event.getThrown() != null) {
                    line += "\n" + new ThrowableProxy(event.getThrown()).getExtendedStackTraceAsString();
                }
                lines.add(line);
            }
        };
        appender.start();
        Logger logger = (Logger) LogManager.getLogger(StmtExecutor.class);
        Level prevLevel = logger.getLevel();
        Tracers prevTracers = Tracers.get();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            Tracers.register(connectContext);
            Tracers.init(Tracers.Mode.TIMER, Tracers.Module.ALL, true, false);
            connectContext.setQueryId(UUIDUtil.genUUID());
            StmtExecutor executor = new StmtExecutor(connectContext, parseOnly(sql));
            executor.setProxy();
            try {
                executor.execute();
            } catch (LargeInPredicateException e) {
                // rethrown for the caller to retry from the parser
            }
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(prevLevel);
            Tracers.set(prevTracers);
            connectContext.setState(new QueryState());
        }
        return lines;
    }

    @Test
    public void testPlanningFailureLogsAreDesensitized() throws Exception {
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase stmt, ConnectContext session) throws Exception {
                traceSecret();
                throw planningFailure;
            }
        };
        List<Exception> failures = List.of(
                new SemanticException("SECRET_ERR"),
                new StarRocksPlannerException("SECRET_ERR", ErrorType.INTERNAL_ERROR),
                new StarRocksPlannerException("SECRET_ERR", ErrorType.USER_ERROR),
                new IOException("SECRET_ERR"),
                new LargeInPredicateException("SECRET_ERR"),
                new StarRocksException("SECRET_ERR"),
                new IllegalStateException("SECRET_ERR"));
        for (Exception failure : failures) {
            failure.initCause(new RuntimeException("SECRET_CAUSE"));
        }
        boolean prevDumpTrace = Config.enable_dump_optimizer_trace_on_error;
        try {
            for (boolean dumpTrace : new boolean[] {true, false}) {
                Config.enable_dump_optimizer_trace_on_error = dumpTrace;
                for (Exception failure : failures) {
                    planningFailure = failure;
                    for (String sql : List.of("SELECT 'SECRET_SQL'", "TRACE TIMES SELECT 'SECRET_SQL'")) {
                        String context = sql + " / " + failure.getClass().getSimpleName() + " / " + dumpTrace;
                        Config.enable_sql_desensitize_in_log = false;
                        String off = String.join("\n", executeAndCaptureLogs(sql));
                        Assertions.assertTrue(off.contains("SECRET_ERR") && off.contains("SECRET_TRACE"),
                                context + ": " + off);

                        Config.enable_sql_desensitize_in_log = true;
                        List<String> on = executeAndCaptureLogs(sql);
                        Assertions.assertFalse(on.isEmpty(), context);
                        for (String line : on) {
                            assertNoLeak(line, "SECRET_SQL", "SECRET_ERR", "SECRET_CAUSE", "SECRET_TRACE");
                        }
                        // the planning stages keep their timings
                        Assertions.assertTrue(on.stream().anyMatch(line -> line.contains("Planner profile")
                                && line.contains("Total: ")), context + ": " + on);
                    }
                }
            }
        } finally {
            planningFailure = null;
            Config.enable_dump_optimizer_trace_on_error = prevDumpTrace;
        }
    }

    @Test
    public void testFailedQueryPlannerProfileLogIsDesensitized() throws Exception {
        new MockUp<MysqlChannel>() {
            @Mock
            public void reset() {
                // the first step of running the planned query: fail it there, after planning succeeded
                traceSecret();
                connectContext.getState().setError("SECRET_ERR");
                throw new IllegalStateException("SECRET_ERR");
            }
        };
        String sql = "SELECT 'SECRET_SQL' FROM t0";
        Config.enable_sql_desensitize_in_log = false;
        String off = String.join("\n", executeAndCaptureLogs(sql));
        Assertions.assertTrue(off.contains("Planner profile") && off.contains("SECRET_TRACE"), off);

        Config.enable_sql_desensitize_in_log = true;
        List<String> on = executeAndCaptureLogs(sql);
        Assertions.assertTrue(on.stream().anyMatch(line -> line.contains("Planner profile")
                && line.contains("Analyzer: ") && line.contains("Optimizer: ")), String.join("\n", on));
        for (String line : on) {
            assertNoLeak(line, "SECRET_SQL", "SECRET_ERR", "SECRET_TRACE");
        }
    }

    @Test
    public void testQueryProfilePlannerKeepsOnlyStageTimings() throws Exception {
        Tracers prevTracers = Tracers.get();
        try {
            for (boolean desensitize : new boolean[] {false, true}) {
                Config.enable_sql_desensitize_in_log = desensitize;
                Tracers.register(connectContext);
                Tracers.init(Tracers.Mode.TIMER, Tracers.Module.ALL, true, false);
                StatementBase stmt = parseOnly("SELECT 'SECRET_SQL' FROM t0");
                StatementPlanner.plan(stmt, connectContext);
                traceSecret();
                StmtExecutor executor = new StmtExecutor(connectContext, stmt);
                RuntimeProfile profile = Deencapsulation.invoke(executor, "buildTopLevelProfile");
                String planner = profile.getChild("Planner").toString();
                if (desensitize) {
                    assertNoLeak(planner, "SECRET_SQL", "SECRET_TRACE");
                    Assertions.assertTrue(planner.contains("Analyzer: ") && planner.contains("Optimizer: "), planner);
                } else {
                    Assertions.assertTrue(planner.contains("SECRET_TRACE"), planner);
                }
            }
        } finally {
            Tracers.set(prevTracers);
        }
    }
}
