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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import kotlin.text.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlanTestNoneDBBase {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @BeforeClass
    public static void beforeClass() throws Exception {
        // disable checking tablets
        Config.tablet_sched_max_scheduling_tablets = -1;
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
    }

    public static void assertContains(String text, String... pattern) {
        for (String s : pattern) {
            Assert.assertTrue(text, text.contains(s));
        }
    }

    public static void assertContains(String text, List<String> patterns) {
        for (String s : patterns) {
            Assert.assertTrue(text, text.contains(s));
        }
    }

    public void assertCContains(String text, String... pattern) {
        try {
            for (String s : pattern) {
                Assert.assertTrue(text, text.contains(s));
            }
        } catch (Error error) {
            collector.addError(error);
        }
    }

    public static void assertNotContains(String text, String pattern) {
        Assert.assertFalse(text, text.contains(pattern));
    }

    public static void setTableStatistics(OlapTable table, long rowCount) {
        for (Partition partition : table.getAllPartitions()) {
            partition.getBaseIndex().setRowCount(rowCount);
        }
    }

    public static void setPartitionStatistics(OlapTable table, String partitionName, long rowCount) {
        for (Partition partition : table.getAllPartitions()) {
            if (partition.getName().equals(partitionName)) {
                partition.getBaseIndex().setRowCount(rowCount);
            }
        }
    }

    public ExecPlan getExecPlan(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
    }

    public String getFragmentPlan(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
    }

    public String getLogicalFragmentPlan(String sql) throws Exception {
        return LogicalPlanPrinter.print(UtFrameUtils.getPlanAndFragment(
                connectContext, sql).second.getPhysicalPlan());
    }

    public String getVerboseExplain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.VERBOSE);
    }

    public String getCostExplain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.COSTS);
    }

    public String getDumpString(String sql) throws Exception {
        UtFrameUtils.getPlanAndFragment(connectContext, sql);
        return GsonUtils.GSON.toJson(connectContext.getDumpInfo());
    }

    public String getThriftPlan(String sql) throws Exception {
        return UtFrameUtils.getPlanThriftString(connectContext, sql);
    }

    public static int getPlanCount(String sql) throws Exception {
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        int planCount = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPlanCount();
        connectContext.getSessionVariable().setUseNthExecPlan(0);
        return planCount;
    }

    public String getSQLFile(String filename) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");

        String sql;
        try (BufferedReader re = new BufferedReader(new FileReader(file))) {
            sql = re.lines().collect(Collectors.joining());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return sql;
    }

    public void runFileUnitTest(String filename, boolean debug) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");

        String mode = "";
        String tempStr;
        StringBuilder sql = new StringBuilder();
        StringBuilder result = new StringBuilder();
        StringBuilder fragment = new StringBuilder();
        StringBuilder comment = new StringBuilder();
        StringBuilder fragmentStatistics = new StringBuilder();
        StringBuilder dumpInfoString = new StringBuilder();
        StringBuilder planEnumerate = new StringBuilder();
        StringBuilder exceptString = new StringBuilder();

        boolean isDebug = debug;
        boolean isComment = false;
        boolean hasResult = false;
        boolean hasFragment = false;
        boolean hasFragmentStatistics = false;
        boolean isDump = false;
        boolean isEnumerate = false;
        int planCount = -1;

        File debugFile = new File(file.getPath() + ".debug");
        BufferedWriter writer = null;

        if (isDebug) {
            try {
                FileUtils.write(debugFile, "", StandardCharsets.UTF_8);
                writer = new BufferedWriter(new FileWriter(debugFile, true));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("DEBUG MODE!");
            System.out.println("DEBUG FILE: " + debugFile.getPath());
        }

        Pattern regex = Pattern.compile("\\[plan-(\\d+)]");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            int nth = 0;
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.startsWith("/*")) {
                    isComment = true;
                    comment.append(tempStr).append("\n");
                }
                if (tempStr.endsWith("*/")) {
                    isComment = false;
                    comment.append(tempStr).append("\n");
                    continue;
                }

                if (isComment || tempStr.startsWith("//")) {
                    comment.append(tempStr);
                    continue;
                }

                Matcher m = regex.matcher(tempStr);
                if (m.find()) {
                    isEnumerate = true;
                    planEnumerate = new StringBuilder();
                    mode = "enum";
                    nth = Integer.parseInt(m.group(1));
                    connectContext.getSessionVariable().setUseNthExecPlan(nth);
                    continue;
                }

                switch (tempStr) {
                    case "[debug]":
                        isDebug = true;
                        // will create new file
                        if (null == writer) {
                            writer = new BufferedWriter(new FileWriter(debugFile, true));
                            System.out.println("DEBUG MODE!");
                        }
                        continue;
                    case "[planCount]":
                        mode = "planCount";
                        continue;
                    case "[sql]":
                        sql = new StringBuilder();
                        mode = "sql";
                        continue;
                    case "[result]":
                        result = new StringBuilder();
                        mode = "result";
                        hasResult = true;
                        continue;
                    case "[fragment]":
                        fragment = new StringBuilder();
                        mode = "fragment";
                        hasFragment = true;
                        continue;
                    case "[fragment statistics]":
                        fragmentStatistics = new StringBuilder();
                        mode = "fragment statistics";
                        hasFragmentStatistics = true;
                        continue;
                    case "[dump]":
                        dumpInfoString = new StringBuilder();
                        mode = "dump";
                        isDump = true;
                        continue;
                    case "[except]":
                        exceptString = new StringBuilder();
                        mode = "except";
                        continue;
                    case "[end]":
                        Pair<String, ExecPlan> pair = null;
                        try {
                            pair = UtFrameUtils.getPlanAndFragment(connectContext, sql.toString());
                        } catch (Exception ex) {
                            if (!exceptString.toString().isEmpty()) {
                                Assert.assertEquals(exceptString.toString(), ex.getMessage());
                                continue;
                            }
                            Assert.fail("Planning failed, message: " + ex.getMessage() + ", sql: " + sql);
                        }

                        try {
                            String fra = null;
                            String statistic = null;
                            String dumpStr = null;

                            if (hasResult && !debug) {
                                checkWithIgnoreTabletList(result.toString().trim(), pair.first.trim());
                            }
                            if (hasFragment) {
                                fra = format(pair.second.getExplainString(TExplainLevel.NORMAL));
                                if (!debug) {
                                    checkWithIgnoreTabletList(fragment.toString().trim(), fra.trim());
                                }
                            }
                            if (hasFragmentStatistics) {
                                statistic = format(pair.second.getExplainString(TExplainLevel.COSTS));
                                if (!debug) {
                                    checkWithIgnoreTabletList(fragmentStatistics.toString().trim(), statistic.trim());
                                }
                            }
                            if (isDump) {
                                dumpStr = Stream.of(toPrettyFormat(getDumpString(sql.toString())).split("\n"))
                                        .filter(s -> !s.contains("\"session_variables\""))
                                        .collect(Collectors.joining("\n"));
                                if (!debug) {
                                    Assert.assertEquals(dumpInfoString.toString().trim(), dumpStr.trim());
                                }
                            }
                            if (isDebug) {
                                debugSQL(writer, hasResult, hasFragment, isDump, hasFragmentStatistics, nth,
                                        sql.toString(), pair.first, fra, dumpStr, statistic, comment.toString());
                            }
                            if (isEnumerate) {
                                Assert.assertEquals("plan count mismatch", planCount, pair.second.getPlanCount());
                                checkWithIgnoreTabletList(planEnumerate.toString().trim(), pair.first.trim());
                                connectContext.getSessionVariable().setUseNthExecPlan(0);
                            }
                        } catch (Error error) {
                            collector.addError(new Throwable(nth + " plan " + "\n" + sql, error));
                        }

                        hasResult = false;
                        hasFragment = false;
                        hasFragmentStatistics = false;
                        isDump = false;
                        comment = new StringBuilder();
                        continue;
                }

                switch (mode) {
                    case "sql":
                        sql.append(tempStr).append("\n");
                        break;
                    case "planCount":
                        planCount = Integer.parseInt(tempStr);
                        break;
                    case "result":
                        result.append(tempStr).append("\n");
                        break;
                    case "fragment":
                        fragment.append(tempStr.trim()).append("\n");
                        break;
                    case "fragment statistics":
                        fragmentStatistics.append(tempStr.trim()).append("\n");
                        break;
                    case "dump":
                        dumpInfoString.append(tempStr).append("\n");
                        break;
                    case "enum":
                        planEnumerate.append(tempStr).append("\n");
                        break;
                    case "except":
                        exceptString.append(tempStr);
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println(sql);
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void runFileUnitTest(String filename) {
        runFileUnitTest(filename, false);
    }

    public static String format(String result) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(result.split("\n")).forEach(d -> sb.append(d.trim()).append("\n"));
        return sb.toString().trim();
    }

    private void debugSQL(BufferedWriter writer, boolean hasResult, boolean hasFragment, boolean hasDump,
                          boolean hasStatistics, int nthPlan, String sql, String plan, String fragment, String dump,
                          String statistic,
                          String comment) {
        try {
            if (!comment.trim().isEmpty()) {
                writer.append(comment).append("\n");
            }
            if (nthPlan <= 1) {
                writer.append("[sql]\n");
                writer.append(sql.trim());
            }

            if (hasResult) {
                writer.append("\n[result]\n");
                writer.append(plan);
            }
            if (nthPlan > 0) {
                writer.append("\n[plan-").append(String.valueOf(nthPlan)).append("]\n");
                writer.append(plan);
            }

            if (hasFragment) {
                writer.append("\n[fragment]\n");
                writer.append(fragment.trim());
            }

            if (hasStatistics) {
                writer.append("\n[fragment statistics]\n");
                writer.append(statistic.trim());
            }

            if (hasDump) {
                writer.append("\n[dump]\n");
                writer.append(dump.trim());
            }

            writer.append("\n[end]\n\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String toPrettyFormat(String json) {
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(jsonObject);
    }

    /**
     * Whether ignore explicit column ref ids when checking the expected plans.
     */
    protected boolean isIgnoreExplicitColRefIds() {
        return false;
    }

    private void checkWithIgnoreTabletList(String expect, String actual) {
        expect = Stream.of(expect.split("\n")).
                filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        if (isIgnoreExplicitColRefIds()) {
            checkWithIgnoreTabletListAndColRefIds(expect, actual);
        } else {
            expect = Stream.of(expect.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            actual = Stream.of(actual.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            Assert.assertEquals(expect, actual);
        }
    }

    private void checkWithIgnoreTabletListAndColRefIds(String expect, String actual) {
        expect = Stream.of(expect.split("\n")).filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+", "").trim())
                .collect(Collectors.joining("\n"));
        actual = Stream.of(actual.split("\n")).filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+", "").trim())
                .collect(Collectors.joining("\n"));
        Assert.assertEquals(expect, actual);
    }

    protected void assertPlanContains(String sql, String... explain) throws Exception {
        String explainString = getFragmentPlan(sql);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    protected void assertLogicalPlanContains(String sql, String... explain) throws Exception {
        String explainString = getLogicalFragmentPlan(sql);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    protected void assertVerbosePlanContains(String sql, String... explain) throws Exception {
        String explainString = getVerboseExplain(sql);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    protected void assertVerbosePlanNotContains(String sql, String... explain) throws Exception {
        String explainString = getVerboseExplain(sql);

        for (String expected : explain) {
            Assert.assertFalse("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    protected void assertExceptionMessage(String sql, String message) {
        try {
            getFragmentPlan(sql);
            throw new Error();
        } catch (Exception e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }

    public Table getTable(String t) {
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        return globalStateMgr.getDb("test").getTable(t);
    }

    public OlapTable getOlapTable(String t) {
        return (OlapTable) getTable(t);
    }

    public static List<Pair<String, String>> zipSqlAndPlan(List<String> sqls, List<String> plans) {
        Preconditions.checkState(sqls.size() == plans.size(), "sqls and plans should have same size");
        List<Pair<String, String>> zips = Lists.newArrayList();
        for (int i = 0; i < sqls.size(); i++) {
            zips.add(Pair.create(sqls.get(i), plans.get(i)));
        }
        return zips;
    }

    protected static void createTables(String dirName, List<String> fileNames) {
        getSqlList(dirName, fileNames).forEach(createTblSql -> {
            System.out.println("create table sql:" + createTblSql);
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected static void createMaterializedViews(String dirName, List<String> fileNames) {
        getSqlList(dirName, fileNames).forEach(sql -> {
            System.out.println("create mv sql:" + sql);
            try {
                starRocksAssert.withMaterializedView(sql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected static List<String> getSqlList(String dirName, List<String> fileNames) {
        ClassLoader loader = PlanTestBase.class.getClassLoader();
        List<String> createTableSqlList = fileNames.stream().map(n -> {
            System.out.println("file name:" + n);
            try {
                return CharStreams.toString(
                        new InputStreamReader(
                                Objects.requireNonNull(loader.getResourceAsStream(dirName + n + ".sql")),
                                Charsets.UTF_8));
            } catch (Throwable e) {
                return null;
            }
        }).collect(Collectors.toList());
        Assert.assertFalse(createTableSqlList.contains(null));
        return createTableSqlList;
    }

    public static String getFileContent(String fileName) throws Exception {
        ClassLoader loader = PlanTestNoneDBBase.class.getClassLoader();
        System.out.println("file name:" + fileName);
        String content = "";
        try {
            content = CharStreams.toString(
                    new InputStreamReader(
                            Objects.requireNonNull(loader.getResourceAsStream(fileName)),
                            Charsets.UTF_8));
        } catch (Throwable e) {
            throw e;
        }
        return content;
    }

    protected static void executeSqlFile(String fileName) throws Exception {
        String sql = getFileContent(fileName);
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode());
        for (StatementBase stmt : statements) {
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, stmt);
            stmtExecutor.execute();
            Assert.assertEquals("", connectContext.getState().getErrorMessage());
        }
    }
}
