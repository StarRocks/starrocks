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
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.jupiter.params.provider.Arguments;
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
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
    }

    @Before
    public void setUp() {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
    }

    public static void assertContains(String text, String... pattern) {
        for (String s : pattern) {
            Assert.assertTrue(text, text.contains(s));
        }
    }

    private static final String NORMAL_PLAN_PREDICATE_PREFIX = "PREDICATES:";
    private static final String LOWER_NORMAL_PLAN_PREDICATE_PREFIX = "predicates:";
    private static final String LOGICAL_PLAN_SCAN_PREFIX = "SCAN ";
    private static final String LOGICAL_PLAN_PREDICATE_PREFIX = " predicate";

    private static String normalizeLogicalPlanPredicate(String predicate) {
        if (predicate.startsWith(LOGICAL_PLAN_SCAN_PREFIX) && predicate.contains(LOGICAL_PLAN_PREDICATE_PREFIX)) {
            String[] splitArray = predicate.split(LOGICAL_PLAN_PREDICATE_PREFIX);
            Preconditions.checkArgument(splitArray.length == 2);
            String first = splitArray[0];
            String second = splitArray[1];
            String predicates = second.substring(1, second.length() - 2);
            StringBuilder sb = new StringBuilder();
            sb.append(first);
            sb.append(LOGICAL_PLAN_PREDICATE_PREFIX + "[");
            String sorted = Arrays.stream(predicates.split(" AND ")).sorted().collect(Collectors.joining(" AND "));
            sorted = Arrays.stream(sorted.split(" OR ")).sorted().collect(Collectors.joining(" OR "));
            sb.append(sorted);
            sb.append("])");
            return sb.toString();
        } else {
            return predicate;
        }
    }

    private static String normalizeLogicalPlan(String plan) {
        return Stream.of(plan.split("\n"))
                .filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+: ", "col\\$: ").trim())
                .map(str -> str.replaceAll("\\[\\d+]", "[col\\$]").trim())
                .map(str -> str.replaceAll("\\[\\d+, \\d+]", "[col\\$, col\\$]").trim())
                .map(str -> str.replaceAll("\\[\\d+, \\d+, \\d+]", "[col\\$, col\\$, col\\$]").trim())
                .map(str -> normalizeLogicalPlanPredicate(str))
                .collect(Collectors.joining("\n"));
    }

    private static String normalizeNormalPlanSeparator(String predicate, String sep) {
        String[] predicates = predicate.split(sep);
        Preconditions.checkArgument(predicates.length == 2);
        return predicates[0] + sep + Arrays.stream(predicates[1].split(",")).sorted().collect(Collectors.joining(","));
    }

    public static String normalizeNormalPlanPredicate(String predicate) {
        if (predicate.contains(NORMAL_PLAN_PREDICATE_PREFIX)) {
            return normalizeNormalPlanSeparator(predicate, NORMAL_PLAN_PREDICATE_PREFIX);
        } else if (predicate.contains(LOWER_NORMAL_PLAN_PREDICATE_PREFIX)) {
            return normalizeNormalPlanSeparator(predicate, LOWER_NORMAL_PLAN_PREDICATE_PREFIX);
        } else {
            return predicate;
        }
    }

    public static String normalizeNormalPlan(String plan) {
        return Stream.of(plan.split("\n")).filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+: ", "col\\$: ").trim())
                .map(str -> normalizeNormalPlanPredicate(str))
                .collect(Collectors.joining("\n"));
    }

    public static void assertContainsIgnoreColRefs(String text, String... pattern) {
        String normT = normalizeNormalPlan(text);
        for (String s : pattern) {
            // If pattern contains multi lines, only check line by line.
            String normS = normalizeNormalPlan(s);
            for (String line : normS.split("\n")) {
                Assert.assertTrue(text, normT.contains(line));
            }
        }
    }

    public static void assertContainsCTEReuse(String sql) throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(100000);
        String plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "  MultiCastDataSinks");
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

    public static void assertNotContains(String text, String... pattern) {
        for (String s : pattern) {
            Assert.assertFalse(text, text.contains(s));
        }
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
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
    }

    public String getFragmentPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
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
        StringBuilder schedulerString = new StringBuilder();

        boolean isDebug = debug;
        boolean isComment = false;
        boolean hasResult = false;
        boolean hasFragment = false;
        boolean hasFragmentStatistics = false;
        boolean isDump = false;
        boolean isEnumerate = false;
        boolean hasScheduler = false;
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
                    case "[scheduler]":
                        schedulerString = new StringBuilder();
                        hasScheduler = true;
                        mode = "scheduler";
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
                            String actualSchedulerPlan = null;

                            if (hasResult && !debug) {
                                checkWithIgnoreTabletList(result.toString().trim(), pair.first.trim());
                            }
                            if (hasFragment) {
                                fra = pair.second.getExplainString(TExplainLevel.NORMAL);
                                if (!debug) {
                                    fra = format(fra);
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
                            if (hasScheduler) {
                                try {
                                    actualSchedulerPlan =
                                            UtFrameUtils.getPlanAndStartScheduling(connectContext, sql.toString()).first;
                                } catch (Exception ex) {
                                    if (!exceptString.toString().isEmpty()) {
                                        Assert.assertEquals(exceptString.toString(), ex.getMessage());
                                        continue;
                                    }
                                    Assert.fail("Scheduling failed, message: " + ex.getMessage() + ", sql: " + sql);
                                }

                                if (!debug) {
                                    checkSchedulerPlan(schedulerString.toString(), actualSchedulerPlan);
                                }
                            }
                            if (isDebug) {
                                debugSQL(writer, hasResult, hasFragment, isDump, hasFragmentStatistics, hasScheduler, nth,
                                        sql.toString(), pair.first, fra, dumpStr, statistic, comment.toString(),
                                        actualSchedulerPlan);
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
                        hasScheduler = false;
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
                    case "scheduler":
                        schedulerString.append(tempStr).append("\n");
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
                          boolean hasStatistics, boolean hasScheduler, int nthPlan, String sql, String plan, String fragment,
                          String dump,
                          String statistic,
                          String comment,
                          String actualSchedulerPlan) {
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

            if (hasScheduler) {
                writer.append("\n[scheduler]\n");
                writer.append(actualSchedulerPlan);
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

    private void checkSchedulerPlan(String expect, String actual) {
        String[] expectedLines = expect.trim().split("\n");
        String[] actualLines = actual.trim().split("\n");

        int ei = 0;
        int ai = 0;

        while (ei < expectedLines.length && ai < actualLines.length) {
            String eline = expectedLines[ei];
            String aline = actualLines[ai];
            ei++;
            ai++;
            Assert.assertEquals(actual, eline, aline);
            if ("INSTANCES".equals(eline.trim())) {
                // The instances of the fragment may be in random order,
                // so we need to extract each instance and check if they have exactly the same elements in any order.
                Map<Long, String> eInstances = Maps.newHashMap();
                Map<Long, String> aInstances = Maps.newHashMap();
                ei = extractInstancesFromSchedulerPlan(expectedLines, ei, eInstances);
                ai = extractInstancesFromSchedulerPlan(actualLines, ai, aInstances);
                assertThat(aInstances).withFailMessage("actual=[" + actual + "], expect=[" + expect + "]")
                        .containsExactlyInAnyOrderEntriesOf(eInstances);
            }
        }
        Assert.assertEquals(ei, ai);
    }

    private static int extractInstancesFromSchedulerPlan(String[] lines, int startIndex, Map<Long, String> instances) {
        int i = startIndex;
        StringBuilder builder = new StringBuilder();
        long beId = -1;
        for (; i < lines.length; i++) {
            String line = lines[i];
            String trimLine = line.trim();
            if (trimLine.isEmpty()) { // The profile Fragment is coming to the end.
                break;
            } else if (trimLine.startsWith("INSTANCE(")) { // Start a new instance.
                if (beId != -1) {
                    instances.put(beId, builder.toString());
                    beId = -1;
                    builder = new StringBuilder();
                }
            } else { // Still in this instance.
                builder.append(line).append("\n");

                Pattern beIdPattern = Pattern.compile("^\\s*BE: (\\d+)$");
                Matcher matcher = beIdPattern.matcher(line);

                if (matcher.find()) {
                    beId = Long.parseLong(matcher.group(1));
                }
            }
        }

        if (beId != -1) {
            instances.put(beId, builder.toString());
        }

        return i;
    }

    private void checkWithIgnoreTabletList(String expect, String actual) {
        if (isIgnoreExplicitColRefIds()) {
            String ignoreExpect = normalizeLogicalPlan(expect);
            String ignoreActual = normalizeLogicalPlan(actual);
            Assert.assertEquals(actual, ignoreExpect, ignoreActual);
        } else {
            expect = Stream.of(expect.split("\n")).
                    filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
            expect = Stream.of(expect.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            actual = Stream.of(actual.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            Assert.assertEquals(expect, actual);
        }
    }

    protected void assertPlanContains(String sql, String... explain) throws Exception {
        String explainString = getFragmentPlan(sql);

        for (String expected : explain) {
            Assert.assertTrue("expected is:\n" + expected + "\n but plan is \n" + explainString,
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

    protected void assertExceptionMsgContains(String sql, String message) {
        try {
            getFragmentPlan(sql);
            throw new Error();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    public Table getTable(String t) {
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        return globalStateMgr.getDb("test").getTable(t);
    }

    public OlapTable getOlapTable(String t) {
        return (OlapTable) getTable(t);
    }

    public static List<Arguments> zipSqlAndPlan(List<String> sqls, List<String> plans) {
        Preconditions.checkState(sqls.size() == plans.size(), "sqls and plans should have same size");
        List<Arguments> arguments = Lists.newArrayList();
        for (int i = 0; i < sqls.size(); i++) {
            arguments.add(Arguments.of(sqls.get(i), plans.get(i)));
        }
        return arguments;
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
