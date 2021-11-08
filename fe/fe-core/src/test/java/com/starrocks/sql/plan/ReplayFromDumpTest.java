// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplayFromDumpTest {
    public static String runningDir = "fe/mocked/ReplayFromDumpTest/" + UUID.randomUUID().toString() + "/";
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    public static List<String> MODEL_LISTS = Lists.newArrayList("[end]", "[dump]", "[result]", "[fragment]",
            "[fragment statistics]");

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
    }

    public String getModelContent(String filename, String model) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");
        StringBuilder modelContentBuilder = new StringBuilder();

        String tempStr;
        boolean hasModelContent = false;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.equals("[" + model + "]")) {
                    hasModelContent = true;
                    continue;
                }

                if (MODEL_LISTS.contains(tempStr)) {
                    hasModelContent = false;
                }

                if (hasModelContent) {
                    modelContentBuilder.append(tempStr).append("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        return modelContentBuilder.toString();
    }

    public QueryDumpInfo getDumpInfoFromJson(String dumpInfoString) {
        return GsonUtils.GSON.fromJson(dumpInfoString, QueryDumpInfo.class);
    }

    public SessionVariable getTestSessionVariable() {
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setMaxTransformReorderJoins(8);
        sessionVariable.setEnableGlobalRuntimeFilter(true);
        return sessionVariable;
    }

    public void compareDumpWithOriginTest(String filename) throws Exception {
        String dumpString = getModelContent(filename, "dump");
        String originCostPlan = Stream.of(getModelContent(filename, "fragment statistics").split("\n")).
                filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        String replayCostPlan = Stream.of(
                PlanTestBase.format(getCostPlanFragment(dumpString, getTestSessionVariable()).second).split("\n"))
                .filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        Assert.assertEquals(originCostPlan, replayCostPlan);
    }

    private String getDumpInfoFromFile(String fileName) throws Exception {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + fileName + ".json");
        StringBuilder sb = new StringBuilder();
        String tempStr;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((tempStr = reader.readLine()) != null) {
                sb.append(tempStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        return sb.toString();
    }

    private Pair<QueryDumpInfo, String> getCostPlanFragment(String dumpJonStr) throws Exception {
        return getCostPlanFragment(dumpJonStr, null);
    }

    private Pair<QueryDumpInfo, String> getCostPlanFragment(String dumpJsonStr, SessionVariable sessionVariable)
            throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpJsonStr);
        if (sessionVariable != null) {
            queryDumpInfo.setSessionVariable(sessionVariable);
        }
        return new Pair<>(queryDumpInfo,
                UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, queryDumpInfo).second.
                        getExplainString(TExplainLevel.COSTS));
    }

    @Test
    public void testTPCH1() throws Exception {
        compareDumpWithOriginTest("tpchcost/q1");
    }

    @Test
    public void testTPCH5() throws Exception {
        compareDumpWithOriginTest("tpchcost/q5");
    }

    @Test
    public void testTPCH7() throws Exception {
        compareDumpWithOriginTest("tpchcost/q7");
    }

    @Test
    public void testTPCH20() throws Exception {
        compareDumpWithOriginTest("tpchcost/q20");
    }

    @Test
    public void testReplayTPCDS02() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds02"));
        SessionVariable replaySessionVariable = replayPair.first.getSessionVariable();
        Assert.assertEquals(replaySessionVariable.getParallelExecInstanceNum(), 4);
        Assert.assertTrue(replayPair.second.contains("|----24:EXCHANGE\n" +
                "  |       cardinality: 73049\n" +
                "  |    \n" +
                "  18:UNION\n" +
                "  |  child exprs: \n" +
                "  |      [143, INT, true] | [164, DECIMAL64(7,2), true]\n" +
                "  |      [179, INT, true] | [200, DECIMAL64(7,2), true]\n"));
    }

    @Test
    public void testSSB10() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/ssb10"));
        Assert.assertTrue(replayPair.second.contains("cardinality: 1597"));
        Assert.assertTrue(replayPair.second.contains("  |----7:EXCHANGE\n"
                + "  |       cardinality: 30"));
    }

    @Test
    public void testTPCDS54() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds54"));
        // Check the size of the left and right tables
        System.out.println(replayPair.second);
        Assert.assertTrue(replayPair.second.contains("  |  \n" +
                "  |----21:EXCHANGE\n" +
                "  |       cardinality: 101\n" +
                "  |    \n" +
                "  5:OlapScanNode\n" +
                "     table: customer, rollup: customer"));
    }

    @Test
    public void testTPCH01() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch01"));
        // check q1 has two agg phase with default column statistics
        Assert.assertTrue(replayPair.second.contains("AGGREGATE (merge finalize)"));
        Assert.assertTrue(replayPair.second.contains("AGGREGATE (update serialize)"));
    }

    @Test
    public void testGroupByLimit() throws Exception {
        // check can generate 1 phase with limit 1
        // This test has column statistics and accurate table row count
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/groupby_limit"));
        Assert.assertTrue(replayPair.second.contains("2:AGGREGATE (update finalize)"));
    }

    @Test
    public void testTPCDS77() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds77"));
        // check can generate plan without exception
        System.out.println(replayPair.second);
    }

    @Test
    public void testTPCDS78() throws Exception {
        // check outer join with isNull predicate on inner table
        // The estimate cardinality of join should not be 0.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds78"));
        Assert.assertTrue(replayPair.second.contains("3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [2: ss_ticket_number, INT, false] = [25: sr_ticket_number, INT, true]\n" +
                "  |  equal join conjunct: [1: ss_item_sk, INT, false] = [24: sr_item_sk, INT, true]\n" +
                "  |  other predicates: 25: sr_ticket_number IS NULL\n" +
                "  |  cardinality: 39142590"));
        Assert.assertTrue(replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [76: ws_order_number, INT, false] = [110: wr_order_number, INT, true]\n" +
                "  |  equal join conjunct: [75: ws_item_sk, INT, false] = [109: wr_item_sk, INT, true]\n" +
                "  |  other predicates: 110: wr_order_number IS NULL\n" +
                "  |  cardinality: 7916106"));
    }
}
