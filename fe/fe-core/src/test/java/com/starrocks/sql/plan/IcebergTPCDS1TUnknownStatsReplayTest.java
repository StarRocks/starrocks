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

import com.google.gson.JsonParser;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Replays TPC-DS query dumps captured on the 1T iceberg cluster while no column statistics had been collected yet
 * (row counts known, every column UNKNOWN): the state a query planned early in a sample run sees
 * (see query_dump/iceberg_tpcds1t_unknown_stats/README.md).
 */
public class IcebergTPCDS1TUnknownStatsReplayTest extends ReplayFromDumpTestBase {
    private static final String DUMP_DIR = "query_dump/iceberg_tpcds1t_unknown_stats";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        FeConstants.setLengthForVarchar = true;
        FeConstants.showJoinLocalShuffleInExplain = true;
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = true;
        UtFrameUtils.replayMaxBackendNum = Integer.MAX_VALUE;
        // plan as the cluster does: ReorderJoinRule skips DP and greedy reorder when column statistics are
        // unknown, except in plain unit tests
        FeConstants.isReplayFromQueryDump = true;
    }

    @AfterAll
    public static void afterClass() throws Exception {
        ReplayFromDumpTestBase.afterClass();
        FeConstants.setLengthForVarchar = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
        UtFrameUtils.replayMaxBackendNum = 3;
        FeConstants.isReplayFromQueryDump = false;
    }

    private static Stream<String> queries() {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File[] files = new File(path + "/" + DUMP_DIR).listFiles((dir, name) -> name.endsWith(".json.gz"));
        return Arrays.stream(Objects.requireNonNull(files))
                .map(f -> f.getName().substring(0, f.getName().length() - ".json.gz".length()))
                .sorted();
    }

    // plans with the dump's own session variables (optimizer timeout included), as the cluster did
    private static String replay(String dump) throws Exception {
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dump);
        return UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dumpInfo).second
                .getExplainString(TExplainLevel.COSTS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    public void testReplay(String query) throws Exception {
        String dump = getDumpInfoFromFile(DUMP_DIR + "/" + query);
        String plan = replay(dump);
        Assertions.assertNotNull(plan, "replay produced no plan for " + query);
        String out = System.getenv("TPCDS1T_UNKNOWN_STATS_REPLAY_OUT");
        if (out != null) {
            Files.writeString(new File(out, query + ".replay").toPath(), plan, StandardCharsets.UTF_8);
            String clusterPlan = JsonParser.parseString(dump).getAsJsonObject().get("explain_info").getAsString();
            Files.writeString(new File(out, query + ".cluster").toPath(), clusterPlan, StandardCharsets.UTF_8);
        }
    }

    // a join whose only equality is the item key of inventory and catalog_sales: many-to-many on 1T
    private static boolean hasItemOnlyInventoryCatalogSalesJoin(String plan) {
        Matcher m = Pattern.compile("join op: [A-Z ]+ \\([A-Z_]+\\)\\n(?:  \\|  .*\\n)*?" +
                "((?:  \\|  equal join conjunct: .*\\n)+)").matcher(plan);
        while (m.find()) {
            String conjuncts = m.group(1);
            if (conjuncts.contains("inv_item_sk") && conjuncts.contains("cs_item_sk") &&
                    conjuncts.split("equal join conjunct").length == 2) {
                return true;
            }
        }
        return false;
    }

    // With no column statistics, the join reorder falls back to left-deep, which puts inventory x catalog_sales
    // on item only at the bottom: many-to-many on 1T, the query times out in sample runs. The replay must
    // reproduce the cluster's plan for a fix to be checked here.
    @Test
    public void testQ72ReproducesClusterPlan() throws Exception {
        String dump = getDumpInfoFromFile(DUMP_DIR + "/q72");
        String clusterPlan = JsonParser.parseString(dump).getAsJsonObject().get("explain_info").getAsString();
        Assertions.assertTrue(hasItemOnlyInventoryCatalogSalesJoin(clusterPlan), clusterPlan);

        String plan = replay(dump);
        Assertions.assertTrue(hasItemOnlyInventoryCatalogSalesJoin(plan), plan);
    }
}
