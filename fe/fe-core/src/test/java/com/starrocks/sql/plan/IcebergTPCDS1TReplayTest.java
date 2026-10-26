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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Replays the query dump of every TPC-DS query captured on a 1T iceberg cluster with real, analyzed statistics
 * (see query_dump/iceberg_tpcds1t/README.md).
 */
public class IcebergTPCDS1TReplayTest extends ReplayFromDumpTestBase {
    private static final String DUMP_DIR = "query_dump/iceberg_tpcds1t";

    // Explain like the cluster did, so a replayed plan can be compared with the dump's explain_info line by line.
    // ReplayFromDumpTestBase turns these display options off for its other dump tests.
    @BeforeAll
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        FeConstants.setLengthForVarchar = true;
        FeConstants.showJoinLocalShuffleInExplain = true;
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = true;
        // mock as many backends as the source cluster had: broadcast vs shuffle and parallelism depend on it
        UtFrameUtils.replayMaxBackendNum = Integer.MAX_VALUE;
    }

    @AfterAll
    public static void afterClass() throws Exception {
        ReplayFromDumpTestBase.afterClass();
        FeConstants.setLengthForVarchar = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
        UtFrameUtils.replayMaxBackendNum = 3;
    }

    private static Stream<String> queries() {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File[] files = new File(path + "/" + DUMP_DIR).listFiles((dir, name) -> name.endsWith(".json.gz"));
        return Arrays.stream(Objects.requireNonNull(files))
                .map(f -> f.getName().substring(0, f.getName().length() - ".json.gz".length()))
                .sorted();
    }

    // Plan with the dump's session variables as captured. getCostPlanFragment would force some of them off
    // (cbo_push_down_aggregate_mode, enable_inner_join_to_semi), which changes the plan from the cluster's.
    private static String replay(String dump) throws Exception {
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dump);
        dumpInfo.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        return UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dumpInfo).second
                .getExplainString(TExplainLevel.COSTS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    public void testReplay(String query) throws Exception {
        String dump = getDumpInfoFromFile(DUMP_DIR + "/" + query);
        String plan = replay(dump);
        Assertions.assertNotNull(plan, "replay produced no plan for " + query);
        String out = System.getenv("TPCDS1T_REPLAY_OUT");
        if (out != null) {
            Files.writeString(new File(out, query + ".replay").toPath(), plan, StandardCharsets.UTF_8);
            String clusterPlan = JsonParser.parseString(dump).getAsJsonObject().get("explain_info").getAsString();
            Files.writeString(new File(out, query + ".cluster").toPath(), clusterPlan, StandardCharsets.UTF_8);
        }
    }
}
