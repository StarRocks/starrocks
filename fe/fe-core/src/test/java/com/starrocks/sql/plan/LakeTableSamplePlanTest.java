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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.SampleMethod;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TTableSampleOptions;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class LakeTableSamplePlanTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("lake_sample_test")
                .useDatabase("lake_sample_test")
                .withTable("CREATE TABLE lake_sample_test.t0 (c0 INT) " +
                        "DUPLICATE KEY(c0) " +
                        "DISTRIBUTED BY HASH(c0) BUCKETS 1");
    }

    private ByteBuffer getCacheDigest(String sql) throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;

        return execPlan.getFragments().stream()
                .filter(fragment -> fragment.getCacheParam() != null)
                .map(fragment -> ByteBuffer.wrap(fragment.getCacheParam().getDigest()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("query cache was not enabled for: " + sql));
    }

    @Test
    public void testLakeScanNodeCarriesSampleOptions() throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));

        String sql = "SELECT c0 FROM t0 " +
                "SAMPLE('method'='by_block', 'seed'='7', 'percent'='0.5')";
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;

        Assertions.assertEquals(1, execPlan.getScanNodes().size());
        ScanNode scanNode = execPlan.getScanNodes().get(0);
        Assertions.assertInstanceOf(OlapScanNode.class, scanNode);

        TPlanNode thriftNode = scanNode.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(TPlanNodeType.LAKE_SCAN_NODE, thriftNode.getNode_type());
        Assertions.assertTrue(thriftNode.isSetLake_scan_node());
        Assertions.assertTrue(thriftNode.getLake_scan_node().isSetSample_options());

        TTableSampleOptions sampleOptions =
                thriftNode.getLake_scan_node().getSample_options();
        Assertions.assertTrue(sampleOptions.isEnable_sampling());
        Assertions.assertEquals(SampleMethod.BY_BLOCK, sampleOptions.getSample_method());
        Assertions.assertEquals(7L, sampleOptions.getRandom_seed());
        Assertions.assertTrue(sampleOptions.isSetProbability_percent_v2());
        Assertions.assertEquals(
                0.5, sampleOptions.getProbability_percent_v2(), 0.000001);
    }

    @Test
    public void testSampleOptionsAffectQueryCacheDigest() throws Exception {
        String savedSessionVariables = connectContext.getSessionVariable().getJsonString();

        try {
            connectContext.getSessionVariable().setEnablePipelineEngine(true);
            connectContext.getSessionVariable().setEnableQueryCache(true);
            connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);

            String baselineSql = "SELECT sum(c0) FROM t0 " +
                    "SAMPLE('method'='by_block', 'seed'='7', 'percent'='0.5')";

            ByteBuffer baseline = getCacheDigest(baselineSql);

            Assertions.assertEquals(baseline, getCacheDigest(baselineSql));

            Assertions.assertNotEquals(baseline, getCacheDigest(
                    "SELECT sum(c0) FROM t0 " +
                            "SAMPLE('method'='by_block', 'seed'='8', 'percent'='0.5')"));
            Assertions.assertNotEquals(baseline, getCacheDigest(
                    "SELECT sum(c0) FROM t0 " +
                            "SAMPLE('method'='by_page', 'seed'='7', 'percent'='0.5')"));
            Assertions.assertNotEquals(baseline, getCacheDigest(
                    "SELECT sum(c0) FROM t0 " +
                            "SAMPLE('method'='by_block', 'seed'='7', 'percent'='1')"));

        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSessionVariables);
        }
    }
}
