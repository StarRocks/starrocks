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

package com.starrocks.qe;

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class TestBucketShuffleRightJoin {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        try {
            starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                    .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                    .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);

            starRocksAssert.withDatabase("test").useDatabase("test");
            String tableFmt = "CREATE TABLE `%s` (\n" +
                    "  `c0` bigint(20) NOT NULL COMMENT \"\",\n" +
                    "  `c1` bigint(20) NOT NULL COMMENT \"\",\n" +
                    "  `c2` bigint(20) NOT NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c0`)\n" +
                    "COMMENT \"OLAP\"\n" +
                    "DISTRIBUTED BY HASH(`c0`) BUCKETS 9 \n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");";
            for (String tableName : Arrays.asList("t0", "t1", "t2")) {
                String createTblSql = String.format(tableFmt, tableName);
                starRocksAssert.withTable(createTblSql);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void test() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "select t0.c0,t0.c1,t0.c2,t1.c2 as ab from " +
                "(select * from t1 where c0 in (8)) t1 right outer join[bucket] " +
                "(select if(murmur_hash3_32(c0)=0,c0,NULL) as c0,c1,c2 from t0) t0 on t0.c0 = t1.c0 " +
                "left join[bucket] t2 on t2.c0 = t1.c0";
        Pair<String, ExecPlan> explainAndExecPlan = UtFrameUtils.getPlanAndFragment(ctx, sql);
        String plan = explainAndExecPlan.first;
        ExecPlan execPlan = explainAndExecPlan.second;
        DefaultCoordinator coord = new DefaultCoordinator.Factory().createQueryScheduler(
                ctx, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        coord.prepareExec();

        boolean bucketShuffleRightJoinPresent = coord.getExecutionDAG().getFragmentsInPreorder()
                .stream().anyMatch(ExecutionFragment::isRightOrFullBucketShuffle);
        Assert.assertTrue(plan, bucketShuffleRightJoinPresent);
    }
}
