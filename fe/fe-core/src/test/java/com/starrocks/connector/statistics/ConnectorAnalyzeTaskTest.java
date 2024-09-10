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

package com.starrocks.connector.statistics;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class ConnectorAnalyzeTaskTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @Test
    public void testMergeTask() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_custkey", "o_orderstatus"));
        task1.mergeTask(task2);
        Assert.assertEquals(3, task1.getColumns().size());
    }

    @Test
    public void testTaskRun() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        ExternalAnalyzeStatus externalAnalyzeStatus = new ExternalAnalyzeStatus(1, "hive0", "partitioned_db", "orders",
                tableUUID, List.of("o_custkey", "o_orderstatus"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        externalAnalyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(externalAnalyzeStatus);

        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        Optional<AnalyzeStatus> result = task1.run();
        Assert.assertTrue(result.isEmpty());

        new MockUp<ConnectorAnalyzeTask>() {
            @Mock
            private AnalyzeStatus executeAnalyze(ConnectContext statsConnectCtx, AnalyzeStatus analyzeStatus) {
                return analyzeStatus;
            }
        };
        // execute analyze when last analyze status is finish
        externalAnalyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        result = task1.run();
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertEquals(List.of("o_orderkey"), externalAnalyzeStatusResult.getColumns());
    }
}
