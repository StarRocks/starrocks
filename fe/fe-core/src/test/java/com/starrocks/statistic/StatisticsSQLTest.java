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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class StatisticsSQLTest extends PlanTestBase {
    private static long t0StatsTableId = 0;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();

        starRocksAssert.withTable("CREATE TABLE `stat0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL,\n" +
                "  `s1` String NULL,\n" +
                "  `j1` JSON NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("stat0");
        t0StatsTableId = t0.getId();
        // Partition partition = new ArrayList<>(t0.getPartitions()).get(0);
        // partition.updateVisibleVersion(2, LocalDateTime.of(2022, 1, 1, 1, 1, 1)
        //         .atZone(Clock.systemDefaultZone().getZone()).toEpochSecond() * 1000);
        // setTableStatistics(t0, 20000000);
    }

    @Test
    public void testSampleStatisticsSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("stat0");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        List<String> columnNames = Lists.newArrayList("v3", "j1", "s1");
        SampleStatisticsCollectJob job = new SampleStatisticsCollectJob(db, t0, columnNames,
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        String sql = job.buildSampleInsertSQL(db.getId(), t0StatsTableId, columnNames, 200);
        starRocksAssert.useDatabase("_statistics_");
        String plan = getFragmentPlan(sql);

        Assert.assertEquals(3, StringUtils.countMatches(plan, "OlapScanNode"));
        assertCContains(plan, "left(");
        assertCContains(plan, "count * 1024");
    }

    @Test
    public void testFullStatisticsSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("stat0");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<Long> pids = t0.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        List<String> columnNames = Lists.newArrayList("j1", "s1");
        FullStatisticsCollectJob job = new FullStatisticsCollectJob(db, t0, pids, columnNames,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        List<List<String>> sqls = job.buildCollectSQLList(1);
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(1, sqls.get(0).size());
        Assert.assertEquals(1, sqls.get(1).size());
        starRocksAssert.useDatabase("_statistics_");
        String plan = getFragmentPlan(sqls.get(0).get(0));
        assertCContains(plan, "count * 1024");

        plan = getFragmentPlan(sqls.get(1).get(0));
        assertCContains(plan, "left(");
        assertCContains(plan, "char_length(");
    }
}
