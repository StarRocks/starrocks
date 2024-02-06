// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/BatchRollupJobTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class BatchRollupJobTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        Config.alter_scheduler_interval_millisecond = 10;
    }

    @Before
    public void before() throws Exception {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        alterJobs.clear();
    }

    @Test
    public void testBatchRollup() throws Exception {
        starRocksAssert.withTable(
                "create table db1.tbl1(k1 int, k2 int, k3 int) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // batch add 3 rollups
        String stmtStr = "alter table db1.tbl1 add rollup r1(k1) duplicate key(k1), r2(k1, k2) " +
                "duplicate key(k1), r3(k2) duplicate key(k2);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmtStr, ctx);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(alterTableStmt);

        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        Assert.assertEquals(3, alterJobs.size());

        Database db = GlobalStateMgr.getCurrentState().getDb("db1");
        Assert.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTable("tbl1");
        Assert.assertNotNull(tbl);

        // 3 rollup jobs may be finished in the loop, so only check the final state at last.
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(500);
            }
            System.out.println("rollup job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }

        // waiting table state to normal
        int retryTimes = 25;
        while (tbl.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(1000);
            retryTimes--;
        }
        Assert.assertEquals(OlapTableState.NORMAL, tbl.getState());
        for (Partition partition : tbl.getPartitions()) {
            Assert.assertEquals(4, partition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        }
    }

    @Test
    public void testCancelBatchRollup() throws Exception {
        starRocksAssert.withTable("create table db1.tbl2(k1 int, k2 int, k3 int) " +
                "distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // batch add 3 rollups
        String stmtStr = "alter table db1.tbl2 add rollup r1(k1) " +
                "duplicate key(k1), r2(k1, k2) duplicate key(k1), r3(k2) duplicate key(k2);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmtStr, ctx);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(alterTableStmt);

        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        Assert.assertEquals(3, alterJobs.size());
        List<Long> jobIds = Lists.newArrayList(alterJobs.keySet());

        Database db = GlobalStateMgr.getCurrentState().getDb("db1");
        Assert.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTable("tbl2");
        Assert.assertNotNull(tbl);
        Assert.assertEquals(OlapTableState.ROLLUP, tbl.getState());

        // cancel rollup jobs
        stmtStr = "cancel alter table rollup from db1.tbl2 (" + Joiner.on(",").join(jobIds) + ")";
        CancelAlterTableStmt cancelStmt = (CancelAlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmtStr, ctx);
        GlobalStateMgr.getCurrentState().cancelAlter(cancelStmt);

        for (AlterJobV2 alterJob : alterJobs.values()) {
            Assert.assertEquals(AlterJobV2.JobState.CANCELLED, alterJob.getJobState());
        }
        Assert.assertEquals(OlapTableState.NORMAL, tbl.getState());
        for (Partition partition : tbl.getPartitions()) {
            Assert.assertEquals(1, partition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        }
    }
}
