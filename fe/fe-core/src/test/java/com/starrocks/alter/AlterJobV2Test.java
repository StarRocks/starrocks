// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/AlterJobV2Test.java

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

import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class AlterJobV2Test {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable(
                        "CREATE TABLE test.schema_change_test(k1 int, k2 int, k3 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable(
                        "CREATE TABLE test.segmentv2(k1 int, k2 int, v1 int sum) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    @Test
    public void testSchemaChange() throws Exception {
        // 1. process a schema change job
        String alterStmtStr = "alter table test.schema_change_test add column k4 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, connectContext);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getSchemaChangeHandler().getAlterJobsV2();
        Assert.assertEquals(1, alterJobs.size());
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // 3. check show alter table column
        String showAlterStmtStr = "show alter table column from test;";
        ShowAlterStmt showAlterStmt =
                (ShowAlterStmt) UtFrameUtils.parseAndAnalyzeStmt(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testRollup() throws Exception {
        // 1. process a rollup job
        String alterStmtStr = "alter table test.schema_change_test add rollup test_rollup(k1, k2);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, connectContext);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // 3. check show alter table column
        String showAlterStmtStr = "show alter table rollup from test;";
        ShowAlterStmt showAlterStmt =
                (ShowAlterStmt) UtFrameUtils.parseAndAnalyzeStmt(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }
}
