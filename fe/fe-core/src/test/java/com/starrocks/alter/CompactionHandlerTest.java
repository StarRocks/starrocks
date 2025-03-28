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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;

public class CompactionHandlerTest {

    private static Database db;
    private static OlapTable olapTable;
    private static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;

    private CompactionHandler compactionHandler = new CompactionHandler();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(GlobalStateMgrTestUtil.testDb1)
                    .useDatabase(GlobalStateMgrTestUtil.testDb1);

        starRocksAssert.withTable("CREATE TABLE testTable1\n" +
                    "(\n" +
                    "    v1 date,\n" +
                    "    v2 int,\n" +
                    "    v3 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(`v1`)\n" +
                    "PARTITION BY RANGE(v1)\n" +
                    "(\n" +
                    "    PARTITION p1 values less than('2020-02-01'),\n" +
                    "    PARTITION p2 values less than('2020-03-01')\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(v1) BUCKETS 3\n" +
                    "PROPERTIES('replication_num' = '1');");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
    }

    @After
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getCompactionMgr().clearPartitions();
        db.dropTable(olapTable.getName());
    }

    private void processAlterClauses(List<String> partitionNames, int expectedValue) {
        CompactionClause compactionClause = new CompactionClause(partitionNames, true, null);
        List<AlterClause> alterList = Collections.singletonList(compactionClause);
        try {
            compactionHandler.process(alterList, db, olapTable);
            Assert.assertEquals(expectedValue, GlobalStateMgr.getCurrentState().getCompactionMgr().getPartitionStatsCount());
        } catch (StarRocksException e) {
            e.printStackTrace();
            Assert.fail("process should not throw exceptions here");
        }
    }

    @Test
    public void testProcessCompactionClauseWithOnePartitionForLakeTable() {
        processAlterClauses(Collections.singletonList("p1"), 1);
    }

    @Test
    public void testProcessCompactionClauseWithTwoPartitionForLakeTable() {
        processAlterClauses(Arrays.asList("p1", "p2"), 2);
    }

    @Test
    public void testProcessCompactionClauseWithNoPartitionsForLakeTable() {
        // compaction should trigger on all partitions
        processAlterClauses(Collections.emptyList(), 2);
    }

    @Test(expected = IllegalStateException.class)
    public void testProcessNonCompactionClause() {
        AlterClause nonCompactionClause = mock(AlterClause.class);
        List<AlterClause> alterList = Collections.singletonList((nonCompactionClause));
        try {
            compactionHandler.process(alterList, db, olapTable);
        } catch (StarRocksException e) {
            Assert.fail("process should not throw user exceptions here");
        }
    }
}
