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
import com.starrocks.common.UserException;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
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

    @Mocked
    private CompactionMgr compactionMgr;

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

        db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
    }

    @Test
    public void testProcessCompactionClauseWithOnePartitionForLakeTable() {
        List<String> partitionNames = Arrays.asList("p1");
        CompactionClause compactionClause = new CompactionClause(partitionNames, true, null);
        List<AlterClause> alterList = Collections.singletonList(compactionClause);
        try {
            new Expectations() {
                {
                    compactionMgr.triggerManualCompaction((PartitionIdentifier) any);
                    times = 1;
                }
            };
            compactionHandler.process(alterList, db, olapTable);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail("process should not throw exceptions here");
        }
    }

    @Test
    public void testProcessCompactionClauseWithTwoPartitionForLakeTable() {
        List<String> partitionNames = Arrays.asList("p1", "p2");
        CompactionClause compactionClause = new CompactionClause(partitionNames, true, null);
        List<AlterClause> alterList = Collections.singletonList(compactionClause);
        try {
            new Expectations() {
                {
                    compactionMgr.triggerManualCompaction((PartitionIdentifier) any);
                    times = 2;
                }
            };
            compactionHandler.process(alterList, db, olapTable);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail("process should not throw exceptions here");
        }
    }


    @Test
    public void testProcessCompactionClauseWithNoPartitionsForLakeTable() {
        List<String> partitionNames = Collections.emptyList();
        CompactionClause compactionClause = new CompactionClause(partitionNames, true, null);
        List<AlterClause> alterList = Collections.singletonList(compactionClause);
        try {
            new Expectations() {
                {
                    compactionMgr.triggerManualCompaction((PartitionIdentifier) any);
                    minTimes = 2;
                }
            };
            compactionHandler.process(alterList, db, olapTable);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail("process should not throw exceptions here");
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testProcessNonCompactionClause() {
        AlterClause nonCompactionClause = mock(AlterClause.class);
        List<AlterClause> alterList = Collections.singletonList(nonCompactionClause);
        try {
            compactionHandler.process(alterList, db, olapTable);
        } catch (UserException e) {
            Assert.fail("process should not throw user exceptions here");
        }
    }
}
