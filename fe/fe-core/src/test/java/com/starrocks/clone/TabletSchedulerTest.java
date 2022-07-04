// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.clone;

import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CloneTask;
import com.starrocks.thrift.TStorageMedium;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class TabletSchedulerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Before
    public void setup() throws Exception {
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getColocateTableIndex();
                minTimes = 0;
                result = new ColocateTableIndex();
            }
        };
    }

    SystemInfoService systemInfoService = new SystemInfoService();
    TabletInvertedIndex tabletInvertedIndex = new TabletInvertedIndex();
    TabletSchedulerStat tabletSchedulerStat = new TabletSchedulerStat();
    @Test
    public void testSubmitBatchTaskIfNotExpired() throws Exception {
        Database badDb = new Database(1, "mal");
        Database goodDB = new Database(2, "bueno");
        Table badTable = new Table(3, "mal", Table.TableType.OLAP, new ArrayList<>());
        Table goodTable = new Table(4, "bueno", Table.TableType.OLAP, new ArrayList<>());
        Partition badPartition = new Partition(5, "mal", null, null);
        Partition goodPartition = new Partition(6, "bueno", null, null);

        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(badDb, new HashSet<>());
        recycleBin.recycleTable(goodDB.getId(), badTable);
        recycleBin.recyclePartition(goodDB.getId(), goodTable.getId(), badPartition,
                null, new DataProperty(TStorageMedium.HDD), (short)2, false);

        List<AgentTask> allTasks = new ArrayList<>();
        List<Triple<Database, Table, Partition>> arguments = Arrays.asList(
                Triple.of(badDb, goodTable, goodPartition), // will discard
                Triple.of(goodDB, badTable, goodPartition), // will discard
                Triple.of(goodDB, goodTable, badPartition), // will discard
                Triple.of(goodDB, goodTable, goodPartition) // only submit this
        );
        for (Triple<Database, Table, Partition> triple : arguments) {
            CloneTask cloneTask = new CloneTask(
                    1,
                    triple.getLeft().getId(),
                    triple.getMiddle().getId(),
                    triple.getRight().getId(),
                    1,
                    1,
                    1,
                    null,
                    TStorageMedium.HDD,
                    1,
                    100000);
            allTasks.add(cloneTask);
        }
        new Expectations() {
            {
                AgentTaskExecutor.submit((AgentBatchTask) any);
                times = 2;
            }
        };

        long almostExpireTime = System.currentTimeMillis() + (Config.catalog_trash_expire_second - 1) * 1000L;
        TabletScheduler tabletScheduler = new TabletScheduler(globalStateMgr, systemInfoService, tabletInvertedIndex, tabletSchedulerStat);
        AgentBatchTask tasks = tabletScheduler.submitBatchTaskIfNotExpired(allTasks, recycleBin, almostExpireTime);
        Assert.assertEquals(4, tasks.getTaskNum());

        long expireTime = System.currentTimeMillis() + (Config.catalog_trash_expire_second + 600) * 1000L;
        tasks = tabletScheduler.submitBatchTaskIfNotExpired(allTasks, recycleBin, expireTime);
        Assert.assertEquals(1, tasks.getTaskNum());

        AgentTaskQueue.clearAllTasks();
    }
}