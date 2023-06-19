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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/LoadManagerTest.java

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

package com.starrocks.load.loadv2;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.meta.MetaContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class LoadManagerTest {
    private LoadMgr loadManager;
    private final String fieldName = "idToLoadJob";

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        File file = new File("./loadManagerTest");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testSerializationNormal(@Mocked GlobalStateMgr globalStateMgr,
                                        @Injectable Database database,
                                        @Injectable Table table) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                GlobalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_56;
            }
        };

        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "");
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        File file = serializeToFile(loadManager);

        LoadMgr newLoadManager = deserializeFromFile(file);

        Map<Long, LoadJob> loadJobs = Deencapsulation.getField(loadManager, fieldName);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assert.assertEquals(loadJobs, newLoadJobs);
    }

    @Test
    public void testSerializationWithJobRemoved(@Mocked MetaContext metaContext,
                                                @Mocked GlobalStateMgr globalStateMgr,
                                                @Injectable Database database,
                                                @Injectable Table table) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
            }
        };

        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "");
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        //make job1 don't serialize
        Config.label_keep_max_second = 1;
        Thread.sleep(2000);

        File file = serializeToFile(loadManager);

        LoadMgr newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);

        Assert.assertEquals(0, newLoadJobs.size());
    }

    @Test
    public void testDeserializationWithJobRemoved(@Mocked MetaContext metaContext,
                                                  @Mocked GlobalStateMgr globalStateMgr,
                                                  @Injectable Database database,
                                                  @Injectable Table table) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                GlobalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        Config.label_keep_max_second = 10;

        // 1. serialize 1 job to file
        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "");
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
        File file = serializeToFile(loadManager);

        // 2. read it directly, expect 1 job
        LoadMgr newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assert.assertEquals(1, newLoadJobs.size());

        // 3. set max keep second to 1, then read it again
        // the job expired, expect read 0 job
        Config.label_keep_max_second = 1;
        Thread.sleep(2000);
        newLoadManager = deserializeFromFile(file);
        newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assert.assertEquals(0, newLoadJobs.size());
    }

    private File serializeToFile(LoadMgr loadManager) throws Exception {
        File file = new File("./loadManagerTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        loadManager.write(dos);
        dos.flush();
        dos.close();
        return file;
    }

    private LoadMgr deserializeFromFile(File file) throws Exception {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        LoadMgr loadManager = new LoadMgr(new LoadJobScheduler());
        loadManager.readFields(dis);
        return loadManager;
    }

    @Test
    public void testRemoveOldLoadJob(@Mocked GlobalStateMgr globalStateMgr,
                                     @Injectable Database db) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                result = db;
            }
        };

        loadManager = new LoadMgr(new LoadJobScheduler());
        int origLabelKeepMaxSecond = Config.label_keep_max_second;
        int origLabelKeepMaxNum = Config.label_keep_max_num;
        Map<Long, LoadJob> idToLoadJob = Deencapsulation.getField(loadManager, "idToLoadJob");
        Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Deencapsulation.getField(
                loadManager, "dbIdToLabelToLoadJobs");
        long currentTimeMs = System.currentTimeMillis();

        // finished insert job
        LoadJob job0 = new InsertLoadJob("job0", 0L, 1L, currentTimeMs - 101000, "", "");
        job0.id = 10;
        job0.finishTimestamp = currentTimeMs - 101000;
        Deencapsulation.invoke(loadManager, "addLoadJob", job0);

        // broker load job
        // loading
        LoadJob job1 = new BrokerLoadJob(1L, "job1", null, null, null);
        job1.state = JobState.LOADING;
        job1.id = 11;
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
        // cancelled
        LoadJob job2 = new BrokerLoadJob(1L, "job2", null, null, null);
        job2.finishTimestamp = currentTimeMs - 3000;
        job2.state = JobState.CANCELLED;
        job2.id = 16;
        Deencapsulation.invoke(loadManager, "addLoadJob", job2);
        // finished
        LoadJob job22 = new BrokerLoadJob(1L, "job2", null, null, null);
        job22.finishTimestamp = currentTimeMs - 1000;
        job22.state = JobState.FINISHED;
        job22.id = 12;
        Deencapsulation.invoke(loadManager, "addLoadJob", job22);

        // spark load job
        // etl
        LoadJob job3 = new SparkLoadJob(2L, "job3", null, null);
        job3.state = JobState.ETL;
        job3.id = 13;
        Deencapsulation.invoke(loadManager, "addLoadJob", job3);
        // cancelled
        LoadJob job4 = new SparkLoadJob(2L, "job4", null, null);
        job4.finishTimestamp = currentTimeMs - 51000;
        job4.state = JobState.CANCELLED;
        job4.id = 14;
        Deencapsulation.invoke(loadManager, "addLoadJob", job4);
        // finished
        LoadJob job42 = new SparkLoadJob(2L, "job4", null, null);
        job42.finishTimestamp = currentTimeMs - 2000;
        job42.state = JobState.FINISHED;
        job42.id = 15;
        Deencapsulation.invoke(loadManager, "addLoadJob", job42);

        Assert.assertEquals(7, idToLoadJob.size());
        Assert.assertEquals(3, dbIdToLabelToLoadJobs.size());

        // test remove jobs by label_keep_max_second
        // remove db 0, job0
        Config.label_keep_max_second = 100;
        Config.label_keep_max_num = 10;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assert.assertEquals(6, idToLoadJob.size());
        Assert.assertFalse(idToLoadJob.containsKey(10L));
        Assert.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assert.assertFalse(dbIdToLabelToLoadJobs.containsKey(0L));

        // remove cancelled job4
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 10;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assert.assertEquals(5, idToLoadJob.size());
        Assert.assertFalse(idToLoadJob.containsKey(14L));
        Assert.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assert.assertEquals(1, dbIdToLabelToLoadJobs.get(2L).get("job4").size());

        // test remove jobs by label_keep_max_num
        // remove cancelled job2, finished job4
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 3;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assert.assertEquals(3, idToLoadJob.size());
        Assert.assertFalse(idToLoadJob.containsKey(15L));
        Assert.assertFalse(idToLoadJob.containsKey(16L));
        Assert.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assert.assertEquals(1, dbIdToLabelToLoadJobs.get(1L).get("job2").size());
        Assert.assertFalse(dbIdToLabelToLoadJobs.get(2L).containsKey("job4"));

        // remove finished job2
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 1;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assert.assertEquals(2, idToLoadJob.size());
        Assert.assertFalse(idToLoadJob.containsKey(12L));
        Assert.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assert.assertFalse(dbIdToLabelToLoadJobs.get(1L).containsKey("job2"));

        // recover config
        Config.label_keep_max_second = origLabelKeepMaxSecond;
        Config.label_keep_max_num = origLabelKeepMaxNum;
    }
}
