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

package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

public class StreamLoadManagerTest2 {

    @Test
    public void testGetRunningTaskCount() {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        Database database = new Database(20000L, "test");
        OlapTable olapTable = new OlapTable(30000L, "test", Lists.newArrayList(),
                KeysType.DUP_KEYS, new PartitionInfo(), new HashDistributionInfo());
        // for warehouse 0
        StreamLoadTask task1 = new StreamLoadTask(40001L, database, olapTable, "label1", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 0);
        task1.setState(StreamLoadTask.State.LOADING);
        streamLoadManager.addLoadTask(task1);
        StreamLoadTask task2 = new StreamLoadTask(40002L, database, olapTable, "label2", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 0);
        task2.setState(StreamLoadTask.State.FINISHED);
        streamLoadManager.addLoadTask(task2);
        StreamLoadTask task3 = new StreamLoadTask(40003L, database, olapTable, "label3", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 0);
        task3.setState(StreamLoadTask.State.CANCELLED);
        streamLoadManager.addLoadTask(task3);

        // for warehouse 1
        StreamLoadTask task4 = new StreamLoadTask(40004L, database, olapTable, "label4", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 1);
        task4.setState(StreamLoadTask.State.LOADING);
        streamLoadManager.addLoadTask(task4);
        StreamLoadTask task5 = new StreamLoadTask(40005L, database, olapTable, "label5", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 1);
        task5.setState(StreamLoadTask.State.FINISHED);
        streamLoadManager.addLoadTask(task5);
        StreamLoadTask task6 = new StreamLoadTask(40006L, database, olapTable, "label6", "root",
                "127.0.0.1", 300000, System.currentTimeMillis(), false, 1);
        task6.setState(StreamLoadTask.State.CANCELLED);
        streamLoadManager.addLoadTask(task6);

        java.util.Map<Long, Long> result = streamLoadManager.getRunningTaskCount();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals((Long) 1L, result.get(1L));
        Assert.assertEquals((Long) 1L, result.get(0L));
    }
}
