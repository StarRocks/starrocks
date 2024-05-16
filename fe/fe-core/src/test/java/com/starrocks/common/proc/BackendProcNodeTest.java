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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/proc/BackendProcNodeTest.java

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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class BackendProcNodeTest {
    private Backend b1;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private TabletInvertedIndex tabletInvertedIndex;

    @Before
    public void setUp() {
        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = 10000L;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                minTimes = 0;
                result = tabletInvertedIndex;

                tabletInvertedIndex.getTabletNumByBackendIdAndPathHash(anyLong, anyLong);
                minTimes = 0;
                result = 1;
            }
        };

        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        disks.put("/home/disk1", new DiskInfo("/home/disk1"));
        ImmutableMap<String, DiskInfo> immutableMap = ImmutableMap.copyOf(disks);
        b1.setDisks(immutableMap);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testResultNormal() throws AnalysisException {
        BackendProcNode node = new BackendProcNode(b1);
        ProcResult result;

        // fetch result
        result = node.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertTrue(result.getRows().size() >= 1);
        Assert.assertEquals(
                Lists.newArrayList("RootPath", "DataUsedCapacity", "OtherUsedCapacity", "AvailCapacity",
                        "TotalCapacity", "TotalUsedPct", "State", "PathHash", "StorageMedium", "TabletNum",
                        "DataTotalCapacity", "DataUsedPct"),
                result.getColumnNames());
    }

}
