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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/InsertLoadJobTest.java

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
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.FailMsg;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InsertLoadJobTest {

    @Test
    public void testGetTableNames(@Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        InsertLoadJob insertLoadJob = new InsertLoadJob("label", 1L, 1L, 1000, "", "", null);
        String tableName = "table1";
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                result = database;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), anyLong);
                result = table;
                table.getName();
                result = tableName;
            }
        };
        Set<String> tableNames = insertLoadJob.getTableNamesForShow();
        Assertions.assertEquals(1, tableNames.size());
        Assertions.assertEquals(true, tableNames.contains(tableName));
        Assertions.assertEquals(JobState.FINISHED, insertLoadJob.getState());
        Assertions.assertEquals(Integer.valueOf(100), Deencapsulation.getField(insertLoadJob, "progress"));

    }

    @Test
    public void testUpdateProgress(@Mocked GlobalStateMgr globalStateMgr,
                                   @Injectable Database database,
                                   @Injectable Table table) throws MetaNotFoundException {
        {
            InsertLoadJob loadJob = new InsertLoadJob("label", 1L,
                    1L, 1000, "", "", null);
            TUniqueId loadId = new TUniqueId(1, 2);

            TUniqueId fragmentId = new TUniqueId(3, 4);
            Set<TUniqueId> fragmentIds = new HashSet<>();
            fragmentIds.add(fragmentId);

            List<Long> backendIds = new ArrayList<>();
            backendIds.add(10001L);

            loadJob.initLoadProgress(loadId, fragmentIds, backendIds);
            loadJob.setEstimateScanRow(100);
            loadJob.setLoadFileInfo(0, 0);

            TReportExecStatusParams params = new TReportExecStatusParams();
            params.setSource_load_rows(40);
            params.setQuery_id(loadId);
            params.setFragment_instance_id(fragmentId);

            loadJob.updateProgress(params);
            Assertions.assertEquals(39, loadJob.getProgress());

            Assertions.assertTrue(loadJob.getTabletCommitInfos().isEmpty());
            Assertions.assertTrue(loadJob.getTabletFailInfos().isEmpty());
        }

        {
            InsertLoadJob loadJob = new InsertLoadJob("label", 1L,
                    1L, 1000, "", "", null);
            TUniqueId loadId = new TUniqueId(1, 2);

            TUniqueId fragmentId = new TUniqueId(3, 4);
            Set<TUniqueId> fragmentIds = new HashSet<>();
            fragmentIds.add(fragmentId);

            List<Long> backendIds = new ArrayList<>();
            backendIds.add(10001L);

            loadJob.initLoadProgress(loadId, fragmentIds, backendIds);
            loadJob.setEstimateScanRow(0);
            loadJob.setLoadFileInfo(10, 100);

            TReportExecStatusParams params = new TReportExecStatusParams();
            params.setQuery_id(loadId);
            params.setFragment_instance_id(fragmentId);
            params.setSource_scan_bytes(80);

            loadJob.updateProgress(params);
            Assertions.assertEquals(80, loadJob.getProgress());

            Assertions.assertTrue(loadJob.getTabletCommitInfos().isEmpty());
            Assertions.assertTrue(loadJob.getTabletFailInfos().isEmpty());
        }
    }

    @Test
    public void testUnprotectedExecuteCancel(@Mocked GlobalStateMgr globalStateMgr,
                                             @Mocked Coordinator coordinator) throws Exception {
        InsertLoadJob insertLoadJob = new InsertLoadJob("label", 1L, 1L, 1000, "", "", coordinator);
        new Expectations() {
            {
                globalStateMgr.getGlobalTransactionMgr().getCallbackFactory().removeCallback(anyLong);
                times = 1;
                globalStateMgr.getGlobalTransactionMgr().abortTransaction(anyLong, anyLong, anyString,
                        (List<TabletCommitInfo>) any, (List<TabletFailInfo>) any, null);
                times = 1;

                coordinator.cancel(anyString);
                times = 1;
            }
        };

        insertLoadJob.unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"), true);
        Assertions.assertEquals(JobState.CANCELLED, insertLoadJob.getState());
    }
}
