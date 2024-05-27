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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;

public class StreamLoadTaskTest {

    @Mocked
    private DefaultCoordinator coord;

    private StreamLoadTask streamLoadTask;

    @Before
    public void setUp() {
        long id = 123L;
        String label = "label_abc";
        long timeoutMs = 10000L;
        long createTimeMs = System.currentTimeMillis();
        boolean isRoutineLoad = false;
        long warehouseId = 0L;
        streamLoadTask =
                new StreamLoadTask(id, new Database(), new OlapTable(), label, timeoutMs, createTimeMs, isRoutineLoad,
                        warehouseId);
    }

    @Test
    public void testAfterCommitted() throws UserException {
        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = false;
            }
        };
        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assert.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assert.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testAfterAborted() throws UserException {
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;

        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assert.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        streamLoadTask.afterAborted(txnState, txnOperated, "");
        Assert.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testNoPartitionsHaveDataLoad() {
        Map<String, String> loadCounters = Maps.newHashMap();
        loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "0");
        loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
        loadCounters.put(LoadJob.UNSELECTED_ROWS, "0");
        loadCounters.put(LoadJob.LOADED_BYTES, "0");

        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.join(anyInt);
                result = true;
                coord.getLoadCounters();
                returns(null, loadCounters);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(UserException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
        ExceptionChecker.expectThrowsWithMsg(UserException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
    }
}
