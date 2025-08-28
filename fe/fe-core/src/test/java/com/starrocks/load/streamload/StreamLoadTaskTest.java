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
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.WarehouseManager;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.WarehouseIdleChecker;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamLoadTaskTest {

    @Mocked
    private DefaultCoordinator coord;

    private StreamLoadTask streamLoadTask;

    @BeforeEach
    public void setUp() {
        long id = 123L;
        String label = "label_abc";
        long timeoutMs = 10000L;
        long createTimeMs = System.currentTimeMillis();
        boolean isRoutineLoad = false;
        streamLoadTask =
                new StreamLoadTask(id, new Database(), new OlapTable(), label, "", "", timeoutMs, createTimeMs, isRoutineLoad,
                        WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    public void testAfterCommitted() throws StarRocksException {
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
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testAfterAborted() throws StarRocksException {
        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = false;
            }
        };
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;

        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        long ts = System.currentTimeMillis();
        streamLoadTask.afterAborted(txnState, txnOperated, "");
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
        Assertions.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(streamLoadTask.getCurrentWarehouseId()));
    }

    @Test
    public void testAfterVisible() {
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        long ts = System.currentTimeMillis();
        streamLoadTask.afterVisible(txnState, txnOperated);
        Assertions.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(streamLoadTask.getCurrentWarehouseId()));
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

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
    }

    @Test
    public void testSetLoadStateWithManualLoadTxnCommitAttachment() {
        ManualLoadTxnCommitAttachment attachment = mock(ManualLoadTxnCommitAttachment.class);
        when(attachment.getLoadedRows()).thenReturn(100L);
        when(attachment.getFilteredRows()).thenReturn(10L);
        when(attachment.getUnselectedRows()).thenReturn(5L);
        when(attachment.getLoadedBytes()).thenReturn(1000L);
        when(attachment.getErrorLogUrl()).thenReturn("http://error.log");
        when(attachment.getBeginTxnTime()).thenReturn(100L);
        when(attachment.getReceiveDataTime()).thenReturn(200L);
        when(attachment.getPlanTime()).thenReturn(300L);

        streamLoadTask.setLoadState(attachment, "Error message");

        TLoadInfo loadInfo = streamLoadTask.toThrift().get(0);

        Assertions.assertEquals(100L, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(10L, loadInfo.getNum_filtered_rows());
        Assertions.assertEquals(5L, loadInfo.getNum_unselected_rows());
        Assertions.assertEquals(1000L, loadInfo.getNum_scan_bytes());
        Assertions.assertEquals("http://error.log", loadInfo.getUrl());
        Assertions.assertEquals("Error message", loadInfo.getError_msg());
    }

    @Test
    public void testSetLoadStateWithRLTaskTxnCommitAttachment() {
        RLTaskTxnCommitAttachment attachment = mock(RLTaskTxnCommitAttachment.class);
        when(attachment.getLoadedRows()).thenReturn(200L);
        when(attachment.getFilteredRows()).thenReturn(20L);
        when(attachment.getUnselectedRows()).thenReturn(10L);
        when(attachment.getLoadedBytes()).thenReturn(2000L);
        when(attachment.getErrorLogUrl()).thenReturn("http://error.log.rl");

        streamLoadTask.setLoadState(attachment, "Another error message");

        TLoadInfo loadInfo = streamLoadTask.toThrift().get(0);

        Assertions.assertEquals(200L, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(20L, loadInfo.getNum_filtered_rows());
        Assertions.assertEquals(10L, loadInfo.getNum_unselected_rows());
        Assertions.assertEquals("http://error.log.rl", loadInfo.getUrl());
        Assertions.assertEquals("Another error message", loadInfo.getError_msg());
    }

    @Test
    public void testBuildProfile() throws StarRocksException {
        streamLoadTask.setCoordinator(coord);
        streamLoadTask.setIsSyncStreamLoad(true);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = true;
                coord.getQueryProfile();
                result = null;
            }
        };
        TUniqueId labelId = new TUniqueId(4, 5);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testDuplicateBeginTxn() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        TUniqueId requestId = new TUniqueId(100056, 560001);
        StreamLoadTask streamLoadTask1 = Mockito.spy(new StreamLoadTask(0, new Database(), new OlapTable(),
                                                                        "", "", "", 10, 10, false,
                                                                        WarehouseManager.DEFAULT_RESOURCE));
        TransactionState.TxnCoordinator coordinator =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "192.168.1.2");
        doThrow(new DuplicatedRequestException("Duplicate request", 0L, ""))
                .when(streamLoadTask1).unprotectedBeginTxn(same(requestId), same(coordinator));
        streamLoadTask1.beginTxn(0, 1, requestId, coordinator, resp);
        Assertions.assertTrue(resp.stateOK());
        streamLoadTask1.beginTxn(0, 1, requestId, coordinator, resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testBeginTxnInVariousStates() throws StarRocksException {
        StreamLoadTask taskReal = new StreamLoadTask(999, new Database(), new OlapTable(), "t_label", "u", "127.0.0.1",
                10000, 2, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        StreamLoadTask task = Mockito.spy(taskReal);
        doNothing().when(task).unprotectedBeginTxn(any(), any());
        TransactionResult resp = new TransactionResult();
        task.beginTxn(0, 2, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe"), resp);
        Assertions.assertTrue(resp.stateOK());
        for (StreamLoadTask.State st : java.util.List.of(StreamLoadTask.State.PREPARED,
                StreamLoadTask.State.COMMITED,
                StreamLoadTask.State.CANCELLED,
                StreamLoadTask.State.FINISHED)) {
            Deencapsulation.invoke(task, "setState", st);
            TransactionResult r = new TransactionResult();
            task.beginTxn(0, 2, null,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe"), r);
            Assertions.assertTrue(r.stateOK());
        }
    }

    @Test
    public void testTryLoadRedirectSuccessAndFailure() throws StarRocksException {
        StreamLoadTask task = new StreamLoadTask(1000, new Database(), new OlapTable(), "t_label", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task, "tableName", "tbl");
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.LOADING);
        java.util.Map<Integer, TNetworkAddress> addrMap = com.google.common.collect.Maps.newHashMap();
        addrMap.put(0, new TNetworkAddress("beHost", 8040));
        Deencapsulation.setField(task, "channelIdToBEHTTPAddress", addrMap);
        TransactionResult respOk = new TransactionResult();
        TNetworkAddress ret = task.tryLoad(0, task.getTableName(), respOk);
        Assertions.assertNotNull(ret);

        StreamLoadTask task2 = new StreamLoadTask(1001, new Database(), new OlapTable(), "t_label2", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task2, "tableName", "tbl2");
        Deencapsulation.invoke(task2, "setState", StreamLoadTask.State.LOADING);
        Deencapsulation.setField(task2, "channelIdToBEHTTPAddress", com.google.common.collect.Maps.newHashMap());
        TransactionResult respErr = new TransactionResult();
        task2.tryLoad(0, task2.getTableName(), respErr);
        Assertions.assertFalse(respErr.stateOK());
    }

    @Test
    public void testExecuteTaskLoadingBranches() {
        StreamLoadTask task = new StreamLoadTask(1002, new Database(), new OlapTable(), "t_label3", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task, "tableName", "tbl3");
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.LOADING);
        java.util.Map<Integer, TNetworkAddress> addrMap = com.google.common.collect.Maps.newHashMap();
        addrMap.put(0, new TNetworkAddress("beHost", 8040));
        Deencapsulation.setField(task, "channelIdToBEHTTPAddress", addrMap);
        TransactionResult resp = new TransactionResult();
        TNetworkAddress addr = task.executeTask(0, task.getTableName(), null, resp);
        Assertions.assertNotNull(addr);

        StreamLoadTask task2 = new StreamLoadTask(1003, new Database(), new OlapTable(), "t_label4", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task2, "tableName", "tbl4");
        Deencapsulation.invoke(task2, "setState", StreamLoadTask.State.LOADING);
        Deencapsulation.setField(task2, "channelIdToBEHTTPAddress", com.google.common.collect.Maps.newHashMap());
        TransactionResult resp2 = new TransactionResult();
        task2.executeTask(0, task2.getTableName(), null, resp2);
        Assertions.assertFalse(resp2.stateOK());
    }

    @Test
    public void testWaitCoordFinishDataQualityFail(@Mocked Coordinator c) {
        StreamLoadTask task = new StreamLoadTask(1005, new Database(), new OlapTable(), "t_label6", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.PREPARING);
        Deencapsulation.setField(task, "coord", c);
        StreamLoadKvParams params = new StreamLoadKvParams(java.util.Map.of("max_filter_ratio", "0.1"));
        Deencapsulation.setField(task, "streamLoadParams", params);
        new Expectations() {
            {
                c.join(anyInt);
                result = true;
                c.getExecStatus();
                result = Status.OK;
                c.getLoadCounters();
                result = java.util.Map.of("dpp.norm.ALL", "100", "dpp.abnorm.ALL", "50",
                        "unselected.rows", "0", "loaded.bytes", "10");
                c.isEnableLoadProfile();
                result = false;
                c.getTrackingUrl();
                result = "url";
            }
        };
        TransactionResult resp = new TransactionResult();
        task.waitCoordFinish(resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testWaitCoordFinishSuccess(@Mocked Coordinator c) {
        StreamLoadTask task = new StreamLoadTask(1006, new Database(), new OlapTable(), "t_label7", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.PREPARING);
        Deencapsulation.setField(task, "coord", c);
        StreamLoadKvParams params = new StreamLoadKvParams(java.util.Map.of("max_filter_ratio", "0.5"));
        Deencapsulation.setField(task, "streamLoadParams", params);
        new Expectations() {
            {
                c.join(anyInt);
                result = true;
                c.getExecStatus();
                result = Status.OK;
                c.getLoadCounters();
                result = java.util.Map.of("dpp.norm.ALL", "100", "dpp.abnorm.ALL", "10",
                        "unselected.rows", "0", "loaded.bytes", "10");
                c.isEnableLoadProfile();
                result = false;
                c.getTrackingUrl();
                result = "url";
            }
        };
        TransactionResult resp = new TransactionResult();
        task.waitCoordFinish(resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testCommitTxnNotPrepared() throws StarRocksException {
        StreamLoadTask task = new StreamLoadTask(1007, new Database(), new OlapTable(), "t_label8", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.BEFORE_LOAD);
        TransactionResult resp = new TransactionResult();
        task.commitTxn(null, resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testManualCancelWhileCommitting() throws StarRocksException {
        StreamLoadTask task = new StreamLoadTask(1008, new Database(), new OlapTable(), "t_label9", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task, "isCommitting", true);
        TransactionResult resp = new TransactionResult();
        task.manualCancelTask(resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testCancelTaskBranches() {
        StreamLoadTask task = new StreamLoadTask(1009, new Database(), new OlapTable(), "t_label10", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.CANCELLED);
        Deencapsulation.setField(task, "errorMsg", "err");
        String r = task.cancelTask("manual");
        Assertions.assertTrue(r.contains("CANCELLED"));
        StreamLoadTask task2 = new StreamLoadTask(1010, new Database(), new OlapTable(), "t_label11", "u", "127.0.0.1",
                10000, 2, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        String r2 = task2.cancelTask("manual");
        Assertions.assertNull(r2);
    }

    @Test
    public void testCheckNeedRemoveAndDataQuality() {
        StreamLoadTask task = new StreamLoadTask(1011, new Database(), new OlapTable(), "t_label12", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.FINISHED);
        Deencapsulation.setField(task, "endTimeMs", System.currentTimeMillis());
        Assertions.assertFalse(task.checkNeedRemove(System.currentTimeMillis(), false));
        long end = (long) Deencapsulation.getField(task, "endTimeMs");
        long later = end + Config.stream_load_task_keep_max_second * 1000L + 10;
        Assertions.assertTrue(task.checkNeedRemove(later, false));
        Assertions.assertTrue((Boolean) Deencapsulation.invoke(task, "checkDataQuality"));
    }

    @Test
    public void testStateHelpersAndStringByType() {
        StreamLoadTask task = new StreamLoadTask(1012, new Database(), new OlapTable(), "t_label13", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals("PARALLEL_STREAM_LOAD", task.getStringByType());
        task.setType(StreamLoadTask.Type.ROUTINE_LOAD);
        Assertions.assertEquals("ROUTINE_LOAD", task.getStringByType());
        task.setType(StreamLoadTask.Type.STREAM_LOAD);
        Assertions.assertEquals("STREAM_LOAD", task.getStringByType());
        task.setType(StreamLoadTask.Type.MULTI_STATEMENT_STREAM_LOAD);
        Assertions.assertEquals("UNKNOWN", task.getStringByType());
    }

    @Test
    public void testShowInfoTrackingUrl() {
        StreamLoadTask task = new StreamLoadTask(1013, new Database(), new OlapTable(), "t_label14", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(task, "trackingUrl", "url");
        java.util.List<java.util.List<String>> rows = task.getShowInfo();
        Assertions.assertEquals(1, rows.size());
        int lastIdx = rows.get(0).size() - 1;
        Assertions.assertTrue(rows.get(0).get(lastIdx).startsWith("select tracking_log"));
    }

    @Test
    public void testToThriftProgress() {
        StreamLoadTask task = new StreamLoadTask(1014, new Database(), new OlapTable(), "t_label15", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.invoke(task, "setState", StreamLoadTask.State.FINISHED);
        var info = task.toThrift().get(0);
        Assertions.assertEquals("100%", info.getProgress());
        StreamLoadTask task2 = new StreamLoadTask(1015, new Database(), new OlapTable(), "t_label16", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        var info2 = task2.toThrift().get(0);
        Assertions.assertEquals("0%", info2.getProgress());
    }

    @Test
    public void testGsonPreAndPostProcess() throws Exception {
        StreamLoadTask task = new StreamLoadTask(1016, new Database(), new OlapTable(), "t_label17", "u", "127.0.0.1",
                10000, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        TUniqueId id = new TUniqueId(1, 2);
        task.setTUniqueId(id);
        task.gsonPreProcess();
        Deencapsulation.setField(task, "loadId", null);
        task.gsonPostProcess();
        Assertions.assertEquals(id.getHi(), task.getTUniqueId().getHi());
    }
}
