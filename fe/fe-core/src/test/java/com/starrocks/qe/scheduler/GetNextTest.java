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

package com.starrocks.qe.scheduler;

import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.rpc.PFetchDataRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test getNext is executed successfully or failed by several conditions.
 * Could also see {@link StartSchedulingTest}.
 */
public class GetNextTest extends SchedulerTestBase {
    private boolean originalEnableProfile;

    @Before
    public void before() {
        originalEnableProfile = connectContext.getSessionVariable().isEnableProfile();
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setEnableProfile(originalEnableProfile);
    }

    @Test
    public void testGetNextFinishSuccessfully() throws Exception {
        final int NUM_PACKAGES = 2;
        AtomicLong nexPacketIdx = new AtomicLong(0L);
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                return submit(() -> {
                    long packetIdx = nexPacketIdx.getAndIncrement();
                    if (packetIdx + 1 < NUM_PACKAGES) {
                        request.setSerializedResult(genResultBatch(2));
                        return genDataResult(false, packetIdx);
                    } else {
                        return genDataResult(true, packetIdx);
                    }
                });
            }
        });

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        RowBatch batch;
        for (int i = 0; i < NUM_PACKAGES; i++) {
            batch = scheduler.getNext();
            if (i + 1 < NUM_PACKAGES) {
                Assert.assertNotNull(batch.getBatch());
                Assert.assertFalse(batch.isEos());
            } else {
                Assert.assertNull(batch.getBatch());
                Assert.assertTrue(batch.isEos());
            }
        }
    }

    @Test
    public void testGetNextReceiveErrorPacketSeq() throws Exception {
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                return submit(() -> {
                    request.setSerializedResult(genResultBatch(2));
                    // packetSeq is incorrectly always 0 for all the packet.
                    return genDataResult(false, 0);
                });
            }
        });

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        RowBatch batch = scheduler.getNext();
        Assert.assertNotNull(batch.getBatch());
        Assert.assertFalse(batch.isEos());

        Assert.assertThrows("rpc failed: receive error packet", RpcException.class, scheduler::getNext);
    }

    @Test
    public void testGetNextThrowException() throws Exception {
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                throw new RuntimeException("test runtime exception");
            }
        });

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        Assert.assertThrows("rpc failed: test runtime exception", RpcException.class, scheduler::getNext);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                !SimpleScheduler.isInBlacklist(BACKEND1_ID) && !SimpleScheduler.isInBlacklist(backend2.getId()) &&
                        !SimpleScheduler.isInBlacklist(backend3.getId()));
    }

    @Test
    public void testGetNextFutureThrowErrorStatus() throws Exception {
        Reference<TStatusCode> fetchDataResultStatusCode = new Reference<>();
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                return submit(() -> {
                    request.setSerializedResult(genResultBatch(2));
                    PFetchDataResult dataResult = genDataResult(false, 0);
                    dataResult.status.statusCode = fetchDataResultStatusCode.getRef().getValue();
                    return dataResult;
                });
            }
        });

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler;

        fetchDataResultStatusCode.setRef(TStatusCode.INTERNAL_ERROR);
        scheduler = startScheduling(sql);
        Assert.assertThrows("Internal_error", UserException.class, scheduler::getNext);

        fetchDataResultStatusCode.setRef(TStatusCode.THRIFT_RPC_ERROR);
        scheduler = startScheduling(sql);
        Assert.assertThrows("rpc failed: Thrift_rpc_error", RpcException.class, scheduler::getNext);
    }

    @Test
    public void testGetNextReachLimit() throws Exception {
        final int NUM_PACKAGES = 2;
        AtomicLong nexPacketIdx = new AtomicLong(0L);
        Set<TUniqueId> cancelledInstanceIds = new ConcurrentSkipListSet<>();
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                return submit(() -> {
                    long packetIdx = nexPacketIdx.getAndIncrement();
                    if (packetIdx + 1 < NUM_PACKAGES) {
                        request.setSerializedResult(genResultBatch(2));
                        return genDataResult(false, packetIdx);
                    } else {
                        return genDataResult(true, packetIdx);
                    }
                });
            }

            @Override
            public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
                TUniqueId id = new TUniqueId(request.finstId.hi, request.finstId.lo);
                cancelledInstanceIds.add(id);
                return super.cancelPlanFragmentAsync(request);
            }
        });

        String sql = "select count(1) from lineitem limit 2";
        DefaultCoordinator scheduler = startScheduling(sql);

        RowBatch batch;

        for (int i = 0; i < NUM_PACKAGES; i++) {
            batch = scheduler.getNext();
            if (i + 1 < NUM_PACKAGES) {
                Assert.assertNotNull(batch.getBatch());
                Assert.assertFalse(batch.isEos());
            } else {
                Assert.assertNull(batch.getBatch());
                Assert.assertTrue(batch.isEos());
            }
        }

        assertThat(cancelledInstanceIds).containsOnlyOnceElementsOf(scheduler.getInstanceIds());
        Assert.assertTrue(scheduler.getExecStatus().ok());

        // Receive cancelled report.
        scheduler.getBackendExecutions().forEach(execution -> {
            TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
            request.setBackend_num(execution.getIndexInJob());
            request.setDone(true);
            request.setStatus(new TStatus(TStatusCode.CANCELLED));
            request.setFragment_instance_id(execution.getInstanceId());

            scheduler.updateFragmentExecStatus(request);
        });

        Assert.assertTrue(scheduler.isDone());
        Assert.assertTrue(scheduler.getExecStatus().ok());
    }

    @Test
    public void testCancelAndGetNext() throws Exception {
        connectContext.getSessionVariable().setEnableProfile(true);

        setBackendService(new MockPBackendService());

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        scheduler.cancel();

        Assert.assertThrows("Cancelled", UserException.class, scheduler::getNext);

        Assert.assertFalse(scheduler.isDone());
        Assert.assertTrue(scheduler.getExecStatus().isCancelled());

        // Receive execution reports.
        scheduler.getBackendExecutions().forEach(execution -> {
            TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
            request.setBackend_num(execution.getIndexInJob());
            request.setDone(true);
            request.setStatus(new TStatus(TStatusCode.CANCELLED));
            request.setFragment_instance_id(execution.getInstanceId());

            scheduler.updateFragmentExecStatus(request);
        });
        Assert.assertTrue(scheduler.isDone());
        Assert.assertTrue(scheduler.getExecStatus().isCancelled());
    }

    private static PFetchDataResult genDataResult(boolean eos, long packetSeq) {
        PFetchDataResult result = new PFetchDataResult();
        StatusPB pStatus = new StatusPB();
        pStatus.statusCode = TStatusCode.OK.getValue();

        PQueryStatistics pQueryStatistics = new PQueryStatistics();
        pQueryStatistics.scanRows = 0L;
        pQueryStatistics.scanBytes = 0L;
        pQueryStatistics.cpuCostNs = 0L;
        pQueryStatistics.memCostBytes = 0L;

        result.status = pStatus;
        result.packetSeq = packetSeq;
        result.queryStatistics = pQueryStatistics;
        result.eos = eos;

        return result;
    }

    private static byte[] genResultBatch(int numRows) throws TException {
        List<ByteBuffer> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            rows.add(ByteBuffer.allocate(8));
        }
        TResultBatch resultBatch = new TResultBatch(rows, false, 0);

        TSerializer serializer = new TSerializer();
        return serializer.serialize(resultBatch);
    }

}
