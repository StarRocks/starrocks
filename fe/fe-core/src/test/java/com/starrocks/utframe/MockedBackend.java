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

package com.starrocks.utframe;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.proto.AbortCompactionRequest;
import com.starrocks.proto.AbortCompactionResponse;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.proto.AbortTxnResponse;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.proto.DeleteDataRequest;
import com.starrocks.proto.DeleteDataResponse;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.proto.DeleteTxnLogRequest;
import com.starrocks.proto.DeleteTxnLogResponse;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.proto.LockTabletMetadataRequest;
import com.starrocks.proto.LockTabletMetadataResponse;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PCollectQueryStatisticsResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PExecShortCircuitResult;
import com.starrocks.proto.PFetchArrowSchemaRequest;
import com.starrocks.proto.PFetchArrowSchemaResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PListFailPointResponse;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.RestoreSnapshotsRequest;
import com.starrocks.proto.RestoreSnapshotsResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.UnlockTabletMetadataRequest;
import com.starrocks.proto.UnlockTabletMetadataResponse;
import com.starrocks.proto.UploadSnapshotsRequest;
import com.starrocks.proto.UploadSnapshotsResponse;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.PBackendService;
import com.starrocks.rpc.PCollectQueryStatisticsRequest;
import com.starrocks.rpc.PExecBatchPlanFragmentsRequest;
import com.starrocks.rpc.PExecPlanFragmentRequest;
import com.starrocks.rpc.PExecShortCircuitRequest;
import com.starrocks.rpc.PFetchDataRequest;
import com.starrocks.rpc.PGetFileSchemaRequest;
import com.starrocks.rpc.PListFailPointRequest;
import com.starrocks.rpc.PMVMaintenanceTaskRequest;
import com.starrocks.rpc.PTriggerProfileReportRequest;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TAgentPublishRequest;
import com.starrocks.thrift.TAgentResult;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TBackendInfo;
import com.starrocks.thrift.TCancelPlanFragmentParams;
import com.starrocks.thrift.TCancelPlanFragmentResult;
import com.starrocks.thrift.TDeleteEtlFilesRequest;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExecPlanFragmentResult;
import com.starrocks.thrift.TExportState;
import com.starrocks.thrift.TExportStatusResult;
import com.starrocks.thrift.TExportTaskRequest;
import com.starrocks.thrift.TFetchDataParams;
import com.starrocks.thrift.TFetchDataResult;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.THeartbeatResult;
import com.starrocks.thrift.TMasterInfo;
import com.starrocks.thrift.TMiniLoadEtlStatusRequest;
import com.starrocks.thrift.TMiniLoadEtlStatusResult;
import com.starrocks.thrift.TMiniLoadEtlTaskRequest;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanCloseResult;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTransmitDataParams;
import com.starrocks.thrift.TTransmitDataResult;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.thrift.TTaskType.CREATE;

/*
 * Mocked Backend
 * A mocked Backend has 3 rpc services.
 *      HeartbeatService.Iface to handle heart beat from Frontend.
 *      BeThriftService to handle agent tasks and other requests from Frontend.
 *      BRpcService to handle the query request from Frontend.
 *
 * Users can create a BE by customizing three rpc services.
 *
 * Better to create a mocked Backend from MockedBackendFactory.
 * In MockedBackendFactory, there default rpc service for above 3 rpc services.
 */
public class MockedBackend {
    private static final AtomicInteger BASE_PORT = new AtomicInteger(8000);
    private static final long PATH_HASH = 123456;

    private final String host;
    private final int brpcPort;
    private final int heartBeatPort;
    private final int beThriftPort;
    private final int httpPort;
    private final int starletPort;

    final MockHeatBeatClient heatBeatClient;

    final MockBeThriftClient thriftClient;

    private final MockPBackendService pbService;

    private final MockLakeService lakeService;

    public MockedBackend(String host) {
        this(host, BASE_PORT.getAndIncrement());
    }

    public MockedBackend(String host, int beThriftPort) {
        this.host = host;
        this.beThriftPort = beThriftPort;

        brpcPort = BASE_PORT.getAndIncrement();
        heartBeatPort = BASE_PORT.getAndIncrement();
        httpPort = BASE_PORT.getAndIncrement();
        starletPort = BASE_PORT.getAndIncrement();

        heatBeatClient = new MockHeatBeatClient(beThriftPort, httpPort, brpcPort, starletPort);
        thriftClient = new MockBeThriftClient(this);
        pbService = new MockPBackendService();

        lakeService = new MockLakeService();

        ((MockGenericPool<?>) ThriftConnectionPool.beHeartbeatPool).register(this);
        ((MockGenericPool<?>) ThriftConnectionPool.backendPool).register(this);

        new MockUp<BrpcProxy>() {
            @Mock
            private synchronized PBackendService getBackendService(TNetworkAddress address) {
                return pbService;
            }

            @Mock
            private synchronized LakeService getLakeServiceImpl(TNetworkAddress address) {
                return lakeService;
            }
        };

    }

    public void setBackendService(PBackendService backendService) {
        new MockUp<BrpcProxy>() {
            @Mock
            private synchronized PBackendService getBackendService(TNetworkAddress address) {
                return backendService;
            }
        };
    }

    public String getHost() {
        return host;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getHeartBeatPort() {
        return heartBeatPort;
    }

    public int getBeThriftPort() {
        return beThriftPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getStarletPort() {
        return starletPort;
    }

    private static class MockHeatBeatClient extends HeartbeatService.Client {
        private final int brpcPort;
        private final int beThriftPort;
        private final int httpPort;
        private final int starletPort;

        public MockHeatBeatClient(int beThriftPort, int beHttpPort, int beBrpcPort, int starletPort) {
            super(null);
            this.brpcPort = beBrpcPort;
            this.beThriftPort = beThriftPort;
            this.httpPort = beHttpPort;
            this.starletPort = starletPort;
        }

        @Override
        public THeartbeatResult heartbeat(TMasterInfo masterInfo) {
            TBackendInfo backendInfo = new TBackendInfo(beThriftPort, httpPort);
            backendInfo.setBrpc_port(brpcPort);
            return new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
        }

        @Override
        public void send_heartbeat(TMasterInfo masterInfo) {
        }

        @Override
        public THeartbeatResult recv_heartbeat() {
            TBackendInfo backendInfo = new TBackendInfo(beThriftPort, httpPort);
            backendInfo.setBrpc_port(brpcPort);
            backendInfo.setStarlet_port(starletPort);
            return new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
        }
    }

    private static class MockBeThriftClient extends BackendService.Client {
        // task queue to save all agent tasks coming from Frontend
        private final BlockingQueue<TAgentTaskRequest> taskQueue = Queues.newLinkedBlockingQueue();
        private final TBackend tBackend;
        private long reportVersion = 0;
        private final LeaderImpl master = new LeaderImpl();

        public MockBeThriftClient(MockedBackend backend) {
            super(null);

            tBackend = new TBackend(backend.getHost(), backend.getBeThriftPort(), backend.getHttpPort());
            new Thread(() -> {
                while (true) {
                    try {
                        TAgentTaskRequest request = taskQueue.take();
                        TFinishTaskRequest finishTaskRequest = new TFinishTaskRequest(tBackend,
                                request.getTask_type(), request.getSignature(), new TStatus(TStatusCode.OK));
                        finishTaskRequest.setReport_version(++reportVersion);
                        if (request.getTask_type() == CREATE) {
                            TTabletInfo tabletInfo = new TTabletInfo();
                            tabletInfo.setPath_hash(PATH_HASH);
                            tabletInfo.setData_size(0);
                            tabletInfo.setTablet_id(request.getSignature());
                            finishTaskRequest.setFinish_tablet_infos(Lists.newArrayList(tabletInfo));
                        }
                        master.finishTask(finishTaskRequest);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        @Override
        public TExecPlanFragmentResult exec_plan_fragment(TExecPlanFragmentParams params) {
            return null;
        }

        @Override
        public TCancelPlanFragmentResult cancel_plan_fragment(TCancelPlanFragmentParams params) {
            return null;
        }

        @Override
        public TTransmitDataResult transmit_data(TTransmitDataParams params) {
            return null;
        }

        @Override
        public TFetchDataResult fetch_data(TFetchDataParams params) {
            return null;
        }

        @Override
        public TAgentResult submit_tasks(List<TAgentTaskRequest> tasks) {
            taskQueue.addAll(tasks);
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult make_snapshot(TSnapshotRequest snapshotRequest) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult release_snapshot(String snapshotPath) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult publish_cluster_state(TAgentPublishRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult submit_etl_task(TMiniLoadEtlTaskRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TMiniLoadEtlStatusResult get_etl_status(TMiniLoadEtlStatusRequest request) {
            return new TMiniLoadEtlStatusResult(new TStatus(TStatusCode.OK), TEtlState.FINISHED);
        }

        @Override
        public TAgentResult delete_etl_files(TDeleteEtlFilesRequest request) {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TStatus submit_export_task(TExportTaskRequest request) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TExportStatusResult get_export_status(TUniqueId taskId) {
            return new TExportStatusResult(new TStatus(TStatusCode.OK), TExportState.FINISHED);
        }

        @Override
        public TStatus erase_export_task(TUniqueId taskId) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TTabletStatResult get_tablet_stat() {
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //            return new TTabletStatResult(Maps.newHashMap());
        }

        @Override
        public TStatus submit_routine_load_task(List<TRoutineLoadTask> tasks) {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TScanOpenResult open_scanner(TScanOpenParams params) {
            return null;
        }

        @Override
        public TScanBatchResult get_next(TScanNextBatchParams params) {
            return null;
        }

        @Override
        public TScanCloseResult close_scanner(TScanCloseParams params) {
            return null;
        }
    }

    public static class MockPBackendService implements PBackendService {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        public <T> Future<T> submit(Callable<T> task) {
            return executor.submit(task);
        }

        @Override
        public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
            return submit(() -> {
                PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;
                result.status = pStatus;
                return result;
            });
        }

        @Override
        public Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(
                PExecBatchPlanFragmentsRequest request) {
            return submit(() -> {
                PExecBatchPlanFragmentsResult result = new PExecBatchPlanFragmentsResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;
                result.status = pStatus;
                return result;
            });
        }

        @Override
        public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
            return submit(() -> {
                PCancelPlanFragmentResult result = new PCancelPlanFragmentResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;
                result.status = pStatus;
                return result;
            });
        }

        @Override
        public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
            return submit(() -> {
                PFetchDataResult result = new PFetchDataResult();
                StatusPB pStatus = new StatusPB();
                pStatus.statusCode = 0;

                PQueryStatistics pQueryStatistics = new PQueryStatistics();
                pQueryStatistics.scanRows = 0L;
                pQueryStatistics.scanBytes = 0L;
                pQueryStatistics.cpuCostNs = 0L;
                pQueryStatistics.memCostBytes = 0L;

                result.status = pStatus;
                result.packetSeq = 0L;
                result.queryStatistics = pQueryStatistics;
                result.eos = true;
                return result;
            });
        }

        @Override
        public Future<PTriggerProfileReportResult> triggerProfileReport(PTriggerProfileReportRequest request) {
            return null;
        }

        @Override
        public Future<PCollectQueryStatisticsResult> collectQueryStatistics(PCollectQueryStatisticsRequest request) {
            return null;
        }

        @Override
        public Future<PProxyResult> getInfo(PProxyRequest request) {
            return null;
        }

        @Override
        public Future<PPulsarProxyResult> getPulsarInfo(PPulsarProxyRequest request) {
            return null;
        }

        @Override
        public Future<PGetFileSchemaResult> getFileSchema(PGetFileSchemaRequest request) {
            throw new NotImplementedException("TODO");
        }

        @Override
        public Future<PMVMaintenanceTaskResult> submitMVMaintenanceTaskAsync(PMVMaintenanceTaskRequest request) {
            throw new NotImplementedException("TODO");
        }

        @Override
        public Future<ExecuteCommandResultPB> executeCommandAsync(ExecuteCommandRequestPB request) {
            throw new NotImplementedException("TODO");
        }

        @Override
        public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(PUpdateFailPointStatusRequest request) {
            return null;
        }

        @Override
        public Future<PListFailPointResponse> listFailPointAsync(PListFailPointRequest request) {
            return null;
        }

        @Override
        public Future<PExecShortCircuitResult> execShortCircuit(PExecShortCircuitRequest request) {
            return null;
        }

        @Override
        public Future<PFetchArrowSchemaResult> fetchArrowSchema(PFetchArrowSchemaRequest request) {
            return null;
        }

        public Future<PProcessDictionaryCacheResult> processDictionaryCache(PProcessDictionaryCacheRequest request) {
            return null;
        }

        @Override
        public Future<PUpdateTransactionStateResponse> updateTransactionState(PUpdateTransactionStateRequest request) {
            throw new NotImplementedException("TODO");
        }
    }

    private static class MockLakeService implements LakeService {
        @Override
        public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<CompactResponse> compact(CompactRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<DeleteTxnLogResponse> deleteTxnLog(DeleteTxnLogRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<DeleteDataResponse> deleteData(DeleteDataRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<TabletStatResponse> getTabletStats(TabletStatRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<DropTableResponse> dropTable(DropTableRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<PublishLogVersionResponse> publishLogVersion(PublishLogVersionRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<PublishLogVersionResponse> publishLogVersionBatch(PublishLogVersionBatchRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<LockTabletMetadataResponse> lockTabletMetadata(LockTabletMetadataRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<UnlockTabletMetadataResponse> unlockTabletMetadata(UnlockTabletMetadataRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<UploadSnapshotsResponse> uploadSnapshots(UploadSnapshotsRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<RestoreSnapshotsResponse> restoreSnapshots(RestoreSnapshotsRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<AbortCompactionResponse> abortCompaction(AbortCompactionRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<VacuumResponse> vacuum(VacuumRequest request) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
