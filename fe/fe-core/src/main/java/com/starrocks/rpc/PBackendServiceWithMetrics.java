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
package com.starrocks.rpc;

import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
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
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;

import java.util.concurrent.Future;

public class PBackendServiceWithMetrics implements PBackendService {
    final PBackendService pBackendService;
    PBackendServiceWithMetrics(PBackendService pBackendService) {
        this.pBackendService = pBackendService;
    }

    private static void increaseMetrics() {
        if (MetricRepo.COUNTER_BACKEND_SERVICE_RPC != null) {
            MetricRepo.COUNTER_BACKEND_SERVICE_RPC.increase(1L);
        }
    }

    @Override
    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
        increaseMetrics();
        return pBackendService.execPlanFragmentAsync(request);
    }

    @Override
    public Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(PExecBatchPlanFragmentsRequest request) {
        increaseMetrics();
        return pBackendService.execBatchPlanFragmentsAsync(request);
    }

    @Override
    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
        increaseMetrics();
        return pBackendService.cancelPlanFragmentAsync(request);
    }

    @Override
    public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
        increaseMetrics();
        return pBackendService.fetchDataAsync(request);
    }

    @Override
    public Future<PTriggerProfileReportResult> triggerProfileReport(PTriggerProfileReportRequest request) {
        increaseMetrics();
        return pBackendService.triggerProfileReport(request);
    }

    @Override
    public Future<PCollectQueryStatisticsResult> collectQueryStatistics(PCollectQueryStatisticsRequest request) {
        increaseMetrics();
        return pBackendService.collectQueryStatistics(request);
    }

    @Override
    public Future<PProxyResult> getInfo(PProxyRequest request) {
        increaseMetrics();
        return pBackendService.getInfo(request);
    }

    @Override
    public Future<PPulsarProxyResult> getPulsarInfo(PPulsarProxyRequest request) {
        increaseMetrics();
        return pBackendService.getPulsarInfo(request);
    }

    @Override
    public Future<PGetFileSchemaResult> getFileSchema(PGetFileSchemaRequest request) {
        increaseMetrics();
        return pBackendService.getFileSchema(request);
    }

    @Override
    public Future<PMVMaintenanceTaskResult> submitMVMaintenanceTaskAsync(PMVMaintenanceTaskRequest request) {
        increaseMetrics();
        return pBackendService.submitMVMaintenanceTaskAsync(request);
    }

    @Override
    public Future<ExecuteCommandResultPB> executeCommandAsync(ExecuteCommandRequestPB request) {
        increaseMetrics();
        return pBackendService.executeCommandAsync(request);
    }

    @Override
    public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(PUpdateFailPointStatusRequest request) {
        increaseMetrics();
        return pBackendService.updateFailPointStatusAsync(request);
    }

    @Override
    public Future<PListFailPointResponse> listFailPointAsync(PListFailPointRequest request) {
        increaseMetrics();
        return pBackendService.listFailPointAsync(request);
    }

    @Override
    public Future<PExecShortCircuitResult> execShortCircuit(PExecShortCircuitRequest request) {
        increaseMetrics();
        return pBackendService.execShortCircuit(request);
    }

    @Override
    public Future<PProcessDictionaryCacheResult> processDictionaryCache(PProcessDictionaryCacheRequest request) {
        increaseMetrics();
        return pBackendService.processDictionaryCache(request);
    }

    @Override
    public Future<PFetchArrowSchemaResult> fetchArrowSchema(PFetchArrowSchemaRequest request) {
        increaseMetrics();
        return pBackendService.fetchArrowSchema(request);
    }

    @Override
    public Future<PUpdateTransactionStateResponse> updateTransactionState(PUpdateTransactionStateRequest request) {
        increaseMetrics();
        return pBackendService.updateTransactionState(request);
    }
}
