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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

public class PBackendServiceWithMetricsTest {
    @Tested
    PBackendServiceWithMetrics pBackendServiceWithMetrics;

    @Injectable
    PBackendService pBackendService;

    @BeforeClass
    public static void setUp() {
        MetricRepo.init();
    }

    @Test
    public void testExecPlanFragmentAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.execPlanFragmentAsync((PExecPlanFragmentRequest) any);
                result = CompletableFuture.completedFuture(new PExecPlanFragmentResult());
            }
        };

        Future<PExecPlanFragmentResult> result = pBackendServiceWithMetrics.execPlanFragmentAsync(new PExecPlanFragmentRequest());
        assertNotNull(result);
    }

    @Test
    public void testExecBatchPlanFragmentsAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.execBatchPlanFragmentsAsync((PExecBatchPlanFragmentsRequest) any);
                result = CompletableFuture.completedFuture(new PExecBatchPlanFragmentsResult());
            }
        };

        Future<PExecBatchPlanFragmentsResult> result =
                pBackendServiceWithMetrics.execBatchPlanFragmentsAsync(new PExecBatchPlanFragmentsRequest());
        assertNotNull(result);
    }

    @Test
    public void testCancelPlanFragmentAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.cancelPlanFragmentAsync((PCancelPlanFragmentRequest) any);
                result = CompletableFuture.completedFuture(new PCancelPlanFragmentResult());
            }
        };

        Future<PCancelPlanFragmentResult> result =
                pBackendServiceWithMetrics.cancelPlanFragmentAsync(new PCancelPlanFragmentRequest());
        assertNotNull(result);
    }

    @Test
    public void testFetchDataAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.fetchDataAsync((PFetchDataRequest) any);
                result = CompletableFuture.completedFuture(new PFetchDataResult());
            }
        };

        Future<PFetchDataResult> result = pBackendServiceWithMetrics.fetchDataAsync(new PFetchDataRequest());
        assertNotNull(result);
    }

    @Test
    public void testTriggerProfileReport() throws Exception {
        new Expectations() {
            {
                pBackendService.triggerProfileReport((PTriggerProfileReportRequest) any);
                result = CompletableFuture.completedFuture(new PTriggerProfileReportResult());
            }
        };

        Future<PTriggerProfileReportResult> result =
                pBackendServiceWithMetrics.triggerProfileReport(new PTriggerProfileReportRequest());
        assertNotNull(result);
    }

    @Test
    public void testCollectQueryStatistics() throws Exception {
        new Expectations() {
            {
                pBackendService.collectQueryStatistics((PCollectQueryStatisticsRequest) any);
                result = CompletableFuture.completedFuture(new PCollectQueryStatisticsResult());
            }
        };

        Future<PCollectQueryStatisticsResult> result =
                pBackendServiceWithMetrics.collectQueryStatistics(new PCollectQueryStatisticsRequest());
        assertNotNull(result);
    }

    @Test
    public void testGetInfo() throws Exception {
        new Expectations() {
            {
                pBackendService.getInfo((PProxyRequest) any);
                result = CompletableFuture.completedFuture(new PProxyResult());
            }
        };

        Future<PProxyResult> result = pBackendServiceWithMetrics.getInfo(new PProxyRequest());
        assertNotNull(result);
    }

    @Test
    public void testGetPulsarInfo() throws Exception {
        new Expectations() {
            {
                pBackendService.getPulsarInfo((PPulsarProxyRequest) any);
                result = CompletableFuture.completedFuture(new PPulsarProxyResult());
            }
        };

        Future<PPulsarProxyResult> result = pBackendServiceWithMetrics.getPulsarInfo(new PPulsarProxyRequest());
        assertNotNull(result);
    }

    @Test
    public void testGetFileSchema() throws Exception {
        new Expectations() {
            {
                pBackendService.getFileSchema((PGetFileSchemaRequest) any);
                result = CompletableFuture.completedFuture(new PGetFileSchemaResult());
            }
        };

        Future<PGetFileSchemaResult> result = pBackendServiceWithMetrics.getFileSchema(new PGetFileSchemaRequest());
        assertNotNull(result);
    }

    @Test
    public void testSubmitMVMaintenanceTaskAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.submitMVMaintenanceTaskAsync((PMVMaintenanceTaskRequest) any);
                result = CompletableFuture.completedFuture(new PMVMaintenanceTaskResult());
            }
        };

        Future<PMVMaintenanceTaskResult> result =
                pBackendServiceWithMetrics.submitMVMaintenanceTaskAsync(new PMVMaintenanceTaskRequest());
        assertNotNull(result);
    }

    @Test
    public void testExecuteCommandAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.executeCommandAsync((ExecuteCommandRequestPB) any);
                result = CompletableFuture.completedFuture(new ExecuteCommandResultPB());
            }
        };

        Future<ExecuteCommandResultPB> result = pBackendServiceWithMetrics.executeCommandAsync(new ExecuteCommandRequestPB());
        assertNotNull(result);
    }

    @Test
    public void testUpdateFailPointStatusAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.updateFailPointStatusAsync((PUpdateFailPointStatusRequest) any);
                result = CompletableFuture.completedFuture(new PUpdateFailPointStatusResponse());
            }
        };

        Future<PUpdateFailPointStatusResponse> result =
                pBackendServiceWithMetrics.updateFailPointStatusAsync(new PUpdateFailPointStatusRequest());
        assertNotNull(result);
    }

    @Test
    public void testListFailPointAsync() throws Exception {
        new Expectations() {
            {
                pBackendService.listFailPointAsync((PListFailPointRequest) any);
                result = CompletableFuture.completedFuture(new PListFailPointResponse());
            }
        };

        Future<PListFailPointResponse> result = pBackendServiceWithMetrics.listFailPointAsync(new PListFailPointRequest());
        assertNotNull(result);
    }

    @Test
    public void testExecShortCircuit() throws Exception {
        new Expectations() {
            {
                pBackendService.execShortCircuit((PExecShortCircuitRequest) any);
                result = CompletableFuture.completedFuture(new PExecShortCircuitResult());
            }
        };

        Future<PExecShortCircuitResult> result = pBackendServiceWithMetrics.execShortCircuit(new PExecShortCircuitRequest());
        assertNotNull(result);
    }

    @Test
    public void testProcessDictionaryCache() throws Exception {
        new Expectations() {
            {
                pBackendService.processDictionaryCache((PProcessDictionaryCacheRequest) any);
                result = CompletableFuture.completedFuture(new PProcessDictionaryCacheResult());
            }
        };

        Future<PProcessDictionaryCacheResult> result =
                pBackendServiceWithMetrics.processDictionaryCache(new PProcessDictionaryCacheRequest());
        assertNotNull(result);
    }

    @Test
    public void testFetchArrowSchema() throws Exception {
        new Expectations() {
            {
                pBackendService.fetchArrowSchema((PFetchArrowSchemaRequest) any);
                result = CompletableFuture.completedFuture(new PFetchArrowSchemaResult());
            }
        };

        Future<PFetchArrowSchemaResult> result = pBackendServiceWithMetrics.fetchArrowSchema(new PFetchArrowSchemaRequest());
        assertNotNull(result);
    }

    @Test
    public void testUpdateTransactionState() throws Exception {
        new Expectations() {
            {
                pBackendService.updateTransactionState((PUpdateTransactionStateRequest) any);
                result = CompletableFuture.completedFuture(new PUpdateTransactionStateResponse());
            }
        };

        Future<PUpdateTransactionStateResponse> result =
                pBackendServiceWithMetrics.updateTransactionState(new PUpdateTransactionStateRequest());
        assertNotNull(result);
    }
}