// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcCallback;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecBatchPlanFragmentsRequest;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentRequest;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataRequest;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PTriggerProfileReportRequest;
import com.starrocks.proto.PTriggerProfileReportResult;

import java.util.concurrent.Future;

public interface PBackendServiceAsync extends PBackendService {
    Future<PExecPlanFragmentResult> execPlanFragment(
            PExecPlanFragmentRequest request, RpcCallback<PExecPlanFragmentResult> callback);

    Future<PExecBatchPlanFragmentsResult> execBatchPlanFragments(
            PExecBatchPlanFragmentsRequest request, RpcCallback<PExecBatchPlanFragmentsResult> callback);


    Future<PCancelPlanFragmentResult> cancelPlanFragment(
            PCancelPlanFragmentRequest request, RpcCallback<PCancelPlanFragmentResult> callback);

    Future<PFetchDataResult> fetchData(PFetchDataRequest request, RpcCallback<PFetchDataResult> callback);

    Future<PTriggerProfileReportResult> triggerProfileReport(
            PTriggerProfileReportRequest request, RpcCallback<PTriggerProfileReportResult> callback);

    Future<PProxyResult> getInfo(PProxyRequest request, RpcCallback<PProxyResult> callback);
}


