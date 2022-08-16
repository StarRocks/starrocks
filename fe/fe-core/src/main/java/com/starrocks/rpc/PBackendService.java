// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.brpc.protocol.BrpcMeta;
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

public interface PBackendService {
    @BrpcMeta(serviceName = "PBackendService", methodName = "exec_plan_fragment")
    PExecPlanFragmentResult execPlanFragment(PExecPlanFragmentRequest request);

    @BrpcMeta(serviceName = "PBackendService", methodName = "exec_batch_plan_fragments")
    PExecBatchPlanFragmentsResult execBatchPlanFragments(PExecBatchPlanFragmentsRequest request);

    @BrpcMeta(serviceName = "PBackendService", methodName = "cancel_plan_fragment")
    PCancelPlanFragmentResult cancelPlanFragment(PCancelPlanFragmentRequest request);

    @BrpcMeta(serviceName = "PBackendService", methodName = "fetch_data")
    PFetchDataResult fetchData(PFetchDataRequest request);

    @BrpcMeta(serviceName = "PBackendService", methodName = "trigger_profile_report")
    PTriggerProfileReportResult triggerProfileReport(PTriggerProfileReportRequest request);

    @BrpcMeta(serviceName = "PBackendService", methodName = "get_info")
    PProxyResult getInfo(PProxyRequest request);
}

