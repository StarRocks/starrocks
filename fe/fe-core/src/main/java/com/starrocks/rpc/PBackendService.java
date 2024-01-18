// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PTriggerProfileReportResult;

import java.util.concurrent.Future;

public interface PBackendService {
    @ProtobufRPC(serviceName = "PBackendService", methodName = "exec_plan_fragment",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request);

    @ProtobufRPC(serviceName = "PBackendService", methodName = "exec_batch_plan_fragments",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(PExecBatchPlanFragmentsRequest request);

    @ProtobufRPC(serviceName = "PBackendService", methodName = "cancel_plan_fragment",
            onceTalkTimeout = 5000)
    Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request);

    // we set timeout to 1 day, because now there is no way to give different timeout for each RPC call
    @ProtobufRPC(serviceName = "PBackendService", methodName = "fetch_data",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 86400000)
    Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request);

    @ProtobufRPC(serviceName = "PBackendService", methodName = "trigger_profile_report",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 10000)
    Future<PTriggerProfileReportResult> triggerProfileReport(PTriggerProfileReportRequest request);

    @ProtobufRPC(serviceName = "PBackendService", methodName = "get_info", onceTalkTimeout = 10000)
    Future<PProxyResult> getInfo(PProxyRequest request);

    @ProtobufRPC(serviceName = "PBackendService", methodName = "get_pulsar_info", onceTalkTimeout = 10000)
    Future<PPulsarProxyResult> getPulsarInfo(PPulsarProxyRequest request);

<<<<<<< HEAD
    @ProtobufRPC(serviceName = "PBackendService", methodName = "execute_command", onceTalkTimeout = 60000)
=======
    @ProtobufRPC(serviceName = "PBackendService", methodName = "execute_command", onceTalkTimeout = 600000)
>>>>>>> 2.5.18
    Future<ExecuteCommandResultPB> executeCommandAsync(ExecuteCommandRequestPB request);
}

