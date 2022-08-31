// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rpc/PBackendService.java

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

package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
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
}

