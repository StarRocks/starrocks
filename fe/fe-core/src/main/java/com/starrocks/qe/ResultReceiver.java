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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ResultReceiver.java

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

package com.starrocks.qe;

import com.starrocks.common.ErrorCode;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.rpc.PFetchDataRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResultReceiver {
    private static final Logger LOG = LogManager.getLogger(ResultReceiver.class);
    private volatile boolean isDone = false;
    private volatile boolean isCancel = false;
    private long packetIdx = 0;
    private final int timeoutMs;
    private final long deadlineMs;
    private final TNetworkAddress address;
    private final PUniqueId finstId;
    private final Long backendId;

    public ResultReceiver(TUniqueId tid, Long backendId, TNetworkAddress address, int timeoutMs) {
        this.finstId = new PUniqueId();
        this.finstId.hi = tid.hi;
        this.finstId.lo = tid.lo;
        this.backendId = backendId;
        this.address = address;
        this.timeoutMs = timeoutMs;
        this.deadlineMs = System.currentTimeMillis() + timeoutMs;
    }

    public RowBatch getNext(Status status) throws TException {
        if (isDone) {
            return null;
        }
        final RowBatch rowBatch = new RowBatch();
        try {
            while (!isDone && !isCancel) {
                PFetchDataRequest request = new PFetchDataRequest(finstId);

                Future<PFetchDataResult> future = BackendServiceClient.getInstance().fetchDataAsync(address, request);
                PFetchDataResult pResult = null;
                while (pResult == null) {
                    long currentTs = System.currentTimeMillis();
                    if (currentTs >= deadlineMs) {
                        throw new TimeoutException("query timeout");
                    }
                    try {
                        pResult = future.get(deadlineMs - currentTs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // continue to get result
                        LOG.info("future get interrupted Exception");
                        if (isCancel) {
                            status.setStatus(Status.CANCELLED);
                            return null;
                        }
                    }
                }
                TStatusCode code = TStatusCode.findByValue(pResult.status.statusCode);
                if (code != TStatusCode.OK) {
                    status.setPstatus(pResult.status);
                    return null;
                }

                rowBatch.setQueryStatistics(pResult.queryStatistics);

                if (packetIdx != pResult.packetSeq) {
                    LOG.warn("receive packet failed, expect={}, receive={}", packetIdx, pResult.packetSeq);
                    status.setRpcStatus("receive error packet");
                    return null;
                }

                packetIdx++;
                isDone = pResult.eos;

                byte[] serialResult = request.getSerializedResult();
                if (serialResult != null && serialResult.length > 0) {
                    TResultBatch resultBatch = new TResultBatch();
                    TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
                    deserializer.deserialize(resultBatch, serialResult);
                    rowBatch.setBatch(resultBatch);
                    rowBatch.setEos(pResult.eos);
                    return rowBatch;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", DebugUtil.printId(finstId), e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlocklist(backendId);
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", DebugUtil.printId(finstId), e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, ErrorCode.ERR_TIMEOUT.formatErrorMsg("Query", timeoutMs / 1000,
                        String.format("please increase the '%s' session variable and retry", SessionVariable.QUERY_TIMEOUT))));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlocklist(backendId);
            }
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, finstId={}", DebugUtil.printId(finstId), e);
            status.setTimeOutStatus(ErrorCode.ERR_TIMEOUT.formatErrorMsg("Query", timeoutMs / 1000,
                    String.format("please increase the '%s' session variable and retry", SessionVariable.QUERY_TIMEOUT)));
        }

        if (isCancel) {
            status.setStatus(Status.CANCELLED);
        }
        return rowBatch;
    }

    public void cancel() {
        isCancel = true;
    }

    public TNetworkAddress getAddress() {
        return address;
    }

    public Long getBackendId() {
        return backendId;
    }
}
