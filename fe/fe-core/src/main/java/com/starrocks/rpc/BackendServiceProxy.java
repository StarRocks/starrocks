// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rpc/BackendServiceProxy.java

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

import com.baidu.bjf.remoting.protobuf.utils.JDKCompilerHelper;
import com.baidu.bjf.remoting.protobuf.utils.compiler.JdkCompiler;
import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.JdkUtils;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);

    private RpcClient rpcClient;
    // TODO(zc): use TNetworkAddress,
    private Map<TNetworkAddress, PBackendService> backendServiceMap;
    private Map<TNetworkAddress, LakeService> lakeServiceMap;

    static {
        int javaRuntimeVersion = JdkUtils.getJavaVersionAsInteger(System.getProperty("java.version"));
        JDKCompilerHelper
                .setCompiler(new JdkCompiler(JdkCompiler.class.getClassLoader(), String.valueOf(javaRuntimeVersion)));
    }

    private BackendServiceProxy() {
        final RpcClientOptions rpcOptions = new RpcClientOptions();
        // If false, different methods to a service endpoint use different connection pool,
        // which will create too many connections.
        // If true, all the methods to a service endpoint use the same connection pool.
        rpcOptions.setShareThreadPoolUnderEachProxy(true);
        rpcOptions.setShareChannelPool(true);
        rpcOptions.setMaxTotoal(Config.brpc_connection_pool_size);
        // After the RPC request sending, the connection will be closed,
        // if the number of total connections is greater than MaxIdleSize.
        // Therefore, MaxIdleSize shouldn't less than MaxTotal for the async requests.
        rpcOptions.setMaxIdleSize(Config.brpc_connection_pool_size);
        rpcOptions.setMaxWait(Config.brpc_idle_wait_max_time);
        rpcClient = new RpcClient(rpcOptions);
        backendServiceMap = Maps.newHashMap();
        lakeServiceMap = Maps.newHashMap();
    }

    public static BackendServiceProxy getInstance() {
        return SingletonHolder.INSTANCE;
    }

    protected synchronized PBackendService getBackendService(TNetworkAddress address) {
        PBackendService service = backendServiceMap.get(address);
        if (service != null) {
            return service;
        }
        ProtobufRpcProxy<PBackendService> proxy = new ProtobufRpcProxy(rpcClient, PBackendService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        service = proxy.proxy();
        backendServiceMap.put(address, service);
        return service;
    }

    public synchronized LakeService getLakeService(TNetworkAddress address) {
        LakeService service = lakeServiceMap.get(address);
        if (service != null) {
            return service;
        }
        ProtobufRpcProxy<LakeService> proxy = new ProtobufRpcProxy(rpcClient, LakeService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        service = proxy.proxy();
        lakeServiceMap.put(address, service);
        return service;
    }

    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
            TNetworkAddress address, TExecPlanFragmentParams tRequest)
            throws TException, RpcException {
        final PExecPlanFragmentRequest pRequest = new PExecPlanFragmentRequest();
        pRequest.setRequest(tRequest);
        try {
            final PBackendService service = getBackendService(address);
            return service.execPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            try {
                // retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = getBackendService(address);
                return service.execPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Execute plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId queryId, TUniqueId finstId, PPlanFragmentCancelReason cancelReason,
            boolean isPipeline) throws RpcException {
        final PCancelPlanFragmentRequest pRequest = new PCancelPlanFragmentRequest();
        PUniqueId uid = new PUniqueId();
        uid.hi = finstId.hi;
        uid.lo = finstId.lo;
        pRequest.finstId = uid;
        pRequest.cancelReason = cancelReason;
        pRequest.isPipeline = isPipeline;
        PUniqueId qid = new PUniqueId();
        qid.hi = queryId.hi;
        qid.lo = queryId.lo;
        pRequest.queryId = qid;
        try {
            final PBackendService service = getBackendService(address);
            return service.cancelPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            // retry
            try {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = getBackendService(address);
                return service.cancelPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Cancel plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PFetchDataResult> fetchDataAsync(
            TNetworkAddress address, PFetchDataRequest request) throws RpcException {
        try {
            PBackendService service = getBackendService(address);
            return service.fetchDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PTriggerProfileReportResult> triggerProfileReportAsync(
            TNetworkAddress address, PTriggerProfileReportRequest request) throws RpcException {
        try {
            final PBackendService service = getBackendService(address);
            return service.triggerProfileReport(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PProxyResult> getInfo(
            TNetworkAddress address, PProxyRequest request) throws RpcException {
        try {
            final PBackendService service = getBackendService(address);
            return service.getInfo(request);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    private static class SingletonHolder {
        private static final BackendServiceProxy INSTANCE = new BackendServiceProxy();
    }
}
