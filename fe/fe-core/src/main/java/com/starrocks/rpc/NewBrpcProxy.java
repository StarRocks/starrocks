// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

// import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
// import com.starrocks.common.Config;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class NewBrpcProxy {
    private static final Logger LOG = LogManager.getLogger(NewBrpcProxy.class);

    private final ConcurrentHashMap<TNetworkAddress, PBackendServiceAsync> backendServiceMap;

    private NewBrpcProxy() {
        backendServiceMap = new ConcurrentHashMap<>();
    }

    public static NewBrpcProxy getInstance() {
        return NewBrpcProxy.SingletonHolder.INSTANCE;
    }

    public PBackendServiceAsync getBackendService(TNetworkAddress address) {
        return backendServiceMap.computeIfAbsent(address, this::createBackendService);
    }

    private PBackendServiceAsync createBackendService(TNetworkAddress address) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(50000);
        clientOption.setMaxTotalConnections(1000);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        String serviceurl = "list://" + address.getHostname() + ":" + Integer.toString(address.getPort());
        LOG.info(serviceurl);
        RpcClient rpcClient = new RpcClient(serviceurl, clientOption);
        PBackendServiceAsync service = com.baidu.brpc.client.BrpcProxy.getProxy(rpcClient, PBackendServiceAsync.class);
        return service;
    }

    private static class SingletonHolder {
        private static final NewBrpcProxy INSTANCE = new NewBrpcProxy();
    }
}

