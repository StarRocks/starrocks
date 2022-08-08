// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.starrocks.thrift.TNetworkAddress;

import java.util.concurrent.ConcurrentHashMap;

public class BrpcProxy {
    private final ConcurrentHashMap<TNetworkAddress, PBackendServiceAsync> backendServiceMap;
    private final ConcurrentHashMap<TNetworkAddress, LakeServiceAsync> lakeServiceMap;

    protected BrpcProxy() {
        backendServiceMap = new ConcurrentHashMap<>();
        lakeServiceMap = new ConcurrentHashMap<>();
    }

    private static BrpcProxy getInstance() {
        return BrpcProxy.SingletonHolder.INSTANCE;
    }

    /**
     * Only used for pseudo cluster or unittest
     */
    public static void setInstance(BrpcProxy proxy) {
        BrpcProxy.SingletonHolder.INSTANCE = proxy;
    }

    public static PBackendServiceAsync getBackendService(TNetworkAddress address) {
        return getInstance().getBackendServiceImpl(address);
    }

    public static LakeServiceAsync getLakeService(TNetworkAddress address) {
        return getInstance().getLakeServiceImpl(address);
    }

    public static LakeServiceAsync getLakeService(String host, int port) {
        return getInstance().getLakeServiceImpl(new TNetworkAddress(host, port));
    }

    protected PBackendServiceAsync getBackendServiceImpl(TNetworkAddress address) {
        return backendServiceMap.computeIfAbsent(address, this::createBackendService);
    }

    protected LakeServiceAsync getLakeServiceImpl(TNetworkAddress address) {
        return lakeServiceMap.computeIfAbsent(address, this::createLakeService);
    }

    private PBackendServiceAsync createBackendService(TNetworkAddress address) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(5000);
        clientOption.setMaxTotalConnections(1000);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        String serviceurl = "list://" + address.getHostname() + ":" + address.getPort();
        RpcClient rpcClient = new RpcClient(serviceurl, clientOption);
        return com.baidu.brpc.client.BrpcProxy.getProxy(rpcClient, PBackendServiceAsync.class);
    }

    private LakeServiceAsync createLakeService(TNetworkAddress address) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(5000);
        clientOption.setMaxTotalConnections(1000);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        String serviceurl = "list://" + address.getHostname() + ":" + address.getPort();
        RpcClient rpcClient = new RpcClient(serviceurl, clientOption);
        return com.baidu.brpc.client.BrpcProxy.getProxy(rpcClient, LakeServiceAsync.class);
    }

    private static class SingletonHolder {
        private static BrpcProxy INSTANCE = new BrpcProxy();
    }
}
