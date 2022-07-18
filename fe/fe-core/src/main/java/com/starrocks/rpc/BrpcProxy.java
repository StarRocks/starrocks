// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.utils.JDKCompilerHelper;
import com.baidu.bjf.remoting.protobuf.utils.compiler.JdkCompiler;
import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.starrocks.common.Config;
import com.starrocks.common.util.JdkUtils;
import com.starrocks.thrift.TNetworkAddress;

import java.util.concurrent.ConcurrentHashMap;

public class BrpcProxy {
    private final RpcClient rpcClient;
    // TODO: Eviction
    private final ConcurrentHashMap<TNetworkAddress, PBackendService> backendServiceMap;
    private final ConcurrentHashMap<TNetworkAddress, LakeService> lakeServiceMap;

    static {
        int javaRuntimeVersion = JdkUtils.getJavaVersionAsInteger(System.getProperty("java.version"));
        JDKCompilerHelper
                .setCompiler(new JdkCompiler(JdkCompiler.class.getClassLoader(), String.valueOf(javaRuntimeVersion)));
    }

    public BrpcProxy() {
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
        backendServiceMap = new ConcurrentHashMap<>();
        lakeServiceMap = new ConcurrentHashMap<>();
    }

    public static BrpcProxy getInstance() {
        return BrpcProxy.SingletonHolder.INSTANCE;
    }

    /**
     * Only used for pseudo cluster or unittest
     */
    public static void setInstance(BrpcProxy proxy) {
        BrpcProxy.SingletonHolder.INSTANCE = proxy;
    }

    public PBackendService getBackendService(TNetworkAddress address) {
        return backendServiceMap.computeIfAbsent(address, this::createBackendService);
    }

    public LakeService getLakeService(TNetworkAddress address) {
        return lakeServiceMap.computeIfAbsent(address, this::createLakeService);
    }

    private PBackendService createBackendService(TNetworkAddress address) {
        ProtobufRpcProxy<PBackendService> proxy = new ProtobufRpcProxy<>(rpcClient, PBackendService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return proxy.proxy();
    }

    private LakeService createLakeService(TNetworkAddress address) {
        ProtobufRpcProxy<LakeService> proxy = new ProtobufRpcProxy<>(rpcClient, LakeService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return proxy.proxy();
    }

    private static class SingletonHolder {
        private static BrpcProxy INSTANCE = new BrpcProxy();
    }
}
