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

import com.starrocks.common.Config;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TFileBrokerService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Constructor;

public class ThriftConnectionPool<SERVER_CLIENT extends org.apache.thrift.TServiceClient> {
    private static final Logger LOG = LogManager.getLogger(ThriftConnectionPool.class);

    static GenericKeyedObjectPoolConfig heartbeatConfig = new GenericKeyedObjectPoolConfig();

    static {
        heartbeatConfig.setLifo(true);            // set Last In First Out strategy
        heartbeatConfig.setMaxIdlePerKey(2);      // (default 2)
        heartbeatConfig.setMinIdlePerKey(1);      // (default 1)
        heartbeatConfig.setMaxTotalPerKey(-1);    // (default -1)
        heartbeatConfig.setMaxTotal(-1);          // (default -1)
        heartbeatConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static GenericKeyedObjectPoolConfig backendConfig = new GenericKeyedObjectPoolConfig();

    static {
        backendConfig.setLifo(true);            // set Last In First Out strategy
        backendConfig.setMaxIdlePerKey(128);    // (default 128)
        backendConfig.setMinIdlePerKey(2);      // (default 2)
        backendConfig.setMaxTotalPerKey(-1);    // (default -1)
        backendConfig.setMaxTotal(-1);          // (default -1)
        backendConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static GenericKeyedObjectPoolConfig brokerPoolConfig = new GenericKeyedObjectPoolConfig();

    static {
        brokerPoolConfig.setLifo(true);            // set Last In First Out strategy
        brokerPoolConfig.setMaxIdlePerKey(128);    // (default 128)
        brokerPoolConfig.setMinIdlePerKey(2);      // (default 2)
        brokerPoolConfig.setMaxTotalPerKey(-1);    // (default -1)
        brokerPoolConfig.setMaxTotal(-1);          // (default -1)
        brokerPoolConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    public static ThriftConnectionPool<HeartbeatService.Client> beHeartbeatPool =
            new ThriftConnectionPool<>("HeartbeatService", heartbeatConfig, Config.heartbeat_timeout_second * 1000);
    public static ThriftConnectionPool<TFileBrokerService.Client> brokerHeartbeatPool =
            new ThriftConnectionPool<>("TFileBrokerService", heartbeatConfig, Config.heartbeat_timeout_second * 1000);
    public static ThriftConnectionPool<FrontendService.Client> frontendPool =
            new ThriftConnectionPool<>("FrontendService", backendConfig, Config.thrift_rpc_timeout_ms);
    public static ThriftConnectionPool<BackendService.Client> backendPool =
            new ThriftConnectionPool<>("BackendService", backendConfig, Config.thrift_rpc_timeout_ms);
    public static ThriftConnectionPool<TFileBrokerService.Client> brokerPool =
            new ThriftConnectionPool<>("TFileBrokerService", brokerPoolConfig, Config.broker_client_timeout_ms);

    private final GenericKeyedObjectPool<TNetworkAddress, SERVER_CLIENT> pool;
    private final String className;
    private final int defaultTimeoutMs;

    public ThriftConnectionPool(String className, GenericKeyedObjectPoolConfig config, int defaultTimeoutMs) {
        this.className = "com.starrocks.thrift." + className + "$Client";
        this.pool = new GenericKeyedObjectPool<>(new ThriftClientFactory(), config);
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    public int getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    public boolean reopen(SERVER_CLIENT object, int timeoutMs) {
        boolean ok = true;
        object.getOutputProtocol().getTransport().close();
        try {
            object.getOutputProtocol().getTransport().open();
            // transport.open() doesn't set timeout, Maybe the timeoutMs change.
            TSocket socket = (TSocket) object.getOutputProtocol().getTransport();
            socket.setTimeout(timeoutMs);
        } catch (TTransportException e) {
            LOG.warn("reopen error", e);
            ok = false;
        }
        return ok;
    }

    public void clearPool(TNetworkAddress addr) {
        pool.clear(addr);
    }

    public boolean peak(SERVER_CLIENT object) {
        return object.getOutputProtocol().getTransport().peek();
    }

    public SERVER_CLIENT borrowObject(TNetworkAddress address) throws Exception {
        return pool.borrowObject(address);
    }

    public SERVER_CLIENT borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
        SERVER_CLIENT serverClient = pool.borrowObject(address);
        TSocket socket = (TSocket) (serverClient.getOutputProtocol().getTransport());
        socket.setTimeout(timeoutMs);
        return serverClient;
    }

    public void returnObject(TNetworkAddress address, SERVER_CLIENT object) {
        if (address == null || object == null) {
            return;
        }
        pool.returnObject(address, object);
    }

    public void invalidateObject(TNetworkAddress address, SERVER_CLIENT object) {
        if (address == null || object == null) {
            return;
        }
        try {
            pool.invalidateObject(address, object);
        } catch (Exception e) {
            LOG.warn("Failed to execute invalidateObject", e);
        }
    }

    private class ThriftClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, SERVER_CLIENT> {

        private Object newInstance(String className, TProtocol protocol) throws Exception {
            Class<?> c = Class.forName(className);
            Constructor<?> cons = c.getConstructor(TProtocol.class);
            return cons.newInstance(protocol);
        }

        @Override
        public SERVER_CLIENT create(TNetworkAddress key) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("before create socket hostname={} key.port={} timeoutMs={}",
                        key.hostname, key.port, defaultTimeoutMs);
            }
            TTransport transport = new TSocket(key.hostname, key.port, defaultTimeoutMs);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            return (SERVER_CLIENT) newInstance(className, protocol);
        }

        @Override
        public PooledObject<SERVER_CLIENT> wrap(SERVER_CLIENT client) {
            return new DefaultPooledObject<>(client);
        }

        @Override
        public boolean validateObject(TNetworkAddress key, PooledObject<SERVER_CLIENT> p) {
            boolean isOpen = p.getObject().getOutputProtocol().getTransport().isOpen();
            LOG.debug("isOpen={}", isOpen);
            return isOpen;
        }

        @Override
        public void destroyObject(TNetworkAddress key, PooledObject<SERVER_CLIENT> p) {
            // InputProtocol and OutputProtocol have the same reference in OurCondition
            if (p.getObject().getOutputProtocol().getTransport().isOpen()) {
                p.getObject().getOutputProtocol().getTransport().close();
            }
        }
    }
}
