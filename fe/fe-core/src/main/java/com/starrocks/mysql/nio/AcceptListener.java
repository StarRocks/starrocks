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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/nio/AcceptListener.java

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

package com.starrocks.mysql.nio;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LogUtil;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.NegotiateState;
import com.starrocks.mysql.ProxyProtocolParser;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import inet.ipaddr.IPAddressString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * listener for accept mysql connections.
 */
public class AcceptListener implements ChannelListener<AcceptingChannel<StreamConnection>> {
    private static final Logger LOG = LogManager.getLogger(AcceptListener.class);
    private final ConnectScheduler connectScheduler;

    public AcceptListener(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    @Override
    public void handleEvent(AcceptingChannel<StreamConnection> channel) {
        try {
            StreamConnection connection = channel.accept();
            if (connection == null) {
                return;
            }
            // connection has been established, so need to call context.cleanup()
            // if exception happens.
            ConnectContext context = new ConnectContext(connection);
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setConnectionId(connectScheduler.getNextConnectionId());
            context.resetConnectionStartTime();
            int connectionId = context.getConnectionId();
            SocketAddress remoteAddr = connection.getPeerAddress();
            LOG.info("Connection established. remote={}, connectionId={}", remoteAddr, connectionId);

            try {
                channel.getWorker().execute(() -> {
                    MysqlProto.NegotiateResult result = null;
                    try {
                        // Set thread local info
                        context.setThreadLocalInfo();

                        try {
                            applyProxyProtocol(context.getMysqlChannel(), context,
                                    Config.mysql_proxy_protocol_networks);
                        } catch (IOException e) {
                            String msg = "PROXY protocol header required but not received: " + e.getMessage();
                            LOG.warn("Rejected connection from {} (connectionId={}): {}",
                                    context.getMysqlChannel().getRemoteHostPortString(), connectionId, e.getMessage());
                            context.getState().setError(msg);
                            MysqlProto.sendResponsePacket(context);
                            throw new AfterConnectedException(msg);
                        }

                        LOG.info("Connection scheduled to worker thread {}. remote={}, connectionId={}",
                                Thread.currentThread().getId(),
                                context.getMysqlChannel().getRemoteHostPortString(), connectionId);

                        // authenticate check failed.
                        result = MysqlProto.negotiate(context);
                        if (result.state() != NegotiateState.OK) {
                            throw new AfterConnectedException(result.state().getMsg());
                        }
                        Pair<Boolean, String> registerResult = connectScheduler.registerConnection(context);
                        if (registerResult.first) {
                            connection.setCloseListener(streamConnection -> connectScheduler.unregisterConnection(context));
                        } else {
                            context.getState().setError(registerResult.second);
                            MysqlProto.sendResponsePacket(context);
                            throw new AfterConnectedException(registerResult.second);
                        }

                        result = MysqlProto.authenticate(context, result.authPacket());
                        if (result.state() != NegotiateState.OK) {
                            throw new AfterConnectedException(result.state().getMsg());
                        }

                        MysqlProto.sendResponsePacket(context);

                        context.setStartTime();
                        ConnectProcessor processor = new ConnectProcessor(context);
                        context.startAcceptQuery(processor);
                    } catch (AfterConnectedException e) {
                        // do not need to print log for this kind of exception.
                        // just clean up the context;
                        context.cleanup();
                        context.getState().setError(e.getMessage());
                    } catch (Throwable e) {
                        if (e instanceof Error) {
                            LOG.error("connect processor exception because ", e);
                        } else {
                            // should be unexpected exception, so print warn log
                            LOG.warn("connect processor exception because ", e);
                        }
                        context.cleanup();
                        context.getState().setError(e.getMessage());
                    } finally {
                        // Ignore the NegotiateState.READ_FIRST_AUTH_PKG_FAILED connections,
                        // because this maybe caused by port probe.
                        if (result != null && result.state() != NegotiateState.READ_FIRST_AUTH_PKG_FAILED) {
                            LogUtil.logConnectionInfoToAuditLogAndQueryQueue(context, result.authPacket());
                            ConnectContext.remove();
                        }
                    }
                });
            } catch (Throwable e) {
                if (e instanceof Error) {
                    LOG.error("connect processor exception because ", e);
                } else {
                    // should be unexpected exception, so print warn log
                    LOG.warn("connect processor exception because ", e);
                }
                context.cleanup();
                ConnectContext.remove();
            }
        } catch (IOException e) {
            LOG.warn("Connection accept failed.", e);
        }
    }

    // Parses and applies a PROXY protocol header when the peer is trusted, updating the channel
    // and context with the real client address. No-ops when proxyNetworks is empty or the peer
    // is not in the trusted list. Returns without updating if the header family is UNKNOWN.
    static void applyProxyProtocol(MysqlChannel channel, ConnectContext context, String proxyNetworks)
            throws IOException {
        String networks = proxyNetworks.trim();
        if (!networks.isEmpty() && isTrustedProxyPeer(channel.getRemoteIp(), networks)) {
            ProxyProtocolParser.Result ppResult = ProxyProtocolParser.parse(channel,
                    Config.mysql_proxy_protocol_header_timeout_ms);
            if (ppResult != null) {
                channel.setRemoteAddress(ppResult.ip, ppResult.port);
                context.setRemoteIP(ppResult.ip);
            }
        }
    }

    // Returns true when peerIp is allowed to supply a PROXY protocol header.
    // networks == "*" -> all peers trusted.
    static boolean isTrustedProxyPeer(String peerIp, String networks) {
        if ("*".equals(networks)) {
            return true;
        }
        IPAddressString address = new IPAddressString(peerIp);
        for (String cidr : networks.split("\\s*;\\s*")) {
            if (!cidr.isEmpty() && new IPAddressString(cidr).contains(address)) {
                return true;
            }
        }
        return false;
    }

    // this exception is only used for some expected exception after connection established.
    // so that we can catch these kind of exceptions and close the channel without printing warning logs.
    private static class AfterConnectedException extends Exception {
        public AfterConnectedException(String msg) {
            super(msg);
        }
    }
}
