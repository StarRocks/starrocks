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

package com.starrocks.http;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TResultSinkFormatType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

// one connection will create one HttpConnectContext
public class HttpConnectContext extends ConnectContext {

    private static final Logger LOG = LogManager.getLogger(HttpConnectContext.class);

    // set if some data is already sent by HttpResultSender
    private boolean sendDate;

    private boolean forwardToLeader;

    // we parse the sql at the beginning for validating, so keep it in context for handle_query
    private StatementBase statement;

    // After the TCP connection is established, the first time `ExecuteSqlAction` runs it registers the `ConnectContext` to the
    // `ConnectScheduler`.
    private boolean registered;

    // used for test. only output result raws
    private boolean onlyOutputResultRaw;

    private volatile ChannelHandlerContext nettyChannel;

    // ip + port
    private String remoteAddress;

    private boolean isKeepAlive;

    // Last HTTP write (LastHttpContent or FullHttpResponse). Awaited before
    // finishHttpRequest/close so drain cannot observe active==0 while bytes remain.
    private volatile ChannelFuture lastHttpWrite;

    // Admitted HTTP requests still in flight on this channel. HTTP/1.1 pipelined requests share
    // one HttpConnectContext, so closing the channel while this is non-zero would kill another
    // admitted request still running; only close once it hits zero (and rejecting).
    private final AtomicInteger channelAdmittedRequests = new AtomicInteger();

    // right now only support json type
    private TResultSinkFormatType resultSinkFormatType;

    public HttpConnectContext() {
        super();
        sendDate = false;
        registered = false;
        onlyOutputResultRaw = false;
    }

    public TResultSinkFormatType getResultSinkFormatType() {
        return resultSinkFormatType;
    }

    public void setResultSinkFormatType(TResultSinkFormatType resultSinkFormatType) {
        this.resultSinkFormatType = resultSinkFormatType;
    }

    public boolean isForwardToLeader() {
        return forwardToLeader;
    }

    public void setForwardToLeader(boolean forwardToLeader) {
        this.forwardToLeader = forwardToLeader;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }

    public boolean getSendDate() {
        return sendDate;
    }

    public void setSendDate(boolean sendDate) {
        this.sendDate = sendDate;
    }

    public ChannelHandlerContext getNettyChannel() {
        return nettyChannel;
    }

    public void setNettyChannel(ChannelHandlerContext nettyChannel) {
        this.nettyChannel = nettyChannel;
        remoteAddress = nettyChannel.channel().remoteAddress().toString().substring(1);
    }

    public StatementBase getStatement() {
        return statement;
    }

    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public boolean isKeepAlive() {
        return isKeepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        isKeepAlive = keepAlive;
    }

    public void setLastHttpWrite(ChannelFuture lastHttpWrite) {
        this.lastHttpWrite = lastHttpWrite;
    }

    public void awaitLastHttpWrite() {
        ChannelFuture f = lastHttpWrite;
        if (f != null) {
            f.awaitUninterruptibly();
        }
    }

    // Called once per request after a successful HTTP admission claim on this channel. Matched by
    // channelAdmittedRequests.decrementAndGet() in completeAdmittedHttpRequest. Synchronized on
    // this context so the count and the decrement-then-close decision in
    // completeAdmittedHttpRequest are atomic w.r.t. each other.
    public void incrementAdmittedRequests() {
        synchronized (this) {
            channelAdmittedRequests.incrementAndGet();
        }
    }

    // Wait for this request's final HTTP write, then drop the admission count. Always decrements
    // the per-channel admitted count; closes the channel only when rejecting and that count
    // reaches zero (so a pipelined admitted request sharing this channel is not killed).
    // If called on the Netty event loop, defer via listener to avoid deadlock.
    public void finishAdmittedHttpRequest() {
        ChannelFuture f = lastHttpWrite;
        if (f != null) {
            Channel ch = f.channel();
            if (ch != null && ch.eventLoop() != null && ch.eventLoop().inEventLoop()) {
                f.addListener(future -> completeAdmittedHttpRequest());
                return;
            }
            f.awaitUninterruptibly();
        }
        completeAdmittedHttpRequest();
    }

    private void completeAdmittedHttpRequest() {
        // Always decrement (do NOT gate on isHttpRejecting(): short-circuit would skip the
        // decrement while the window is still open, inflating the count so it never returns to
        // zero once rejecting). Decrement, the close decision, and the close itself all share one
        // monitor with incrementAdmittedRequests: otherwise a decrement-to-zero racing an increment
        // for a just-admitted pipelined request could close the channel under it. HTTP/1.1
        // pipelined requests share one HttpConnectContext; closing on one request's completion
        // while another admitted request is still running on the same channel would kill it.
        synchronized (this) {
            int remaining = channelAdmittedRequests.decrementAndGet();
            if (GracefulExitFlag.isHttpRejecting() && remaining == 0) {
                ChannelHandlerContext ch = nettyChannel;
                if (ch != null && ch.channel().isActive()) {
                    ch.close();
                }
            }
        }
        GracefulExitFlag.finishHttpRequest();
    }

    public boolean isOnlyOutputResultRaw() {
        return onlyOutputResultRaw;
    }

    public void setOnlyOutputResultRaw(boolean onlyOutputResultRaw) {
        this.onlyOutputResultRaw = onlyOutputResultRaw;
    }

    @Override
    public synchronized void cleanup() {
        try {
            super.cleanup();
        } finally {
            if (nettyChannel != null) {
                nettyChannel.close();
            }
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(this);
        }
    }

    @Override
    public void kill(boolean killConnection, String cancelledMessage) {
        LOG.warn("kill query, {}, kill connection: {}", remoteAddress, killConnection);
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (killConnection) {
            isKilled = true;
        }
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }

        if (killConnection) {
            nettyChannel.close().addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    LOG.info("close the connection because someone kill the query");
                } else {
                    // close failed, something went wrong?
                    Throwable cause = channelFuture.cause();
                    LOG.error("close failed，exception:  " + cause.toString());
                }
            });
        }
    }

    @Override
    public String getCommandStr() {
        return "HTTP.Query";
    }
}
