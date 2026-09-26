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

    // Last HTTP write for this context. SQL keep-alive requests share this single slot, so it is
    // a context-level drain barrier rather than a strict per-request future.
    private volatile ChannelFuture lastHttpWrite;

    // SQL keep-alive only. Pipelined SQL requests share one HttpConnectContext;
    // do not close the channel while this per-channel count is non-zero. Non-SQL actions use a
    // per-request context and must not touch this counter.
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

    // SQL keep-alive only: HTTP/1.1 pipelined requests share one channel-scoped
    // HttpConnectContext. Non-SQL actions must not call this — they get a fresh
    // context per request, so the count is not per-channel.
    public void incrementAdmittedRequests() {
        synchronized (this) {
            channelAdmittedRequests.incrementAndGet();
        }
    }

    // SQL keep-alive: wait for this request's last write, then drop the per-channel
    // admitted count. Close the channel only when rejecting and that count reaches
    // zero (do not kill another pipelined admitted request on the same context).
    public void finishAdmittedHttpRequest() {
        runAfterLastHttpWrite(this::completeAdmittedHttpRequest);
    }

    // Non-SQL: one context per request, so there is no per-channel admitted count.
    // Await the write, drop the global admission, and if rejecting close this
    // request's channel (immediate release of keep-alive).
    public void finishHttpRequestAndMaybeClose() {
        runAfterLastHttpWrite(this::completeHttpRequestAndMaybeClose);
    }

    private void runAfterLastHttpWrite(Runnable next) {
        ChannelFuture f = lastHttpWrite;
        if (f != null) {
            Channel ch = f.channel();
            if (ch != null && ch.eventLoop() != null && ch.eventLoop().inEventLoop()) {
                f.addListener(future -> next.run());
                return;
            }
            f.awaitUninterruptibly();
        }
        next.run();
    }

    private void completeAdmittedHttpRequest() {
        // Always decrement (do NOT gate on isHttpRejecting(): short-circuit would skip the
        // decrement while the window is still open, inflating the count so it never returns to
        // zero once rejecting). Decrement, the close decision, and the close itself all share one
        // monitor with incrementAdmittedRequests: otherwise a decrement-to-zero racing an increment
        // for a just-admitted pipelined SQL request could close the channel under it.
        synchronized (this) {
            int remaining = channelAdmittedRequests.decrementAndGet();
            if (GracefulExitFlag.isHttpRejecting() && remaining == 0) {
                closeNettyChannelIfActive();
            }
        }
        GracefulExitFlag.finishHttpRequest();
    }

    private void completeHttpRequestAndMaybeClose() {
        if (GracefulExitFlag.isHttpRejecting()) {
            closeNettyChannelIfActive();
        }
        GracefulExitFlag.finishHttpRequest();
    }

    private void closeNettyChannelIfActive() {
        ChannelHandlerContext ch = nettyChannel;
        if (ch != null && ch.channel().isActive()) {
            ch.close();
        }
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
