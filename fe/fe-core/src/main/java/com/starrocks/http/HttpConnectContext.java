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
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TResultSinkFormatType;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// one connection will create one HttpConnectContext
public class HttpConnectContext extends ConnectContext {

    private static final Logger LOG = LogManager.getLogger(HttpConnectContext.class);

    // set if some data is already sent by HttpResultSender
    private boolean sendDate;

    private boolean forwardToLeader;

    // we parse the sql at the begining for validating, so keep it in context for handle_query
    private StatementBase statement;

    // for http sql, we need register connectContext to connectScheduler
    // when connection is established
    private boolean initialized;

    // print connectionId or not, Mainly for ease of testing
    private boolean disablePrintConnectionId;

    private volatile ChannelHandlerContext nettyChannel;

    // ip + port
    private String remoteAddres;

    private boolean isKeepAlive;

    // right now only support json type
    private TResultSinkFormatType resultSinkFormatType;

    public HttpConnectContext() {
        super();
        sendDate = false;
        initialized = false;
        disablePrintConnectionId = false;
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

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
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
        remoteAddres = nettyChannel.channel().remoteAddress().toString().substring(1);
    }

    public StatementBase getStatement() {
        return statement;
    }

    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }

    public boolean get_disable_print_connection_id() {
        return disablePrintConnectionId;
    }

    public void set_disable_print_connection_id(boolean disablePrintConnectionId) {
        this.disablePrintConnectionId = disablePrintConnectionId;
    }

    public String getRemoteAddres() {
        return remoteAddres;
    }

    public boolean isKeepAlive() {
        return isKeepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        isKeepAlive = keepAlive;
    }

    @Override
    public void kill(boolean killConnection) {
        LOG.warn("kill query, {}, kill connection: {}", remoteAddres, killConnection);
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (killConnection) {
            isKilled = true;
        }
        QueryQueueManager.getInstance().cancelQuery(this);
        if (executorRef != null) {
            executorRef.cancel();
        }

        if (killConnection) {
            nettyChannel.close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        LOG.info("close the connection because someone kill the query");
                    } else {
                        // close failed, something went wrong?
                        Throwable cause = channelFuture.cause();
                        LOG.error("close failed，exception:  " + cause.toString());
                    }
                }

            });
        }
    }
}
