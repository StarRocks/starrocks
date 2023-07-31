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

import com.google.gson.JsonObject;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.starrocks.http.BaseResponse.HEADER_QUERY_ID;

public class HttpResultSender {
    private static final Logger LOG = LogManager.getLogger(HttpResultSender.class);

    private final HttpConnectContext context;

    public HttpResultSender(HttpConnectContext context) {
        this.context = context;
    }

    // for select
    public RowBatch sendQueryResult(Coordinator coord, ExecPlan execPlan) throws Exception {
        RowBatch batch;
        ChannelHandlerContext nettyChannel = context.getNettyChannel();
        // if some data already sent to client, when exception occurs,we just close the channel
        context.setSendDate(true);
        sendHeader(nettyChannel);
        // write connectId
        if (!context.get_disable_print_connection_id()) {
            nettyChannel.write(JsonSerializer.getConnectId(context.getConnectionId()));
        }
        // write column meta data
        ByteBuf metaData = JsonSerializer.getMetaData(execPlan.getColNames(), execPlan.getOutputExprs());
        nettyChannel.writeAndFlush(metaData);

        while (true) {
            batch = coord.getNext();
            if (batch.getBatch() != null) {
                writeResultBatch(batch.getBatch(), nettyChannel, coord);
                context.updateReturnRows(batch.getBatch().getRows().size());
            }
            if (batch.isEos()) {
                ByteBuf statisticData = JsonSerializer.getStatistic(batch.getQueryStatistics());
                nettyChannel.writeAndFlush(statisticData);
                sendEmptyLastContent();
                break;
            }
        }
        return batch;
    }

    public void sendExplainResult(String explainString) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("explain", explainString);
        String res = jsonObject.toString();

        sendHeader(context.getNettyChannel());

        sendFinalChunk(context.getNettyChannel(), Unpooled.wrappedBuffer(res.getBytes(StandardCharsets.UTF_8)));
        context.getState().setEof();
    }

    public void sendShowResult(ShowResultSet resultSet) throws IOException {
        sendHeader(context.getNettyChannel());
        sendFinalChunk(context.getNettyChannel(), JsonSerializer.getShowResult(resultSet));
        context.getState().setEof();
    }

    // BE already transferred results into json format, FE just need to Forward json objects to the client
    private void writeResultBatch(TResultBatch resultBatch, ChannelHandlerContext channel, Coordinator coord) {
        int rowsSize = resultBatch.getRowsSize();
        for (ByteBuffer row : resultBatch.getRows()) {
            // when channel is not writeable, sleep a while to balance read/write speed to avoid oom
            while (!channel.channel().isWritable()) {
                // if channel is closed, cancel query
                if (!channel.channel().isActive()) {
                    coord.cancel();
                    return;
                }
                Thread.yield();
            }
            // only flush once
            if (row != resultBatch.getRows().get(rowsSize - 1)) {
                channel.write(Unpooled.wrappedBuffer(row));
            } else {
                channel.writeAndFlush(Unpooled.wrappedBuffer(row));
            }
        }
    }

    private void sendHeader(ChannelHandlerContext nettyChannel) {
        HttpResponse responseObj = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        responseObj.headers().set(HttpHeaderNames.CONTENT_TYPE.toString(), "application/x-ndjson; charset=utf-8");
        responseObj.headers().set(HEADER_QUERY_ID, context.getQueryId());
        HttpUtil.setTransferEncodingChunked(responseObj, true);

        nettyChannel.write(responseObj);
    }

    private void sendFinalChunk(ChannelHandlerContext nettyChannel, ByteBuf json) {
        nettyChannel.writeAndFlush(json);
        sendEmptyLastContent();
    }

    private void sendEmptyLastContent() {
        if (context.isKeepAlive()) {
            context.getNettyChannel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } else {
            context.getNettyChannel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

}
