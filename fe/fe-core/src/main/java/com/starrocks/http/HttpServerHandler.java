// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/HttpServerHandler.java

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

import com.starrocks.http.action.IndexAction;
import com.starrocks.http.action.NotFoundAction;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LogManager.getLogger(HttpServerHandler.class);

    private ActionController controller = null;
    protected FullHttpRequest fullRequest = null;
    protected HttpRequest request = null;
    private BaseAction action = null;

    public HttpServerHandler(ActionController controller) {
        super();
        this.controller = controller;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            this.request = (HttpRequest) msg;
            if (LOG.isDebugEnabled()) {
                LOG.debug("request: url:[{}]", request.uri());
            }
            try {
                validateRequest(ctx, request);
            } catch (Exception e) {
                LOG.warn("accept bad request: {}, error: {}", request.uri(), e.getMessage(), e);
                writeResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Bad Request. <br/> " + e.getMessage());
                return;
            }
            BaseRequest req = new BaseRequest(ctx, request);
            action = getAction(req);
            if (action != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("action: {} ", action.getClass().getName());
                }

                HttpServerHandlerMetrics metrics = HttpServerHandlerMetrics.getInstance();
                long startTime = System.currentTimeMillis();
                try {
                    metrics.handlingRequestsNum.increase(1L);
                    action.handleRequest(req);
                } finally {
                    long latency = System.currentTimeMillis() - startTime;
                    metrics.handlingRequestsNum.increase(-1L);
                    metrics.requestHandleLatencyMs.update(latency);
                    LOG.info("receive http request. url: {}, thread id: {}, startTime: {}, latency: {} ms",
                            req.getRequest().uri(), Thread.currentThread().getId(), startTime, latency);
                }
            }
        } else {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        HttpServerHandlerMetrics.getInstance().httpConnectionsNum.increase(1L);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        HttpServerHandlerMetrics.getInstance().httpConnectionsNum.increase(-1L);
        super.channelInactive(ctx);
    }

    private void validateRequest(ChannelHandlerContext ctx, HttpRequest request) {
        DecoderResult decoderResult = request.decoderResult();
        if (decoderResult.isFailure()) {
            throw new HttpRequestException(decoderResult.cause().getMessage());
        }
    }

    private void writeResponse(ChannelHandlerContext context, HttpResponseStatus status, String content) {
        FullHttpResponse responseObj = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(content.getBytes()));
        responseObj.headers().set(HttpHeaderNames.CONTENT_TYPE.toString(), "text/html");
        responseObj.headers().set(HttpHeaderNames.CONTENT_LENGTH.toString(), responseObj.content().readableBytes());
        context.writeAndFlush(responseObj).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.warn(String.format("[remote=%s] Exception caught: %s",
                ctx.channel().remoteAddress(), cause.getMessage()), cause);
        ctx.close();
    }

    private BaseAction getAction(BaseRequest request) {
        String uri = request.getRequest().uri();
        // ignore this request, which is a default request from client's browser.
        if (uri.endsWith("/favicon.ico")) {
            return NotFoundAction.getNotFoundAction();
        } else if (uri.equals("/")) {
            return new IndexAction(controller);
        }

        // Map<String, String> params = Maps.newHashMap();
        BaseAction action = (BaseAction) controller.getHandler(request);
        if (action == null) {
            action = NotFoundAction.getNotFoundAction();
        }

        return action;
    }
}
