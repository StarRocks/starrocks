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

import com.starrocks.common.Config;
import com.starrocks.http.action.IndexAction;
import com.starrocks.http.action.NotFoundAction;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.concurrent.Executor;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LogManager.getLogger(HttpServerHandler.class);
    // keep connectContext when channel is open
    public static final AttributeKey<HttpConnectContext> HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY =
            AttributeKey.valueOf("httpContextKey");
    protected HttpRequest request = null;
    private final ActionController controller;
    private final Executor executor;
    private BaseAction action = null;

    public HttpServerHandler(ActionController controller, Executor executor) {
        super();
        this.controller = controller;
        this.executor = executor;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (!(msg instanceof HttpRequest)) {
                return;
            }

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

            // get HttpConnectContext from channel, HttpConnectContext's lifetime is same as channel
            HttpConnectContext connectContext = ctx.channel().attr(HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY).get();
            BaseRequest req = new BaseRequest(ctx, request, connectContext);
            action = getAction(req);
            if (action != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("action: {} ", action.getClass().getName());
                }
                if (action.supportAsyncHandler()) {
                    handleActionAsync(req);
                } else {
                    handleActionSync(req);
                }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void handleActionSync(BaseRequest request) {
        RequestHandlingWatch watch = new RequestHandlingWatch(request, false);
        try {
            action.handleRequest(request);
        } catch (Exception e) {
            handleException(request, e);
        } finally {
            watch.finish();
        }
    }

    private void handleActionAsync(BaseRequest request) {
        RequestHandlingWatch watch = new RequestHandlingWatch(request, true);
        try {
            executor.execute(() -> {
                try {
                    action.handleRequest(request);
                } catch (Exception e) {
                    handleException(request, e);
                } finally {
                    // HttpServerHandler will flush automatically in channelReadComplete. For synchronous handling,
                    // the response is written in channelRead which is before channelReadComplete, so no need to
                    // trigger flush manually. But for asynchronous handling, the response can be written after
                    // channelReadComplete, so we need to trigger the flush manually.
                    request.getContext().flush();
                    watch.finish();
                }
            });
        } catch (Exception exception) {
            handleException(request, exception);
            watch.finish();
        }
    }

    private void handleException(BaseRequest request, Exception exception) {
        writeResponse(request.getContext(), HttpResponseStatus.INTERNAL_SERVER_ERROR,
                String.format("failed to handle request, error: %s:%s ", exception.getClass(), exception.getMessage()));
        LOG.debug("Failed to handle request: {}", request.getRequest().uri(), exception);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        HttpServerHandlerMetrics.getInstance().httpConnectionsNum.increase(1L);
        // create HttpConnectContext when channel is established, and store it in channel attr
        ctx.channel().attr(HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY).setIfAbsent(new HttpConnectContext());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        HttpServerHandlerMetrics.getInstance().httpConnectionsNum.increase(-1L);
        if (action != null) {
            action.handleChannelInactive(ctx);
        }
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn(String.format("[remote=%s] Exception caught: %s",
                ctx.channel().remoteAddress(), cause.getMessage()), cause);
        ctx.close();
    }

    private BaseAction getAction(BaseRequest request) {
        String uri = request.getRequest().uri();
        String baseUri = URI.create(uri).getPath();
        // ignore this request, which is a default request from client's browser.
        if (baseUri.endsWith("/favicon.ico")) {
            return NotFoundAction.getNotFoundAction();
        } else if (baseUri.equals("/")) {
            return new IndexAction(controller);
        }

        // Map<String, String> params = Maps.newHashMap();
        BaseAction action = (BaseAction) controller.getHandler(request);
        if (action == null) {
            action = NotFoundAction.getNotFoundAction();
        }

        return action;
    }

    private static class RequestHandlingWatch {
        private final BaseRequest request;
        private final boolean asyncHandling;
        private final long startTime;

        public RequestHandlingWatch(BaseRequest request, boolean asyncHandling) {
            this.request = request;
            this.asyncHandling = asyncHandling;
            this.startTime = System.currentTimeMillis();
            HttpServerHandlerMetrics.getInstance().handlingRequestsNum.increase(1L);
            if (asyncHandling) {
                // HttpServerHandler will release the request object after channelRead finishes. To ensure
                // the request object can be accessed in the async executor safely, retain the request.
                ReferenceCountUtil.retain(request.getRequest());
            }
        }

        public void finish() {
            long latency = System.currentTimeMillis() - startTime;
            HttpServerHandlerMetrics.getInstance().handlingRequestsNum.increase(-1L);
            HttpServerHandlerMetrics.getInstance().requestHandleLatencyMs.update(latency);
            if (latency >= Config.http_slow_request_threshold_ms) {
                String uri;
                try {
                    uri = WebUtils.sanitizeHttpReqUri(request.getRequest().uri());
                } catch (Exception e) {
                    uri = "failed to sanitize uri, error=" + e.getMessage();
                }
                LOG.warn("receive slow http request. uri: {}, startTime: {}, latency: {} ms",
                        uri, startTime, latency);
            }
            if (asyncHandling) {
                ReferenceCountUtil.release(request.getRequest());
            }
        }

    }
}
