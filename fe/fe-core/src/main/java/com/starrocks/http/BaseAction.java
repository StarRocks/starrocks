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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/BaseAction.java

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseAction implements IAction {
    private static final Logger LOG = LogManager.getLogger(BaseAction.class);

    protected ActionController controller;
    protected GlobalStateMgr globalStateMgr;

    public BaseAction(ActionController controller) {
        this.controller = controller;
        // TODO(zc): remove this instance
        this.globalStateMgr = GlobalStateMgr.getCurrentState();
    }

    @Override
    public void handleRequest(BaseRequest request) throws Exception {
        BaseResponse response = new BaseResponse();
        LOG.info("receive http request. url={}", request.getRequest().uri());
        try {
            execute(request, response);
        } catch (Exception e) {
            LOG.warn("fail to process url: {}", request.getRequest().uri(), e);
            if (e instanceof UnauthorizedException) {
                response.updateHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), "Basic realm=\"\"");
                writeResponse(request, response, HttpResponseStatus.UNAUTHORIZED);
            } else {
                writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            }
        }
    }

    public abstract void execute(BaseRequest request, BaseResponse response) throws DdlException;

    protected void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
        // if (HttpHeaders.is100ContinueExpected(request.getRequest())) {
        // ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        // HttpResponseStatus.CONTINUE));
        // }

        FullHttpResponse responseObj = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.wrappedBuffer(response.getContent().toString().getBytes(StandardCharsets.UTF_8)));
        Preconditions.checkNotNull(responseObj);
        HttpMethod method = request.getRequest().method();

        checkDefaultContentTypeHeader(response, responseObj);
        if (!method.equals(HttpMethod.HEAD)) {
            response.updateHeader(HttpHeaderNames.CONTENT_LENGTH.toString(),
                    String.valueOf(responseObj.content().readableBytes()));
        }
        writeCustomHeaders(response, responseObj);
        writeCookies(response, responseObj);

        boolean keepAlive = HttpUtil.isKeepAlive(request.getRequest());
        if (!keepAlive) {
            request.getContext().write(responseObj).addListener(ChannelFutureListener.CLOSE);
        } else {
            responseObj.headers().set(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.KEEP_ALIVE.toString());
            request.getContext().write(responseObj);
        }
    }

    // Object only support File or byte[]
    protected void writeObjectResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status,
                                       Object obj, String fileName, boolean isOctStream) {
        Preconditions.checkState((obj instanceof File) || (obj instanceof byte[]));

        HttpResponse responseObj = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (HttpUtil.isKeepAlive(request.getRequest())) {
            response.updateHeader(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.KEEP_ALIVE.toString());
        }

        if (isOctStream) {
            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(),
                    HttpHeaderValues.APPLICATION_OCTET_STREAM.toString());
            response.updateHeader(HttpHeaderNames.CONTENT_DISPOSITION.toString(),
                    HttpHeaderValues.ATTACHMENT.toString() + "; " + HttpHeaderValues.FILENAME.toString() + "=" +
                            fileName);
        }

        ChannelFuture sendFileFuture;
        ChannelFuture lastContentFuture;

        try {
            Object writable = null;
            long contentLen = 0;
            boolean sslEnable = request.getContext().pipeline().get(SslHandler.class) != null;
            if (obj instanceof File) {
                RandomAccessFile rafFile = new RandomAccessFile((File) obj, "r");
                contentLen = rafFile.length();
                if (!sslEnable) {
                    // use zero-copy file transfer.
                    writable = new DefaultFileRegion(rafFile.getChannel(), 0, contentLen);
                } else {
                    // cannot use zero-copy file transfer.
                    writable = new ChunkedFile(rafFile, 0, contentLen, 8192);
                }
            } else if (obj instanceof byte[]) {
                contentLen = ((byte[]) obj).length;
                if (!sslEnable) {
                    writable = Unpooled.wrappedBuffer((byte[]) obj);
                } else {
                    writable = new ChunkedStream(new ByteArrayInputStream((byte[]) obj));
                }
            }

            response.updateHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(contentLen));
            writeCookies(response, responseObj);
            writeCustomHeaders(response, responseObj);

            // Write headers
            request.getContext().write(responseObj);

            // Write object
            if (!sslEnable) {
                sendFileFuture = request.getContext().write(writable, request.getContext().newProgressivePromise());
                // Write the end marker.
                lastContentFuture = request.getContext().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } else {
                sendFileFuture = request.getContext().writeAndFlush(
                        new HttpChunkedInput((ChunkedInput<ByteBuf>) writable),
                        request.getContext().newProgressivePromise());
                // HttpChunkedInput will write the end marker (LastHttpContent) for us.
                lastContentFuture = sendFileFuture;
            }
        } catch (FileNotFoundException ignore) {
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        } catch (IOException e1) {
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                if (total < 0) { // total unknown
                    LOG.debug("{} Transfer progress: {}", future.channel(), progress);
                } else {
                    LOG.debug("{} Transfer progress: {} / {}", future.channel(), progress, total);
                }
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                LOG.debug("{} Transfer complete.", future.channel());
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    LOG.error("something wrong. ", cause);
                }
            }
        });

        // Decide whether to close the connection or not.
        boolean keepAlive = HttpUtil.isKeepAlive(request.getRequest());
        if (!keepAlive) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    // Set 'CONTENT_TYPE' header if it hasn't been set.
    protected void checkDefaultContentTypeHeader(BaseResponse response, Object responseOj) {
        if (!Strings.isNullOrEmpty(response.getContentType())) {
            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), response.getContentType());
        } else {
            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "text/html");
        }
    }

    protected void writeCustomHeaders(BaseResponse response, HttpResponse responseObj) {
        for (Map.Entry<String, List<String>> entry : response.getCustomHeaders().entrySet()) {
            responseObj.headers().add(entry.getKey(), entry.getValue());
        }
    }

    protected void writeCookies(BaseResponse response, HttpResponse responseObj) {
        for (Cookie cookie : response.getCookies()) {
            responseObj.headers().add(HttpHeaderNames.SET_COOKIE.toString(), ServerCookieEncoder.LAX.encode(cookie));
        }
    }

    public static class ActionAuthorizationInfo {
        public String fullUserName;
        public String remoteIp;
        public String password;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("user: ").append(fullUserName).append(", remote ip: ").append(remoteIp);
            sb.append(", password: ").append(password);
            return sb.toString();
        }

        public static ActionAuthorizationInfo of(String fullUserName, String password, String remoteIp) {
            ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
            authInfo.fullUserName = fullUserName;
            authInfo.remoteIp = remoteIp;
            authInfo.password = password;
            return authInfo;
        }
    }

    protected void checkGlobalAuth(UserIdentity currentUser, PrivPredicate predicate) throws UnauthorizedException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(currentUser, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    // For new RBAC privilege framework
    protected void checkActionOnSystem(UserIdentity currentUser, PrivilegeType... systemActions)
            throws UnauthorizedException {
        for (PrivilegeType systemAction : systemActions) {
            if (!PrivilegeActions.checkSystemAction(currentUser, null, systemAction)) {
                throw new UnauthorizedException("Access denied; you need (at least one of) the "
                        + systemAction.name() + " privilege(s) for this operation");
            }
        }
    }

    // We check whether user owns db_admin and user_admin role in new RBAC privilege framework for
    // operation which checks `PrivPredicate.ADMIN` in global table in old Auth framework.
    protected void checkUserOwnsAdminRole(UserIdentity currentUser) throws UnauthorizedException {
        try {
            Set<Long> userOwnedRoles = AuthorizationMgr.getOwnedRolesByUser(currentUser);
            if (!(currentUser.equals(UserIdentity.ROOT) ||
                    userOwnedRoles.contains(PrivilegeBuiltinConstants.ROOT_ROLE_ID) ||
                    (userOwnedRoles.contains(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID) &&
                            userOwnedRoles.contains(PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID)))) {
                throw new UnauthorizedException(
                        "Access denied; you need own root role or own db_admin and user_admin roles for this " +
                                "operation");
            }
        } catch (PrivilegeException e) {
            UnauthorizedException newException = new UnauthorizedException(
                    "Access denied; you need own db_admin and user_admin roles for this operation");
            newException.initCause(e);
        }
    }

    protected void checkDbAuth(UserIdentity currentUser, String db, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(currentUser, db, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkTblAuth(UserIdentity currentUser, String db, String tbl, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, db, tbl, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkTableAction(ConnectContext context, String db, String tbl,
                                    PrivilegeType action) throws UnauthorizedException {
        if (!PrivilegeActions.checkTableAction(context, db, tbl, action)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + action.name() + " privilege(s) for this operation");
        }
    }

    // return currentUserIdentity from StarRocks auth
    public static UserIdentity checkPassword(ActionAuthorizationInfo authInfo)
            throws UnauthorizedException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr.isUsingNewPrivilege()) {
            UserIdentity currentUser =
                    globalStateMgr.getAuthenticationMgr().checkPlainPassword(
                            authInfo.fullUserName, authInfo.remoteIp, authInfo.password);
            if (currentUser == null) {
                throw new UnauthorizedException("Access denied for "
                        + authInfo.fullUserName + "@" + authInfo.remoteIp);
            }
            return currentUser;
        }
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!GlobalStateMgr.getCurrentState().getAuth().checkPlainPassword(authInfo.fullUserName,
                authInfo.remoteIp, authInfo.password, currentUser)) {
            throw new UnauthorizedException("Access denied for "
                    + authInfo.fullUserName + "@" + authInfo.remoteIp);
        }
        Preconditions.checkState(currentUser.size() == 1);
        return currentUser.get(0);
    }

    public ActionAuthorizationInfo getAuthorizationInfo(BaseRequest request)
            throws UnauthorizedException {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        if (!parseAuthInfo(request, authInfo)) {
            LOG.info("parse auth info failed, Authorization header {}, url {}",
                    request.getAuthorizationHeader(), request.getRequest().uri());
            throw new UnauthorizedException("Need auth information.");
        }
        LOG.debug("get auth info: {}", authInfo);
        return authInfo;
    }

    private boolean parseAuthInfo(BaseRequest request, ActionAuthorizationInfo authInfo) {
        String encodedAuthString = request.getAuthorizationHeader();
        if (Strings.isNullOrEmpty(encodedAuthString)) {
            return false;
        }
        String[] parts = encodedAuthString.split(" ");
        if (parts.length != 2) {
            return false;
        }
        encodedAuthString = parts[1];
        ByteBuf buf = null;
        ByteBuf decodeBuf = null;
        try {
            buf = Unpooled.copiedBuffer(ByteBuffer.wrap(encodedAuthString.getBytes()));

            // The authString is a string connecting user-name and password with
            // a colon(':')
            decodeBuf = Base64.decode(buf);
            String authString = decodeBuf.toString(CharsetUtil.UTF_8);
            // Note that password may contain colon, so can not simply use a
            // colon to split.
            int index = authString.indexOf(":");
            authInfo.fullUserName = authString.substring(0, index);
            final String[] elements = authInfo.fullUserName.split("@");
            if (elements.length == 2) {
                authInfo.fullUserName = elements[0];
            }
            authInfo.password = authString.substring(index + 1);
            authInfo.remoteIp = request.getHostString();
        } finally {
            // release the buf and decode buf after using Unpooled.copiedBuffer
            // or it will get memory leak
            if (buf != null) {
                buf.release();
            }

            if (decodeBuf != null) {
                decodeBuf.release();
            }
        }
        return true;
    }

    // Refer to {@link #parseAuthInfo(BaseRequest, ActionAuthorizationInfo)}
    public static ActionAuthorizationInfo parseAuthInfo(String fullUserName, String password, String host) {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        final String[] elements = fullUserName.split("@");
        if (elements.length == 2) {
            authInfo.fullUserName = elements[0];
        } else {
            authInfo.fullUserName = fullUserName;
        }
        authInfo.password = password;
        authInfo.remoteIp = host;

        LOG.debug("Parse result for the input [{} {} {}]: {}", fullUserName, password, host, authInfo);

        return authInfo;
    }

    protected int checkIntParam(String strParam) {
        return Integer.parseInt(strParam);
    }

    protected long checkLongParam(String strParam) {
        return Long.parseLong(strParam);
    }
}
