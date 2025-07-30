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


package com.starrocks.httpv2.controller;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.httpv2.HttpAuthManager;
import com.starrocks.httpv2.entity.ActionAuthorizationInfo;
import com.starrocks.httpv2.entity.Result;
import com.starrocks.httpv2.enums.Status;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class BaseController {

    private static final Logger LOG = LogManager.getLogger(BaseController.class);

    private static final String STARROCKS_SESSION_ID = "STARROCKS_SESSION_ID";

    private static final int STARROCKS_SESSION_EXPIRED_TIME = 3600 * 24;

    protected Result<Object> authenticate(HttpServletRequest request, HttpServletResponse response) {
        Result<Object> result = new Result<>();
        UserIdentity currentUser = null;
        try {
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            currentUser = checkPassword(authInfo);

            com.starrocks.httpv2.HttpAuthManager.SessionValue value = new com.starrocks.httpv2.HttpAuthManager.SessionValue();
            value.currentUser = currentUser;
            addSession(response, value);

            result.setCode(Status.SUCCESS.getCode());
            result.setMsg(Status.LOGIN_SUCCESS.getMsg());
            return result;
        } catch (AccessDeniedException e) {
            result.setCode(Status.USER_LOGIN_FAILURE.getCode());
            result.setMsg(Status.USER_LOGIN_FAILURE.getMsg());
            return result;
        }
    }

    private  UserIdentity checkPassword(ActionAuthorizationInfo authInfo) throws
            AccessDeniedException {
        try {
            return AuthenticationHandler.authenticate(new ConnectContext(), authInfo.fullUserName,
                    authInfo.remoteIp, authInfo.password.getBytes(StandardCharsets.UTF_8));
        } catch (AuthenticationException e) {
            throw new AccessDeniedException("Access denied for " + authInfo.fullUserName + "@" + authInfo.remoteIp);
        }
    }

    private ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request)
            throws AccessDeniedException {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        try {
            if (!parseAuthInfo(request, authInfo)) {
                LOG.info("parse auth info failed, url {}", request.getRequestURI());
                throw new AccessDeniedException("Need auth information.");
            }
            LOG.debug("get auth info: {}", authInfo);
        } catch (Exception e) {
            throw new AccessDeniedException(e.getMessage());
        }
        return authInfo;
    }

    protected ActionAuthorizationInfo checkAuthWithCookie(HttpServletRequest request,
                                                          HttpServletResponse response) throws AccessDeniedException {
        ActionAuthorizationInfo authInfo = null;
        String authorizationHeaderValue = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorizationHeaderValue != null) {
            authInfo = getAuthorizationInfo(request);
            UserIdentity currentUser = checkPassword(authInfo);

            checkUserOwnsAdminRole(currentUser);

            HttpAuthManager.SessionValue value = new HttpAuthManager.SessionValue();
            value.currentUser = currentUser;
            value.password = authInfo.password;
            addSession(response, value);

            ConnectContext ctx = new ConnectContext();
            ctx.setRemoteIP(authInfo.remoteIp);
            ctx.setCurrentUserIdentity(currentUser);
            ctx.setQualifiedUser(currentUser.getUser());
            ctx.setCurrentRoleIds(currentUser);
            ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            ctx.setQueryId(UUIDUtil.genUUID());

            ctx.setThreadLocalInfo();
            if (LOG.isDebugEnabled()) {
                LOG.debug("check auth without cookie success for user: {}, thread: {}",
                        currentUser, Thread.currentThread().getId());
            }
            return authInfo;
        }

        authInfo = checkCookie(request, response);


        if (authInfo == null) {
            throw new AccessDeniedException("Cookie is invalid");
        }

        return authInfo;
    }


    private  boolean parseAuthInfo(HttpServletRequest request, ActionAuthorizationInfo authInfo) {
        String encodedAuthString = request.getHeader(HttpHeaders.AUTHORIZATION);
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
            if (authString.isEmpty()) {
                return false;
            }
            // Note that password may contain colon, so can not simply use a
            // colon to split.
            int index = authString.indexOf(":");
            if (index == -1) {
                return false;
            }
            authInfo.fullUserName = authString.substring(0, index);
            final String[] elements = authInfo.fullUserName.split("@");
            if (elements.length == 2) {
                authInfo.fullUserName = elements[0];
            }
            authInfo.password = authString.substring(index + 1);
            authInfo.remoteIp = request.getRemoteHost();
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

    private void addSession(HttpServletResponse response, HttpAuthManager.SessionValue value) {
        String key = UUID.randomUUID().toString();
        Cookie cookie = new Cookie(STARROCKS_SESSION_ID, key);
        cookie.setMaxAge(STARROCKS_SESSION_EXPIRED_TIME);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        response.addCookie(cookie);
        HttpAuthManager.getInstance().addSessionValue(key, value);
    }

    private void checkUserOwnsAdminRole(UserIdentity currentUser) throws AccessDeniedException {
        try {
            Set<Long> userOwnedRoles = AuthorizationMgr.getOwnedRolesByUser(currentUser);
            if (!(currentUser.equals(UserIdentity.ROOT) ||
                    userOwnedRoles.contains(PrivilegeBuiltinConstants.ROOT_ROLE_ID) ||
                    (userOwnedRoles.contains(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID) &&
                            userOwnedRoles.contains(PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID)))) {
                throw new AccessDeniedException();
            }
        } catch (PrivilegeException e) {
            throw new AccessDeniedException();
        }
    }

    private ActionAuthorizationInfo checkCookie(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException {

        Cookie[] cookies = request.getCookies();

        String sessionId = retrieveCookieValue(cookies, STARROCKS_SESSION_ID);

        if (StringUtils.isEmpty(sessionId)) {
            return null;
        }

        HttpAuthManager authMgr = HttpAuthManager.getInstance();

        HttpAuthManager.SessionValue sessionValue = authMgr.getSessionValue(sessionId);

        if (sessionValue == null) {
            return null;
        }

        checkUserOwnsAdminRole(sessionValue.currentUser);

        refreshCookieAge(request, STARROCKS_SESSION_ID, response);

        ConnectContext ctx = new ConnectContext();

        ctx.setQualifiedUser(sessionValue.currentUser.getUser());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setRemoteIP(request.getRemoteHost());
        ctx.setCurrentUserIdentity(sessionValue.currentUser);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setCurrentRoleIds(sessionValue.currentUser);

        ctx.setThreadLocalInfo();
        if (LOG.isDebugEnabled()) {
            LOG.debug("check cookie success for user: {}, thread: {}",
                    sessionValue.currentUser, Thread.currentThread().getId());
        }
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = sessionValue.currentUser.getUser();
        authInfo.remoteIp = request.getRemoteHost();
        authInfo.password = sessionValue.password;
        return authInfo;
    }

    private String retrieveCookieValue(Cookie[] cookies, String key) {
        if (cookies == null) {
            return null;
        }

        for (Cookie cookie : cookies) {
            if (null != cookie && key.equals(cookie.getName())) {
                return cookie.getValue();
            }
        }

        return null;
    }


    private void refreshCookieAge(HttpServletRequest request, String cookieName, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        for (Cookie cookie : cookies) {
            if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                cookie.setMaxAge(STARROCKS_SESSION_EXPIRED_TIME);
                cookie.setPath("/");
                cookie.setHttpOnly(true);
                cookie.setSecure(false);
                response.addCookie(cookie);
            }
        }
    }

    protected void clearSession(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        for (Cookie cookie : cookies) {
            if (cookie.getName() != null && cookie.getName().equals(STARROCKS_SESSION_EXPIRED_TIME)) {
                String sessionId = cookie.getValue();
                HttpAuthManager.getInstance().removeSession(sessionId);
            }
        }
    }

}
