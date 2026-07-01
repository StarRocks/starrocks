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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/RestBaseAction.java

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

package com.starrocks.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseAction;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.WebUtils;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;

public class RestBaseAction extends BaseAction {
    protected static final String CATALOG_KEY = "catalog";

    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    protected static final String LABEL_KEY = "label";
    protected static final String WAREHOUSE_KEY = "warehouse";
    protected static final String USER_KEY = "user";

    private static final Logger LOG = LogManager.getLogger(RestBaseAction.class);

    protected static ObjectMapper mapper = new ObjectMapper();

    public RestBaseAction(ActionController controller) {
        super(controller);
    }

    /**
     * Whether this action requires HTTP Basic Auth. {@code true} by default — auth is
     * performed by {@link #execute(BaseRequest, BaseResponse)} before dispatching to
     * {@link #executeWithoutPassword(BaseRequest, BaseResponse)}. Subclasses may override
     * {@code execute} for pre-dispatch bypasses (see {@code LoadAction#tryInternalTokenBypass});
     * the normal path must still call {@code super.execute(...)} so the auth pipeline runs.
     * <p>
     * Subclasses opt out by returning {@code false}:
     * <ul>
     *   <li>Probes / OAuth callback / internal token-protected endpoints — always {@code false}.</li>
     *   <li>Endpoints that historically accepted anonymous requests and are gated for backward
     *       compatibility — return {@link Config#enable_http_auth} so they require Basic only when
     *       the operator opts in.</li>
     * </ul>
     */
    public boolean needAuth() {
        return true;
    }

    @Override
    public void handleRequest(BaseRequest request) {
        BaseResponse response = new BaseResponse();
        String url = request.getRequest().uri();
        try {
            url = WebUtils.sanitizeHttpReqUri(request.getRequest().uri());
            execute(request, response);
        } catch (AccessDeniedException accessDeniedException) {
            LOG.warn("failed to process url: {}", url, accessDeniedException);
            response.updateHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), "Basic realm=\"\"");
            response.appendContent(new RestBaseResult(getErrorRespWhenUnauthorized(accessDeniedException)).toJson());
            writeResponse(request, response, HttpResponseStatus.UNAUTHORIZED);
        } catch (DdlException e) {
            LOG.warn("fail to process url: {}", url, e);
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        } catch (Exception e) {
            LOG.warn("fail to process url: {}", url, e);
            String msg = e.getMessage();
            if (msg == null) {
                msg = e.toString();
            }
            response.appendContent(new RestBaseResult(msg).toJson());
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @VisibleForTesting
    public String getErrorRespWhenUnauthorized(AccessDeniedException accessDeniedException) {
        if (Strings.isNullOrEmpty(accessDeniedException.getMessage())) {
            ConnectContext context = ConnectContext.get();
            if (context != null) {
                AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                UserIdentity userIdentity = context.getCurrentUserIdentity();
                List<String> activatedRoles = authorizationMgr.getRoleNamesByRoleIds(context.getCurrentRoleIds());
                List<String> inactivatedRoles =
                        authorizationMgr.getInactivatedRoleNamesByUser(userIdentity, activatedRoles);
                return "Access denied for user " + userIdentity + ". " +
                        String.format(ErrorCode.ERR_ACCESS_DENIED_HINT_MSG_FORMAT, activatedRoles, inactivatedRoles);
            }
            return "Access denied.";
        } else {
            return accessDeniedException.getMessage();
        }
    }

    // Subclasses may override execute to short-circuit before auth runs (see
    // LoadAction#tryInternalTokenBypass), but if a subclass falls through to
    // the normal Basic-auth path it MUST call super.execute(...) — do not
    // reimplement auth/ctx population, otherwise group-derived roles will be
    // lost and requireXxxIfHttpAuthEnabled() helpers will deny legitimate users.
    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException, AccessDeniedException {
        HttpConnectContext ctx = request.getConnectContext();

        if (needAuth()) {
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            // 3.3 has no AuthenticationHandler / group-derived roles; checkPassword() verifies the
            // Basic credentials and returns the UserIdentity, and ctx.setCurrentRoleIds(UserIdentity)
            // derives the role set from that identity.
            UserIdentity currentUser = checkPassword(authInfo);

            // Change user for ConnectContext if necessary
            UserIdentity prevUserIdentity = ctx.getCurrentUserIdentity();
            Set<Long> prevRoleIds = ctx.getCurrentRoleIds();
            String prevUserName = ctx.getQualifiedUser();

            ctx.setCurrentUserIdentity(currentUser);
            ctx.setCurrentRoleIds(currentUser);
            ctx.setQualifiedUser(authInfo.fullUserName);

            if (ctx.isRegistered() && prevUserName != null && !prevUserName.equals(authInfo.fullUserName)) {
                ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
                Pair<Boolean, String> userChangeRes =
                        connectScheduler.onUserChanged(ctx, prevUserName, ctx.getQualifiedUser());
                if (!userChangeRes.first) {
                    ctx.setCurrentUserIdentity(prevUserIdentity);
                    ctx.setCurrentRoleIds(prevRoleIds);
                    ctx.setQualifiedUser(prevUserName);
                    throw new StarRocksHttpException(SERVICE_UNAVAILABLE, userChangeRes.second);
                }
            }
        }

        // ctx lifetime is the same as the channel
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setNettyChannel(request.getContext());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setRemoteIP(request.getHostString());
        ctx.setThreadLocalInfo();
        executeWithoutPassword(request, response);
    }

    // Subclasses implement this. Auth + ConnectContext setup are already done by the final
    // {@link #execute(BaseRequest, BaseResponse)} above; if {@link #needAuth()} returns false,
    // the user-identity fields on ctx are left unset.
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        throw new DdlException("Not implemented");
    }

    // ---------- privilege helpers, gated by Config.enable_http_auth ----------
    // Endpoints historically accepted anonymous (or any-authenticated) callers and we
    // can't tighten them by default without breaking running scripts. These helpers
    // let the subclass declare the intended privilege; the check actually runs only
    // when the operator opts in via `enable_http_auth=true`.

    /** Require INSERT on any table within the given db. */
    protected void requireDbInsertIfHttpAuthEnabled(String dbName) throws AccessDeniedException {
        if (!Config.enable_http_auth) {
            return;
        }
        ConnectContext context = ConnectContext.get();
        Authorizer.checkActionInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), dbName,
                PrivilegeType.INSERT);
    }

    /** Require SYSTEM.OPERATE — for db/data/query operations. */
    protected void requireOperateIfHttpAuthEnabled() throws AccessDeniedException {
        if (!Config.enable_http_auth) {
            return;
        }
        ConnectContext context = ConnectContext.get();
        Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                PrivilegeType.OPERATE);
    }

    public void sendResult(BaseRequest request, BaseResponse response, RestBaseResult result) {
        response.appendContent(result.toJson());
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    public void sendResult(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
        writeResponse(request, response, status);
    }

    public void sendResult(BaseRequest request, BaseResponse response) {
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    public void sendResultByJson(BaseRequest request, BaseResponse response, Object obj) {
        String result = "";
        try {
            result = mapper.writeValueAsString(obj);
        } catch (Exception e) {
            //  do nothing
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }

    public void redirectTo(BaseRequest request, BaseResponse response, TNetworkAddress addr)
            throws DdlException {
        String urlStr = request.getRequest().uri();
        URI urlObj;
        URI resultUriObj;
        try {
            urlObj = new URI(urlStr);
            resultUriObj = new URI("http", null, addr.getHostname(),
                    addr.getPort(), urlObj.getPath(), urlObj.getQuery(), null);
        } catch (URISyntaxException e) {
            LOG.warn(e.getMessage(), e);
            throw new DdlException(e.getMessage());
        }
        response.updateHeader(HttpHeaderNames.LOCATION.toString(), resultUriObj.toASCIIString());
        writeResponse(request, response, HttpResponseStatus.TEMPORARY_REDIRECT);
    }

    public boolean redirectToLeader(BaseRequest request, BaseResponse response) throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr.isLeader()) {
            return false;
        }
        Pair<String, Integer> leaderIpAndPort = globalStateMgr.getNodeMgr().getLeaderIpAndHttpPort();
        redirectTo(request, response,
                new TNetworkAddress(leaderIpAndPort.first, leaderIpAndPort.second));
        return true;
    }
}
