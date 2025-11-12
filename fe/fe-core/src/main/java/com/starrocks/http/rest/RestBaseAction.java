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
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;


public class RestBaseAction extends BaseAction {

    private static final Logger LOG = LogManager.getLogger(RestBaseAction.class);

    protected static final String CATALOG_KEY = "catalog";
    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    protected static final String LABEL_KEY = "label";
    public static final String WAREHOUSE_KEY = "warehouse";
    protected static final String USER_KEY = "user";

    protected static final String PAGE_NUM_KEY = "page_num";
    protected static final String PAGE_SIZE_KEY = "page_size";

    protected static final int DEFAULT_PAGE_NUM = 0;
    protected static final int DEFAULT_PAGE_SIZE = 100;

    protected static final String JSON_CONTENT_TYPE = "application/json; charset=UTF-8";
    protected static ObjectMapper mapper = new ObjectMapper();

    private static final Set<String> SKIP_HEADERS = Set.of("content-length");

    // to make things simple, let `http_internal_redirect_default_timeout_ms` be a static config
    private static final RequestConfig REQUEST_CONFIG = RequestConfig.custom()
            .setConnectTimeout(Config.http_internal_redirect_default_timeout_ms)
            .setSocketTimeout(Config.http_internal_redirect_default_timeout_ms)
            .build();

    public RestBaseAction(ActionController controller) {
        super(controller);
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
                return "Access denied for user " + userIdentity  + ". " +
                        String.format(ErrorCode.ERR_ACCESS_DENIED_HINT_MSG_FORMAT, activatedRoles, inactivatedRoles);
            }
            return "Access denied.";
        } else {
            return accessDeniedException.getMessage();
        }
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException, AccessDeniedException {
        ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
        // check password
        UserIdentity currentUser = checkPassword(authInfo);
        // ctx lifetime is the same as the channel
        HttpConnectContext ctx = request.getConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setNettyChannel(request.getContext());
        ctx.setQualifiedUser(authInfo.fullUserName);
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setRemoteIP(authInfo.remoteIp);
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setCurrentRoleIds(currentUser);
        ctx.setThreadLocalInfo();
        executeWithoutPassword(request, response);
    }

    // If user password should be checked, the derived class should implement this method, NOT 'execute()',
    // otherwise, override 'execute()' directly
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        throw new DdlException("Not implemented");
    }

    public void sendResult(BaseRequest request, BaseResponse response, RestBaseResult result) {
        sendResult(request, response, HttpResponseStatus.OK, result);
    }

    public void sendResult(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
        sendResult(request, response, status, null);
    }

    public void sendResult(BaseRequest request, BaseResponse response) {
        sendResult(request, response, HttpResponseStatus.OK);
    }

    public void sendResult(BaseRequest request,
                           BaseResponse response,
                           HttpResponseStatus status,
                           RestBaseResult result) {
        if (null != result) {
            response.setContentType(JSON_CONTENT_TYPE);
            response.appendContent(result.toJson());
        }
        writeResponse(request, response, status);
    }

    public void sendResultByJson(BaseRequest request, BaseResponse response, Object obj) {
        String result = "";
        try {
            result = mapper.writeValueAsString(obj);
        } catch (Exception e) {
            //  do nothing
        }

        // send result
        response.setContentType(JSON_CONTENT_TYPE);
        response.getContent().append(result);
        sendResult(request, response);
    }

    public void redirectTo(BaseRequest request, BaseResponse response, TNetworkAddress addr)
            throws DdlException {
        URI resultUriObj = getRedirectToUri(request, addr);
        response.updateHeader(HttpHeaderNames.LOCATION.toString(), resultUriObj.toString());
        writeResponse(request, response, HttpResponseStatus.TEMPORARY_REDIRECT);
    }

    private URI getRedirectToUri(BaseRequest request, TNetworkAddress addr) throws DdlException {
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
        return resultUriObj;
    }

    private boolean shouldRetry(Throwable t) {
        return t instanceof IOException || t.getCause() instanceof IOException;
    }

    private void handleHttp200(BaseRequest request, BaseResponse response, HttpResponse newResponse) throws IOException {
        HttpEntity entity = newResponse.getEntity();
        if (entity != null) {
            response.appendContent(EntityUtils.toString(entity));
        }
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    private void handleHttp307(BaseRequest request, BaseResponse response, HttpResponse newResponse) {
        Header header = newResponse.getFirstHeader(HttpHeaders.LOCATION);
        if (header != null) {
            response.updateHeader(HttpHeaderNames.LOCATION.toString(), header.getValue());
            writeResponse(request, response, HttpResponseStatus.TEMPORARY_REDIRECT);
        } else {
            throw new IllegalStateException("Receive Http 307 temporary redirect, but no any available new location.");
        }
    }

    private HttpRequest generateNewHttpRequest(BaseRequest request, TNetworkAddress addr) throws DdlException {
        HttpMethod method = request.getRequest().method();
        URI newUri = getRedirectToUri(request, addr);
        LOG.debug("Get new http request for redirect uri {}", newUri);
        RequestBuilder builder = RequestBuilder.create(method.name())
                .setUri(newUri);

        for (Map.Entry<String, String> header : request.getRequest().headers().entries()) {
            if (!SKIP_HEADERS.contains(header.getKey())) {
                LOG.debug("Add header '{}:{}'", header.getKey(), header.getValue());
                builder.addHeader(header.getKey(), header.getValue());
            }
        }

        return builder.build();
    }

    // Internal Redirect to FE Leader. Only redirect to FE leader.
    // That means if fe leader return a new location, we will return this location to the client. Because the FE doesn't
    // have the capability to deal with large datasets. If we redirect data here, it might result in a significantly
    // unexpected consequence.
    private void internalRedirectTo(BaseRequest request, BaseResponse response, TNetworkAddress addr, int maxRetryTimes)
            throws DdlException {
        String host = addr.getHostname();
        int port = addr.getPort();

        HttpHost newHost = new HttpHost(host, port);
        HttpRequest newRequest = generateNewHttpRequest(request, addr);

        Exception lastException = null;
        for (int numTries = 0; numTries <= maxRetryTimes; ++numTries) {
            try (CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(REQUEST_CONFIG).build()) {
                HttpResponse newResponse = client.execute(newHost, newRequest);
                if (newResponse != null && newResponse.getStatusLine() != null) {
                    switch (newResponse.getStatusLine().getStatusCode()) {
                        case HttpStatus.SC_OK:
                            handleHttp200(request, response, newResponse);
                            return;
                        case HttpStatus.SC_TEMPORARY_REDIRECT:
                            handleHttp307(request, response, newResponse);
                            return;
                        default:
                            LOG.warn("Unhandled status code: {}", newResponse.getStatusLine().getStatusCode());
                    }
                }
            } catch (Exception e) {
                lastException = e;
                if (!shouldRetry(e)) {
                    break;
                }
                LOG.warn("Internal redirect request to leader failed, numTries: {}, reason: {}", numTries, e.getMessage());
            }
        }
        LOG.error("Internal redirect request to leader failed. uri: {}", getRedirectToUri(request, addr), lastException);
        // throw exception, so that upper callers can write error responses to client
        throw new DdlException("Internal redirect request to leader failed.");
    }

    public boolean redirectToLeaderWithRetry(BaseRequest request, BaseResponse response, int maxRetryTimes)
            throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr.isLeader()) {
            return false;
        }
        Pair<String, Integer> leaderIpAndPort = globalStateMgr.getNodeMgr().getLeaderIpAndHttpPort();

        String redirectHost = leaderIpAndPort.first;
        if (Config.stream_load_force_use_ip) {
            InetAddressValidator validator = InetAddressValidator.getInstance();
            if (!validator.isValidInet4Address(redirectHost) && !validator.isValidInet6Address(redirectHost)) {
                try {
                    InetAddress ipAddress = InetAddress.getByName(redirectHost);
                    redirectHost = ipAddress.getHostAddress();
                } catch (UnknownHostException ex) {
                    LOG.warn("get redirect host for leader {} failed!", redirectHost);
                }
            }
        }
        if (Config.emr_internal_redirect) {
            internalRedirectTo(request, response,
                    new TNetworkAddress(leaderIpAndPort.first, leaderIpAndPort.second), maxRetryTimes);
        } else {
            redirectTo(request, response, new TNetworkAddress(redirectHost, leaderIpAndPort.second));
        }
        return true;
    }

    /**
     * Get single parameter value.
     *
     * @param request       http request
     * @param paramName     parameter name
     * @param typeConverter convert the String parameter value to target type
     * @return parameter value, or {@code null} if missing
     */
    protected static <T> T getSingleParameter(BaseRequest request,
                                              String paramName,
                                              Function<String, T> typeConverter) {
        return getSingleParameterOrDefault(request, paramName, null, typeConverter);
    }

    /**
     * Get single parameter value.
     *
     * @param request       http request
     * @param paramName     parameter name
     * @param typeConverter convert the String parameter value to target type
     * @return parameter value
     * @throws StarRocksHttpException if parameter is missing
     */
    protected static <T> T getSingleParameterRequired(BaseRequest request,
                                                      String paramName,
                                                      Function<String, T> typeConverter) {
        String value = request.getSingleParameter(paramName);
        if (null == value) {
            throw new StarRocksHttpException(
                    HttpResponseStatus.BAD_REQUEST,
                    String.format("Missing parameter %s", paramName)
            );
        }
        return typeConverter.apply(value);
    }

    /**
     * Get single parameter value.
     *
     * @param request       http request
     * @param paramName     parameter name
     * @param defaultValue  default parameter value if missing
     * @param typeConverter convert the String parameter value to target type
     * @return parameter value, or {@code defaultValue} if missing
     */
    protected static <T> T getSingleParameterOrDefault(BaseRequest request,
                                                       String paramName,
                                                       T defaultValue,
                                                       Function<String, T> typeConverter) {
        String value = request.getSingleParameter(paramName);
        return Optional.ofNullable(value).map(typeConverter).orElse(defaultValue);
    }

    protected static int getPageNum(BaseRequest request) {
        return getSingleParameterOrDefault(request, PAGE_NUM_KEY, DEFAULT_PAGE_NUM, value -> {
            int pn = NumberUtils.toInt(value, DEFAULT_PAGE_NUM);
            return pn <= 0 ? DEFAULT_PAGE_NUM : pn;
        });
    }

    protected static int getPageSize(BaseRequest request) {
        return getSingleParameterOrDefault(request, PAGE_SIZE_KEY, DEFAULT_PAGE_SIZE, value -> {
            int ps = NumberUtils.toInt(value, DEFAULT_PAGE_SIZE);
            return ps <= 0 ? DEFAULT_PAGE_SIZE : ps;
        });
    }

    public boolean redirectToLeader(BaseRequest request, BaseResponse response) throws DdlException {
        // Default max retry times is 3, in case of leader connect failed
        return redirectToLeaderWithRetry(request, response, 3);
    }
}
