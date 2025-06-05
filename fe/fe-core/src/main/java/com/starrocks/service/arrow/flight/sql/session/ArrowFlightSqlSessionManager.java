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

package com.starrocks.service.arrow.flight.sql.session;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlSessionManager {

    private final LoadingCache<String, ArrowFlightSqlTokenInfo> tokenCache;

    public ArrowFlightSqlSessionManager() {
        this.tokenCache = CacheBuilder.newBuilder()
                .maximumSize(Math.min(Config.arrow_token_cache_size, Config.qe_max_connection))
                .expireAfterWrite(Config.arrow_token_cache_expire_second, TimeUnit.SECONDS)
                .removalListener((RemovalNotification<String, ArrowFlightSqlTokenInfo> notification) -> {
                    ArrowFlightSqlConnectContext context =
                            ExecuteEnv.getInstance().getScheduler().getArrowFlightSqlConnectContext(notification.getKey());
                    if (context != null) {
                        context.kill(true, "token is expired or evicted");
                    }
                })
                .build(new CacheLoader<>() {
                    @NotNull
                    @Override
                    public ArrowFlightSqlTokenInfo load(@NotNull String key) {
                        // Since we only use add, getIfPresent, expired and evicted function of cache,
                        // this method should never be invoked.
                        return ArrowFlightSqlTokenInfo.createInvalidTokenInfo();
                    }
                });
    }

    public String initializeSession(String username, String remoteIP, String password) {
        String token = UUIDUtil.genUUID().toString();
        ArrowFlightSqlConnectContext ctx = new ArrowFlightSqlConnectContext(token);
        ctx.setRemoteIP(remoteIP);

        try {
            AuthenticationHandler.authenticate(
                    ctx, username, remoteIP, password.getBytes(StandardCharsets.UTF_8));
        } catch (AuthenticationException e) {
            throw CallStatus.UNAUTHENTICATED
                    .withDescription("Access denied for user: " + username)
                    .withCause(e)
                    .toRuntimeException();
        }

        ArrowFlightSqlTokenInfo tokenInfo = new ArrowFlightSqlTokenInfo(ctx.getCurrentUserIdentity(), token);
        tokenCache.put(token, tokenInfo);

        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));

        // Assign connection ID
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        ctx.setConnectionId(connectScheduler.getNextConnectionId());
        ctx.resetConnectionStartTime();

        Pair<Boolean, String> isSuccessAndErrorMsg = connectScheduler.registerConnection(ctx);
        if (!isSuccessAndErrorMsg.first) {
            String errorMsg = isSuccessAndErrorMsg.second;
            ctx.getState().setError(errorMsg);
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("failed to register connection: " + errorMsg)
                    .toRuntimeException();
        }

        return token;
    }

    public void validateToken(String token) throws IllegalArgumentException {
        if (StringUtils.isEmpty(token)) {
            throw new IllegalArgumentException("bearer token is empty");
        }

        ArrowFlightSqlTokenInfo tokenInfo = tokenCache.getIfPresent(token);
        if (tokenInfo == null) {
            throw new IllegalArgumentException(String.format("invalid bearer token [%s], please try to reconnect. " +
                    "Maybe the token is expired or evicted, could modify fe.conf " +
                    "[arrow_token_cache_expire_second] and [arrow_token_cache_size]", token));
        }
    }

    public void closeSession(String token) {
        tokenCache.invalidate(token);
    }

    @NotNull
    public ArrowFlightSqlConnectContext validateAndGetConnectContext(String token) throws FlightRuntimeException {
        ArrowFlightSqlConnectContext connectContext =
                ExecuteEnv.getInstance().getScheduler().getArrowFlightSqlConnectContext(token);
        if (connectContext == null) {
            throw CallStatus.NOT_FOUND
                    .withDescription("cannot find connect arrow context of the token [" + token + "]")
                    .toRuntimeException();
        }
        return connectContext;
    }
}
