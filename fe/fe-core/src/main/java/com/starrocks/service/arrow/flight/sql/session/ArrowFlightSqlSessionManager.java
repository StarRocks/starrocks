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

import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.arrow.flight.CallStatus;

public class ArrowFlightSqlSessionManager {

    private final ArrowFlightSqlTokenManager arrowFlightSqlTokenManager;

    public ArrowFlightSqlSessionManager(ArrowFlightSqlTokenManager arrowFlightSqlTokenManager) {
        this.arrowFlightSqlTokenManager = arrowFlightSqlTokenManager;
    }

    public ArrowFlightSqlConnectContext getConnectContext(String peerIdentity) {
        try {
            ArrowFlightSqlTokenInfo arrowFlightSqlTokenInfo =
                    arrowFlightSqlTokenManager.validateToken(peerIdentity);

            String token = arrowFlightSqlTokenInfo.getToken();
            ArrowFlightSqlConnectContext arrowFlightSqlConnectContext =
                    ExecuteEnv.getInstance().getScheduler().getArrowFlightSqlConnectContext(token);
            if (arrowFlightSqlConnectContext == null) {
                arrowFlightSqlConnectContext =
                        createArrowFlightSqlConnectContext(token, arrowFlightSqlTokenInfo);
            }
            return arrowFlightSqlConnectContext;
        } catch (Exception e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    private ArrowFlightSqlConnectContext createArrowFlightSqlConnectContext(String token,
                                                                            ArrowFlightSqlTokenInfo arrowFlightSqlTokenInfo) {
        UserIdentity currentUser = arrowFlightSqlTokenInfo.getCurrentUser();
        ArrowFlightSqlConnectContext ctx = new ArrowFlightSqlConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser(currentUser.getUser());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setRemoteIP(currentUser.getHost());
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setCurrentRoleIds(currentUser);
        ctx.setToken(token);

        Pair<Boolean, String> result = ExecuteEnv.getInstance().getScheduler().registerConnection(ctx);
        if (!result.first.booleanValue()) {
            ctx.getState().setError(result.second);
            throw new IllegalArgumentException(result.second);
        }

        return ctx;
    }

}
