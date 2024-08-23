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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.UserIdentity;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlTokenManager implements AutoCloseable {

    private LoadingCache<String, ArrowFlightSqlTokenInfo> tokenCache;

    public ArrowFlightSqlTokenManager() {
        this.tokenCache =
                CacheBuilder.newBuilder()
                        .maximumSize(1024)
                        .expireAfterWrite(3600, TimeUnit.MINUTES)
                        .removalListener(new RemovalListener<String, ArrowFlightSqlTokenInfo>() {
                            @Override
                            public void onRemoval(
                                    RemovalNotification<String, ArrowFlightSqlTokenInfo> notification) {
                                ConnectContext context =
                                        ExecuteEnv.getInstance().getScheduler()
                                                .getContext(notification.getKey());
                                if (context != null) {
                                    ExecuteEnv.getInstance().getScheduler().registerConnection(context);
                                }
                            }
                        })
                        .build(new CacheLoader<String, ArrowFlightSqlTokenInfo>() {
                            @Override
                            public ArrowFlightSqlTokenInfo load(String key) throws Exception {
                                return new ArrowFlightSqlTokenInfo();
                            }
                        });
    }

    public String createToken(UserIdentity currentUser) {
        String token = UUID.randomUUID().toString();
        ArrowFlightSqlTokenInfo arrowFlightSqlTokenInfo = new ArrowFlightSqlTokenInfo();
        arrowFlightSqlTokenInfo.setCurrentUser(currentUser);
        tokenCache.put(token, arrowFlightSqlTokenInfo);
        return token;
    }

    public ArrowFlightSqlTokenInfo validateToken(String token) {
        try {
            return tokenCache.getUnchecked(token);
        } catch (CacheLoader.InvalidCacheLoadException ignored) {
            throw new IllegalArgumentException("Invalid Token");
        }
    }

    public void invalidateToken(String token) {
        tokenCache.invalidate(token);
    }

    @Override
    public void close() throws Exception {
        tokenCache.invalidateAll();
    }

}