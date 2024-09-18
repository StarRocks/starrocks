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
import com.starrocks.encryption.EncryptionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.UserIdentity;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlTokenManager implements AutoCloseable {

    private final String arrowFlightSqlAseKey;

    private LoadingCache<String, ArrowFlightSqlTokenInfo> tokenCache;

    private final SecureRandom generator = new SecureRandom();

    public ArrowFlightSqlTokenManager(String arrowFlightSqlAseKey) {
        this.arrowFlightSqlAseKey = arrowFlightSqlAseKey;
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
                                                .getArrowFlightSqlConnectContext(notification.getKey());
                                if (context != null) {
                                    ExecuteEnv.getInstance().getScheduler().unregisterConnection(context);
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

    public String createToken(UserIdentity currentUser) throws Exception {
        String token = new BigInteger(130, generator).toString(32);
        String encryptedToken = EncryptionUtil.aesEncrypt(token, arrowFlightSqlAseKey);
        ArrowFlightSqlTokenInfo arrowFlightSqlTokenInfo = new ArrowFlightSqlTokenInfo();
        arrowFlightSqlTokenInfo.setToken(token);
        arrowFlightSqlTokenInfo.setCurrentUser(currentUser);
        tokenCache.put(token, arrowFlightSqlTokenInfo);
        return encryptedToken;
    }

    public ArrowFlightSqlTokenInfo validateToken(String encryptedToken) {
        if (encryptedToken == null || encryptedToken.isEmpty()) {
            throw new IllegalArgumentException("Invalid Token");
        }

        try {
            String token = EncryptionUtil.aesDecrypt(encryptedToken, arrowFlightSqlAseKey);
            ArrowFlightSqlTokenInfo tokenInfo = tokenCache.getIfPresent(token);
            return tokenInfo;
        } catch (Exception e) {
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