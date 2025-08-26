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

import com.starrocks.catalog.UserIdentity;

/**
 * Represents a token and its associated user identity for an Arrow Flight SQL session.
 * <p>
 * This class is used to store and retrieve session-related information in the Arrow Flight SQL
 * token cache. Each token is associated with a specific user and is used to authenticate
 * and manage the session lifecycle.
 * <p>
 * The token is a unique identifier (UUID) generated at session initialization and is used
 * to retrieve the session context. The session is considered valid as long as the token
 * is present in the cache and not expired or evicted.
 * <p>
 * This class is immutable and thread-safe.
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Created during session initialization in {@link ArrowFlightSqlSessionManager}.</li>
 *   <li>Stored in a Guava {@code LoadingCache} with a configurable expiration time.</li>
 *   <li>Used to retrieve the {@code UserIdentity} and token string for session validation.</li>
 * </ul>
 *
 * <h2>Invalid Token</h2>
 * <p>
 * The {@link #createInvalidTokenInfo()} method returns a special instance with {@code null} values,
 * used as a fallback in the cache loader. This instance should not be used for valid session operations.
 *
 * @see ArrowFlightSqlSessionManager
 * @see com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext
 */
public class ArrowFlightSqlTokenInfo {

    private final UserIdentity currentUser;
    private final String token;

    /**
     * Creates an instance of {@code ArrowFlightSqlTokenInfo} with null values.
     * This is used as a fallback in the cache loader and should not be used in normal session operations.
     *
     * @return an invalid {@code ArrowFlightSqlTokenInfo} instance
     */
    public static ArrowFlightSqlTokenInfo createInvalidTokenInfo() {
        return new ArrowFlightSqlTokenInfo(null, null);
    }

    /**
     * Constructs a new {@code ArrowFlightSqlTokenInfo} with the given user identity and token.
     *
     * @param currentUser the user identity associated with the session
     * @param token       the unique token string used to identify the session
     */
    public ArrowFlightSqlTokenInfo(UserIdentity currentUser, String token) {
        this.currentUser = currentUser;
        this.token = token;
    }

    /**
     * Returns the token string associated with this session.
     *
     * @return the session token
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns the user identity associated with this session.
     *
     * @return the user identity
     */
    public UserIdentity getCurrentUser() {
        return currentUser;
    }
}
