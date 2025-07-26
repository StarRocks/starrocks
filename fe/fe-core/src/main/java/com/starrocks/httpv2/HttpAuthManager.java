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

package com.starrocks.httpv2;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;


public final class HttpAuthManager {

    private static final Logger LOG = LogManager.getLogger(HttpAuthManager.class);

    private static long SESSION_EXPIRE_TIME = 2; // hour

    private static long SESSION_MAX_SIZE = 100; // avoid to store too many

    private static HttpAuthManager instance = new HttpAuthManager();

    public static class SessionValue {
        public UserIdentity currentUser;
        public String password;
    }

    // session_id => session value
    private Cache<String, SessionValue> authSessions = CacheBuilder.newBuilder()
            .maximumSize(SESSION_MAX_SIZE)
            .expireAfterAccess(SESSION_EXPIRE_TIME, TimeUnit.HOURS)
            .build();

    private HttpAuthManager() {
        // do nothing
    }

    public static HttpAuthManager getInstance() {
        return instance;
    }

    public SessionValue getSessionValue(String sessionId) {
        return authSessions.getIfPresent(sessionId);
    }

    public void addSessionValue(String key, SessionValue value) {
        authSessions.put(key, value);
    }

    public void removeSession(String sessionId) {
        if (StringUtils.isNotBlank(sessionId)) {
            authSessions.invalidate(sessionId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Remove session: {} ", sessionId);
            }
        }
    }

    public Cache<String, SessionValue> getAuthSessions() {
        return authSessions;
    }
}
