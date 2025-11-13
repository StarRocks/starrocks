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

package com.starrocks.connector.gcp;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Instant;

public class TemporaryGCPAccessTokenProvider implements AccessTokenProvider {
    private static final Logger LOG = LogManager.getLogger(TemporaryGCPAccessTokenProvider.class);

    public static final String ACCESS_TOKEN_KEY = "fs.gs.temporary.access.token";
    public static final String TOKEN_EXPIRATION_KEY = "fs.gs.temporary.token.expiration";

    private Configuration config;
    private String accessToken;
    private volatile long expirationTimeMs = Long.MAX_VALUE;

    public TemporaryGCPAccessTokenProvider() {
        // Default constructor
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
        if (config != null) {
            String configToken = config.get(ACCESS_TOKEN_KEY);
            if (configToken != null && !configToken.isEmpty()) {
                this.accessToken = configToken;
            }

            this.expirationTimeMs = config.getLong(TOKEN_EXPIRATION_KEY, Long.MAX_VALUE);
        }
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public AccessTokenType getAccessTokenType() {
        return AccessTokenType.GENERIC;
    }

    @Override
    public AccessToken getAccessToken() {
        if (accessToken == null || accessToken.isEmpty()) {
            return null;
        }

        if (isTokenExpired()) {
            LOG.warn("GCP Access token has expired. Please refresh the token.");
            return null;
        }
        return new AccessToken(accessToken, Instant.ofEpochMilli(expirationTimeMs));
    }

    @Override
    public void refresh() throws IOException {
        // do nothing, as this is a temporary token provider
    }

    public boolean isTokenExpired() {
        return System.currentTimeMillis() >= expirationTimeMs;
    }
}