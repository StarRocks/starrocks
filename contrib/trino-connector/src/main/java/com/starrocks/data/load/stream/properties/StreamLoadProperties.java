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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/StarRocksFE.java

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

package com.starrocks.data.load.stream.properties;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamLoadProperties
        implements Serializable
{
    private final String jdbcUrl;
    private final String[] loadUrls;
    private final String username;
    private final String password;

    private final String labelPrefix;

    private final StreamLoadTableProperties tableProperties;

    /**
     * 最大缓存空间
     */
    private final long maxCacheBytes;

    /**
     * http client settings ms
     */
    private final int connectTimeout;

    private final Map<String, String> headers;

    private StreamLoadProperties(Builder builder)
    {
        this.jdbcUrl = builder.jdbcUrl;
        this.loadUrls = builder.loadUrls;
        this.username = builder.username;
        this.password = builder.password;
        this.labelPrefix = builder.labelPrefix;
        this.tableProperties = builder.tableProperties;
        this.maxCacheBytes = builder.maxCacheBytes;
        this.connectTimeout = builder.connectTimeout;
        this.headers = Collections.unmodifiableMap(builder.headers);
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String[] getLoadUrls()
    {
        return loadUrls;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getLabelPrefix()
    {
        return labelPrefix;
    }

    public StreamLoadTableProperties getTableProperties()
    {
        return tableProperties;
    }

    public long getMaxCacheBytes()
    {
        return maxCacheBytes;
    }

    public int getConnectTimeout()
    {
        return connectTimeout;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String jdbcUrl;
        private String[] loadUrls;
        private String username;
        private String password;
        private String labelPrefix = "";
        private long maxCacheBytes = (long) (Runtime.getRuntime().freeMemory() * 0.7);
        private StreamLoadTableProperties tableProperties;
        private int connectTimeout = 60000;
        private Map<String, String> headers = new HashMap<>();

        public Builder jdbcUrl(String jdbcUrl)
        {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder loadUrls(String... loadUrls)
        {
            this.loadUrls = loadUrls;
            return this;
        }

        public Builder cacheMaxBytes(long maxCacheBytes)
        {
            if (maxCacheBytes <= 0) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, must greater to 0");
            }
            if (maxCacheBytes > Runtime.getRuntime().maxMemory()) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, current maxMemory is " + Runtime.getRuntime().maxMemory());
            }
            this.maxCacheBytes = maxCacheBytes;
            return this;
        }

        public Builder connectTimeout(int connectTimeout)
        {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder username(String username)
        {
            this.username = username;
            return this;
        }

        public Builder password(String password)
        {
            this.password = password;
            return this;
        }

        public Builder labelPrefix(String labelPrefix)
        {
            this.labelPrefix = labelPrefix;
            return this;
        }

        public Builder tableProperties(StreamLoadTableProperties tableProperties)
        {
            this.tableProperties = tableProperties;
            return this;
        }

        public StreamLoadProperties build()
        {
            return new StreamLoadProperties(this);
        }
    }
}
