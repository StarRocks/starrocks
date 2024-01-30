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

package io.trino.plugin.starrocks;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.List;

public class StarRocksConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private List<String> loadUrls = ImmutableList.of();
    private String labelPrefix = "trino-";
    private long maxCacheBytes = 268435456L;
    private int connectTimeout = 30000;
    private long chunkLimit = Long.MAX_VALUE;

    @NotNull
    @Size(min = 1)
    public List<String> getLoadUrls()
    {
        return loadUrls;
    }

    @Config("starrocks.client.load-url")
    public StarRocksConfig setLoadUrls(String commaSeparatedList)
    {
        this.loadUrls = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    public String getLabelPrefix()
    {
        return labelPrefix;
    }

    @Config("starrocks.client.label-prefix")
    public StarRocksConfig setLabelPrefix(String labelPrefix)
    {
        this.labelPrefix = labelPrefix;
        return this;
    }

    public long getMaxCacheBytes()
    {
        return maxCacheBytes;
    }

    @Config("starrocks.client.max-cache-bytes")
    public StarRocksConfig setMaxCacheBytes(long maxCacheBytes)
    {
        this.maxCacheBytes = maxCacheBytes;
        return this;
    }

    public int getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("starrocks.client.connect-timeout")
    public StarRocksConfig setConnectTimeout(int connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public long getChunkLimit()
    {
        return chunkLimit;
    }

    @Config("starrocks.client.chunk-limit")
    public StarRocksConfig setChunkLimit(long chunkLimit)
    {
        this.chunkLimit = chunkLimit;
        return this;
    }
}
