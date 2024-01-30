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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class StarRocksWriteConfig
{
    static final int MAX_ALLOWED_QUERY_TIMEOUT = 259_200;
    static final int MIN_ALLOWED_QUERY_TIMEOUT = 1;

    private int queryTimeout = 3000;

    @Min(MIN_ALLOWED_QUERY_TIMEOUT)
    @Max(MAX_ALLOWED_QUERY_TIMEOUT)
    public int getQueryTimeout()
    {
        return queryTimeout;
    }

    @Config("write.query-timeout")
    @ConfigDescription("The query timeout duration in seconds")
    public StarRocksWriteConfig setQueryTimeout(int queryTimeout)
    {
        this.queryTimeout = queryTimeout;
        return this;
    }
}
