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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/http/StarRocksHttpTestCase.java

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

package com.starrocks.http;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class SslUtil {

    private static final Logger LOG = LogManager.getLogger(SslUtil.class);

    public static String[] filterCipherSuites(String[] defaults) {
        List<Pattern> whitelist = Config.sslCipherWhitelist.isEmpty()
                ? Collections.emptyList()
                : Arrays.stream(Config.sslCipherWhitelist.split("\\s*,\\s*"))
                .filter(s -> !s.isEmpty())
                .map(Pattern::compile)
                .toList();
        List<Pattern> blacklist = Config.sslCipherBlacklist.isEmpty()
                ? Collections.emptyList()
                : Arrays.stream(Config.sslCipherBlacklist.split("\\s*,\\s*"))
                .filter(s -> !s.isEmpty())
                .map(Pattern::compile)
                .toList();
        String[] filtered = Arrays.stream(defaults)
                .filter(Objects::nonNull)
                .filter(c -> whitelist.isEmpty() || whitelist.stream()
                        .anyMatch(p -> p.matcher(c).matches()))
                .filter(c -> blacklist.stream()
                        .noneMatch(p -> p.matcher(c).matches()))
                .toArray(String[]::new);

        if (filtered.length == 0) {
            LOG.warn("No cipher suites left, please check your whitelist and blacklist settings.");
        }
        return filtered;
    }
}
