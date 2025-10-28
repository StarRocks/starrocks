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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/AuditLogBuilder.java

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

package com.starrocks.qe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class AuditInternalLog {
    private static final Logger LOG = LogManager.getLogger(AuditInternalLog.class);

    public enum InternalType {
        QUERY,
        DML,
    }


    public static void handleInternalLog(InternalType type, String queryId, String sql, Stopwatch watch) {
        if (Config.internal_log_json_format) {
            Map<String, Object> logMap = new HashMap<>();
            ObjectMapper objectMapper = new ObjectMapper();
            logMap.put("executeType", type.name());
            logMap.put("queryId", queryId);
            logMap.put("sql", sql);
            logMap.put("time", watch.elapsed().toMillis());
            try {
                AuditLog.getStatisticAudit().info(objectMapper.writeValueAsString(logMap));
            } catch (JsonProcessingException e) {
                LOG.error("Failed to write internal log", e);
            }
        } else {
            AuditLog.getStatisticAudit().info("statistic execute: {} | QueryId: [{}] | SQL: {}", type.name(),
                    queryId, sql);
        }

    }
}
