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

import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.AuditField;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.plugin.AuditPlugin;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;

// A builtin Audit plugin, registered when FE start.
// it will receive "AFTER_QUERY" AuditEventy and print it as a log in fe.audit.log
public class AuditLogBuilder extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLogBuilder.class);

    private final PluginInfo pluginInfo;

    public AuditLogBuilder() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLogBuilder", PluginType.AUDIT,
                "builtin audit logger", DigitalVersion.fromString("0.12.0"),
                DigitalVersion.fromString("1.8.31"), AuditLogBuilder.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(EventType type) {
        return type == EventType.AFTER_QUERY;
    }

    @Override
    public void exec(AuditEvent event) {
        try {
            StringBuilder sb = new StringBuilder();
            long queryTime = 0;
            // get each field with annotation "AuditField" in AuditEvent
            // and assemble them into a string.
            Field[] fields = event.getClass().getFields();
            for (Field f : fields) {
                AuditField af = f.getAnnotation(AuditField.class);
                if (af == null) {
                    continue;
                }

                if (af.value().equals("Timestamp")) {
                    continue;
                }

                // fields related to big queries are not written into audit log by default,
                // they will be written into big query log.
                if (af.value().equals("BigQueryLogCPUSecondThreshold") ||
                        af.value().equals("BigQueryLogScanBytesThreshold") ||
                        af.value().equals("BigQueryLogScanRowsThreshold")) {
                    continue;
                }

                if (af.value().equals("Time")) {
                    queryTime = (long) f.get(event);
                }

                // Ignore -1 by default, ignore 0 if annotated with ignore_zero
                Object value = f.get(event);
                if (value instanceof Long) {
                    long longValue = (Long) value;
                    if (longValue == -1 || (longValue == 0 && af.ignore_zero())) {
                        continue;
                    }
                }
                if (value instanceof Integer) {
                    int intValue = (Integer) value;
                    if (intValue == -1 || (intValue == 0 && af.ignore_zero())) {
                        continue;
                    }
                }
                if (value instanceof Double) {
                    double doubleValue = (Double) value;
                    if (doubleValue == -1 || (doubleValue == 0 && af.ignore_zero())) {
                        continue;
                    }
                }
                sb.append("|").append(af.value()).append("=").append(value);
            }

            String auditLog = sb.toString();
            AuditLog.getQueryAudit().log(auditLog);
            // slow query
            if (queryTime > Config.qe_slow_log_ms) {
                AuditLog.getSlowAudit().log(auditLog);
            }

            if (isBigQuery(event)) {
                sb.append("|bigQueryLogCPUSecondThreshold=").append(event.bigQueryLogCPUSecondThreshold);
                sb.append("|bigQueryLogScanBytesThreshold=").append(event.bigQueryLogScanBytesThreshold);
                sb.append("|bigQueryLogScanRowsThreshold=").append(event.bigQueryLogScanRowsThreshold);
                String bigQueryLog = sb.toString();
                AuditLog.getBigQueryAudit().log(bigQueryLog);
            }
        } catch (Exception e) {
            LOG.debug("failed to process audit event", e);
        }
    }

    private boolean isBigQuery(AuditEvent event) {
        if (event.bigQueryLogCPUSecondThreshold >= 0 &&
                event.cpuCostNs > event.bigQueryLogCPUSecondThreshold * 1000000000L) {
            return true;
        }
        if (event.bigQueryLogScanBytesThreshold >= 0 && event.scanBytes > event.bigQueryLogScanBytesThreshold) {
            return true;
        }
        return event.bigQueryLogScanRowsThreshold >= 0 && event.scanRows > event.bigQueryLogScanRowsThreshold;
    }
}
