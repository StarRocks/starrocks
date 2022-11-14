// This file is made available under Elastic License 2.0.
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

    private PluginInfo pluginInfo;

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

                if (af.value().equals("Time")) {
                    queryTime = (long) f.get(event);
                }
                sb.append("|").append(af.value()).append("=").append(String.valueOf(f.get(event)));
            }

            String auditLog = sb.toString();
            AuditLog.getQueryAudit().log(auditLog);
            // slow query
            if (queryTime > Config.qe_slow_log_ms) {
                AuditLog.getSlowAudit().log(auditLog);
            }

            if (isBigQuery(event)) {
                sb.append("|cpuSecondThreshold=").append(Config.big_query_log_cpu_second_threshold);
                sb.append("|scanBytesThreshold=").append(Config.big_query_log_scan_bytes_threshold);
                sb.append("|scanRowsThreshold=").append(Config.big_query_log_scan_rows_threshold);
                String bigQueryLog = sb.toString();
                AuditLog.getBigQueryAudit().log(bigQueryLog);
            }
        } catch (Exception e) {
            LOG.debug("failed to process audit event", e);
        }
    }

    private boolean isBigQuery(AuditEvent event) {
        if (Config.enable_big_query_log) {
            if (Config.big_query_log_cpu_second_threshold >= 0 &&
                    event.cpuCostNs > Config.big_query_log_cpu_second_threshold * 1000000000L) {
                return true;
            }
            if (Config.big_query_log_scan_rows_threshold >= 0 &&
                    event.scanRows > Config.big_query_log_scan_rows_threshold) {
                return true;
            }
            if (Config.big_query_log_scan_bytes_threshold >= 0 &&
                    event.scanBytes > Config.big_query_log_scan_bytes_threshold) {
                return true;
            }
        }
        return false;
    }
}
