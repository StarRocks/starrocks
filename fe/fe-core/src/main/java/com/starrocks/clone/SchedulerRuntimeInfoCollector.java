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
package com.starrocks.clone;

import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.clone.DynamicPartitionScheduler.CREATE_PARTITION_MSG;
import static com.starrocks.clone.DynamicPartitionScheduler.DROP_PARTITION_MSG;
import static com.starrocks.clone.DynamicPartitionScheduler.DYNAMIC_PARTITION_STATE;
import static com.starrocks.clone.DynamicPartitionScheduler.LAST_SCHEDULER_TIME;
import static com.starrocks.clone.DynamicPartitionScheduler.LAST_UPDATE_TIME;

public class SchedulerRuntimeInfoCollector {
    private static final Logger LOG = LogManager.getLogger(SchedulerRuntimeInfoCollector.class);

    private static final String DEFAULT_RUNTIME_VALUE = FeConstants.NULL_STRING;

    // runtime information for dynamic partitions key -> <tableName -> value>
    private final Map<String, Map<String, String>> runtimeInfos = Maps.newConcurrentMap();

    public SchedulerRuntimeInfoCollector() {
    }

    public String getRuntimeInfo(String tableName, String key) {
        Map<String, String> tableRuntimeInfo = runtimeInfos.getOrDefault(tableName, createDefaultRuntimeInfo());
        return tableRuntimeInfo.getOrDefault(key, DEFAULT_RUNTIME_VALUE);
    }

    public void removeRuntimeInfo(String tableName) {
        runtimeInfos.remove(tableName);
    }

    public void createOrUpdateRuntimeInfo(String tableName, String key, String value) {
        Map<String, String> runtimeInfo = runtimeInfos.get(tableName);
        if (runtimeInfo == null) {
            runtimeInfo = createDefaultRuntimeInfo();
            runtimeInfo.put(key, value);
            runtimeInfos.put(tableName, runtimeInfo);
        } else {
            runtimeInfo.put(key, value);
        }
    }

    private Map<String, String> createDefaultRuntimeInfo() {
        Map<String, String> defaultRuntimeInfo = Maps.newConcurrentMap();
        defaultRuntimeInfo.put(LAST_UPDATE_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(LAST_SCHEDULER_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DYNAMIC_PARTITION_STATE, DynamicPartitionScheduler.State.NORMAL.toString());
        defaultRuntimeInfo.put(CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        return defaultRuntimeInfo;
    }

    public void recordCreatePartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic add partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, DynamicPartitionScheduler.State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, msg);
    }

    public void clearCreatePartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, DynamicPartitionScheduler.State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    public void recordDropPartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic drop partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, DynamicPartitionScheduler.State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, msg);
    }

    public void clearDropPartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, DynamicPartitionScheduler.State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }
}
