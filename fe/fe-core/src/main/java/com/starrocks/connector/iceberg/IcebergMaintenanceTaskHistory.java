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

package com.starrocks.connector.iceberg;

import com.starrocks.common.Config;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Bounded in-memory history of iceberg metadata maintenance tasks, newest first.
 * Lives on the leader FE only (both the auto maintenance daemon and manual
 * ALTER TABLE EXECUTE run on the leader); lost on restart or leader change.
 * Bounded by Config.iceberg_maintenance_task_history_max_number and
 * Config.iceberg_maintenance_task_history_ttl_second, enforced on both insert and read.
 */
public class IcebergMaintenanceTaskHistory {
    private final Deque<IcebergMaintenanceTaskRecord> history = new ArrayDeque<>();

    public synchronized void addRecord(IcebergMaintenanceTaskRecord record) {
        history.addFirst(record);
        evictUnlocked();
    }

    public synchronized List<IcebergMaintenanceTaskRecord> getRecords() {
        evictUnlocked();
        return new ArrayList<>(history);
    }

    public synchronized int size() {
        return history.size();
    }

    private void evictUnlocked() {
        int maxNumber = Math.max(1, Config.iceberg_maintenance_task_history_max_number);
        while (history.size() > maxNumber) {
            history.removeLast();
        }
        long cutoffMs = System.currentTimeMillis() - Config.iceberg_maintenance_task_history_ttl_second * 1000L;
        while (!history.isEmpty() && history.peekLast().getEndTimeMs() < cutoffMs) {
            history.removeLast();
        }
    }
}
