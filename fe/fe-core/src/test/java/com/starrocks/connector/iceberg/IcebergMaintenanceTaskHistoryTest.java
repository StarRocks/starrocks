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
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class IcebergMaintenanceTaskHistoryTest {

    private static IcebergMaintenanceTaskRecord newRecord(String table) {
        IcebergMaintenanceTaskRecord record = IcebergMaintenanceTaskRecord.start(
                "cat", "db", table, IcebergMaintenanceTaskRecord.TRIGGER_REASON_SCHEDULE, null);
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        record.setStatus(IcebergMaintenanceTaskRecord.STATUS_SUCCESS);
        record.finish(stats);
        return record;
    }

    @Test
    public void testNewestFirstOrdering() {
        IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
        history.addRecord(newRecord("t1"));
        history.addRecord(newRecord("t2"));
        history.addRecord(newRecord("t3"));

        List<IcebergMaintenanceTaskRecord> records = history.getRecords();
        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals("t3", records.get(0).getTableName());
        Assertions.assertEquals("t1", records.get(2).getTableName());
    }

    @Test
    public void testMaxNumberEviction() {
        int oldMax = Config.iceberg_maintenance_task_history_max_number;
        try {
            Config.iceberg_maintenance_task_history_max_number = 3;
            IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
            for (int i = 0; i < 5; i++) {
                history.addRecord(newRecord("t" + i));
            }
            List<IcebergMaintenanceTaskRecord> records = history.getRecords();
            Assertions.assertEquals(3, records.size());
            // the newest three records survive
            Assertions.assertEquals("t4", records.get(0).getTableName());
            Assertions.assertEquals("t2", records.get(2).getTableName());
        } finally {
            Config.iceberg_maintenance_task_history_max_number = oldMax;
        }
    }

    @Test
    public void testTtlEviction() throws Exception {
        int oldTtl = Config.iceberg_maintenance_task_history_ttl_second;
        try {
            IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
            IcebergMaintenanceTaskRecord oldRecord = newRecord("t_old");
            history.addRecord(oldRecord);
            Thread.sleep(50);

            Config.iceberg_maintenance_task_history_ttl_second = 0;
            history.addRecord(newRecord("t_new"));

            boolean oldRetained = history.getRecords().stream()
                    .anyMatch(r -> r.getTaskId().equals(oldRecord.getTaskId()));
            Assertions.assertFalse(oldRetained);
        } finally {
            Config.iceberg_maintenance_task_history_ttl_second = oldTtl;
        }
    }

    @Test
    public void testTtlEvictionOnRead() throws Exception {
        int oldTtl = Config.iceberg_maintenance_task_history_ttl_second;
        try {
            IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
            history.addRecord(newRecord("t_old"));
            Thread.sleep(50);

            // No further insert happens; lowering the TTL must take effect on the next read.
            Config.iceberg_maintenance_task_history_ttl_second = 0;
            Assertions.assertTrue(history.getRecords().isEmpty());
        } finally {
            Config.iceberg_maintenance_task_history_ttl_second = oldTtl;
        }
    }

    @Test
    public void testConcurrentAdd() throws Exception {
        int oldMax = Config.iceberg_maintenance_task_history_max_number;
        try {
            Config.iceberg_maintenance_task_history_max_number = 100;
            IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
            int threads = 8;
            CountDownLatch latch = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                new Thread(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            history.addRecord(newRecord("t"));
                        }
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
            Assertions.assertEquals(100, history.size());
        } finally {
            Config.iceberg_maintenance_task_history_max_number = oldMax;
        }
    }

    @Test
    public void testRecordFields() {
        IcebergMaintenanceTaskRecord record = IcebergMaintenanceTaskRecord.start(
                "c1", "d1", "t1", IcebergMaintenanceTaskRecord.TRIGGER_REASON_MANUAL,
                "ALTER TABLE t1 EXECUTE expire_snapshots()");
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);
        stats.addOrphanDetected(2);
        stats.addOrphanRemoved(2, 42L);
        record.setStatus(IcebergMaintenanceTaskRecord.STATUS_SUCCESS);
        record.setFailureReason(null);
        record.finish(stats);

        Assertions.assertNotNull(record.getTaskId());
        Assertions.assertEquals("c1", record.getCatalogName());
        Assertions.assertEquals("d1", record.getDatabaseName());
        Assertions.assertEquals("t1", record.getTableName());
        Assertions.assertEquals("remove_orphan_files", record.getAction());
        Assertions.assertEquals("manual", record.getTriggerReason());
        Assertions.assertTrue(record.getEndTimeMs() >= record.getStartTimeMs());
        Assertions.assertEquals("success", record.getStatus());
        Assertions.assertTrue(record.getDetailsJson().contains("\"orphan_file_removed_count\":2"));
        Assertions.assertTrue(record.getDetailsJson().contains("\"orphan_bytes_removed\":42"));
    }

    @Test
    public void testFailureReasonTruncated() {
        IcebergMaintenanceTaskRecord record = IcebergMaintenanceTaskRecord.start(
                "c1", "d1", "t1", IcebergMaintenanceTaskRecord.TRIGGER_REASON_SCHEDULE, null);
        record.setFailureReason("x".repeat(10000));
        Assertions.assertEquals(4096, record.getFailureReason().length());
    }
}
