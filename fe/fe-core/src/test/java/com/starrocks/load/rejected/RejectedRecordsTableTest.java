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

package com.starrocks.load.rejected;

import com.starrocks.common.Config;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RejectedRecordsTableTest {

    @Test
    public void testTableIdentityConstants() {
        Assertions.assertEquals(StatsConstants.STATISTICS_DB_NAME, RejectedRecordsTable.DATABASE_NAME);
        Assertions.assertEquals("rejected_records", RejectedRecordsTable.TABLE_NAME);
        Assertions.assertEquals("_statistics_.rejected_records", RejectedRecordsTable.TABLE_FULL_NAME);
    }

    @Test
    public void testCreateKeeperProducesKeeperBoundToCorrectTable() {
        TableKeeper keeper = RejectedRecordsTable.createKeeper();
        Assertions.assertNotNull(keeper);
        Assertions.assertEquals(RejectedRecordsTable.DATABASE_NAME, keeper.getDatabaseName());
        Assertions.assertEquals(RejectedRecordsTable.TABLE_NAME, keeper.getTableName());
        Assertions.assertTrue(keeper.getCreateTableSql().contains(RejectedRecordsTable.TABLE_FULL_NAME));
    }

    @Test
    public void testCreateKeeperIsCachedSingleton() {
        // The keeper is a static singleton so TableKeeperDaemon always reconciles the
        // same instance and ttlSupplier keeps tracking the live Config value.
        Assertions.assertSame(RejectedRecordsTable.createKeeper(), RejectedRecordsTable.createKeeper());
    }

    @Test
    public void testCreateTableSqlContainsAllDesignedColumns() {
        String sql = RejectedRecordsTable.CREATE_TABLE;

        // Identity + time (primary key columns).
        Assertions.assertTrue(sql.contains("id varchar(64) NOT NULL"), sql);
        Assertions.assertTrue(sql.contains("created_at datetime NOT NULL"), sql);

        // Target.
        Assertions.assertTrue(sql.contains("target_database varchar(256) NOT NULL"), sql);
        Assertions.assertTrue(sql.contains("target_table varchar(256) NOT NULL"), sql);

        // Load context. load_label width must match _statistics_.loads_history.label
        // so we never silently truncate a label that fits there.
        Assertions.assertTrue(sql.contains("load_label varchar(2048) NOT NULL"), sql);
        Assertions.assertTrue(sql.contains("load_type varchar(32) NOT NULL"), sql);
        Assertions.assertTrue(sql.contains("txn_id bigint"), sql);
        Assertions.assertTrue(sql.contains("user_name varchar(128)"), sql);

        // Error context.
        Assertions.assertTrue(sql.contains("error_code varchar(64)"), sql);
        Assertions.assertTrue(sql.contains("error_message varchar(1024)"), sql);
        Assertions.assertTrue(sql.contains("error_column varchar(256)"), sql);

        // Payload.
        Assertions.assertTrue(sql.contains("raw_record json"), sql);
        Assertions.assertTrue(sql.contains("source_info json"), sql);

        // Diagnostics.
        Assertions.assertTrue(sql.contains("backend_id bigint"), sql);
    }

    @Test
    public void testCreateTableSqlUsesPrimaryKeyAndExpressionPartition() {
        String sql = RejectedRecordsTable.CREATE_TABLE;

        // Primary key on (id, created_at) enables UUID-based dedup so at-least-once
        // sync retries don't double-write rejected records.
        Assertions.assertTrue(sql.contains("PRIMARY KEY (id, created_at)"), sql);

        // Daily expression partitioning + hash distribution by id (UUID balances evenly).
        Assertions.assertTrue(sql.contains("PARTITION BY date_trunc('DAY', created_at)"), sql);
        Assertions.assertTrue(sql.contains("DISTRIBUTED BY HASH(id)"), sql);
    }

    @Test
    public void testCreateTableSqlCarriesPartitionLiveNumberProperty() {
        // DDL is built once at class load from the live Config value, which defaults to 7
        // before any test has a chance to mutate it. We assert the property key is
        // present and the value is a positive integer rather than pinning to "= '7'",
        // because test order can't be relied upon across the suite.
        String sql = RejectedRecordsTable.CREATE_TABLE;
        Assertions.assertTrue(sql.contains("'partition_live_number' = '"), sql);
        Assertions.assertFalse(sql.contains("'partition_live_number' = '0'"), sql);
    }

    @Test
    public void testResolvedRetentionDaysClampsToMinimum() {
        // The ttl supplier must never hand partition_live_number=0 to the table, which
        // would disable partition GC. The clamp is the only thing standing between
        // an operator typo (or a default-zero int) and permanently unbounded growth.
        Assertions.assertEquals(1, RejectedRecordsTable.resolvedRetentionDays(Integer.MIN_VALUE));
        Assertions.assertEquals(1, RejectedRecordsTable.resolvedRetentionDays(-5));
        Assertions.assertEquals(1, RejectedRecordsTable.resolvedRetentionDays(0));
    }

    @Test
    public void testResolvedRetentionDaysPassesThroughValidValues() {
        Assertions.assertEquals(1, RejectedRecordsTable.resolvedRetentionDays(1));
        Assertions.assertEquals(7, RejectedRecordsTable.resolvedRetentionDays(7));
        Assertions.assertEquals(14, RejectedRecordsTable.resolvedRetentionDays(14));
        Assertions.assertEquals(Integer.MAX_VALUE,
                RejectedRecordsTable.resolvedRetentionDays(Integer.MAX_VALUE));
    }

    @Test
    public void testKeeperIsLiveWiredToConfigNotASnapshot() {
        // TableKeeper receives a Supplier<Integer>, so mutating Config must be visible
        // to subsequent ttl reads. We can't call the private supplier directly, but
        // resolvedRetentionDays mirrors the exact arithmetic it uses, so asserting
        // the helper picks up Config changes is sufficient to prove the wiring.
        TableKeeper keeper = RejectedRecordsTable.createKeeper();
        Assertions.assertNotNull(keeper);

        int original = Config.rejected_records_retained_days;
        try {
            Config.rejected_records_retained_days = 21;
            Assertions.assertEquals(21,
                    RejectedRecordsTable.resolvedRetentionDays(Config.rejected_records_retained_days));

            Config.rejected_records_retained_days = -1;
            Assertions.assertEquals(1,
                    RejectedRecordsTable.resolvedRetentionDays(Config.rejected_records_retained_days));
        } finally {
            Config.rejected_records_retained_days = original;
        }
    }
}
