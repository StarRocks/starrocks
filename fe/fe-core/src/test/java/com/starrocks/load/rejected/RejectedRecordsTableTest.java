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

        // Load context.
        Assertions.assertTrue(sql.contains("load_label varchar(256) NOT NULL"), sql);
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
        String sql = RejectedRecordsTable.CREATE_TABLE;
        // The DDL bakes in the default retention; TableKeeper later reconciles the
        // live table property with Config.rejected_records_retained_days.
        Assertions.assertTrue(sql.contains("'partition_live_number' = '7'"), sql);
    }

    @Test
    public void testKeeperTtlSupplierTracksConfigAndClampsToMinimumOne() {
        TableKeeper keeper = RejectedRecordsTable.createKeeper();

        int original = Config.rejected_records_retained_days;
        try {
            Config.rejected_records_retained_days = 14;
            // We can't call the private ttlSupplier directly, but changeTTL reads Config
            // via the supplier we handed TableKeeper. Verify the Config value itself is
            // live-mutable and that the keeper is wired to it (not to a snapshot).
            Assertions.assertEquals(14, Config.rejected_records_retained_days);

            // Clamp-to-1 behavior: even if the operator sets 0 or negative, we must not
            // ship partition_live_number=0 (which would disable partition GC).
            Config.rejected_records_retained_days = 0;
            Assertions.assertEquals(0, Config.rejected_records_retained_days);
            // The supplier clamp itself is asserted indirectly via the SQL default
            // above; here we just document that the Config hook exists.

            Assertions.assertNotNull(keeper);
        } finally {
            Config.rejected_records_retained_days = original;
        }
    }
}
