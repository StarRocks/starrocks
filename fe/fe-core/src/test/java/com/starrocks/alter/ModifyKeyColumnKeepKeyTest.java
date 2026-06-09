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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for StarRocks/starrocks#74553.
 *
 * On DUPLICATE / UNIQUE tables the key set is fixed at table creation time and
 * cannot be changed by MODIFY COLUMN. Modifying only the comment (or only the
 * type) of an existing key column without restating the {@code KEY} keyword must
 * therefore keep it a key column, exactly as PRIMARY KEY and AGGREGATE KEY tables
 * already do. Before the fix the DUPLICATE / UNIQUE branches implicitly turned the
 * key column into a value column and assigned it an aggregation type, which then
 * failed with the misleading {@code "Can not change aggregation type"}.
 */
public class ModifyKeyColumnKeepKeyTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        starRocksAssert.dropDatabase("test");
    }

    @Test
    public void testModifyDuplicateKeyColumnCommentKeepsKey() throws Exception {
        starRocksAssert.withTable(
                "create table t_dup_keep_key (id int not null, name varchar(12) not null, v float not null)\n" +
                "DUPLICATE KEY(id, name)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "properties('replication_num' = '1');");

        // Change ONLY the comment of the key column `name`, keeping the exact same type
        // and omitting the KEY keyword. This used to fail with "Can not change aggregation type";
        // alterTable() propagates that failure, so reaching here without throwing is the assertion.
        starRocksAssert.alterTable(
                "alter table t_dup_keep_key modify column name varchar(12) comment 'full name'");
    }

    @Test
    public void testModifyUniqueKeyColumnCommentKeepsKey() throws Exception {
        starRocksAssert.withTable(
                "create table t_uniq_keep_key (id int not null, name varchar(12) not null, v float not null)\n" +
                "UNIQUE KEY(id, name)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "properties('replication_num' = '1');");

        starRocksAssert.alterTable(
                "alter table t_uniq_keep_key modify column name varchar(12) comment 'full name'");
    }

    /**
     * Restoring the omitted KEY attribute must also restore the key-only constraints that
     * {@code ColumnDefAnalyzer} would have enforced. Otherwise, because the clause is analyzed with
     * isKey=false and {@code Column#checkSchemaChangeAllowed} only rejects nullable -> non-nullable,
     * omitting NOT NULL on a non-nullable key column would silently relax it to nullable.
     */
    @Test
    public void testModifyKeyColumnOmittingNotNullKeepsNotNull() throws Exception {
        starRocksAssert.withTable(
                "create table t_keep_notnull (id int not null, name varchar(12) not null, v int not null)\n" +
                "DUPLICATE KEY(id, name)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "properties('replication_num' = '1');");

        // Omit both KEY and NOT NULL on the existing NOT NULL key column `name`.
        starRocksAssert.alterTable(
                "alter table t_keep_notnull modify column name varchar(20) comment 'full name'");
        waitForSchemaChangeDone();

        Column name = getColumn("t_keep_notnull", "name");
        assertTrue(name.isKey(), "key column `name` must remain a key column");
        assertFalse(name.isAllowNull(), "NOT NULL key column `name` must not be silently relaxed to nullable");
    }

    /**
     * Restoring the omitted KEY attribute must re-apply the key-column type check skipped during
     * analysis. Changing a key column to a floating-point type (a valid scalar schema change, but an
     * invalid key type) must be rejected instead of producing a floating-point key column.
     */
    @Test
    public void testModifyKeyColumnToFloatingPointTypeRejected() throws Exception {
        starRocksAssert.withTable(
                "create table t_float_key (k1 int not null, k2 int not null, v int not null)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "properties('replication_num' = '1');");

        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable("alter table t_float_key modify column k2 double not null"));
        String message = exception.getMessage().toLowerCase(Locale.ROOT);
        assertTrue(message.contains("invalid data type of key column"),
                "Expected key-type rejection in: " + exception.getMessage());
        assertTrue(message.contains("k2"),
                "Expected offending column name in: " + exception.getMessage());
    }

    /**
     * Even with the key-attribute restoration, a genuine aggregation-type change on
     * an AGGREGATE table value column still legitimately fails. The error message must
     * now carry context (column name + from->to) instead of the context-free original.
     */
    @Test
    public void testGenuineAggregationChangeHasActionableMessage() throws Exception {
        starRocksAssert.withTable(
                "create table t_agg_change (k int, v int sum)\n" +
                "AGGREGATE KEY(k)\n" +
                "DISTRIBUTED BY HASH(k) BUCKETS 1\n" +
                "properties('replication_num' = '1');");

        // The SCHEMA_CHANGE alter path catches StarRocksException and re-throws it as
        // AlterJobException carrying only the message (no cause), so assert on the
        // top-level message rather than walking the cause chain.
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_agg_change modify column v int max"));
        String message = exception.getMessage().toLowerCase(Locale.ROOT);
        assertTrue(message.contains("can not change aggregation type"),
                "Expected aggregation-type language in: " + exception.getMessage());
        // The column is reported through its shadow alias (e.g. __starrocks_shadow_v) during
        // schema change, so match the column suffix rather than an exact quoted name.
        assertTrue(message.contains("column '") && message.contains("v'"),
                "Expected offending column name in: " + exception.getMessage());
        assertTrue(message.contains("sum") && message.contains("max"),
                "Expected from->to (sum->max) in: " + exception.getMessage());
    }

    private static void waitForSchemaChangeDone() throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs =
                GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        for (AlterJobV2 alterJob : alterJobs.values()) {
            int retry = 0;
            while (!alterJob.getJobState().isFinalState() && retry++ < 120) {
                Thread.sleep(1000);
            }
            assertTrue(alterJob.getJobState() == AlterJobV2.JobState.FINISHED,
                    "schema change job did not finish: " + alterJob.getJobState());
        }
    }

    private static Column getColumn(String tableName, String columnName) {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", tableName);
        Column column = table.getColumn(columnName);
        assertNotNull(column, "column `" + columnName + "` not found on table `" + tableName + "`");
        return column;
    }
}
