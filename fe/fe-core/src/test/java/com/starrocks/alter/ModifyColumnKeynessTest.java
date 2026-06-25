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
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression test for issue #74553: MODIFY COLUMN on a key column of a DUPLICATE or UNIQUE table,
 * without restating the KEY keyword, used to fail with a misleading "Can not change aggregation type"
 * error. The omitted KEY keyword silently demoted the key column to a value column, which then
 * implicitly acquired an aggregation type and clashed with the original column's null aggregation type.
 *
 * <p>A MODIFY COLUMN must preserve the existing keyness of a column (a keyness flip is not a supported
 * MODIFY COLUMN operation), so modifying only the comment or type of a key column must succeed and keep
 * the column as a key. This already worked for PRIMARY KEY and AGGREGATE tables.
 */
public class ModifyColumnKeynessTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    private static Column baseColumn(String tableName, String columnName) {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getDb("test").getTable(tableName);
        return table.getBaseSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst().orElseThrow();
    }

    // The MODIFY COLUMN analysis (where the bug threw) runs synchronously inside alterTable(), while the
    // resulting schema-change job is applied asynchronously. Wait for outstanding jobs to reach a final
    // state before inspecting the post-alter base schema.
    private static void waitSchemaChangeJobDone() throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            int waitedMs = 0;
            while (!alterJobV2.getJobState().isFinalState()) {
                Thread.sleep(100);
                waitedMs += 100;
                if (waitedMs > 60000) {
                    fail("schema change job " + alterJobV2.getJobId() + " did not finish, state: "
                            + alterJobV2.getJobState());
                }
            }
        }
    }

    @Test
    public void testDuplicateModifyKeyColumnWithoutKeyword() throws Exception {
        starRocksAssert.withTable("create table dup_t (\n" +
                "  id   int          not null,\n" +
                "  name varchar(12)  not null,\n" +
                "  v    float        not null\n" +
                ")\n" +
                "duplicate key(id, name)\n" +
                "distributed by hash(id) buckets 1\n" +
                "properties('replication_num' = '1');");

        // Modify only the comment of the key column `name`, without restating KEY. Before the fix this
        // threw "Can not change aggregation type".
        assertDoesNotThrow(() ->
                starRocksAssert.alterTable("alter table dup_t modify column name varchar(12) comment 'full name';"));
        waitSchemaChangeJobDone();

        Column name = baseColumn("dup_t", "name");
        assertTrue(name.isKey(), "key column must stay a key after MODIFY COLUMN without the KEY keyword");
        assertNull(name.getAggregationType(), "a key column must not acquire an aggregation type");
    }

    @Test
    public void testUniqueModifyKeyColumnWithoutKeyword() throws Exception {
        starRocksAssert.withTable("create table uniq_t (\n" +
                "  id   int          not null,\n" +
                "  name varchar(12)  not null,\n" +
                "  v    int          not null\n" +
                ")\n" +
                "unique key(id, name)\n" +
                "distributed by hash(id) buckets 1\n" +
                "properties('replication_num' = '1');");

        assertDoesNotThrow(() ->
                starRocksAssert.alterTable("alter table uniq_t modify column name varchar(12) comment 'full name';"));
        waitSchemaChangeJobDone();

        Column name = baseColumn("uniq_t", "name");
        assertTrue(name.isKey(), "key column must stay a key after MODIFY COLUMN without the KEY keyword");
        assertNull(name.getAggregationType(), "a key column must not acquire an aggregation type");
    }

    // Guards against an over-broad fix: modifying a *value* column must still leave it a value column with
    // its model-specific aggregation type, not turn it into a key.
    @Test
    public void testDuplicateModifyValueColumnStaysValue() throws Exception {
        starRocksAssert.withTable("create table dup_val_t (\n" +
                "  id   int          not null,\n" +
                "  name varchar(12)  not null,\n" +
                "  v    float        not null\n" +
                ")\n" +
                "duplicate key(id, name)\n" +
                "distributed by hash(id) buckets 1\n" +
                "properties('replication_num' = '1');");

        assertDoesNotThrow(() ->
                starRocksAssert.alterTable("alter table dup_val_t modify column v double;"));
        waitSchemaChangeJobDone();

        Column v = baseColumn("dup_val_t", "v");
        assertFalse(v.isKey(), "value column must not become a key");
        assertTrue(v.getAggregationType() == AggregateType.NONE,
                "value column on a duplicate table keeps aggregation type NONE");
    }
}
