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

package com.starrocks.scheduler.history;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Pair;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class TableKeeperTest {

    private static final List<Pair<String, String>> EXPECTED_COLUMNS = Lists.newArrayList(
            Pair.create("id", "bigint"), Pair.create("label", "bigint"), Pair.create("txn_id", "bigint"));

    private static final String ADD_TXN_ID = "ALTER TABLE _statistics_.t ADD COLUMN (`txn_id` bigint)";

    private static OlapTable table(String... columnNames) {
        List<Column> columns = Lists.newArrayList();
        for (String name : columnNames) {
            columns.add(new Column(name, IntegerType.BIGINT));
        }
        return new OlapTable(1L, "t", columns, KeysType.DUP_KEYS, new SinglePartitionInfo(),
                new HashDistributionInfo(3, Lists.newArrayList(columns.get(0))));
    }

    // A keeper bound to an in-memory table instead of the one in the metastore.
    private static class TestKeeper extends TableKeeper {
        private final OlapTable table;

        TestKeeper(OlapTable table, List<Pair<String, String>> expectedColumns) {
            super("_statistics_", "t", "CREATE TABLE t", null, expectedColumns);
            this.table = table;
        }

        // How many times the table was read under its lock, and whether it is held right now.
        private int lockedReads = 0;
        private boolean locked = false;

        @Override
        protected Optional<OlapTable> mayGetTable() {
            return Optional.ofNullable(table);
        }

        @Override
        protected <T> T withTableReadLock(OlapTable table, Supplier<T> reader) {
            lockedReads++;
            locked = true;
            try {
                return reader.get();
            } finally {
                locked = false;
            }
        }
    }

    // Records every DDL the keeper issues; fails each of them when asked to.
    private static List<String> captureDdl(boolean fail) {
        List<String> statements = Lists.newArrayList();
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDDL(String sql) {
                statements.add(sql);
                if (fail) {
                    throw new RuntimeException("alter rejected");
                }
            }
        };
        return statements;
    }

    @Test
    public void testIsSchemaReady() {
        Assertions.assertFalse(new TestKeeper(null, EXPECTED_COLUMNS).isSchemaReady());
        Assertions.assertFalse(new TestKeeper(table("id", "label"), EXPECTED_COLUMNS).isSchemaReady());

        OlapTable complete = table("id", "label", "txn_id");
        TableKeeper keeper = new TestKeeper(complete, EXPECTED_COLUMNS);
        Assertions.assertTrue(keeper.isSchemaReady());

        // A table under schema change already lists the columns being added, but they are
        // not usable until the job is done.
        complete.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertFalse(keeper.isSchemaReady());
        complete.setState(OlapTable.OlapTableState.NORMAL);
        Assertions.assertTrue(keeper.isSchemaReady());

        // A keeper that expects no particular columns only needs the table to exist.
        OlapTable altering = table("id");
        altering.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertTrue(new TestKeeper(altering, Lists.newArrayList()).isSchemaReady());
    }

    @Test
    public void testAddMissingColumns() {
        List<String> statements = captureDdl(false);

        new TestKeeper(table("id", "label", "txn_id"), EXPECTED_COLUMNS).addMissingColumns();
        new TestKeeper(null, EXPECTED_COLUMNS).addMissingColumns();
        new TestKeeper(table("id"), Lists.newArrayList()).addMissingColumns();
        Assertions.assertTrue(statements.isEmpty(), statements.toString());

        new TestKeeper(table("id", "label"), EXPECTED_COLUMNS).addMissingColumns();
        Assertions.assertEquals(Lists.newArrayList(ADD_TXN_ID), statements);
    }

    @Test
    public void testSchemaIsReadUnderTheTableLockAndAlteredOutsideIt() {
        List<Boolean> lockedAtDdl = Lists.newArrayList();
        TestKeeper keeper = new TestKeeper(table("id", "label"), EXPECTED_COLUMNS);
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDDL(String sql) {
                lockedAtDdl.add(keeper.locked);
            }
        };

        // State and columns are read in one locked section, so they describe the same moment.
        Assertions.assertFalse(keeper.isSchemaReady());
        Assertions.assertEquals(1, keeper.lockedReads);

        // The ALTER takes the table lock itself, so the read lock is released before it runs.
        keeper.addMissingColumns();
        Assertions.assertEquals(2, keeper.lockedReads);
        Assertions.assertEquals(Lists.newArrayList(false), lockedAtDdl);

        // No table, nothing to lock.
        TestKeeper absent = new TestKeeper(null, EXPECTED_COLUMNS);
        Assertions.assertFalse(absent.isSchemaReady());
        absent.addMissingColumns();
        Assertions.assertEquals(0, absent.lockedReads);
    }

    @Test
    public void testAddMissingColumnsWaitsForRunningAlter() {
        List<String> statements = captureDdl(false);
        OlapTable table = table("id", "label");
        TableKeeper keeper = new TestKeeper(table, EXPECTED_COLUMNS);

        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        keeper.addMissingColumns();
        keeper.addMissingColumns();
        Assertions.assertTrue(statements.isEmpty(), statements.toString());
        Assertions.assertFalse(keeper.isSchemaReady());

        table.setState(OlapTable.OlapTableState.NORMAL);
        keeper.addMissingColumns();
        Assertions.assertEquals(Lists.newArrayList(ADD_TXN_ID), statements);
    }

    @Test
    public void testAddMissingColumnsRetriesAfterAlterFailure() {
        List<String> statements = captureDdl(true);
        TableKeeper keeper = new TestKeeper(table("id", "label"), EXPECTED_COLUMNS);

        // A rejected ALTER neither escapes the keeper nor marks the schema ready, and the
        // next cycle issues it again.
        keeper.addMissingColumns();
        Assertions.assertFalse(keeper.isSchemaReady());
        keeper.addMissingColumns();
        Assertions.assertEquals(Lists.newArrayList(ADD_TXN_ID, ADD_TXN_ID), statements);
    }
}
