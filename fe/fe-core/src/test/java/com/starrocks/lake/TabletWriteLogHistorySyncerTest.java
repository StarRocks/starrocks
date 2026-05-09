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

package com.starrocks.lake;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.LocalMetastore;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TabletWriteLogHistorySyncerTest {

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
        FeConstants.runningUnitTest = false;
    }

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSyncDataWithSchemaMigrationTableNull() {
        AtomicReference<List<String>> executedDMLsRef = new AtomicReference<>(new ArrayList<>());

        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                executedDMLsRef.get().add(sql);
            }

            @Mock
            public void executeDDL(String sql) {
                // Should not be called when table is null
                Assertions.fail("executeDDL should not be called when table is null");
            }
        };

        // Mock LocalMetastore to return null table
        new MockUp<LocalMetastore>() {
            @Mock
            public Optional<com.starrocks.catalog.Table> mayGetTable(String dbName, String tableName) {
                return Optional.empty();
            }
        };

        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        syncer.syncData();

        // syncData should still execute DML even when table is null (ensureTableSchema returns early)
        Assertions.assertEquals(1, executedDMLsRef.get().size());
    }

    @Test
    public void testSyncDataWithSchemaMigrationAllColumnsExist() {
        AtomicReference<List<String>> executedDDLsRef = new AtomicReference<>(new ArrayList<>());
        AtomicReference<List<String>> executedDMLsRef = new AtomicReference<>(new ArrayList<>());

        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                executedDMLsRef.get().add(sql);
            }

            @Mock
            public void executeDDL(String sql) {
                executedDDLsRef.get().add(sql);
            }
        };

        // Mock table that already has all expected columns
        new MockUp<OlapTable>() {
            @Mock
            public Column getColumn(String name) {
                // All columns already exist
                if ("sst_input_files".equals(name) || "sst_input_bytes".equals(name) ||
                        "sst_output_files".equals(name) || "sst_output_bytes".equals(name)) {
                    return new Column();
                }
                return null;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Optional<com.starrocks.catalog.Table> mayGetTable(String dbName, String tableName) {
                return Optional.of(new OlapTable());
            }
        };

        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        syncer.syncData();

        // No ALTER TABLE should be executed since all columns exist
        Assertions.assertEquals(0, executedDDLsRef.get().size());
        // DML should still be executed
        Assertions.assertEquals(1, executedDMLsRef.get().size());
    }

    @Test
    public void testSyncDataWithSchemaMigrationMissingColumns() {
        AtomicReference<List<String>> executedDDLsRef = new AtomicReference<>(new ArrayList<>());
        AtomicReference<List<String>> executedDMLsRef = new AtomicReference<>(new ArrayList<>());

        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                executedDMLsRef.get().add(sql);
            }

            @Mock
            public void executeDDL(String sql) {
                executedDDLsRef.get().add(sql);
            }
        };

        // Mock table that is missing all SST columns
        new MockUp<OlapTable>() {
            @Mock
            public Column getColumn(String name) {
                // No SST columns exist
                return null;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Optional<com.starrocks.catalog.Table> mayGetTable(String dbName, String tableName) {
                return Optional.of(new OlapTable());
            }
        };

        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        syncer.syncData();

        // 4 ALTER TABLE statements should be executed for the 4 missing columns
        Assertions.assertEquals(4, executedDDLsRef.get().size());
        Assertions.assertTrue(executedDDLsRef.get().get(0).contains("sst_input_files"));
        Assertions.assertTrue(executedDDLsRef.get().get(1).contains("sst_input_bytes"));
        Assertions.assertTrue(executedDDLsRef.get().get(2).contains("sst_output_files"));
        Assertions.assertTrue(executedDDLsRef.get().get(3).contains("sst_output_bytes"));
        // DML should still be executed
        Assertions.assertEquals(1, executedDMLsRef.get().size());
    }

    @Test
    public void testSyncDataSchemaMigrationRunsOnlyOnce() {
        AtomicReference<List<String>> executedDDLsRef = new AtomicReference<>(new ArrayList<>());
        AtomicReference<List<String>> executedDMLsRef = new AtomicReference<>(new ArrayList<>());

        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                executedDMLsRef.get().add(sql);
            }

            @Mock
            public void executeDDL(String sql) {
                executedDDLsRef.get().add(sql);
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Column getColumn(String name) {
                return null;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Optional<com.starrocks.catalog.Table> mayGetTable(String dbName, String tableName) {
                return Optional.of(new OlapTable());
            }
        };

        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();

        // First call - should trigger schema migration
        syncer.syncData();
        Assertions.assertEquals(4, executedDDLsRef.get().size());
        Assertions.assertEquals(1, executedDMLsRef.get().size());

        // Second call - should NOT trigger schema migration again
        syncer.syncData();
        Assertions.assertEquals(4, executedDDLsRef.get().size()); // No new DDLs
        Assertions.assertEquals(2, executedDMLsRef.get().size()); // But DML should still run
    }

    @Test
    public void testSyncDataWithDDLException() {
        AtomicReference<List<String>> executedDDLsRef = new AtomicReference<>(new ArrayList<>());
        AtomicReference<List<String>> executedDMLsRef = new AtomicReference<>(new ArrayList<>());

        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                executedDMLsRef.get().add(sql);
            }

            @Mock
            public void executeDDL(String sql) {
                executedDDLsRef.get().add(sql);
                throw new RuntimeException("ALTER TABLE failed");
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Column getColumn(String name) {
                return null;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Optional<com.starrocks.catalog.Table> mayGetTable(String dbName, String tableName) {
                return Optional.of(new OlapTable());
            }
        };

        TabletWriteLogHistorySyncer syncer = new TabletWriteLogHistorySyncer();
        // Should not throw even when DDL fails
        syncer.syncData();

        // DDL was attempted for all 4 columns (each fails but continues)
        Assertions.assertEquals(4, executedDDLsRef.get().size());
        // DML should still execute after DDL failure
        Assertions.assertEquals(1, executedDMLsRef.get().size());
    }

    @Test
    public void testBuildSyncSql() {
        String sql = TabletWriteLogHistorySyncer.SQLBuilder.buildSyncSql();
        Assertions.assertTrue(sql.contains("sst_input_files"));
        Assertions.assertTrue(sql.contains("sst_input_bytes"));
        Assertions.assertTrue(sql.contains("sst_output_files"));
        Assertions.assertTrue(sql.contains("sst_output_bytes"));
        Assertions.assertTrue(sql.contains("be_tablet_write_log"));
        Assertions.assertTrue(sql.contains("INSERT INTO"));
    }

    @Test
    public void testCreateKeeper() {
        TableKeeper keeper = TabletWriteLogHistorySyncer.createKeeper();
        Assertions.assertNotNull(keeper);
    }
}
