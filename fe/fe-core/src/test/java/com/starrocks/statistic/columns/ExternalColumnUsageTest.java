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

package com.starrocks.statistic.columns;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Optional;

class ExternalColumnUsageTest {

    private IcebergTable mockTable(String uuid, String catalog, String db, String table) {
        IcebergTable t = Mockito.mock(IcebergTable.class);
        Mockito.when(t.getUUID()).thenReturn(uuid);
        Mockito.when(t.getCatalogName()).thenReturn(catalog);
        Mockito.when(t.getCatalogDBName()).thenReturn(db);
        Mockito.when(t.getCatalogTableName()).thenReturn(table);
        Mockito.when(t.getName()).thenReturn(table);
        return t;
    }

    @Test
    public void testBuild() {
        IcebergTable table = mockTable("iceberg_catalog.db1.t1.uuid-1234", "iceberg_catalog", "db1", "t1");
        Column column = new Column("c1", IntegerType.INT);

        Optional<ExternalColumnUsage> mayUsage =
                ExternalColumnUsage.build(column, table, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertTrue(mayUsage.isPresent());
        ExternalColumnUsage usage = mayUsage.get();
        Assertions.assertEquals(StatisticUtils.hashTableUuidForPkStorage("iceberg_catalog.db1.t1.uuid-1234"),
                usage.getTableUuidHash());
        Assertions.assertEquals("iceberg_catalog", usage.getCatalogName());
        Assertions.assertEquals("db1", usage.getDbName());
        Assertions.assertEquals("t1", usage.getTableName());
        Assertions.assertEquals("c1", usage.getColumnName());
        Assertions.assertEquals("predicate", usage.getUseCaseString());
    }

    @Test
    public void testBuildSkipsOverlongColumnName() {
        IcebergTable table = mockTable("catalog.db.t", "catalog", "db", "t");
        String longName = "c".repeat(200);
        Column column = new Column(longName, IntegerType.INT);

        Optional<ExternalColumnUsage> mayUsage =
                ExternalColumnUsage.build(column, table, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertTrue(mayUsage.isEmpty());
    }

    @Test
    public void testColumnNameBoundaryStaysUnderBePrimaryKeyLimit() {
        // BE's primary_key_limit_size defaults to 128 bytes; with a 3-field VARCHAR PK
        // (fe_id, table_uuid, column_name) where column_name is last, the worst-case budget
        // (10-digit fe_id + 2, 32-char table_uuid + 2, column_name with no separator) allows up
        // to 82 bytes for column_name. MAX_COLUMN_NAME_LENGTH (80) must stay under that ceiling.
        IcebergTable table = mockTable("catalog.db.t", "catalog", "db", "t");

        Column atLimit = new Column("c".repeat(80), IntegerType.INT);
        Assertions.assertTrue(ExternalColumnUsage.build(atLimit, table, ColumnUsage.UseCase.PREDICATE).isPresent());

        Column overLimit = new Column("c".repeat(81), IntegerType.INT);
        Assertions.assertTrue(ExternalColumnUsage.build(overLimit, table, ColumnUsage.UseCase.PREDICATE).isEmpty());
    }

    @Test
    public void testEqualityIsByTableUuidHashAndColumnName() {
        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "catalogA", "dbA", "tA", "c1",
                ColumnUsage.UseCase.PREDICATE);
        ExternalColumnUsage usage2 = new ExternalColumnUsage("hash1", "catalogB", "dbB", "tB", "c1",
                ColumnUsage.UseCase.JOIN);
        Assertions.assertEquals(usage1, usage2);

        ExternalColumnUsage usage3 = new ExternalColumnUsage("hash1", "catalogA", "dbA", "tA", "c2",
                ColumnUsage.UseCase.PREDICATE);
        Assertions.assertNotEquals(usage1, usage3);
    }

    @Test
    public void testUseNowAndMerge() {
        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c1",
                ColumnUsage.UseCase.PREDICATE);
        usage1.setLastUsed(LocalDateTime.parse("2025-01-01T00:00:00"));

        ExternalColumnUsage usage2 = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c1",
                EnumSet.of(ColumnUsage.UseCase.JOIN));
        usage2.setLastUsed(LocalDateTime.parse("2025-01-02T00:00:00"));

        ExternalColumnUsage merged = usage1.merge(usage2);
        Assertions.assertEquals(EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN),
                merged.getUseCases());
        Assertions.assertEquals(LocalDateTime.parse("2025-01-02T00:00:00"), merged.getLastUsed());

        usage1.useNow(ColumnUsage.UseCase.GROUP_BY);
        Assertions.assertTrue(usage1.getUseCases().contains(ColumnUsage.UseCase.GROUP_BY));
    }

    @Test
    public void testNeedPersist() {
        ExternalColumnUsage usage = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c1",
                ColumnUsage.UseCase.PREDICATE);
        usage.setLastUsed(LocalDateTime.parse("2025-01-02T00:00:00"));
        Assertions.assertTrue(usage.needPersist(LocalDateTime.parse("2025-01-01T00:00:00")));
        Assertions.assertFalse(usage.needPersist(LocalDateTime.parse("2025-01-03T00:00:00")));
    }
}
