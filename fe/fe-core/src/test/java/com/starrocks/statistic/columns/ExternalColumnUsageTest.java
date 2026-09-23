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

        ExternalColumnUsage usage = ExternalColumnUsage.build(column, table, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(StatisticUtils.hashTableUuidForPkStorage("iceberg_catalog.db1.t1.uuid-1234"),
                usage.getTableUuidHash());
        Assertions.assertEquals("iceberg_catalog", usage.getCatalogName());
        Assertions.assertEquals("db1", usage.getDbName());
        Assertions.assertEquals("t1", usage.getTableName());
        Assertions.assertEquals("c1", usage.getColumnName());
        Assertions.assertEquals("predicate", usage.getUseCaseString());
    }

    @Test
    public void testBuildAcceptsLongAndMultibyteColumnNames() {
        // column_name is never part of the persisted PK directly (only its hash is), so its length
        // and encoding (e.g. multibyte CJK characters, where Java's String.length() undercounts the
        // actual UTF-8 byte size) can never make the PK exceed BE's primary_key_limit_size.
        IcebergTable table = mockTable("catalog.db.t", "catalog", "db", "t");

        Column longName = new Column("c".repeat(200), IntegerType.INT);
        ExternalColumnUsage longUsage = ExternalColumnUsage.build(longName, table, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(32, longUsage.getColumnNameHash().length());

        Column multibyteName = new Column("列".repeat(50), IntegerType.INT);
        ExternalColumnUsage multibyteUsage =
                ExternalColumnUsage.build(multibyteName, table, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals("列".repeat(50), multibyteUsage.getColumnName());
        Assertions.assertEquals(32, multibyteUsage.getColumnNameHash().length());
    }

    @Test
    public void testColumnNameHashIsDeterministicAndDistinguishesNames() {
        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c1",
                ColumnUsage.UseCase.PREDICATE);
        ExternalColumnUsage usage1Again = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c1",
                ColumnUsage.UseCase.JOIN);
        ExternalColumnUsage usage2 = new ExternalColumnUsage("hash1", "catalog", "db", "t", "c2",
                ColumnUsage.UseCase.PREDICATE);

        Assertions.assertEquals(usage1.getColumnNameHash(), usage1Again.getColumnNameHash());
        Assertions.assertNotEquals(usage1.getColumnNameHash(), usage2.getColumnNameHash());
        Assertions.assertEquals(32, usage1.getColumnNameHash().length());
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
