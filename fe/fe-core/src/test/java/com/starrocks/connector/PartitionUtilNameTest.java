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

package com.starrocks.connector;

import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionUtilNameTest {

    private static String name(LiteralExpr literal, PrimitiveType type) {
        PartitionKey key = new PartitionKey();
        key.pushColumn(literal, type);
        return PartitionUtil.generateMVPartitionName(key);
    }

    private static void assertDistinct(List<String> names) {
        Assertions.assertEquals(names.size(), names.stream().distinct().count(), names.toString());
        names.forEach(n -> Assertions.assertTrue(n.matches("p[a-zA-Z0-9_]*"), n));
    }

    @Test
    public void testSignedIntegersDoNotCollide() {
        List<String> names = Stream.of(-22L, -2L, -1L, 1L, 2L, 22L)
                .map(v -> name(new IntLiteral(v), PrimitiveType.INT))
                .collect(Collectors.toList());
        assertDistinct(names);
        // A value that strips to itself keeps the name it always had.
        Assertions.assertEquals("p1", name(new IntLiteral(1), PrimitiveType.INT));
    }

    @Test
    public void testStringsThatStripAlikeDoNotCollide() {
        List<String> names = Stream.of("a-b", "ab", "a_b", "a b", "a.b", "-1", "1")
                .map(v -> name(new StringLiteral(v), PrimitiveType.VARCHAR))
                .collect(Collectors.toList());
        assertDistinct(names);
        Assertions.assertEquals("pab", name(new StringLiteral("ab"), PrimitiveType.VARCHAR));
        Assertions.assertEquals("pa_b", name(new StringLiteral("a_b"), PrimitiveType.VARCHAR));
    }

    @Test
    public void testDateNamesAreUnchanged() throws Exception {
        // The names existing views already carry: a date, a datetime, and a string holding either.
        Assertions.assertEquals("p20240101",
                name(new DateLiteral(LocalDateTime.of(2024, 1, 1, 0, 0), Type.DATE), PrimitiveType.DATE));
        Assertions.assertEquals("p20240101103000",
                name(new DateLiteral(LocalDateTime.of(2024, 1, 1, 10, 30), Type.DATETIME), PrimitiveType.DATETIME));
        Assertions.assertEquals("p20240101", name(new StringLiteral("2024-01-01"), PrimitiveType.VARCHAR));
        Assertions.assertEquals("p20240101", name(new StringLiteral("2024/01/01"), PrimitiveType.VARCHAR));
        Assertions.assertEquals("p20240101103000",
                name(new StringLiteral("2024-01-01 10:30:00"), PrimitiveType.VARCHAR));
    }

    @Test
    public void testNameIsStable() {
        Assertions.assertEquals(name(new IntLiteral(-1), PrimitiveType.INT),
                name(new IntLiteral(-1), PrimitiveType.INT));
        Assertions.assertTrue(name(new IntLiteral(-1), PrimitiveType.INT).startsWith("p1_"));
    }

    private static HiveTable table(List<Column> columns) {
        return HiveTable.builder()
                .setTableName("partition_name_test")
                .setFullSchema(columns)
                .setPartitionColumnNames(columns.stream().map(Column::getName).collect(Collectors.toList()))
                .build();
    }

    @Test
    public void testRangeCellsKeepSignedIntegerPartitions() throws Exception {
        Column column = new Column("k", Type.INT);
        HiveTable table = table(List.of(column));
        Map<String, Range<PartitionKey>> cells = PartitionUtil.getRangePartitionMapOfExternalTable(table, column,
                List.of("k=-22", "k=-2", "k=-1", "k=1", "k=2", "k=22"), null);
        Assertions.assertEquals(6, cells.size());
        Assertions.assertEquals("-1", cells.get(name(new IntLiteral(-1), PrimitiveType.INT))
                .lowerEndpoint().getKeys().get(0).getStringValue());
    }

    @Test
    public void testListCellsKeepStringsThatStripAlike() throws Exception {
        Column column = new Column("k", Type.VARCHAR);
        HiveTable table = table(List.of(column));
        List<String> values = List.of("a-b", "ab", "a_b", "a b", "a.b");
        List<String> partitions = values.stream().map(v -> "k=" + v).collect(Collectors.toList());
        Map<String, PCell> cells = PartitionUtil.getMVPartitionToCells(table, List.of(column), partitions);
        Assertions.assertEquals(values.size(), cells.size());
        for (String value : values) {
            PListCell cell = (PListCell) cells.get(name(new StringLiteral(value), PrimitiveType.VARCHAR));
            Assertions.assertEquals(List.of(List.of(value)), cell.getPartitionItems());
        }
    }

    @Test
    public void testRangeCellsRejectResidualNameCollision() {
        Column column = new Column("k", Type.VARCHAR);
        HiveTable table = table(List.of(column));
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> PartitionUtil.getRangePartitionMapOfExternalTable(table, column,
                        List.of("k=2024-01-01", "k=20240101"), null));
        Assertions.assertTrue(error.getMessage().contains("2024-01-01"));
        Assertions.assertTrue(error.getMessage().contains("20240101"));
        Assertions.assertTrue(error.getMessage().contains("materialized view partition name p20240101"));
    }

    @Test
    public void testListCellsRejectResidualNameCollision() {
        Column first = new Column("first", Type.VARCHAR);
        Column second = new Column("second", Type.VARCHAR);
        List<Column> columns = List.of(first, second);
        HiveTable table = table(columns);
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> PartitionUtil.getMVPartitionToCells(table, columns,
                        List.of("first=a_b/second=c", "first=a/second=b_c")));
        Assertions.assertTrue(error.getMessage().contains("materialized view partition name pa_b_c"));
    }

    @Test
    public void testSameKeyFromDifferentBasePartitionsIsAllowed() throws Exception {
        Column column = new Column("k", Type.INT);
        Column other = new Column("other", Type.VARCHAR);
        HiveTable table = table(List.of(column, other));
        List<String> partitions = List.of("k=-1/other=a", "k=-1/other=b");
        Assertions.assertEquals(1,
                PartitionUtil.getRangePartitionMapOfExternalTable(table, column, partitions, null).size());
        Assertions.assertEquals(1, PartitionUtil.getMVPartitionToCells(table, List.of(column), partitions).size());
    }
}
