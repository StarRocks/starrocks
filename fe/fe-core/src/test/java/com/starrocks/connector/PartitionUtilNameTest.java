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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
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
    public void testDateNamesAreUnchanged() {
        // The names existing views already carry: a date, a datetime, and a string holding either.
        Assertions.assertEquals("p20240101",
                name(new DateLiteral(LocalDateTime.of(2024, 1, 1, 0, 0), DateType.DATE), PrimitiveType.DATE));
        Assertions.assertEquals("p20240101103000",
                name(new DateLiteral(LocalDateTime.of(2024, 1, 1, 10, 30), DateType.DATETIME), PrimitiveType.DATETIME));
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

    private static void mockPartitionColumns(List<Column> columns) {
        new MockUp<PartitionUtil>() {
            @Mock
            public List<Column> getPartitionColumns(Table table) {
                return columns;
            }
        };
    }

    @Test
    public void testSignedIntegerRangeCells(@Mocked HiveTable table, @Mocked JDBCTable jdbcTable)
            throws AnalysisException {
        Column column = new Column("k", IntegerType.INT);
        mockPartitionColumns(List.of(column));
        new Expectations() {
            {
                table.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;
                jdbcTable.getType();
                result = Table.TableType.JDBC;
                minTimes = 0;
            }
        };
        Assertions.assertEquals(2, PartitionUtil.getRangePartitionMapOfExternalTable(
                table, column, List.of("k=-1", "k=1"), null).size());
        Assertions.assertEquals(2, PartitionUtil.getRangePartitionMapOfJDBCTable(
                jdbcTable, column, List.of("-1", "1"), null).size());
    }

    @Test
    public void testListCellsKeepCollidingStringsApart(@Mocked HiveTable table) throws AnalysisException {
        Column column = new Column("k", TypeFactory.createVarcharType(32));
        mockPartitionColumns(List.of(column));
        new Expectations() {
            {
                table.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;
            }
        };
        Assertions.assertEquals(5, PartitionUtil.getMVPartitionToCells(table, List.of(column),
                List.of("k=a-b", "k=ab", "k=a_b", "k=a b", "k=a.b")).size());
    }

    @Test
    public void testResidualNameCollisionsFailWithContext(@Mocked HiveTable table) {
        Column column = new Column("k", TypeFactory.createVarcharType(32));
        mockPartitionColumns(List.of(column));
        new Expectations() {
            {
                table.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;
            }
        };
        List<String> partitions = List.of("k=2024-01-01", "k=20240101");
        AnalysisException rangeError = Assertions.assertThrows(AnalysisException.class,
                () -> PartitionUtil.getRangePartitionMapOfExternalTable(table, column, partitions, null));
        AnalysisException listError = Assertions.assertThrows(AnalysisException.class,
                () -> PartitionUtil.getMVPartitionToCells(table, List.of(column), partitions));
        for (AnalysisException error : List.of(rangeError, listError)) {
            Assertions.assertTrue(error.getMessage().contains("2024-01-01"), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains("20240101"), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains("p20240101"), error.getMessage());
        }
    }

    @Test
    public void testRepeatedProjectedValuesAreAllowed(@Mocked HiveTable table) throws AnalysisException {
        Column column = new Column("k", IntegerType.INT);
        Column other = new Column("other", TypeFactory.createVarcharType(32));
        mockPartitionColumns(List.of(column, other));
        new Expectations() {
            {
                table.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;
            }
        };
        List<String> partitions = List.of("k=1/other=x", "k=1/other=y");
        Assertions.assertEquals(1, PartitionUtil.getRangePartitionMapOfExternalTable(
                table, column, partitions, null).size());
        Assertions.assertEquals(1, PartitionUtil.getMVPartitionToCells(table, List.of(column), partitions).size());
    }
}
