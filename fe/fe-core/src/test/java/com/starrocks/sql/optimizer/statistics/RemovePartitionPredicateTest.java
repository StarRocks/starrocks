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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// The decision to drop partition conjuncts from a Delta Lake scan's selectivity follows the scan's own statistics,
// and only conjuncts the kernel scan prunes files with are dropped.
public class RemovePartitionPredicateTest {
    private static final Statistics TABLE_METADATA =
            Statistics.builder().setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
    private static final Statistics ANALYZE =
            Statistics.builder().setStatsSource(Statistics.StatsSource.ANALYZE).build();
    private static final Statistics NONE = Statistics.builder().build();

    private final StatisticsCalculator calculator = new StatisticsCalculator();
    private DeltaLakeTable table;
    private ColumnRefOperator id;
    private ColumnRefOperator dt;
    private ColumnRefOperator region;

    @BeforeEach
    public void setUp() {
        StructType schema = new StructType()
                .add("id", io.delta.kernel.types.IntegerType.INTEGER)
                .add("dt", io.delta.kernel.types.StringType.STRING)
                .add("Region", io.delta.kernel.types.StringType.STRING);
        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.when(metadata.getSchema()).thenReturn(schema);
        Mockito.when(metadata.getPartitionColNames()).thenReturn(Set.of("dt", "Region"));
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Mockito.when(snapshot.getMetadata()).thenReturn(metadata);

        Column idCol = new Column("id", IntegerType.INT, true);
        Column dtCol = new Column("dt", StringType.STRING, true);
        Column regionCol = new Column("Region", StringType.STRING, true);
        // a mutable list, as loaded by DeltaUtils, so a caller growing it would show up
        table = new DeltaLakeTable(1L, "delta_catalog", "delta_db", "t1", ImmutableList.of(idCol, dtCol, regionCol),
                new ArrayList<>(List.of("dt", "Region")), snapshot, null,
                new MetastoreTable("delta_db", "t1", "file:///tmp/delta/t1", 0));
        id = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        dt = new ColumnRefOperator(2, StringType.STRING, "dt", true);
        region = new ColumnRefOperator(3, StringType.STRING, "Region", true);
    }

    private LogicalDeltaLakeScanOperator scan(ScalarOperator predicate) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        for (ColumnRefOperator ref : List.of(id, dt, region)) {
            Column column = table.getColumn(ref.getName());
            refToColumn.put(ref, column);
            columnToRef.put(column, ref);
        }
        return new LogicalDeltaLakeScanOperator(table, refToColumn, columnToRef, Operator.DEFAULT_LIMIT, predicate);
    }

    private ScalarOperator remove(ScalarOperator predicate, Statistics statistics) {
        return calculator.removePartitionPredicate(predicate, scan(predicate), statistics);
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    @Test
    public void testPushedPartitionConjunctsAreRemoved() {
        ScalarOperator dtEq = eq(dt, ConstantOperator.createVarchar("2026-11-01"));
        ScalarOperator dtRange = new BinaryPredicateOperator(BinaryType.GE, dt,
                ConstantOperator.createVarchar("2026-10-01"));
        ScalarOperator dtIn = new InPredicateOperator(false, dt, ConstantOperator.createVarchar("a"),
                ConstantOperator.createVarchar("b"));
        // the partition column name is not lower case in the Delta log
        ScalarOperator regionEq = eq(region, ConstantOperator.createVarchar("us"));
        ScalarOperator idEq = eq(id, ConstantOperator.createInt(1));

        Assertions.assertEquals(ConstantOperator.TRUE,
                remove(Utils.compoundAnd(dtEq, dtRange, regionEq), TABLE_METADATA));
        // IN is not pushed down to the Delta kernel scan on this branch, so it is kept
        Assertions.assertEquals(dtIn, remove(Utils.compoundAnd(dtEq, dtIn), TABLE_METADATA));
        Assertions.assertEquals(idEq, remove(Utils.compoundAnd(dtEq, idEq), TABLE_METADATA));
        // the table's partition column list is not grown by the call
        Assertions.assertEquals(List.of("dt", "Region"), table.getPartitionColumnNames());
    }

    @Test
    public void testNotPushedPartitionConjunctsAreKept() {
        // a non-identity cast does not prune files, so the row count still holds the rows it filters out
        ScalarOperator castEq = eq(new CastOperator(IntegerType.INT, dt), ConstantOperator.createInt(20261101));
        Assertions.assertEquals(castEq, remove(castEq, TABLE_METADATA));

        // an IN list over the pushdown limit is not pushed down either
        List<ScalarOperator> args = new ArrayList<>();
        args.add(dt);
        for (int i = 0; i < 1001; i++) {
            args.add(ConstantOperator.createVarchar("v" + i));
        }
        ScalarOperator longIn = new InPredicateOperator(false, args);
        Assertions.assertEquals(longIn, remove(longIn, TABLE_METADATA));
    }

    @Test
    public void testFollowsScanStatsSource() {
        ScalarOperator dtEq = eq(dt, ConstantOperator.createVarchar("2026-11-01"));
        // ANALYZE statistics cover the whole table
        Assertions.assertEquals(dtEq, remove(dtEq, ANALYZE));
        // default statistics are not computed from the pruned files
        Assertions.assertEquals(dtEq, remove(dtEq, NONE));
        Assertions.assertEquals(dtEq, remove(dtEq, null));
        Assertions.assertEquals(ConstantOperator.TRUE, remove(dtEq, TABLE_METADATA));
    }

    @Test
    public void testNoSnapshotKeepsPredicate() {
        table.clearMetadata();
        ScalarOperator dtEq = eq(dt, ConstantOperator.createVarchar("2026-11-01"));
        Assertions.assertEquals(dtEq, remove(dtEq, TABLE_METADATA));
    }
}
