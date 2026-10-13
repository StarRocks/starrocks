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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.TestTables;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.StringType.STRING;

// Iceberg identity-partition eligibility for StatisticUtils#isDirectValuePartitionColumn -
// ExternalSampleStatisticsCollectJob routes exactly these columns through every partition instead
// of the sampled subset (see StatisticUtilsTest for the Hive/Hudi/Delta cases, which don't need the
// Iceberg test-table fixtures this class pulls in via TableTestBase).
public class DirectValuePartitionColumnTest extends TableTestBase {

    private IcebergTable wrap(TestTables.TestTable nativeTable, List<Column> columns) {
        return new IcebergTable(1, nativeTable.name(), "iceberg_catalog", "resource",
                "iceberg_db", nativeTable.name(), "", columns, nativeTable, Maps.newHashMap());
    }

    @Test
    public void testSingleColumnIdentityPartitionIsEligible() {
        // mockedNativeTableB: SCHEMA_B(k1 int, k2 int), SPEC_B = identity(k2)
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable table = wrap(mockedNativeTableB, columns);
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(table, "k2"));
    }

    @Test
    public void testCompositeIdentityPartitionColumnIsEligible() {
        // Composite identity spec: every row in a partition still shares the same value for each
        // individual identity field, so a marginal column within it now qualifies (unlike the older,
        // more conservative single-field-spec restriction).
        PartitionSpec compositeSpec = PartitionSpec.builderFor(SCHEMA_H).identity("k2").identity("k3").build();
        TestTables.TestTable compositeTable = create(SCHEMA_H, compositeSpec, "th_composite", 1);
        List<Column> columns = Lists.newArrayList(
                new Column("k1", STRING), new Column("k2", STRING), new Column("k3", STRING),
                new Column("k4", STRING), new Column("k5", STRING));
        IcebergTable table = wrap(compositeTable, columns);
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(table, "k2"));
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(table, "k3"));
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(table, "k1"));
    }

    @Test
    public void testNonIdentityTransformIsNotEligible() {
        // mockedNativeTableD: SCHEMA_D(k1 int, ts timestamptz), SPEC_D_5 = hour(ts)
        List<Column> columns = Lists.newArrayList(new Column("k1", INT));
        IcebergTable table = wrap(mockedNativeTableD, columns);
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(table, "ts"));
    }

    @Test
    public void testUnpartitionedTableIsNotEligible() {
        // mockedNativeTableH: SCHEMA_H, PartitionSpec.unpartitioned()
        List<Column> columns = Lists.newArrayList(new Column("k1", STRING));
        IcebergTable table = wrap(mockedNativeTableH, columns);
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(table, "k1"));
    }

    @Test
    public void testPartitionSpecEvolutionIsNotEligible() {
        // mockedNativeTableC: SCHEMA_B(k1 int, k2 int), SPEC_B = identity(k2), format version 2
        mockedNativeTableC.updateSpec().addField("k1").commit();
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable table = wrap(mockedNativeTableC, columns);
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(table, "k2"));
    }

    @Test
    public void testNonIcebergNonExplicitPartitionTableIsNotEligible() {
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(null, "k1"));
    }

    @Test
    public void testHivePartitionColumnIsUnconditionallyEligible() {
        new MockUp<Table>() {
            @Mock
            public boolean isHiveTable() {
                return true;
            }

            @Mock
            public List<String> getPartitionColumnNames() {
                return ImmutableList.of("dt");
            }
        };
        Table hiveTable = new Table(Table.TableType.HIVE);
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(hiveTable, "dt"));
        Assertions.assertFalse(StatisticUtils.isDirectValuePartitionColumn(hiveTable, "not_a_partition_col"));
    }

    @Test
    public void testHudiPartitionColumnIsUnconditionallyEligible() {
        new MockUp<Table>() {
            @Mock
            public boolean isHudiTable() {
                return true;
            }

            @Mock
            public List<String> getPartitionColumnNames() {
                return ImmutableList.of("dt");
            }
        };
        Table hudiTable = new Table(Table.TableType.HUDI);
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(hudiTable, "dt"));
    }

    @Test
    public void testDeltaLakePartitionColumnIsUnconditionallyEligible() {
        new MockUp<Table>() {
            @Mock
            public boolean isDeltalakeTable() {
                return true;
            }

            @Mock
            public List<String> getPartitionColumnNames() {
                return ImmutableList.of("dt");
            }
        };
        Table deltaTable = new Table(Table.TableType.DELTALAKE);
        Assertions.assertTrue(StatisticUtils.isDirectValuePartitionColumn(deltaTable, "dt"));
    }
}
