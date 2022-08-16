// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import mockit.Expectations;
import mockit.Mocked;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class IcebergTableStatisticCalculatorTest {

    @Test
    public void testmakeTableStatistics(@Mocked Table iTable) {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        fields.add(Types.NestedField.of(2, false, "col2", new Types.DateType()));
        Schema schema = new Schema(fields);

        new Expectations() {
            {
                iTable.schema();
                result = schema;
            }
            {
                // empty iceberg's snapshot is null or snapshot is not null but no datafile.
                // so here mock iceberg table with null snapshot
                iTable.currentSnapshot();
                result = null;
            }
        };

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1000, Type.BIGINT, "col1", true);
        colRefToColumnMetaMap.put(columnRefOperator, new Column("col1", Type.BIGINT));
        Statistics statistics = IcebergTableStatisticCalculator.getTableStatistics(null, iTable, colRefToColumnMetaMap);
        Assert.assertTrue(statistics != null);
        statistics.getColumnStatistic(columnRefOperator);
    }
}
