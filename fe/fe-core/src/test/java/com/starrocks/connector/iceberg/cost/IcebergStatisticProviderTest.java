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


package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergStatisticProviderTest extends TableTestBase {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
    }

    @Test
    public void testMakeTableStatistics() {
        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        table.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), table, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));

        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, null, colRefToColumnMetaMap);
        Assert.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testMakeTableStatisticsWithStructField() {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        fields.add(Types.NestedField.of(2, false, "col2", new Types.DateType()));

        List<Types.NestedField> structFields = new ArrayList<>();
        structFields.add(Types.NestedField.of(4, false, "col4", new Types.LongType()));
        structFields.add(Types.NestedField.of(5, false, "col5", new Types.DoubleType()));
        fields.add(Types.NestedField.of(3, false, "col3", Types.StructType.of(structFields)));

        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = fields.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));

        Map<Integer, ByteBuffer> bounds = Maps.newHashMap();
        bounds.put(1, ByteBuffer.allocate(8));
        bounds.put(2, ByteBuffer.allocate(8));
        bounds.put(4, ByteBuffer.allocate(8));
        bounds.put(5, ByteBuffer.allocate(8));

        Map<Integer, Object> result = IcebergFileStats.toMap(idToTypeMapping, bounds);
        Assert.assertNotNull(result);
    }
}
