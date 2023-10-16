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


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;
import static org.junit.Assert.assertEquals;

public class PushDownMinMaxConjunctsRuleTest extends TableTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);
    }

    @Test
    public void transformIceberg(@Mocked IcebergTable table) {
        ExternalScanPartitionPruneRule rule0 = ExternalScanPartitionPruneRule.ICEBERG_SCAN;

        ColumnRefOperator colRef = new ColumnRefOperator(1, Type.INT, "id", true);
        Column col = new Column("id", Type.INT, true);
        PredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryType.EQ, colRef,
                ConstantOperator.createInt(1));

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        colRefToColumnMetaMap.put(colRef, col);
        columnMetaToColRefMap.put(col, colRef);
        OptExpression scan =
                new OptExpression(new LogicalIcebergScanOperator(table, colRefToColumnMetaMap, columnMetaToColRefMap,
                        -1, binaryPredicateOperator));

        assertEquals(0, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());

        rule0.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(2, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());

        PredicateOperator binaryPredicateOperatorNoPushDown = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, Type.INT, "id_noexist", true), ConstantOperator.createInt(1));

        OptExpression scanNoPushDown =
                new OptExpression(new LogicalIcebergScanOperator(table, colRefToColumnMetaMap, columnMetaToColRefMap,
                        -1, binaryPredicateOperatorNoPushDown));

        assertEquals(0,
                ((LogicalIcebergScanOperator) scanNoPushDown.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());

        rule0.transform(scanNoPushDown, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(0,
                ((LogicalIcebergScanOperator) scanNoPushDown.getOp()).getScanOperatorPredicates().getMinMaxConjuncts().size());
    }

    @Test
    public void testPartitionPrune() {
        ExternalScanPartitionPruneRule rule0 = ExternalScanPartitionPruneRule.ICEBERG_SCAN;

        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        ColumnRefOperator colRef1 = new ColumnRefOperator(1, Type.INT, "id", true);
        Column col1 = new Column("id", Type.INT, true);
        ColumnRefOperator colRef2 = new ColumnRefOperator(2, Type.STRING, "data", true);
        Column col2 = new Column("data", Type.STRING, true);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        colRefToColumnMetaMap.put(colRef1, col1);
        columnMetaToColRefMap.put(col2, colRef2);
        OptExpression scan =
                new OptExpression(new LogicalIcebergScanOperator(icebergTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                        -1, null));

        new MockUp<IcebergTable>() {
            @Mock
            public List<Column> getPartitionColumns() {
                return ImmutableList.of(new Column("data", STRING));
            }
        };

        rule0.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));
        assertEquals(1, ((LogicalIcebergScanOperator) scan.getOp()).getScanOperatorPredicates()
                .getSelectedPartitionIds().size());

    }
}
