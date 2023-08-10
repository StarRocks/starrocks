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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DistributionPrunerRuleTest {

    @Test
    public void transform(@Mocked OlapTable olapTable, @Mocked Partition partition, @Mocked MaterializedIndex index,
                          @Mocked HashDistributionInfo distributionInfo) {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(300);
        for (long i = 0; i < 300; i++) {
            tabletIds.add(i);
        }

        List<Column> columns = Lists.newArrayList(
                new Column("dealDate", Type.DATE, false),
                new Column("main_brand_id", Type.CHAR, false),
                new Column("item_third_cate_id", Type.CHAR, false),
                new Column("channel", Type.CHAR, false),
                new Column("shop_type", Type.CHAR, false)
        );

        // filters
        PartitionColumnFilter dealDateFilter = new PartitionColumnFilter();
        dealDateFilter.setLowerBound(new StringLiteral("2019-08-22"), true);
        dealDateFilter.setUpperBound(new StringLiteral("2019-08-22"), true);

        PartitionColumnFilter mainBrandFilter = new PartitionColumnFilter();
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1323"));
        inList.add(new StringLiteral("2528"));
        inList.add(new StringLiteral("9610"));
        inList.add(new StringLiteral("3893"));
        inList.add(new StringLiteral("6121"));
        mainBrandFilter.setInPredicate(new InPredicate(new SlotRef(null, "main_brand_id"), inList, false));

        PartitionColumnFilter itemThirdFilter = new PartitionColumnFilter();
        List<Expr> inList2 = Lists.newArrayList();
        inList2.add(new StringLiteral("9719"));
        inList2.add(new StringLiteral("11163"));
        itemThirdFilter.setInPredicate(new InPredicate(new SlotRef(null, "item_third_cate_id"), inList2, false));

        PartitionColumnFilter channelFilter = new PartitionColumnFilter();
        List<Expr> inList3 = Lists.newArrayList();
        inList3.add(new StringLiteral("1"));
        inList3.add(new StringLiteral("3"));
        channelFilter.setInPredicate(new InPredicate(new SlotRef(null, "channel"), inList3, false));

        PartitionColumnFilter shopTypeFilter = new PartitionColumnFilter();
        List<Expr> inList4 = Lists.newArrayList();
        inList4.add(new StringLiteral("2"));
        shopTypeFilter.setInPredicate(new InPredicate(new SlotRef(null, "shop_type"), inList4, false));

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("dealDate", dealDateFilter);
        filters.put("main_brand_id", mainBrandFilter);
        filters.put("item_third_cate_id", itemThirdFilter);
        filters.put("channel", channelFilter);
        filters.put("shop_type", shopTypeFilter);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();

        ColumnRefOperator column1 = columnRefFactory.create("dealDate", ScalarType.DATE, false);
        scanColumnMap.put(column1, new Column("dealDate", Type.DATE, false));
        ColumnRefOperator column2 = columnRefFactory.create("main_brand_id", ScalarType.CHAR, false);
        scanColumnMap.put(column2, new Column("main_brand_id", Type.CHAR, false));
        ColumnRefOperator column3 = columnRefFactory.create("item_third_cate_id", ScalarType.CHAR, false);
        scanColumnMap.put(column3, new Column("item_third_cate_id", Type.CHAR, false));
        ColumnRefOperator column4 = columnRefFactory.create("channel", ScalarType.CHAR, false);
        scanColumnMap.put(column4, new Column("channel", Type.CHAR, false));
        ColumnRefOperator column5 = columnRefFactory.create("shop_type", ScalarType.CHAR, false);
        scanColumnMap.put(column5, new Column("shop_type", Type.CHAR, false));

        BinaryPredicateOperator binaryPredicateOperator1 =
                new BinaryPredicateOperator(BinaryType.GE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2019, 8, 22, 0, 0, 0)));
        BinaryPredicateOperator binaryPredicateOperator2 =
                new BinaryPredicateOperator(BinaryType.LE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2019, 8, 22, 0, 0, 0)));

        InPredicateOperator inPredicateOperator1 = new InPredicateOperator(column2,
                ConstantOperator.createChar("1323"), ConstantOperator.createChar("2528"),
                ConstantOperator.createChar("9610"), ConstantOperator.createChar("3893"),
                ConstantOperator.createChar("6121"));

        InPredicateOperator inPredicateOperator2 = new InPredicateOperator(column3,
                ConstantOperator.createChar("9719"), ConstantOperator.createChar("11163"));

        InPredicateOperator inPredicateOperator3 =
                new InPredicateOperator(column4, ConstantOperator.createChar("1"), ConstantOperator.createChar("3"));

        InPredicateOperator inPredicateOperator4 = new InPredicateOperator(column5, ConstantOperator.createChar("2"));

        ScalarOperator predicate =
                Utils.compoundAnd(binaryPredicateOperator1, binaryPredicateOperator2, inPredicateOperator1,
                        inPredicateOperator2, inPredicateOperator3, inPredicateOperator4);
        LogicalOlapScanOperator operator =
                new LogicalOlapScanOperator(olapTable, scanColumnMap, Maps.newHashMap(), null, -1, predicate,
                        1, Lists.newArrayList(1L), null, false, Lists.newArrayList(), Lists.newArrayList(), null,
                        false);
        operator.setPredicate(predicate);

        new Expectations() {
            {
                olapTable.getPartition(anyLong);
                result = partition;

                partition.getIndex(anyLong);
                result = index;

                partition.getDistributionInfo();
                result = distributionInfo;

                index.getTabletIdsInOrder();
                result = tabletIds;

                distributionInfo.getDistributionColumns();
                result = columns;

                distributionInfo.getType();
                result = DistributionInfo.DistributionInfoType.HASH;

                distributionInfo.getBucketNum();
                result = tabletIds.size();
            }
        };

        DistributionPruneRule rule = new DistributionPruneRule();

        assertEquals(0, operator.getSelectedTabletId().size());
        OptExpression optExpression =
                rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), new ColumnRefFactory()))
                        .get(0);

        assertEquals(20, ((LogicalOlapScanOperator) optExpression.getOp()).getSelectedTabletId().size());

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
        LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator.Builder()
                .withOperator(olapScanOperator)
                .setSelectedTabletId(Lists.newArrayList(1L, 2L, 3L))
                .build();
        assertEquals(3, newScanOperator.getSelectedTabletId().size());
    }
}