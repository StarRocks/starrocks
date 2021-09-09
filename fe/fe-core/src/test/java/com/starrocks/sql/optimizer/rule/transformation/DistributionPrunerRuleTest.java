// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.starrocks.catalog.Type;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

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

        LogicalOlapScanOperator operator = new LogicalOlapScanOperator(olapTable);
        operator.setSelectedPartitionId(Lists.newArrayList(1L));
        operator.setSelectedIndexId(1);
        operator.setColumnFilters(filters);

        DistributionPruneRule rule = new DistributionPruneRule();

        assertEquals(0, operator.getSelectedTabletId().size());
        rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(20, operator.getSelectedTabletId().size());
    }
}