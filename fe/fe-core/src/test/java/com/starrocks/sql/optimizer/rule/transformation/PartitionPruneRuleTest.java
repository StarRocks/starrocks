// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
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

public class PartitionPruneRuleTest {
    @Test
    public void transform1(@Mocked OlapTable olapTable, @Mocked RangePartitionInfo partitionInfo) {
        FeConstants.runningUnitTest = true;
        Partition part1 = new Partition(1, "p1", null, null);
        Partition part2 = new Partition(2, "p2", null, null);
        Partition part3 = new Partition(3, "p3", null, null);
        Partition part4 = new Partition(4, "p4", null, null);
        Partition part5 = new Partition(5, "p5", null, null);

        List<Column> columns = Lists.newArrayList(
                new Column("dealDate", Type.DATE, false)
        );

        Map<Long, Range<PartitionKey>> keyRange = Maps.newHashMap();

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(new DateLiteral(2019, 11, 1), PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(new DateLiteral(2020, 2, 1), PrimitiveType.DATE);

        PartitionKey p3 = new PartitionKey();
        p3.pushColumn(new DateLiteral(2020, 5, 1), PrimitiveType.DATE);

        PartitionKey p4 = new PartitionKey();
        p4.pushColumn(new DateLiteral(2020, 8, 1), PrimitiveType.DATE);

        PartitionKey p5 = new PartitionKey();
        p5.pushColumn(new DateLiteral(2020, 11, 1), PrimitiveType.DATE);

        PartitionKey p6 = new PartitionKey();
        p6.pushColumn(new DateLiteral(2021, 2, 1), PrimitiveType.DATE);

        keyRange.put(1L, Range.closed(p1, p2));
        keyRange.put(2L, Range.closed(p2, p3));
        keyRange.put(3L, Range.closed(p3, p4));
        keyRange.put(4L, Range.closed(p4, p5));
        keyRange.put(5L, Range.closed(p5, p6));

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.RANGE;

                partitionInfo.getIdToRange(false);
                result = keyRange;

                partitionInfo.getPartitionColumns();
                result = columns;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2, part3, part4, part5);
                minTimes = 0;

                olapTable.getPartition(1);
                result = part1;
                minTimes = 0;
                olapTable.getPartition(2);
                result = part2;
                minTimes = 0;
                olapTable.getPartition(3);
                result = part3;
                olapTable.getPartition(4);
                result = part4;
                olapTable.getPartition(5);
                result = part5;
            }
        };

        // filters
        PartitionColumnFilter dealDateFilter = new PartitionColumnFilter();
        dealDateFilter.setLowerBound(new DateLiteral(2020, 6, 1), true);
        dealDateFilter.setUpperBound(new DateLiteral(2020, 12, 1), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("dealDate", dealDateFilter);

        LogicalOlapScanOperator operator = new LogicalOlapScanOperator(olapTable);
        operator.setColumnFilters(filters);

        PartitionPruneRule rule = new PartitionPruneRule();

        assertEquals(0, operator.getSelectedPartitionId().size());
        rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(3, operator.getSelectedPartitionId().size());
    }

    @Test
    public void transform2(@Mocked OlapTable olapTable, @Mocked RangePartitionInfo partitionInfo) {
        FeConstants.runningUnitTest = true;
        Partition part1 = new Partition(1, "p1", null, null);
        Partition part2 = new Partition(2, "p2", null, null);
        Partition part3 = new Partition(3, "p3", null, null);
        Partition part4 = new Partition(4, "p4", null, null);
        Partition part5 = new Partition(5, "p5", null, null);

        List<Column> columns = Lists.newArrayList(
                new Column("dealDate", Type.DATE, false),
                new Column("main_brand_id", Type.INT, false)
        );

        Map<Long, Range<PartitionKey>> keyRange = Maps.newHashMap();

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(new DateLiteral(2019, 11, 1), PrimitiveType.DATE);
        p1.pushColumn(new IntLiteral(100), PrimitiveType.INT);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(new DateLiteral(2020, 2, 1), PrimitiveType.DATE);
        p2.pushColumn(new IntLiteral(200), PrimitiveType.INT);

        PartitionKey p3 = new PartitionKey();
        p3.pushColumn(new DateLiteral(2020, 5, 1), PrimitiveType.DATE);
        p3.pushColumn(new IntLiteral(300), PrimitiveType.INT);

        PartitionKey p4 = new PartitionKey();
        p4.pushColumn(new DateLiteral(2020, 8, 1), PrimitiveType.DATE);
        p4.pushColumn(new IntLiteral(400), PrimitiveType.INT);

        PartitionKey p5 = new PartitionKey();
        p5.pushColumn(new DateLiteral(2020, 11, 1), PrimitiveType.DATE);
        p5.pushColumn(new IntLiteral(500), PrimitiveType.INT);

        PartitionKey p6 = new PartitionKey();
        p6.pushColumn(new DateLiteral(2021, 2, 1), PrimitiveType.DATE);
        p6.pushColumn(new IntLiteral(600), PrimitiveType.INT);

        keyRange.put(1L, Range.closed(p1, p2));
        keyRange.put(2L, Range.closed(p2, p3));
        keyRange.put(3L, Range.closed(p3, p4));
        keyRange.put(4L, Range.closed(p4, p5));
        keyRange.put(5L, Range.closed(p5, p6));

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.RANGE;

                partitionInfo.getIdToRange(false);
                result = keyRange;

                partitionInfo.getPartitionColumns();
                result = columns;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2, part3, part4, part5);
                minTimes = 0;

                olapTable.getPartition(1);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(2);
                result = part2;
                minTimes = 0;

                olapTable.getPartition(3);
                result = part3;
                minTimes = 0;

                olapTable.getPartition(4);
                result = part4;
                minTimes = 0;

                olapTable.getPartition(5);
                result = part5;
                minTimes = 0;
            }
        };

        // filters
        PartitionColumnFilter dealDateFilter = new PartitionColumnFilter();
        dealDateFilter.setLowerBound(new DateLiteral(2020, 8, 1), true);
        dealDateFilter.setUpperBound(new DateLiteral(2020, 12, 1), true);

        PartitionColumnFilter mainBrandFilter = new PartitionColumnFilter();
        mainBrandFilter.setLowerBound(new IntLiteral(150), true);
        mainBrandFilter.setUpperBound(new IntLiteral(150), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("dealDate", dealDateFilter);
        filters.put("main_brand_id", mainBrandFilter);

        LogicalOlapScanOperator operator = new LogicalOlapScanOperator(olapTable);
        operator.setColumnFilters(filters);

        PartitionPruneRule rule = new PartitionPruneRule();

        assertEquals(0, operator.getSelectedPartitionId().size());
        rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(3, operator.getSelectedPartitionId().size());
    }
}