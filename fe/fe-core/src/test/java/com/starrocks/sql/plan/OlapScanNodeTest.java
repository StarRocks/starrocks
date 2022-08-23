// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OlapScanNodeTest {

    @Test
    public void transformForSingleItemListPartition(@Mocked TableRef tableRef,
                                                    @Mocked OlapTable olapTable,
                                                    @Mocked ListPartitionInfo partitionInfo)
            throws UserException {
        FeConstants.runningUnitTest = true;
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setRef(tableRef);
        tuple.setTable(olapTable);
        PartitionNames partitionNames = new PartitionNames(false, Lists.newArrayList("p1", "p2"));

        Partition part1 = new Partition(10001L, "p1", null, null);
        Partition part2 = new Partition(10002L, "p2", null, null);
        List<Partition> partitionList = Lists.newArrayList(part1, part2);

        List<LiteralExpr> p1 = Lists.newArrayList(
                new PartitionValue("guangdong").getValue(Type.STRING),
                new PartitionValue("shanghai").getValue(Type.STRING));

        List<LiteralExpr> p2 = Lists.newArrayList(
                new PartitionValue("beijing").getValue(Type.STRING),
                new PartitionValue("chongqing").getValue(Type.STRING));

        Map<Long, List<LiteralExpr>> literalExprValues = new HashMap<>();
        literalExprValues.put(10001L, p1);
        literalExprValues.put(10002L, p2);

        List<Column> partitionColumns = new ArrayList<>();
        partitionColumns.add(new Column("province", Type.STRING));

        new Expectations() {
            {
                tableRef.getPartitionNames();
                result = partitionNames;
                minTimes = 0;

                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.LIST;

                partitionInfo.getLiteralExprValues();
                result = literalExprValues;

                partitionInfo.getPartitionColumns();
                result = partitionColumns;
                minTimes = 0;

                olapTable.getPartitions();
                result = partitionList;
                minTimes = 0;

                olapTable.getPartition(10001L);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(10002L);
                result = part2;
                minTimes = 0;
            }
        };

        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(0), tuple, "null");

        Map<String, PartitionColumnFilter> columnFilters = new HashMap<>();
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();
        partitionColumnFilter.setLowerBound(new PartitionValue("chongqing").getValue(Type.STRING), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("chongqing").getValue(Type.STRING), true);
        columnFilters.put("province", partitionColumnFilter);
        olapScanNode.setColumnFilters(columnFilters);

        olapScanNode.init(analyzer);
    }

    @Test
    public void transformForMultiItemListPartition(@Mocked TableRef tableRef,
                                                   @Mocked OlapTable olapTable,
                                                   @Mocked ListPartitionInfo partitionInfo)
            throws UserException {
        FeConstants.runningUnitTest = true;
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setRef(tableRef);
        tuple.setTable(olapTable);
        PartitionNames partitionNames = new PartitionNames(false, Lists.newArrayList("p1", "p2"));

        Partition part1 = new Partition(10001L, "p1", null, null);
        Partition part2 = new Partition(10002L, "p2", null, null);
        List<Partition> partitionList = Lists.newArrayList(part1, part2);

        List<List<LiteralExpr>> p1 = new ArrayList<>();
        List<LiteralExpr> pItem1 = Lists.newArrayList(
                new PartitionValue("2022-04-01").getValue(Type.DATE),
                new PartitionValue("beijing").getValue(Type.STRING));
        p1.add(pItem1);

        List<List<LiteralExpr>> p2 = new ArrayList<>();
        List<LiteralExpr> pItem2 = Lists.newArrayList(
                new PartitionValue("2022-04-02").getValue(Type.DATE),
                new PartitionValue("shanghai").getValue(Type.STRING));
        p2.add(pItem2);

        Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = new HashMap<>();
        multiLiteralExprValues.put(10001L, p1);
        multiLiteralExprValues.put(10002L, p2);

        List<Column> partitionColumns = new ArrayList<>();
        partitionColumns.add(new Column("dt", Type.DATE));
        partitionColumns.add(new Column("province", Type.STRING));

        new Expectations() {
            {
                tableRef.getPartitionNames();
                result = partitionNames;
                minTimes = 0;

                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.LIST;

                partitionInfo.getMultiLiteralExprValues();
                result = multiLiteralExprValues;

                partitionInfo.getPartitionColumns();
                result = partitionColumns;
                minTimes = 0;

                olapTable.getPartitions();
                result = partitionList;
                minTimes = 0;

                olapTable.getPartition(10001L);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(10002L);
                result = part2;
                minTimes = 0;
            }
        };

        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(0), tuple, "null");

        Map<String, PartitionColumnFilter> columnFilters = new HashMap<>();
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();
        partitionColumnFilter.setLowerBound(new PartitionValue("2022-04-01").getValue(Type.DATE), true);
        partitionColumnFilter.setUpperBound(new PartitionValue("2022-04-01").getValue(Type.DATE), true);
        columnFilters.put("dt", partitionColumnFilter);
        olapScanNode.setColumnFilters(columnFilters);

        olapScanNode.init(analyzer);
    }
}
