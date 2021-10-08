// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HdfsFileFormat;
import com.starrocks.external.hive.HiveMetaClient;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HdfsScanNodeTest {
    @Mocked
    private Catalog catalog;
    @Mocked
    private HiveTable table;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private HiveMetaClient client;
    @Mocked
    private SystemInfoService infoService;

    private ImmutableMap<Long, Backend> idToBackends;
    private DescriptorTable descTable;
    private TupleDescriptor tupleDesc;

    @Before
    public void setUp() {
        Backend be = new Backend(0, "127.0.0.1", 9050);
        be.setAlive(true);
        idToBackends = ImmutableMap.of(0L, be);

        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor();
        tupleDesc.setTable(table);

        new Expectations() {
            {
                analyzer.getDescTbl();
                result = descTable;
                Catalog.getCurrentSystemInfo();
                result = infoService;
                infoService.getIdToBackend();
                result = idToBackends;
                client.getHdfsDataNodeIp(anyLong);
                result = "127.0.0.1";
                table.getName();
                result = "hive_table";
            }
        };
    }

    @Test
    public void testSimpleCase() throws UserException {
        List<Column> partitionCols = Lists.newArrayList();
        Column partitionCol = new Column("part_col", Type.INT);
        partitionCols.add(partitionCol);

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        PartitionKey partitionKey = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), partitionCols);
        partitionKeys.put(partitionKey, 0L);

        HdfsFileBlockDesc blockDesc = new HdfsFileBlockDesc(0, 100, new long[] {0}, null, client);
        HdfsFileDesc fileDesc = new HdfsFileDesc("/00000_0", "", 100, ImmutableList.of(blockDesc));
        HivePartition p0 = new HivePartition(HdfsFileFormat.PARQUET, ImmutableList.of(fileDesc), "path/part_col=1");

        new Expectations() {
            {
                analyzer.getUnassignedConjuncts((PlanNode) any);
                result = Lists.newArrayList();
                table.getPartitionColumns();
                result = partitionCols;
                table.getPartitionKeys();
                result = partitionKeys;
                table.getPartitions((List<PartitionKey>) any);
                result = Lists.newArrayList(p0);
                table.getExtrapolatedRowCount(anyLong);
                result = -1;
                table.getPartitionStatsRowCount((List<PartitionKey>) any);
                result = 10;
            }
        };

        HdfsScanNode scanNode = new HdfsScanNode(new PlanNodeId(0), tupleDesc, "HdfsScanNode");
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);

        // check scan range locations
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locations.size());
        Assert.assertEquals(1, scanNode.getNumInstances());

        // check stats
        Assert.assertEquals(10, scanNode.getCardinality());
        // 100 / 10
        Assert.assertEquals(10, scanNode.getAvgRowSize(), 0.1);
        Assert.assertEquals(1, scanNode.getNumNodes());
    }

    @Test
    public void testPartitionPrune() throws UserException {
        List<Column> partitionCols = Lists.newArrayList();
        Column partitionCol = new Column("part_col", Type.INT);
        partitionCols.add(partitionCol);

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        PartitionKey key1 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), partitionCols);
        PartitionKey key2 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("2")), partitionCols);
        PartitionKey key3 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), partitionCols);
        PartitionKey key4 = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("4")), partitionCols);
        partitionKeys.put(key1, 0L);
        partitionKeys.put(key2, 1L);
        partitionKeys.put(key3, 2L);
        partitionKeys.put(key4, 3L);

        HdfsFileBlockDesc blockDesc = new HdfsFileBlockDesc(0, 100, new long[] {0}, null, client);
        HdfsFileBlockDesc blockDesc1 = new HdfsFileBlockDesc(100, 100, new long[] {0}, null, client);
        HdfsFileDesc fileDesc = new HdfsFileDesc("/00000_0", "", 100, ImmutableList.of(blockDesc));
        HdfsFileDesc fileDesc1 = new HdfsFileDesc("/00000_1", "", 200, ImmutableList.of(blockDesc, blockDesc1));
        HivePartition p2 = new HivePartition(HdfsFileFormat.PARQUET, ImmutableList.of(fileDesc, fileDesc1),
                "path/part_col=2");
        HivePartition p3 = new HivePartition(HdfsFileFormat.PARQUET, ImmutableList.of(fileDesc), "path/part_col=3");

        SlotDescriptor partColSlotDesc = new SlotDescriptor(new SlotId(0), tupleDesc);
        partColSlotDesc.setColumn(partitionCol);
        tupleDesc.addSlot(partColSlotDesc);
        SlotRef partColSlotRef = new SlotRef(partColSlotDesc);
        // part_col > 1 and part_col <= 3
        Expr pred1 = new BinaryPredicate(BinaryPredicate.Operator.GT, partColSlotRef, new IntLiteral(1, Type.INT));
        Expr pred2 = new BinaryPredicate(BinaryPredicate.Operator.LE, partColSlotRef, new IntLiteral(3, Type.INT));

        new Expectations() {
            {
                analyzer.getUnassignedConjuncts((PlanNode) any);
                result = Lists.newArrayList(pred1, pred2);
                table.getPartitionColumns();
                result = partitionCols;
                table.getPartitionKeys();
                result = partitionKeys;
                table.getPartitions((List<PartitionKey>) any);
                result = Lists.newArrayList(p2, p3);
                table.getExtrapolatedRowCount(400);
                result = 50;
            }
        };

        HdfsScanNode scanNode = new HdfsScanNode(new PlanNodeId(0), tupleDesc, "HdfsScanNode");
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);

        // check scan range locations
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(4, locations.size());
        Assert.assertEquals(4, scanNode.getNumInstances());

        // check stats
        Assert.assertEquals(50, scanNode.getCardinality());
        // (100 + (100 + 200)) / 50
        Assert.assertEquals(8, scanNode.getAvgRowSize(), 0.1);
        String explainString = scanNode.getNodeExplainString("", TExplainLevel.VERBOSE);
        Assert.assertTrue(explainString.contains("partitions=2/4"));
        Assert.assertTrue(explainString.contains("cardinality=50"));
    }

    @Test
    public void testMinMaxConjuncts() throws UserException {
        List<Column> partitionCols = Lists.newArrayList();
        Column partitionCol = new Column("part_col", Type.INT);
        partitionCols.add(partitionCol);

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        PartitionKey partitionKey = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), partitionCols);
        partitionKeys.put(partitionKey, 0L);

        HdfsFileBlockDesc blockDesc = new HdfsFileBlockDesc(0, 100, new long[] {0}, null, client);
        HdfsFileDesc fileDesc = new HdfsFileDesc("/00000_0", "", 100, ImmutableList.of(blockDesc));
        HivePartition p0 = new HivePartition(HdfsFileFormat.PARQUET, ImmutableList.of(fileDesc), "path/part_col=1");

        SlotDescriptor partColSlotDesc = new SlotDescriptor(new SlotId(0), tupleDesc);
        partColSlotDesc.setColumn(partitionCol);
        tupleDesc.addSlot(partColSlotDesc);
        SlotRef partColSlotRef = new SlotRef(partColSlotDesc);
        Expr partColPred = new BinaryPredicate(BinaryPredicate.Operator.EQ, partColSlotRef,
                new IntLiteral(1, Type.INT));

        // add another column that is not partition column
        Column intCol = new Column("int_col", Type.INT);
        SlotDescriptor intColSlotDesc = new SlotDescriptor(new SlotId(1), tupleDesc);
        intColSlotDesc.setColumn(intCol);
        tupleDesc.addSlot(intColSlotDesc);
        SlotRef intColSlotRef = new SlotRef(intColSlotDesc);
        Expr intColPred = new BinaryPredicate(BinaryPredicate.Operator.EQ, intColSlotRef, new IntLiteral(2, Type.INT));

        new Expectations() {
            {
                analyzer.getUnassignedConjuncts((PlanNode) any);
                result = Lists.newArrayList(partColPred, intColPred);
                table.getPartitionColumns();
                result = partitionCols;
                table.getPartitionKeys();
                result = partitionKeys;
                table.getPartitions((List<PartitionKey>) any);
                result = Lists.newArrayList(p0);
                table.getExtrapolatedRowCount(anyLong);
                result = -1;
                table.getPartitionStatsRowCount((List<PartitionKey>) any);
                result = 10;
            }
        };

        HdfsScanNode scanNode = new HdfsScanNode(new PlanNodeId(0), tupleDesc, "HdfsScanNode");
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);

        Assert.assertTrue(scanNode.isVectorized());
        TPlanNode tPlanNode = new TPlanNode();
        scanNode.toThrift(tPlanNode);
        System.out.println(tPlanNode);

        // check result
        Assert.assertEquals(TPlanNodeType.HDFS_SCAN_NODE, tPlanNode.node_type);

        // partColPred has been removed after partition prune, so only intCol conjuncts | min max conjuncts left.
        // check partition conjuncts and conjuncts
        Assert.assertFalse(tPlanNode.hdfs_scan_node.isSetPartition_conjuncts());
        Assert.assertEquals(1, tPlanNode.conjuncts.size());

        // check min max conjuncts
        List<TExpr> minMaxConjuncts = tPlanNode.hdfs_scan_node.min_max_conjuncts;
        Assert.assertEquals(2, minMaxConjuncts.size());

        TExpr min = minMaxConjuncts.get(0);
        Assert.assertEquals(3, min.nodes.size());
        Assert.assertEquals(TExprNodeType.BINARY_PRED, min.nodes.get(0).node_type);
        Assert.assertEquals(TExprOpcode.GE, min.nodes.get(0).opcode);
        Assert.assertTrue(min.nodes.get(0).use_vectorized);
        Assert.assertEquals(TExprNodeType.SLOT_REF, min.nodes.get(1).node_type);
        Assert.assertEquals(TExprNodeType.INT_LITERAL, min.nodes.get(2).node_type);
        Assert.assertEquals(2, min.nodes.get(2).int_literal.value);

        TExpr max = minMaxConjuncts.get(1);
        Assert.assertEquals(3, max.nodes.size());
        Assert.assertEquals(TExprNodeType.BINARY_PRED, max.nodes.get(0).node_type);
        Assert.assertEquals(TExprOpcode.LE, max.nodes.get(0).opcode);
        Assert.assertTrue(min.nodes.get(0).use_vectorized);
        Assert.assertEquals(TExprNodeType.SLOT_REF, max.nodes.get(1).node_type);
        Assert.assertEquals(TExprNodeType.INT_LITERAL, max.nodes.get(2).node_type);
        Assert.assertEquals(2, max.nodes.get(2).int_literal.value);
    }
}