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

package com.starrocks.load;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TOpType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LoadPlannerTest {
    private long jobId;
    private long txnId;
    private TUniqueId loadId;
    private BrokerDesc brokerDesc;

    // config
    private int loadParallelInstanceNum;

    // backends
    private ImmutableMap<Long, Backend> idToBackend;

    private static ConnectContext ctx;
    private boolean strictMode = false;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private long timeoutS = 3600;
    private long startTime = 0;
    private boolean partialUpdate = false;
    private Map<String, String> sessionVariables = null;
    private long loadMemLimit = 1000000;
    private long execMemLimit = 1000000;


    @Mocked
    Partition partition;
    @Mocked
    OlapTableSink sink;

    @Before
    public void setUp() throws IOException {
        jobId = 1L;
        txnId = 2L;
        loadId = new TUniqueId(3, 4);
        brokerDesc = new BrokerDesc("broker0", null);

        loadParallelInstanceNum = Config.load_parallel_instance_num;
        Config.eliminate_shuffle_load_by_replicated_storage = false;

        // backends
        Map<Long, Backend> idToBackendTmp = Maps.newHashMap();
        Backend b1 = new Backend(0L, "host0", 9050);
        b1.setAlive(true);
        idToBackendTmp.put(0L, b1);
        Backend b2 = new Backend(1L, "host1", 9050);
        b2.setAlive(true);
        idToBackendTmp.put(1L, b2);
        idToBackend = ImmutableMap.copyOf(idToBackendTmp);
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @After
    public void tearDown() {
        Config.load_parallel_instance_num = loadParallelInstanceNum;
        Config.eliminate_shuffle_load_by_replicated_storage = true;
    }

    @Test
    public void testParallelInstance(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                                     @Injectable Database db, @Injectable OlapTable table) throws UserException {
        // table schema
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", Type.BIGINT, true);
        columns.add(c1);
        Column c2 = new Column("c2", Type.BIGINT, true);
        columns.add(c2);
        List<String> columnNames = Lists.newArrayList("c1", "c2");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("c1");
                result = columns.get(0);
                table.getColumn("c2");
                result = columns.get(1);
            }
        };

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2");
        DataDescription desc =
                new DataDescription("testTable", null, files, columnNames, null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 268435456, true));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 268435456, true));
        fileStatusesList.add(fileStatusList);

        // load_parallel_instance_num: 1
        Config.load_parallel_instance_num = 1;
        long startTime = System.currentTimeMillis();
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 2);

        planner.plan();
        Assert.assertEquals(1, planner.getScanNodes().size());
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(2, locationsList.size());

        // load_parallel_instance_num: 2
        Config.load_parallel_instance_num = 2;
        planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 2);
        planner.plan();
        scanNode = (FileScanNode) planner.getScanNodes().get(0);
        locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(4, locationsList.size());

        // load_parallel_instance_num: 2, pipeline
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        Config.enable_pipeline_load = true;
        Config.load_parallel_instance_num = 2;
        planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 2);

        planner.plan();
        scanNode = (FileScanNode) planner.getScanNodes().get(0);
        locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(4, locationsList.size());
        Assert.assertEquals(2, planner.getFragments().get(0).getPipelineDop());
        Assert.assertEquals(1, planner.getFragments().get(0).getParallelExecNum());
    }

    @Test
    public void testVectorizedLoad(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                                   @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.TINYINT, true, null, true, null, ""));
        columns.add(new Column("k2", Type.INT, true, null, false, null, ""));
        columns.add(new Column("k3", ScalarType.createVarchar(50), true, null, true, null, ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, false, null, ""));

        Function f1 = new Function(new FunctionName(FunctionSet.SUBSTR), new Type[] {Type.VARCHAR, Type.INT, Type.INT},
                Type.VARCHAR, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("k1");
                result = columns.get(0);
                table.getColumn("k2");
                result = columns.get(1);
                table.getColumn("k3");
                result = columns.get(2);
                table.getColumn("v");
                result = columns.get(3);
                table.getColumn("k33");
                result = null;
                globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                returns(f1, f1, f2);
            }
        };

        // column mappings
        String sql = "LOAD LABEL label0 (DATA INFILE('path/k2=1/file1') INTO TABLE t2 FORMAT AS 'orc' (k1,k33,v) " +
                "COLUMNS FROM PATH AS (k2) set (k3 = substr(k33,1,5))) WITH BROKER 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("path/k2=1/file1");
        List<String> columnNames = Lists.newArrayList("k1", "k33", "v");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "ORC", Lists.newArrayList("k2"),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "ORC");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("path/k2=1/file1", false, 268435456, true));
        fileStatusesList.add(fileStatusList);

        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 1. check fragment
        List<PlanFragment> fragments = planner.getFragments();
        Assert.assertEquals(1, fragments.size());
        PlanFragment fragment = fragments.get(0);
        TPlanFragment tPlanFragment = fragment.toThrift();
        List<TPlanNode> nodes = tPlanFragment.plan.nodes;
        Assert.assertEquals(1, nodes.size());
        TPlanNode tPlanNode = nodes.get(0);
        Assert.assertEquals(TPlanNodeType.FILE_SCAN_NODE, tPlanNode.node_type);

        // 2. check scan node column expr
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locationsList.size());
        TScanRangeLocations location = locationsList.get(0);
        TBrokerScanRangeParams params = location.scan_range.broker_scan_range.params;
        Map<Integer, TExpr> exprOfDestSlot = params.expr_of_dest_slot;

        // 2.1 check k1
        TExpr k1Expr = exprOfDestSlot.get(0);
        Assert.assertEquals(1, k1Expr.nodes.size());
        TExprNode node = k1Expr.nodes.get(0);
        Assert.assertEquals(TExprNodeType.SLOT_REF, node.node_type);
        Assert.assertEquals(TPrimitiveType.TINYINT, node.type.types.get(0).scalar_type.type);

        // 2.2 check k2 from path
        TExpr k2Expr = exprOfDestSlot.get(1);
        Assert.assertEquals(2, k2Expr.nodes.size());
        TExprNode castNode = k2Expr.nodes.get(0);
        Assert.assertEquals(TExprNodeType.CAST_EXPR, castNode.node_type);
        Assert.assertEquals(TPrimitiveType.INT, castNode.fn.ret_type.types.get(0).scalar_type.type);
        node = k2Expr.nodes.get(1);
        Assert.assertEquals(TExprNodeType.SLOT_REF, node.node_type);
        Assert.assertEquals(TPrimitiveType.VARCHAR, node.type.types.get(0).scalar_type.type);

        // 2.3 check k3 mapping
        TExpr k3Expr = exprOfDestSlot.get(2);
        Assert.assertEquals(4, k3Expr.nodes.size());
        node = k3Expr.nodes.get(0);
        Assert.assertEquals(TExprNodeType.FUNCTION_CALL, node.node_type);
        Assert.assertEquals("substr", node.fn.name.function_name);
        node = k3Expr.nodes.get(1);
        Assert.assertEquals(TExprNodeType.SLOT_REF, node.node_type);
        Assert.assertEquals(TPrimitiveType.VARCHAR, node.type.types.get(0).scalar_type.type);
        node = k3Expr.nodes.get(2);
        Assert.assertEquals(TExprNodeType.INT_LITERAL, node.node_type);
        Assert.assertEquals(1, node.int_literal.value);
        node = k3Expr.nodes.get(3);
        Assert.assertEquals(TExprNodeType.INT_LITERAL, node.node_type);
        Assert.assertEquals(5, node.int_literal.value);
    }

    @Test
    public void testPartialUpdatePlan(@Mocked GlobalStateMgr globalStateMgr,
                                      @Mocked SystemInfoService systemInfoService,
                                      @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.TINYINT, true, null, true, null, ""));
        columns.add(new Column("k2", Type.INT, true, null, false, null, ""));
        columns.add(new Column("k3", ScalarType.createVarchar(50), true, null, true, null, ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, false, null, ""));

        Function f1 = new Function(new FunctionName(FunctionSet.SUBSTR), new Type[] {Type.VARCHAR, Type.INT, Type.INT},
                Type.VARCHAR, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                minTimes = 0;
                result = KeysType.PRIMARY_KEYS;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("k1");
                result = columns.get(0);
                table.getColumn("k2");
                result = columns.get(1);
                table.getColumn("k3");
                result = columns.get(2);
                table.getColumn("v");
                result = columns.get(3);
                table.getColumn("k33");
                result = null;
                globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                returns(f1, f1, f2);
                table.getColumn(Load.LOAD_OP_COLUMN);
                minTimes = 0;
                result = null;
            }
        };

        // column mappings
        String sql = "LOAD LABEL label0 (DATA INFILE('path/k2=1/file1') INTO TABLE t2 FORMAT AS 'orc' (k1,k33,v) " +
                "COLUMNS FROM PATH AS (k2) set (k3 = substr(k33,1,5))) WITH BROKER 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("path/k2=1/file1");
        List<String> columnNames = Lists.newArrayList("k1", "k33", "v");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "ORC", Lists.newArrayList("k2"),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "ORC");
        brokerFileGroup.parse(db, desc);
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("path/k2=1/file1", false, 268435456, true));
        fileStatusesList.add(fileStatusList);

        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 1. check fragment
        List<PlanFragment> fragments = planner.getFragments();
        Assert.assertEquals(1, fragments.size());
        PlanFragment fragment = fragments.get(0);
        TPlanFragment tPlanFragment = fragment.toThrift();
        List<TPlanNode> nodes = tPlanFragment.plan.nodes;
        Assert.assertEquals(1, nodes.size());
        TPlanNode tPlanNode = nodes.get(0);
        Assert.assertEquals(TPlanNodeType.FILE_SCAN_NODE, tPlanNode.node_type);
    }

    @Test
    public void testLoadWithOpColumnDefault(@Mocked GlobalStateMgr globalStateMgr,
                                            @Mocked SystemInfoService systemInfoService,
                                            @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("pk", Type.BIGINT, true, null, false, null, ""));
        columns.add(new Column("v1", Type.INT, false, null, false, null, ""));
        columns.add(new Column("v2", ScalarType.createVarchar(50), false, null, true, null, ""));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("pk");
                result = columns.get(0);
                table.getColumn("v1");
                result = columns.get(1);
                table.getColumn("v2");
                result = columns.get(2);
                table.getColumn(Load.LOAD_OP_COLUMN);
                result = null;
            }
        };

        // column mappings
        String sql =
                "LOAD LABEL label0 (DATA INFILE('/path/file1') INTO TABLE t2 columns terminated by ',') with broker 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("/path/file1");
        List<String> columnNames = Lists.newArrayList("pk", "v1", "v2");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "CSV", Lists.newArrayList(),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", ",");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "CSV");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("/path/file1", false, 128000000, true));
        fileStatusesList.add(fileStatusList);

        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 2. check scan node column expr
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locationsList.size());
        TScanRangeLocations location = locationsList.get(0);
        TBrokerScanRangeParams params = location.scan_range.broker_scan_range.params;
        Map<Integer, TExpr> exprOfDestSlot = params.expr_of_dest_slot;

        // get last slot: id == 3
        TExpr opExpr = exprOfDestSlot.get(3);
        Assert.assertEquals(1, opExpr.nodes.size());
        Assert.assertEquals(TExprNodeType.INT_LITERAL, opExpr.nodes.get(0).node_type);
    }

    @Test
    public void testLoadWithOpColumnDelete(@Mocked GlobalStateMgr globalStateMgr,
                                           @Mocked SystemInfoService systemInfoService,
                                           @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("pk", Type.BIGINT, true, null, false, null, ""));
        columns.add(new Column("v1", Type.INT, false, null, false, null, ""));
        columns.add(new Column("v2", ScalarType.createVarchar(50), false, null, true, null, ""));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("pk");
                result = columns.get(0);
                table.getColumn("v1");
                result = columns.get(1);
                table.getColumn("v2");
                result = columns.get(2);
                table.getColumn(Load.LOAD_OP_COLUMN);
                result = null;
            }
        };

        // column mappings
        String sql =
                "LOAD LABEL label0 (DATA INFILE('/path/file1') INTO TABLE t2 columns terminated by ',' " +
                        "set ( __op = 'delete')) with broker 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("/path/file1");
        List<String> columnNames = Lists.newArrayList("pk", "v1", "v2");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "CSV", Lists.newArrayList(),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", ",");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "CSV");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("/path/file1", false, 128000000, true));
        fileStatusesList.add(fileStatusList);

        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 2. check scan node column expr
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locationsList.size());
        TScanRangeLocations location = locationsList.get(0);
        TBrokerScanRangeParams params = location.scan_range.broker_scan_range.params;
        Map<Integer, TExpr> exprOfDestSlot = params.expr_of_dest_slot;

        // get last slot: id == 3
        TExpr opExpr = exprOfDestSlot.get(3);
        Assert.assertEquals(1, opExpr.nodes.size());
        Assert.assertEquals(TExprNodeType.INT_LITERAL, opExpr.nodes.get(0).node_type);
        Assert.assertEquals(TOpType.DELETE.getValue(), opExpr.nodes.get(0).int_literal.value);
    }

    @Test
    public void testLoadWithOpColumnExpr(@Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked SystemInfoService systemInfoService,
                                         @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("pk", Type.BIGINT, true, null, false, null, ""));
        columns.add(new Column("v1", Type.INT, false, null, false, null, ""));
        columns.add(new Column("v2", ScalarType.createVarchar(50), false, null, true, null, ""));

        Function f1 = new Function(new FunctionName("casttobigint"), new Type[] {Type.VARCHAR},
                Type.BIGINT, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        Function f3 = new Function(new FunctionName("casttotinyint"), new Type[] {Type.VARCHAR},
                Type.TINYINT, true);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("c0");
                result = null;
                table.getColumn("c1");
                result = null;
                table.getColumn("c2");
                result = null;
                table.getColumn("c3");
                result = null;
                result = columns.get(0);
                table.getColumn("pk");
                result = columns.get(0);
                table.getColumn("v1");
                result = columns.get(1);
                table.getColumn("v2");
                result = columns.get(2);
                table.getColumn(Load.LOAD_OP_COLUMN);
                result = null;
                globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                returns(f1, f2, f3);
            }
        };

        // column mappings
        String sql =
                "LOAD LABEL label0 (DATA INFILE('/path/file1') INTO TABLE t2 columns terminated by ',' " +
                        "(c0,c1,c2,c3) set (pk=c0, v1=c1, v2=c2, __op = c3)) with broker 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("/path/file1");
        List<String> columnNames = Lists.newArrayList("c0", "c1", "c2", "c3");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "CSV", Lists.newArrayList(),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", ",");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "CSV");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("/path/file1", false, 128000000, true));
        fileStatusesList.add(fileStatusList);

        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 2. check scan node column expr
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locationsList.size());
        TScanRangeLocations location = locationsList.get(0);
        TBrokerScanRangeParams params = location.scan_range.broker_scan_range.params;
        Map<Integer, TExpr> exprOfDestSlot = params.expr_of_dest_slot;

        // get last slot: id == 3
        TExpr opExpr = exprOfDestSlot.get(3);
        Assert.assertEquals(2, opExpr.nodes.size());
        Assert.assertEquals(TExprNodeType.CAST_EXPR, opExpr.nodes.get(0).node_type);
        Assert.assertEquals(TExprNodeType.SLOT_REF, opExpr.nodes.get(1).node_type);
    }

    @Test
    public void testLoadWithOpAutoMapping(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked SystemInfoService systemInfoService,
                                          @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("pk", Type.BIGINT, true, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("123")), ""));
        columns.add(new Column("v1", Type.INT, false, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("231")), ""));
        columns.add(new Column("v2", ScalarType.createVarchar(50), false, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("asdf")), ""));

        Function f1 = new Function(new FunctionName("casttobigint"), new Type[] {Type.VARCHAR},
                Type.BIGINT, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        Function f3 = new Function(new FunctionName("casttotinyint"), new Type[] {Type.VARCHAR},
                Type.TINYINT, true);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                table.getColumn("pk");
                result = columns.get(0);
                table.getColumn("v1");
                result = columns.get(1);
                table.getColumn("v2");
                result = columns.get(2);
                table.getColumn(Load.LOAD_OP_COLUMN);
                result = null;
                returns(f1, f2, f3);
            }
        };

        // column mappings
        String sql =
                "LOAD LABEL label0 (DATA INFILE('/path/file1') INTO TABLE t2 columns terminated by ','" +
                        " (pk,v1,v2,__op)) with broker 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("/path/file1");
        List<String> columnNames = Lists.newArrayList("pk", "v1", "v2", "__op");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "CSV", Lists.newArrayList(),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", ",");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "CSV");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("/path/file1", false, 128000000, true));
        fileStatusesList.add(fileStatusList);


        // plan
        LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                brokerDesc, fileGroups, fileStatusesList, 1);
        planner.plan();

        // 2. check scan node column expr
        FileScanNode scanNode = (FileScanNode) planner.getScanNodes().get(0);
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(1, locationsList.size());
        TScanRangeLocations location = locationsList.get(0);
        TBrokerScanRangeParams params = location.scan_range.broker_scan_range.params;
        Map<Integer, TExpr> exprOfDestSlot = params.expr_of_dest_slot;

        TExpr opExpr = exprOfDestSlot.get(3);
        Assert.assertEquals(1, opExpr.nodes.size());
        Assert.assertEquals(TExprNodeType.SLOT_REF, opExpr.nodes.get(0).node_type);
    }

    @Test
    public void testShuffle(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                            @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.TINYINT, true, null, true, null, ""));
        columns.add(new Column("k2", Type.INT, true, null, false, null, ""));
        columns.add(new Column("k3", ScalarType.createVarchar(50), true, null, true, null, ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, false, null, ""));

        List<Column> keyColumns = Lists.newArrayList();
        keyColumns.add(columns.get(0));
        keyColumns.add(columns.get(1));
        keyColumns.add(columns.get(2));

        Function f1 = new Function(new FunctionName(FunctionSet.SUBSTR), new Type[] {Type.VARCHAR, Type.INT, Type.INT},
                Type.VARCHAR, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.UNIQUE_KEYS;
                table.getDefaultReplicationNum();
                result = 3;
                table.getBaseIndexId();
                result = 1;
                table.getKeyColumnsByIndexId((long) 1);
                result = keyColumns;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("k1");
                result = columns.get(0);
                table.getColumn("k2");
                result = columns.get(1);
                table.getColumn("k3");
                result = columns.get(2);
                table.getColumn("v");
                result = columns.get(3);
                table.getColumn("k33");
                result = null;
                globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                returns(f1, f1, f2);
            }
        };

        // column mappings
        String sql = "LOAD LABEL label0 (DATA INFILE('path/k2=1/file1') INTO TABLE t2 FORMAT AS 'orc' (k1,k33,v) " +
                "COLUMNS FROM PATH AS (k2) set (k3 = substr(k33,1,5))) WITH BROKER 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(sql,
                ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("path/k2=1/file1");
        List<String> columnNames = Lists.newArrayList("k1", "k33", "v");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "ORC", Lists.newArrayList("k2"),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "ORC");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("path/k2=1/file1", false, 268435456, true));
        fileStatusesList.add(fileStatusList);

        {
            // plan
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.plan();

            // check fragment
            List<PlanFragment> fragments = planner.getFragments();
            Assert.assertEquals(2, fragments.size());
        }
        {
            Config.eliminate_shuffle_load_by_replicated_storage = true;
            // plan
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.plan();

            // check fragment
            List<PlanFragment> fragments = planner.getFragments();
            Assert.assertEquals(1, fragments.size());
        }
    }

    @Test
    public void testAggShuffle(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                               @Injectable Database db, @Injectable OlapTable table) throws Exception {
        // table schema
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.TINYINT, true, null, true, null, ""));
        columns.add(new Column("k2", Type.INT, true, null, false, null, ""));
        columns.add(new Column("k3", ScalarType.createVarchar(50), true, null, true, null, ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.REPLACE, false, null, ""));

        List<Column> keyColumns = Lists.newArrayList();
        keyColumns.add(columns.get(0));
        keyColumns.add(columns.get(1));
        keyColumns.add(columns.get(2));

        Map<Long, List<Column>> indexSchema = Maps.newHashMap();
        indexSchema.put((long) 1, columns);

        Function f1 = new Function(new FunctionName(FunctionSet.SUBSTR), new Type[] {Type.VARCHAR, Type.INT, Type.INT},
                Type.VARCHAR, true);
        Function f2 = new Function(new FunctionName("casttoint"), new Type[] {Type.VARCHAR},
                Type.INT, true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getKeysType();
                result = KeysType.AGG_KEYS;
                table.getDefaultReplicationNum();
                result = 3;
                table.getBaseIndexId();
                result = 1;
                table.getIndexIdToSchema();
                result = indexSchema;
                table.getKeyColumnsByIndexId((long) 1);
                result = keyColumns;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("k1");
                result = columns.get(0);
                table.getColumn("k2");
                result = columns.get(1);
                table.getColumn("k3");
                result = columns.get(2);
                table.getColumn("v");
                result = columns.get(3);
                table.getColumn("k33");
                result = null;
                globalStateMgr.getFunction((Function) any, (Function.CompareMode) any);
                returns(f1, f1, f2);
            }
        };

        // column mappings
        String sql = "LOAD LABEL label0 (DATA INFILE('path/k2=1/file1') INTO TABLE t2 FORMAT AS 'orc' (k1,k33,v) " +
                "COLUMNS FROM PATH AS (k2) set (k3 = substr(k33,1,5))) WITH BROKER 'broker0'";
        LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(sql,
                ctx.getSessionVariable().getSqlMode()).get(0);
        List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                "columnMappingList");

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("path/k2=1/file1");
        List<String> columnNames = Lists.newArrayList("k1", "k33", "v");
        DataDescription desc = new DataDescription("t2", null, files, columnNames,
                null, null, "ORC", Lists.newArrayList("k2"),
                false, columnMappingList, null, null);
        Deencapsulation.invoke(desc, "analyzeColumns");
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "ORC");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("path/k2=1/file1", false, 268435456, true));
        fileStatusesList.add(fileStatusList);

        {
            // plan
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.plan();

            // check fragment
            List<PlanFragment> fragments = planner.getFragments();
            Assert.assertEquals(2, fragments.size());
        }
        {
            Config.eliminate_shuffle_load_by_replicated_storage = true;
            // plan
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.plan();

            // check fragment
            List<PlanFragment> fragments = planner.getFragments();
            Assert.assertEquals(1, fragments.size());
        }
        {
            // set partial update mode
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.setPartialUpdateMode(TPartialUpdateMode.COLUMN_UPSERT_MODE);
            planner.plan();
        }
        {
            // set condition update
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.setMergeConditionStr("v");
            planner.plan();
        }
        {
            // complete table sink
            LoadPlanner planner = new LoadPlanner(jobId, loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, startTime, partialUpdate, ctx, sessionVariables, loadMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusesList, 1);
            planner.setPartialUpdateMode(TPartialUpdateMode.COLUMN_UPSERT_MODE);
            planner.plan();
            planner.completeTableSink(100);
        }
    }
}