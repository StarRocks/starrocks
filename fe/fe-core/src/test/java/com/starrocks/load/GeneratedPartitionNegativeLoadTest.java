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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.loadv2.SparkLoadPendingTask;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.planner.StreamLoadScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GeneratedPartitionNegativeLoadTest {
    private static final String ROUTING_COLUMN = "__generated_partition_column_0";

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Injectable
    private LocalMetastore localMetastore;
    @Injectable
    private Database db;
    @Injectable
    private OlapTable table;
    @Injectable
    private SparkLoadJob sparkLoadJob;
    @Injectable
    private SparkResource sparkResource;
    @Injectable
    private BrokerDesc brokerDesc;
    @Mocked
    private StreamLoadScanNode scanNode;
    @Mocked
    private OlapTableSink sink;
    @Injectable
    private Partition partition;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.mockInitWarehouseEnv();
    }

    private List<Column> schema(boolean trusted, AggregateType userAggregate) {
        Column key = new Column("k", IntegerType.BIGINT, true, null, false, null, "");
        Column value = new Column("v", IntegerType.BIGINT, false, userAggregate, false, null, "");
        Column routing;
        if (trusted) {
            ColumnDef definition = ColumnDef.newInternalGeneratedPartitionColumn(
                    ROUTING_COLUMN, new TypeDef(IntegerType.BIGINT), new SlotRef(null, "k"));
            definition.setAggregateType(AggregateType.REPLACE);
            routing = ColumnBuilder.buildGeneratedColumn(null, definition);
        } else {
            routing = new Column(ROUTING_COLUMN, IntegerType.BIGINT, false, AggregateType.REPLACE, true, null, "");
            routing.setGeneratedColumnExpr(ColumnIdExpr.create(new SlotRef(null, "k")));
        }
        return List.of(key, value, routing);
    }

    private void prepare(KeysType keysType, List<Column> columns) {
        new Expectations() {
            {
                table.getKeysType();
                result = keysType;
                minTimes = 0;
                table.getBaseSchema();
                result = columns;
                minTimes = 0;
                table.getFullSchema();
                result = columns;
                minTimes = 0;
                table.getPartitions();
                result = List.of(partition);
                minTimes = 0;
                table.getDefaultReplicationNum();
                result = (short) 3;
                minTimes = 0;
                table.enableReplicatedStorage();
                result = false;
                minTimes = 0;
                table.getIndexMetaIdToSchema();
                result = Map.of(1L, columns);
                minTimes = 0;
                partition.getId();
                result = 1L;
                minTimes = 0;
                db.getFullName();
                result = "negative_test";
                minTimes = 0;
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;
                localMetastore.getTable(anyString, anyString);
                result = table;
                minTimes = 0;
                scanNode.getChildren();
                result = new ArrayList<>();
                minTimes = 0;
                scanNode.getId();
                result = new PlanNodeId(0);
                minTimes = 0;
            }
        };
    }

    private StreamLoadInfo streamLoadInfo() throws Exception {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadId(new TUniqueId(1, 2));
        request.setTxnId(3);
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setNegative(true);
        return StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
    }

    private TExecPlanFragmentParams classicPlan() throws Exception {
        StreamLoadInfo info = streamLoadInfo();
        return new StreamLoadPlanner(new ConnectContext(), db, table, info).plan(info.getId());
    }

    private LoadPlanner pipelinePlanner() throws Exception {
        StreamLoadInfo info = streamLoadInfo();
        return new LoadPlanner(1, info.getId(), 3, 4, "negative_test", table, true, "+00:00", false,
                new ConnectContext(), Collections.emptyMap(), 0, 0, true, 1,
                Collections.emptyList(), info);
    }

    private void pipelineTuple(LoadPlanner planner, List<Column> columns) {
        Deencapsulation.invoke(planner, "generateTupleDescriptor", columns, false);
    }

    private DataDescription dataDescription() {
        return new DataDescription("t", null, List.of("fixture.csv"), null, null, null, null, true, null);
    }

    private BrokerFileGroup brokerGroup() throws Exception {
        DataDescription description = dataDescription();
        BrokerFileGroup group = new BrokerFileGroup(description);
        group.parse(db, description);
        return group;
    }

    private EtlFileGroup sparkGroup() {
        // Isolate the eligibility/configuration path; no Spark submission or external storage is involved.
        new MockUp<Load>() {
            @Mock
            public void initColumns(Table target, List<ImportColumnDesc> expressions,
                                    Map<String, Pair<String, List<String>>> hadoopFunctions) {
            }

            @Mock
            public List<ImportColumnDesc> getMaterializedShadowColumnDesc(Table target, String database, boolean analyze) {
                return List.of(new ImportColumnDesc(ROUTING_COLUMN, new SlotRef(null, "k")));
            }

            @Mock
            public List<ImportColumnDesc> getSchemaChangeShadowColumnDesc(Table target, Map<String, Expr> expressions) {
                return Collections.emptyList();
            }
        };
        SparkLoadPendingTask task = new SparkLoadPendingTask(
                sparkLoadJob, Collections.emptyMap(), sparkResource, brokerDesc);
        BrokerFileGroup group = new BrokerFileGroup(dataDescription());
        // Exercise Spark's own gate without first passing through BrokerFileGroup.parse.
        Deencapsulation.setField(group, "isNegative", true);
        return Deencapsulation.invoke(task, "createEtlFileGroup", group, Collections.emptySet(), db, table);
    }

    private void assertRejectedByEveryPlanner(List<Column> columns, String rejectedColumn) throws Exception {
        String message = "Column is not SUM AggreateType. column:" + rejectedColumn;
        Assertions.assertEquals(message, Assertions.assertThrows(DdlException.class, this::classicPlan).getMessage());
        LoadPlanner planner = pipelinePlanner();
        Assertions.assertEquals(message, Assertions.assertThrows(DdlException.class,
                () -> pipelineTuple(planner, columns)).getMessage());
        Assertions.assertEquals(message, Assertions.assertThrows(DdlException.class, this::brokerGroup).getMessage());
        Assertions.assertEquals(message, Assertions.assertThrows(LoadException.class,
                this::sparkGroup).getMessage());
    }

    @Test
    public void testTrustedRoutingReplaceAllowsNegativeSumLoad() throws Exception {
        List<Column> columns = schema(true, AggregateType.SUM);
        prepare(KeysType.AGG_KEYS, columns);
        TExecPlanFragmentParams plan = classicPlan();
        Assertions.assertEquals(columns.size(), plan.desc_tbl.slotDescriptors.size());
        LoadPlanner planner = pipelinePlanner();
        pipelineTuple(planner, columns);
        TupleDescriptor tuple = Deencapsulation.getField(planner, "tupleDesc");
        Assertions.assertEquals(columns.size(), tuple.getSlots().size());
        Assertions.assertTrue(brokerGroup().isNegative());
        EtlFileGroup spark = sparkGroup();
        Assertions.assertTrue(spark.isNegative);
        Assertions.assertTrue(spark.columnMappings.containsKey(ROUTING_COLUMN));
        Assertions.assertEquals(AggregateType.REPLACE, columns.get(2).getAggregationType());
    }

    @Test
    public void testUserReplaceRemainsRejected() throws Exception {
        List<Column> columns = schema(true, AggregateType.REPLACE);
        prepare(KeysType.AGG_KEYS, columns);
        assertRejectedByEveryPlanner(columns, "v");
    }

    @Test
    public void testUserMaxRemainsRejected() throws Exception {
        List<Column> columns = schema(true, AggregateType.MAX);
        prepare(KeysType.AGG_KEYS, columns);
        assertRejectedByEveryPlanner(columns, "v");
    }

    @Test
    public void testReservedPrefixAndLegacyDefaultFalseDoNotGrantExemption() throws Exception {
        List<Column> columns = schema(false, AggregateType.SUM);
        prepare(KeysType.AGG_KEYS, columns);
        Assertions.assertFalse(columns.get(2).isInternalGeneratedPartitionColumn());
        assertRejectedByEveryPlanner(columns, ROUTING_COLUMN);
    }

    private void assertNonAggregateRemainsRejected(KeysType keysType) throws Exception {
        List<Column> fullSchema = schema(true, AggregateType.SUM);
        List<Column> columns = List.of(fullSchema.get(0), fullSchema.get(2));
        prepare(keysType, columns);
        Assertions.assertThrows(DdlException.class, this::classicPlan);
        LoadPlanner planner = pipelinePlanner();
        Assertions.assertThrows(DdlException.class, () -> pipelineTuple(planner, columns));
        Assertions.assertEquals("Only aggregate table can specify NEGATIVE",
                Assertions.assertThrows(DdlException.class, this::brokerGroup).getMessage());
        Assertions.assertThrows(LoadException.class, this::sparkGroup);
    }

    @Test
    public void testDuplicateModelDoesNotGainExemption() throws Exception {
        assertNonAggregateRemainsRejected(KeysType.DUP_KEYS);
    }

    @Test
    public void testUniqueModelDoesNotGainExemption() throws Exception {
        assertNonAggregateRemainsRejected(KeysType.UNIQUE_KEYS);
    }

    @Test
    public void testOnlyTrustedRoutingReplaceAvoidsAggregateShuffle() throws Exception {
        prepare(KeysType.AGG_KEYS, schema(true, AggregateType.SUM));
        Assertions.assertFalse(pipelinePlanner().needShufflePlan());
        prepare(KeysType.AGG_KEYS, schema(false, AggregateType.SUM));
        Assertions.assertTrue(pipelinePlanner().needShufflePlan());
        prepare(KeysType.AGG_KEYS, schema(true, AggregateType.REPLACE));
        Assertions.assertTrue(pipelinePlanner().needShufflePlan());
    }
}
