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

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.ConnectorSinkShuffleMode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.PaimonBucketRoutingNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TPaimonBucketRouting;
import com.starrocks.thrift.TPaimonTableSink;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.IntegerType;
import org.apache.paimon.table.FileStoreTable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaimonSinkShuffleSpecTest {
    @Test
    public void testEveryFixedLayoutForcesRoutingEvenNeverAndUnpartitioned() {
        SessionVariable never = mock(SessionVariable.class);
        when(never.getConnectorSinkShuffleMode()).thenReturn(ConnectorSinkShuffleMode.NEVER);
        for (Map<String, String> options : List.of(Map.of("bucket", "1"),
                Map.of("bucket", "17", "bucket-key", "id"))) {
            FileStoreTable nativeTable = mock(FileStoreTable.class);
            when(nativeTable.options()).thenReturn(options);
            PaimonTable table = mock(PaimonTable.class);
            when(table.getNativeTable()).thenReturn(nativeTable);
            Assertions.assertInstanceOf(PaimonSinkShuffleSpec.class, ConnectorSinkShuffleSpec.forTable(table).orElseThrow());
            Assertions.assertTrue(new PaimonSinkShuffleSpec(table).shouldShuffle(null, never));
        }
        FileStoreTable unaware = mock(FileStoreTable.class);
        when(unaware.options()).thenReturn(Map.of("bucket", "-1"));
        PaimonTable table = mock(PaimonTable.class);
        when(table.getNativeTable()).thenReturn(unaware);
        Assertions.assertFalse(new PaimonSinkShuffleSpec(table).shouldShuffle(null, never));
    }

    @Test
    public void testFinalExchangeRoutesNativeBucketNotRawBucketKey() throws Exception {
        for (DataPartition sourcePartition : List.of(DataPartition.RANDOM, DataPartition.UNPARTITIONED)) {
            ExecPlan plan = inputPlan(sourcePartition);
            PlanNode oldRoot = plan.getTopFragment().getPlanRoot();
            oldRoot.setLimit(5); // Existing LIMIT/gather remains BELOW the mandatory native calculation.
            TPaimonTableSink sink = sink(7);
            int bucket = PaimonSinkShuffleSpec.installBucketRouting(plan, sink);
            PlanFragment output = plan.getTopFragment();
            Assertions.assertInstanceOf(ExchangeNode.class, output.getPlanRoot());
            Assertions.assertEquals(TPartitionType.HASH_PARTITIONED, output.getDataPartition().getType());
            Assertions.assertTrue(output.hasPaimonTableSink());
            Assertions.assertTrue(output.hasTableSink());
            Assertions.assertTrue(output.forceSetTableSinkDop());
            Assertions.assertFalse(output.isUseRuntimeAdaptiveDop());
            List<Expr> keys = output.getDataPartition().getPartitionExprs();
            Assertions.assertEquals(2, keys.size());
            Assertions.assertEquals(new SlotId(bucket), ((SlotRef) keys.get(1)).getSlotId());
            // Native bucket key 'id' is schema index 1, while the only other route key is partition 'p'.
            Assertions.assertEquals(((SlotRef) plan.getOutputExprs().get(0)).getSlotId(),
                    ((SlotRef) keys.get(0)).getSlotId());
            Assertions.assertNotEquals(((SlotRef) plan.getOutputExprs().get(1)).getSlotId(),
                    ((SlotRef) keys.get(0)).getSlotId());
            Assertions.assertEquals(2, plan.getOutputExprs().size()); // No route column in native RecordBatch.
            PlanFragment sender = plan.getFragments().get(1);
            Assertions.assertEquals(3, sender.getOutputExprs().size());
            PaimonBucketRoutingNode routing = (PaimonBucketRoutingNode) sender.getPlanRoot();
            Assertions.assertSame(oldRoot, routing.getChild(0));
            Assertions.assertEquals(5, oldRoot.getLimit());
            Assertions.assertFalse(routing.getSlotMap().containsKey(new SlotId(bucket))); // No placeholder.
            Assertions.assertFalse(routing.isTrivial());
            TPlanNode wire = routing.treeToThrift().getNodes().get(0);
            Assertions.assertEquals(TPlanNodeType.PAIMON_BUCKET_ROUTING_NODE, wire.getNode_type());
            TPaimonBucketRouting route = wire.getProject_node().getPaimon_bucket_routing();
            Assertions.assertEquals(1, route.getProtocol_version());
            Assertions.assertEquals(bucket, route.getBucket_slot_id());
            Assertions.assertEquals(2, route.getSchema_exprsSize());
            TPaimonTableSink decoded = new TPaimonTableSink();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(decoded, route.getSink_payload());
            Assertions.assertEquals(sink.getSchema_json(), decoded.getSchema_json());
            Assertions.assertEquals(sink.getOptions(), decoded.getOptions());
            Assertions.assertEquals(sink.getCloud_configuration(), decoded.getCloud_configuration());
            Assertions.assertEquals(bucket, decoded.getRouting_bucket_slot_id());
            Assertions.assertFalse(decoded.isBucket_routed());
            Assertions.assertFalse(sink.isSetBucket_routed()); // No premature promise to the writer.
        }
    }

    @Test
    public void testUnpartitionedFixedLayoutStillUsesNativeBucketAndKeepsParallelWriters() {
        ExecPlan plan = inputPlan(DataPartition.RANDOM);
        TPaimonTableSink sink = sink(13);
        sink.setPartition_columns(List.of());
        int bucket = PaimonSinkShuffleSpec.installBucketRouting(plan, sink);
        PlanFragment output = plan.getTopFragment();
        output.setPipelineDop(8);
        Assertions.assertEquals(8, output.getPipelineDop());
        Assertions.assertEquals(1, output.getDataPartition().getPartitionExprs().size());
        Assertions.assertEquals(new SlotId(bucket),
                ((SlotRef) output.getDataPartition().getPartitionExprs().get(0)).getSlotId());
        Assertions.assertFalse(output.isUseRuntimeAdaptiveDop());
        Assertions.assertTrue(bucket > 101); // Does not collide with explicit optimizer slot IDs.
    }

    @Test
    public void testAppendUnawareIsUnchangedAndInvalidContractsFailClosed() {
        ExecPlan plan = inputPlan(DataPartition.RANDOM);
        PlanFragment original = plan.getTopFragment();
        TPaimonTableSink unaware = sink(-1);
        Assertions.assertEquals(-1, PaimonSinkShuffleSpec.installBucketRouting(plan, unaware));
        Assertions.assertSame(original, plan.getTopFragment());
        unaware.setPrimary_key_columns(List.of("id"));
        Assertions.assertThrows(SemanticException.class, () -> PaimonSinkShuffleSpec.installBucketRouting(plan, unaware));
        TPaimonTableSink incomplete = sink(3);
        incomplete.setColumn_names(List.of("p"));
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonSinkShuffleSpec.installBucketRouting(plan, incomplete));
        TPaimonTableSink oversized = sink(3);
        oversized.setSchema_json("x".repeat(PaimonBucketRoutingNode.MAX_SINK_PAYLOAD_BYTES));
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonSinkShuffleSpec.installBucketRouting(plan, oversized));
    }

    private static ExecPlan inputPlan(DataPartition partition) {
        ExecPlan plan = new ExecPlan();
        plan.getConnectContext().getSessionVariable().setEnablePipelineEngine(true);
        DescriptorTable descriptors = plan.getDescTbl();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        // Explicit optimizer IDs deliberately leave the automatic allocator behind.
        for (int i = 0; i < 2; ++i) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple, new SlotId(100 + i));
            slot.setType(IntegerType.INT);
            slot.setIsNullable(false);
            slot.setIsMaterialized(true);
            plan.getOutputExprs().add(new SlotRef(slot));
        }
        tuple.computeMemLayout();
        EmptySetNode source = new EmptySetNode(plan.getNextNodeId(), new ArrayList<>(tuple.getId().asList()));
        PlanFragment fragment = new PlanFragment(plan.getNextFragmentId(), source, partition);
        fragment.setOutputExprs(plan.getOutputExprs());
        plan.getFragments().add(fragment);
        return plan;
    }

    private static TPaimonTableSink sink(int buckets) {
        TPaimonTableSink sink = new TPaimonTableSink();
        sink.setBucket_count(buckets);
        sink.setColumn_names(List.of("p", "id"));
        sink.setPartition_columns(List.of("p"));
        sink.setBucket_columns(List.of("id"));
        sink.setPrimary_key_columns(List.of());
        sink.setWriter_protocol_version(1);
        sink.setLocation("file:///unused");
        sink.setTable_identity("routing-test");
        sink.setSchema_id(9);
        sink.setSchema_json("{frozen-native-schema}");
        sink.setOptions(Map.of("bucket", Integer.toString(buckets), "bucket-key", "id"));
        sink.setCloud_configuration(new TCloudConfiguration());
        return sink;
    }
}
