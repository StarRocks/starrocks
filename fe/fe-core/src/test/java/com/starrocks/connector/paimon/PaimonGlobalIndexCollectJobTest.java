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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.TableName;
import com.starrocks.planner.PaimonGlobalIndexScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TRowFormat;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonGlobalIndexCollectJobTest {
    private static final long BUDGET = 64L * 1024 * 1024;
    private ConnectContext caller;
    private PaimonTable table;
    private PaimonGlobalIndexRequest request;

    @BeforeAll
    static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    void setUp() {
        caller = UtFrameUtils.createDefaultCtx();
        caller.setStartTime();
        caller.setThreadLocalInfo();
        TWorkGroup group = new TWorkGroup();
        group.setName("caller_group");
        caller.setResourceGroup(group);
        table = mock(PaimonTable.class);
        when(table.getCatalogName()).thenReturn("pa`imon");
        when(table.getCatalogDBName()).thenReturn("d`b");
        when(table.getCatalogTableName()).thenReturn("t`bl");
        request = PaimonGlobalIndexRequest.create("file:/orders", "main", 42, 0, 1000,
                new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(1, IntegerType.INT, "k", true),
                        ConstantOperator.createInt(1)), Map.of(1, 7));
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    private PaimonGlobalIndexCollectJob job(long budget) {
        return new PaimonGlobalIndexCollectJob(table, request, budget);
    }

    // Keep collection, context setup and result validation real; replace only execution of the internal SQL.
    private PaimonGlobalIndexCollectJob collectingJob(int expected, TResultBatch... batches) {
        var job = spy(job(BUDGET));
        var coordinator = mock(Coordinator.class);
        var scan = mock(PaimonGlobalIndexScanNode.class);
        when(coordinator.getScanNodes()).thenReturn(List.of(scan));
        when(scan.getScanRangeLocations(0)).thenReturn(java.util.Collections.nCopies(expected, new TScanRangeLocations()));
        doReturn(coordinator).when(job).getMetadataJobCoord();
        doAnswer(call -> {
            for (TResultBatch batch : batches) {
                job.getResultConsumer().accept(batch);
            }
            job.getContext().getState().setEof();
            return null;
        }).when(job).asyncCollectMetadata();
        return job;
    }

    private static byte[] result(long... ids) throws Exception {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        for (long id : ids) {
            rows.add(id);
        }
        return new GlobalIndexResultSerializer().serialize(GlobalIndexResult.create(rows));
    }

    private static ByteBuffer row(long id, byte[] bitmap) throws Exception {
        ByteBuffer data = ByteBuffer.allocate(16 + bitmap.length).order(ByteOrder.LITTLE_ENDIAN);
        data.putLong(id).putLong(bitmap.length).put(bitmap);
        return wire(new byte[] {1, 1}, data.array());
    }

    private static ByteBuffer wire(byte[] nulls, byte[] data) throws Exception {
        TRowFormat format = new TRowFormat();
        format.setNull_bits(nulls);
        format.setPacked_data(data);
        return ByteBuffer.wrap(new TSerializer(new TCompactProtocol.Factory()).serialize(format));
    }

    private static TResultBatch batch(ByteBuffer... rows) {
        TResultBatch result = new TResultBatch();
        result.setRows(List.of(rows));
        return result;
    }

    private void rejects(ByteBuffer row, String message) {
        Exception error = assertThrows(IllegalStateException.class, () -> job(BUDGET).getResultConsumer().accept(batch(row)));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    void testMergeOutOfOrderShardsAndPreserveCallerPolicy() throws Exception {
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(caller.getSessionVariable(),
                new SystemVariable(SessionVariable.QUERY_MEM_LIMIT, new IntLiteral(32 * 1024 * 1024)), false);
        caller.getSessionVariable().setSqlSelectLimit(1);
        caller.getSessionVariable().setEnableSPMRewrite(true);
        caller.getSessionVariable().setEnableMetadataProfile(true);
        ByteBuffer first = row(2, result(3, (1L << 32) + 7));
        // RPC buffers need not start at offset zero, and the consumer must not advance their positions.
        ByteBuffer sliced = ByteBuffer.allocate(first.remaining() + 5);
        sliced.position(5);
        sliced.put(first.duplicate()).flip().position(5);
        var job = collectingJob(3, batch(sliced, row(0, result(1, 3))), batch(row(1, result())));
        var aggregate = job.collect(caller).results();
        assertEquals(3, aggregate.getLongCardinality());
        assertTrue(aggregate.contains(1) && aggregate.contains(3) && aggregate.contains((1L << 32) + 7));
        assertEquals(5, sliced.position());
        assertTrue(job.getResultQueue().isEmpty());
        assertEquals(TResultSinkType.CUSTOMIZED, job.getSinkType());
        assertEquals("SELECT shard_id, index_result FROM `pa``imon`.`d``b`.`t``bl$paimon_global_index`"
                + " WHERE predicate = '" + request.toTransportString() + "'", job.getSql());
        var child = job.getContext();
        assertSame(caller, child.getParent());
        assertTrue(child.isMetadataContext());
        assertFalse(child.isNeedQueued());
        assertFalse(child.getSessionVariable().isEnableSPMRewrite());
        assertFalse(child.getSessionVariable().isEnableShortCircuit());
        assertTrue(child.getSessionVariable().isEnableProfile());
        assertEquals("caller_group", child.getSessionVariable().getResourceGroup());
        assertEquals(32 * 1024 * 1024, child.getSessionVariable().getQueryMemLimit());
        assertEquals(SessionVariable.DEFAULT_SELECT_LIMIT, child.getSessionVariable().getSqlSelectLimit());
        assertEquals(1, child.getSessionVariable().getChunkSize());
        assertEquals(1, child.getSessionVariable().getPipelineDop());
        assertEquals(1, caller.getSessionVariable().getSqlSelectLimit());
        assertTrue(caller.getSessionVariable().isEnableSPMRewrite());
    }

    @Test
    void testResourceGroupClassificationUsesCallerIdentity() throws Exception {
        caller.setResourceGroup(null);
        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext context, ResourceGroupClassifier.QueryType queryType) {
                assertSame(caller, context);
                assertEquals(ResourceGroupClassifier.QueryType.SELECT, queryType);
                return new TWorkGroup().setName("classified_for_caller");
            }
        };
        var job = collectingJob(1, batch(row(0, result())));
        job.collect(caller);
        assertEquals("classified_for_caller", job.getContext().getSessionVariable().getResourceGroup());
    }

    @Test
    void testPaimonCppV1AndJavaV2Results() throws Exception {
        // BE paimon-cpp fixture: inclusive ranges [7, 9] and [20, 20].
        byte[] cpp = HexFormat.of().parseHex(
                "000000010000001f0100000000000000000000003b30000001000003000200070002001400000000000000");
        var merged = collectingJob(2, batch(row(0, cpp), row(1, result(20, 30)))).collect(caller).results();
        assertEquals(5, merged.getLongCardinality());
        for (long id : new long[] {7, 8, 9, 20, 30}) {
            assertTrue(merged.contains(id));
        }
        assertEquals(0, collectingJob(1, batch(row(0, result()))).collect(caller).results().getLongCardinality());
    }

    @Test
    void testManyDisjointPortableBitmapContainers() throws Exception {
        long[] ids = new long[4096];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = ((long) i << 32) + (1L << 16);
        }
        var merged = collectingJob(1, batch(row(0, result(ids)))).collect(caller).results();
        assertEquals(ids.length, merged.getLongCardinality());
        for (long id : ids) {
            assertTrue(merged.contains(id));
        }
    }

    @Test
    void testMissingDuplicateAndInvalidShardIds() throws Exception {
        for (var job : List.of(collectingJob(1), collectingJob(0),
                collectingJob(2, batch(row(0, result()))), collectingJob(1, batch(row(1, result()))))) {
            assertTrue(assertThrows(IllegalStateException.class, () -> job.collect(caller))
                    .getMessage().contains("Incomplete Paimon Global Index shard results"));
        }
        var duplicate = collectingJob(2, batch(row(0, result()), row(0, result())));
        assertTrue(assertThrows(IllegalStateException.class, () -> duplicate.collect(caller))
                .getMessage().contains("duplicate"));
        rejects(row(-1, result()), "Invalid or duplicate");
        rejects(row(PaimonGlobalIndexPlanner.MAX_SHARDS, result()), "Invalid or duplicate");
    }

    @Test
    void testRejectMalformedCustomizedRows() throws Exception {
        rejects(wire(new byte[] {1, 0}, new byte[16]), "Missing");
        rejects(wire(new byte[] {1}, new byte[16]), "Missing");
        rejects(wire(new byte[] {1, 1}, ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(0).putLong(Long.MAX_VALUE).array()), "Invalid or duplicate");
        rejects(ByteBuffer.wrap(new byte[] {1, 2, 3}), "Cannot collect");
    }

    @Test
    void testRejectInvalidResultEnvelopeBeforeSdkAllocation() throws Exception {
        byte[] valid = result(1);
        int lengthOffset = ByteBuffer.wrap(valid).getInt() == 2 ? 5 : 4;
        List<byte[]> invalid = new ArrayList<>();
        byte[] version = valid.clone();
        ByteBuffer.wrap(version).putInt(99);
        invalid.add(version);
        byte[] length = valid.clone();
        ByteBuffer.wrap(length).putInt(lengthOffset, Integer.MAX_VALUE);
        invalid.add(length);
        byte[] scores = valid.clone();
        ByteBuffer.wrap(scores).putInt(scores.length - 4, 1);
        invalid.add(scores);
        byte[] header = valid.clone();
        ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).putLong(lengthOffset + 4, Long.MAX_VALUE);
        invalid.add(header);
        byte[] trailing = new byte[valid.length + 1];
        System.arraycopy(valid, 0, trailing, 0, valid.length - 4);
        ByteBuffer.wrap(trailing).putInt(lengthOffset, ByteBuffer.wrap(valid).getInt(lengthOffset) + 1);
        invalid.add(trailing);
        if (lengthOffset == 5) {
            byte[] kind = valid.clone();
            kind[4] = 1;
            invalid.add(kind);
        }
        for (byte[] bytes : invalid) {
            rejects(row(0, bytes), "Cannot collect");
        }
    }

    @Test
    void testBoundSerializedAndExpandedResultMemory() throws Exception {
        var tiny = job(8);
        assertTrue(assertThrows(IllegalStateException.class,
                () -> tiny.getResultConsumer().accept(batch(row(0, result())))).getMessage().contains("collection budget"));
        var expanded = job(1024);
        assertTrue(assertThrows(IllegalStateException.class,
                () -> expanded.getResultConsumer().accept(batch(row(0, result(1, 2, 3)))))
                .getMessage().contains("bitmap exceeds"));
        var cumulative = job(1024);
        cumulative.getResultConsumer().accept(batch(row(0, result(1, 2))));
        assertTrue(assertThrows(IllegalStateException.class,
                () -> cumulative.getResultConsumer().accept(batch(row(1, result(1)))))
                .getMessage().contains("bitmap exceeds"));
        rejects(ByteBuffer.allocate((int) request.getResultBytesLimit() + 1025), "collection budget");
    }

    @Test
    void testFailedAndUnexpectedPlansDoNotReturnPartialResults() {
        var failed = collectingJob(1);
        doAnswer(call -> {
            failed.getContext().getState().setError("backend failed");
            return null;
        }).when(failed).asyncCollectMetadata();
        assertTrue(assertThrows(IllegalStateException.class, () -> failed.collect(caller))
                .getMessage().contains("backend failed"));
        var wrongPlan = collectingJob(1);
        when(wrongPlan.getMetadataJobCoord().getScanNodes()).thenReturn(List.of());
        assertTrue(assertThrows(IllegalStateException.class, () -> wrongPlan.collect(caller))
                .getMessage().contains("Unexpected"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPoliciesPreventInternalExecution(boolean rowPolicy) {
        new MockUp<Authorizer>() {
            @Mock
            public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName name, List<Column> columns) {
                assertSame(caller, context);
                return rowPolicy ? Map.of() : Map.of("k", mock(Expr.class));
            }

            @Mock
            public Expr getRowAccessPolicy(ConnectContext context, TableName name) {
                assertSame(caller, context);
                return rowPolicy ? mock(Expr.class) : null;
            }
        };
        var job = collectingJob(1);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> job.collect(caller))
                .getMessage().contains("row or masking policies"));
        verify(job, never()).asyncCollectMetadata();
    }
}
