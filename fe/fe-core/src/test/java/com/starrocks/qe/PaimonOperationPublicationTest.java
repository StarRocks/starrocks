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

package com.starrocks.qe;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.PaimonCommitUnknownException;
import com.starrocks.connector.paimon.PaimonSinkExtra;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.PaimonTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionSchedule;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.slot.SlotProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TPaimonCommitInfo;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.GlobalTransactionMgr;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PaimonOperationPublicationTest {
    private static final TUniqueId INSTANCE_ID = new TUniqueId(1, 2);

    @AfterEach
    public void clearContext() {
        ConnectContext.remove();
    }

    @Test
    public void testEveryEmptyWriteModeReachesContextualFinish() throws Exception {
        // FULL/STATIC must publish a deletion even with no native CommitMessages. APPEND/DYNAMIC
        // are connector-confirmed no-ops; the FE must not mistake either case for an OLAP abort.
        for (String mode : List.of("APPEND", "FULL", "STATIC", "DYNAMIC")) {
            try (Fixture fixture = new Fixture(mode, 0, 0)) {
                doAnswer(invocation -> {
                    fixture.operation.transition(PaimonSinkExtra.PublicationState.WRITING,
                            PaimonSinkExtra.PublicationState.PREPARED);
                    fixture.operation.transition(PaimonSinkExtra.PublicationState.PREPARED,
                            PaimonSinkExtra.PublicationState.COMMITTED);
                    return null;
                }).when(fixture.metadata).finishSink(eq("p"), eq("db"), eq("t"), anyList(), isNull(),
                        eq(fixture.operation), eq(fixture.context));

                fixture.executor.handleDMLStmt(fixture.plan, fixture.statement);

                verify(fixture.metadata).finishSink("p", "db", "t", fixture.profile.getSinkCommitInfos(), null,
                        fixture.operation, fixture.context);
                assertEquals(QueryState.MysqlStateType.OK, fixture.queryState.getStateType());
                assertTrue(fixture.queryState.getInfoMessage().contains("VISIBLE"));
                verifyNoInteractions(fixture.transactions, fixture.loads);
            }
        }
    }

    @Test
    public void testFilteredRowsAbortNativeOperationWithoutFeTransaction() throws Exception {
        try (Fixture fixture = new Fixture("FULL", 0, 1)) {
            fixture.executor.handleDMLStmt(fixture.plan, fixture.statement);

            assertEquals(QueryState.MysqlStateType.ERR, fixture.queryState.getStateType());
            verify(fixture.metadata).abortSink("p", "db", "t", fixture.profile.getSinkCommitInfos(),
                    fixture.operation, fixture.context);
            verify(fixture.metadata, never()).abortSink(anyString(), anyString(), anyString(), anyList());
            verify(fixture.metadata, never()).finishSink(anyString(), anyString(), anyString(), anyList(), any(),
                    any(), any());
            verifyNoInteractions(fixture.transactions, fixture.loads);
        }
    }

    @Test
    public void testExecutionFailureAbortsWithNativePayloadAndContext() throws Exception {
        try (Fixture fixture = new Fixture("APPEND", 0, 0)) {
            doThrow(new IllegalStateException("native writer failed")).when(fixture.coordinator).exec();

            Exception failure = assertThrows(Exception.class,
                    () -> fixture.executor.handleDMLStmt(fixture.plan, fixture.statement));

            assertTrue(failure.getMessage().contains("native writer failed"));
            verify(fixture.metadata).abortSink("p", "db", "t", fixture.profile.getSinkCommitInfos(),
                    fixture.operation, fixture.context);
            verifyNoInteractions(fixture.transactions, fixture.loads);
        }
    }

    @Test
    public void testCoordinatorCreationFailureHasNullSafeAbort() throws Exception {
        try (Fixture fixture = new Fixture("APPEND", 0, 0)) {
            fixture.schedulerFailure = new IllegalStateException("scheduler unavailable");

            assertThrows(Exception.class, () -> fixture.executor.handleDMLStmt(fixture.plan, fixture.statement));

            verify(fixture.metadata).abortSink("p", "db", "t", List.of(), fixture.operation, fixture.context);
            verifyNoInteractions(fixture.transactions, fixture.loads);
        }
    }

    @Test
    public void testUnknownCommitIsTypedAndNeverAborted() throws Exception {
        try (Fixture fixture = new Fixture("FULL", 0, 0)) {
            PaimonCommitUnknownException unknown = new PaimonCommitUnknownException(
                    fixture.operation.getOperationId(), new IllegalStateException("snapshot response was lost"));
            doAnswer(invocation -> {
                fixture.operation.transition(PaimonSinkExtra.PublicationState.WRITING,
                        PaimonSinkExtra.PublicationState.PREPARED);
                fixture.operation.transition(PaimonSinkExtra.PublicationState.PREPARED,
                        PaimonSinkExtra.PublicationState.PUBLISHING);
                fixture.operation.transition(PaimonSinkExtra.PublicationState.PUBLISHING,
                        PaimonSinkExtra.PublicationState.UNKNOWN);
                throw unknown;
            }).when(fixture.metadata).finishSink(anyString(), anyString(), anyString(), anyList(), any(), any(), any());

            assertSame(unknown, assertThrows(PaimonCommitUnknownException.class,
                    () -> fixture.executor.handleDMLStmt(fixture.plan, fixture.statement)));

            assertFalse(fixture.operation.canAbort());
            assertEquals(QueryState.MysqlStateType.ERR, fixture.queryState.getStateType());
            verify(fixture.metadata, never()).abortSink(anyString(), anyString(), anyString(), anyList(), any(), any());
            verifyNoInteractions(fixture.transactions, fixture.loads);
        }
    }

    @Test
    public void testUnconfirmedFinishDoesNotAcknowledgeVisible() throws Exception {
        try (Fixture fixture = new Fixture("FULL", 0, 0)) {
            assertThrows(StarRocksConnectorException.class,
                    () -> fixture.executor.handleDMLStmt(fixture.plan, fixture.statement));
            assertEquals(QueryState.MysqlStateType.ERR, fixture.queryState.getStateType());
            verify(fixture.metadata).abortSink("p", "db", "t", fixture.profile.getSinkCommitInfos(),
                    fixture.operation, fixture.context);
        }
    }

    @Test
    public void testMissingZeroRowDriverBlocksEmptyOverwrite() {
        PaimonSinkExtra operation = operation("FULL");
        QueryRuntimeProfile profile = profile(operation, 2);
        FragmentInstanceExecState execution = execution(INSTANCE_ID);
        TSinkCommitInfo first = marker(operation, INSTANCE_ID, 0, 0);

        assertThrows(StarRocksConnectorException.class,
                () -> profile.updateLoadInformation(execution, report(INSTANCE_ID, List.of(first))));
        assertThrows(StarRocksConnectorException.class, () -> profile.validatePaimonSinkCommitInfos(0));
        assertEquals(List.of(first), profile.getSinkCommitInfos(), "partial payload remains available for native abort");
    }

    @Test
    public void testEmptyReportCannotSatisfyAnEmptyOverwrite() {
        PaimonSinkExtra operation = operation("STATIC");
        QueryRuntimeProfile profile = profile(operation, 1);
        assertThrows(StarRocksConnectorException.class,
                () -> profile.updateLoadInformation(execution(INSTANCE_ID), report(INSTANCE_ID, List.of())));
        assertThrows(StarRocksConnectorException.class, () -> profile.validatePaimonSinkCommitInfos(0));
    }

    @Test
    public void testSourceOnlyFragmentNeedsNoProducerAndAllSinkDriversDo() {
        PaimonSinkExtra operation = operation("FULL");
        QueryRuntimeProfile profile = profile(operation, 2);
        TUniqueId sourceId = new TUniqueId(1, 3);
        profile.updateLoadInformation(execution(sourceId), report(sourceId, List.of()));
        profile.updateLoadInformation(execution(INSTANCE_ID), report(INSTANCE_ID, List.of(
                marker(operation, INSTANCE_ID, 0, 0), marker(operation, INSTANCE_ID, 1, 0))));
        profile.validatePaimonSinkCommitInfos(0);
        assertEquals("2", profile.getQueryProfile().getInfoString("PaimonPreparedProducers"));
    }

    @Test
    public void testPreparedRowsMustMatchLoadCounters() {
        PaimonSinkExtra operation = operation("APPEND");
        QueryRuntimeProfile profile = profile(operation, 1);
        profile.updateLoadInformation(execution(INSTANCE_ID), report(INSTANCE_ID,
                List.of(marker(operation, INSTANCE_ID, 0, 1))));
        assertThrows(StarRocksConnectorException.class, () -> profile.validatePaimonSinkCommitInfos(0));
    }

    @Test
    public void testMalformedCounterDoesNotDiscardNativeAbortPayload() {
        PaimonSinkExtra operation = operation("APPEND");
        QueryRuntimeProfile profile = profile(operation, 1);
        TSinkCommitInfo marker = marker(operation, INSTANCE_ID, 0, 0);
        TReportExecStatusParams report = report(INSTANCE_ID, List.of(marker));
        report.setLoad_counters(Map.of(LoadEtlTask.DPP_NORMAL_ALL, "malformed"));
        assertThrows(NumberFormatException.class,
                () -> profile.updateLoadInformation(execution(INSTANCE_ID), report));
        assertEquals(List.of(marker), profile.getSinkCommitInfos());
        // The coordinator marks this failure before retiring the instance, so later validation cannot pass.
        profile.recordPaimonReportFailure();
        assertThrows(StarRocksConnectorException.class, () -> profile.validatePaimonSinkCommitInfos(0));
    }

    @Test
    public void testCoordinatorRejectsMalformedReportBeforeRetiringInstance() {
        PaimonSinkExtra operation = operation("FULL");
        QueryRuntimeProfile profile = profile(operation, 1);
        profile.attachInstances(List.of(INSTANCE_ID));
        FragmentInstanceExecState execution = execution(INSTANCE_ID);
        when(execution.updateExecStatus(any())).thenReturn(true);
        when(execution.isFinished()).thenReturn(true);
        when(execution.getState()).thenReturn(FragmentInstanceExecState.State.FINISHED);
        JobSpec spec = mock(JobSpec.class);
        when(spec.isLoadType()).thenReturn(true);
        when(spec.getSlotProvider()).thenReturn(mock(SlotProvider.class));
        ConnectContext context = mock(ConnectContext.class);
        when(context.getState()).thenReturn(new QueryState());
        ExecutionDAG dag = mock(ExecutionDAG.class);
        when(dag.getExecution(0)).thenReturn(execution);
        DefaultCoordinator coordinator = mock(DefaultCoordinator.class, CALLS_REAL_METHODS);
        Deencapsulation.setField(coordinator, "jobSpec", spec);
        Deencapsulation.setField(coordinator, "executionDAG", dag);
        Deencapsulation.setField(coordinator, "queryProfile", profile);
        Deencapsulation.setField(coordinator, "connectContext", context);
        Deencapsulation.setField(coordinator, "scheduler", mock(ExecutionSchedule.class));
        Deencapsulation.setField(coordinator, "externalResourcesCleared", new java.util.concurrent.atomic.AtomicBoolean(true));
        Deencapsulation.setField(coordinator, "queryStatus", new Status());
        Deencapsulation.setField(coordinator, "lock", new java.util.concurrent.locks.ReentrantLock());

        coordinator.updateFragmentExecStatus(report(INSTANCE_ID, List.of()));

        assertFalse(coordinator.getExecStatus().ok());
        assertThrows(StarRocksConnectorException.class, () -> profile.validatePaimonSinkCommitInfos(0));
        assertTrue(profile.isFinished(), "malformed reports must fail closed without hanging the query");
    }

    @Test
    public void testMetadataMgrPreservesUnknownTypeAndCause() {
        PaimonCommitUnknownException unknown = new PaimonCommitUnknownException("operation", new IllegalStateException());
        ConnectorMetadata connector = new ConnectorMetadata() {
            @Override
            public void finishSink(String db, String table, List<TSinkCommitInfo> infos,
                                   String branch, Object extra, ConnectContext context) {
                throw unknown;
            }
        };
        MetadataMgr metadata = metadata(connector);
        assertSame(unknown, assertThrows(PaimonCommitUnknownException.class,
                () -> metadata.finishSink("p", "db", "t", List.of(), null, operation("FULL"), null)));
        assertTrue(unknown.getCause() instanceof IllegalStateException);
    }

    @Test
    public void testContextualAbortDefaultPreservesLegacyConnector() {
        AtomicInteger aborts = new AtomicInteger();
        ConnectorMetadata connector = new ConnectorMetadata() {
            @Override
            public void abortSink(String db, String table, List<TSinkCommitInfo> infos) {
                aborts.incrementAndGet();
            }
        };
        metadata(connector).abortSink("p", "db", "t", List.of(), new Object(), null);
        assertEquals(1, aborts.get());
    }

    @Test
    public void testPaimonCtasFailureRetainsTargetInEveryPublicationState() throws Exception {
        ConnectContext context = mock(ConnectContext.class);
        when(context.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        CreateTableAsSelectStmt ctas = mock(CreateTableAsSelectStmt.class);
        CreateTableStmt create = mock(CreateTableStmt.class);
        when(ctas.getCreateTableStmt()).thenReturn(create);
        when(create.getEngineName()).thenReturn("paimon");
        try (MockedStatic<DDLStmtExecutor> ddl = mockStatic(DDLStmtExecutor.class)) {
            for (PaimonSinkExtra.PublicationState state : PaimonSinkExtra.PublicationState.values()) {
                PaimonSinkExtra operation = operation("APPEND");
                if (state != PaimonSinkExtra.PublicationState.WRITING) {
                    operation.transition(PaimonSinkExtra.PublicationState.WRITING, state);
                }
                StmtExecutor executor = new StmtExecutor(context, ctas);
                Deencapsulation.setField(executor, "paimonOperation", operation);
                executor.dropTableCreatedByCTAS(ctas);
                String cleanup = Deencapsulation.getField(executor, "paimonCtasCleanup");
                assertTrue(cleanup.contains("target retained"));
            }
            ddl.verifyNoInteractions();
        }
    }

    @Test
    public void testExplicitTransactionRejectsPaimonCtasBeforeCreate() {
        ConnectContext context = mock(ConnectContext.class);
        when(context.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        when(context.getTxnId()).thenReturn(7L);
        CreateTableAsSelectStmt ctas = mock(CreateTableAsSelectStmt.class);
        CreateTableStmt create = mock(CreateTableStmt.class);
        when(ctas.getCreateTableStmt()).thenReturn(create);
        when(create.getEngineName()).thenReturn("paimon");
        MetadataMgr metadata = mock(MetadataMgr.class);
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        when(context.getGlobalStateMgr()).thenReturn(state);
        when(state.getMetadataMgr()).thenReturn(metadata);

        StmtExecutor executor = new StmtExecutor(context, ctas);
        assertThrows(com.starrocks.sql.analyzer.SemanticException.class,
                () -> executor.handleCreateTableAsSelectStmt(0));
        verifyNoInteractions(metadata);
    }

    private static MetadataMgr metadata(ConnectorMetadata connector) {
        return new MetadataMgr(mock(LocalMetastore.class), mock(TemporaryTableMgr.class),
                mock(ConnectorMgr.class), mock(ConnectorTblMetaInfoMgr.class)) {
            @Override
            public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
                return Optional.of(connector);
            }
        };
    }

    private static PaimonSinkExtra operation(String mode) {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.STRING())
                .partitionKeys("dt");
        Schema schema = builder.build();
        Map<String, String> options = Map.of("bucket", "-1", "file.format", "parquet");
        TableSchema tableSchema = new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                schema.partitionKeys(), schema.primaryKeys(), options, null);
        FileStoreTable nativeTable = mock(FileStoreTable.class);
        when(nativeTable.schema()).thenReturn(tableSchema);
        when(nativeTable.options()).thenReturn(options);
        when(nativeTable.location()).thenReturn(new Path("file:///test/paimon/t"));
        when(nativeTable.partitionKeys()).thenReturn(schema.partitionKeys());
        PaimonTable table = mock(PaimonTable.class);
        when(table.getNativeTable()).thenReturn(nativeTable);
        when(table.getNativeWriteIdentity()).thenReturn("table-identity");
        return PaimonSinkExtra.from(table, mode, mode.equals("STATIC") ? Map.of("dt", "2026-10-29") : Map.of(),
                "operation_1", "UTC");
    }

    private static QueryRuntimeProfile profile(PaimonSinkExtra operation, int dop) {
        QueryRuntimeProfile profile = new QueryRuntimeProfile(null, mock(JobSpec.class), false);
        profile.initPaimonSink(operation, Map.of(INSTANCE_ID, dop));
        return profile;
    }

    private static FragmentInstanceExecState execution(TUniqueId instanceId) {
        FragmentInstanceExecState execution = mock(FragmentInstanceExecState.class);
        when(execution.getInstanceId()).thenReturn(instanceId);
        return execution;
    }

    private static TSinkCommitInfo marker(PaimonSinkExtra operation, TUniqueId instanceId, int driver, long rows) {
        TPaimonCommitInfo info = new TPaimonCommitInfo();
        info.setOperation_id(operation.getOperationId());
        info.setProducer_id(DebugUtil.printId(instanceId) + ":" + driver);
        info.setAttempt_id(0);
        info.setSerializer_version(12);
        info.setCommit_messages(ByteBuffer.allocate(4).putInt(0).array());
        info.setSchema_id(operation.getSchemaId());
        info.setTable_identity(operation.getTableIdentity());
        info.setRow_count(rows);
        info.setPrepare_complete(true);
        TSinkCommitInfo result = new TSinkCommitInfo();
        result.setPaimon_commit_info(info);
        return result;
    }

    private static TReportExecStatusParams report(TUniqueId instanceId, List<TSinkCommitInfo> markers) {
        TReportExecStatusParams report = new TReportExecStatusParams();
        report.setFragment_instance_id(instanceId);
        report.setDone(true);
        report.setStatus(new TStatus(TStatusCode.OK));
        report.setSink_commit_infos(markers);
        return report;
    }

    private static final class Fixture implements AutoCloseable {
        private final ConnectContext context = mock(ConnectContext.class);
        private final QueryState queryState = new QueryState();
        private final MetadataMgr metadata = mock(MetadataMgr.class);
        private final GlobalTransactionMgr transactions = mock(GlobalTransactionMgr.class);
        private final LoadMgr loads = mock(LoadMgr.class);
        private final DefaultCoordinator coordinator = mock(DefaultCoordinator.class);
        private final PaimonSinkExtra operation;
        private final QueryRuntimeProfile profile;
        private final ExecPlan plan = mock(ExecPlan.class);
        private final InsertStmt statement;
        private final StmtExecutor executor;
        private final MockedStatic<GlobalStateMgr> global;
        private final MockedStatic<AnalyzerUtils> analyzer;
        private final MockedConstruction<DefaultCoordinator.Factory> factory;
        private RuntimeException schedulerFailure;

        private Fixture(String mode, long rows, long filteredRows) throws Exception {
            MetricRepo.init();
            GlobalStateMgr state = mock(GlobalStateMgr.class);
            when(state.getMetadataMgr()).thenReturn(metadata);
            when(state.getGlobalTransactionMgr()).thenReturn(transactions);
            when(state.getLoadMgr()).thenReturn(loads);
            when(metadata.getDb(context, "p", "db")).thenReturn(new Database(100, "db"));
            global = mockStatic(GlobalStateMgr.class);
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            analyzer = mockStatic(AnalyzerUtils.class);
            analyzer.when(() -> AnalyzerUtils.isStatisticsJob(any(), any())).thenReturn(false);
            SessionVariable variables = new SessionVariable();
            variables.setInsertMaxFilterRatio(0);
            when(context.getSessionVariable()).thenReturn(variables);
            when(context.getGlobalStateMgr()).thenReturn(state);
            when(context.getState()).thenReturn(queryState);
            when(context.getSerializer()).thenReturn(MysqlSerializer.newInstance());
            when(context.getExecutionId()).thenReturn(new TUniqueId(5, 6));
            when(context.getQueryId()).thenReturn(new UUID(5, 6));
            ConnectContext.threadLocalInfo.set(context);
            operation = operation(mode);
            profile = profile(operation, 1);
            profile.updateLoadInformation(execution(INSTANCE_ID), report(INSTANCE_ID,
                    List.of(marker(operation, INSTANCE_ID, 0, rows))));
            TableRef target = new TableRef(QualifiedName.of(List.of("p", "db", "t")), null, NodePosition.ZERO);
            statement = new InsertStmt(target, null, null, null, mock(QueryStatement.class), !mode.equals("APPEND"),
                    Map.of(), NodePosition.ZERO);
            statement.setTargetTable(new Table(Table.TableType.PAIMON));
            statement.setOrigStmt(new OriginStatement("insert into p.db.t select 1", 0));
            PlanFragment fragment = mock(PlanFragment.class);
            PaimonTableSink sink = mock(PaimonTableSink.class);
            when(sink.getPaimonSinkExtra()).thenReturn(operation);
            when(fragment.getSink()).thenReturn(sink);
            when(fragment.getPlanRoot()).thenReturn(mock(PlanNode.class));
            when(plan.getFragments()).thenReturn(List.of(fragment));
            when(plan.getScanNodes()).thenReturn(List.of());
            when(plan.getDescTbl()).thenReturn(new DescriptorTable());
            when(coordinator.getQueryRuntimeProfile()).thenReturn(profile);
            when(coordinator.getSinkCommitInfos()).thenReturn(profile.getSinkCommitInfos());
            when(coordinator.isDone()).thenReturn(true);
            when(coordinator.getExecStatus()).thenReturn(new Status());
            when(coordinator.getTrackingUrl()).thenReturn("");
            Map<String, String> counters = new HashMap<>();
            counters.put(LoadEtlTask.DPP_NORMAL_ALL, Long.toString(rows));
            counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, Long.toString(filteredRows));
            when(coordinator.getLoadCounters()).thenReturn(counters);
            factory = mockConstruction(DefaultCoordinator.Factory.class, (mock, construction) ->
                    when(mock.createInsertScheduler(any(), anyList(), anyList(), any(), any())).thenAnswer(invocation -> {
                        if (schedulerFailure != null) {
                            throw schedulerFailure;
                        }
                        return coordinator;
                    }));
            executor = new StmtExecutor(context, statement);
        }

        @Override
        public void close() {
            factory.close();
            analyzer.close();
            global.close();
            ConnectContext.remove();
        }
    }
}
