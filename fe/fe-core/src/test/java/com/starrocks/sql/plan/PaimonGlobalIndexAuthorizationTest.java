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

package com.starrocks.sql.plan;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.connector.metadata.TableMetaMetadata;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.connector.paimon.PaimonGlobalIndexService;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PaimonGlobalIndexAuthorizationTest extends ConnectorPlanTestBase {
    private static final String CATALOG = "paimon_index_auth";
    private static final String BASE_TABLE = CATALOG + ".db1.indexed_table";
    private static final String INDEX_TABLE = CATALOG + ".db1.`indexed_table$global_index`";
    private static final String QUERY = "SELECT id FROM " + BASE_TABLE + " WHERE id = 1";
    private static Catalog nativeCatalog;
    private static PaimonMetadata metadata;
    private static PaimonTable table;
    private static PaimonGlobalIndexRequest request;
    private static String encodedBitmap;

    @BeforeAll
    public static void prepareIndexedTableAndUsers() throws Exception {
        String warehouse = newFolder(temp, "authorization").toURI().toString();
        nativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        nativeCatalog.createDatabase("db1", false);
        Identifier identifier = Identifier.create("db1", "indexed_table");
        nativeCatalog.createTable(identifier, Schema.newBuilder().column("id", DataTypes.INT())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true").build(), false);
        FileStoreTable nativeTable = (FileStoreTable) nativeCatalog.getTable(identifier);
        BatchWriteBuilder writer = nativeTable.newBatchWriteBuilder();
        try (BatchTableWrite write = writer.newWrite(); BatchTableCommit commit = writer.newCommit()) {
            write.write(GenericRow.of(1));
            write.write(GenericRow.of(2));
            commit.commit(write.prepareCommit());
        }
        SortedGlobalIndexBuilder builder = new SortedGlobalIndexBuilder(nativeTable, "btree").withIndexField("id");
        try (IOManager ioManager = new IOManagerImpl(temp.getAbsolutePath());
                BatchTableCommit commit = nativeTable.newBatchWriteBuilder().newCommit()) {
            List<CommitMessage> messages = new ArrayList<>();
            for (DataSplit split : builder.scan().orElseThrow().getRight()) {
                messages.addAll(builder.build(split, ioManager));
            }
            commit.commit(messages);
        }

        Map<String, String> properties = Map.of("type", "paimon", "paimon.catalog.type", "filesystem",
                "paimon.catalog.warehouse", warehouse);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("paimon", CATALOG, "", properties);
        metadata = spy(new PaimonMetadata(CATALOG, new HdfsEnvironment(), nativeCatalog,
                new ConnectorProperties(ConnectorType.PAIMON, properties)));
        ((MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr()).registerMockedMetadata(CATALOG,
                new CatalogConnectorMetadata(metadata, new InformationSchemaMetadata(CATALOG),
                        new TableMetaMetadata(CATALOG, "paimon")));
        table = (PaimonTable) metadata.getTable(connectContext, "db1", "indexed_table");
        nativeTable = (FileStoreTable) table.getNativeTable();
        IndexCondition condition = new IndexCondition(new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "id", true), ConstantOperator.createInt(1)), Map.of(1, 0));
        request = PaimonGlobalIndexRequest.create(nativeTable.location().toString(), "main",
                nativeTable.latestSnapshot().orElseThrow().id(), nativeTable.schema().id(),
                nativeTable.schema().timeMillis(), condition);
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        rows.add(0L);
        encodedBitmap = Base64.getEncoder().encodeToString(new GlobalIndexResultSerializer()
                .serialize(GlobalIndexResult.create(rows)));

        for (String user : List.of("index_base_only", "index_both", "index_virtual_only", "index_all_tables")) {
            starRocksAssert.withUser(user);
        }
        String previousCatalog = connectContext.getCurrentCatalog();
        try {
            connectContext.setCurrentCatalog(CATALOG);
            executeGrant("GRANT SELECT ON TABLE db1.indexed_table TO USER index_base_only");
            executeGrant("GRANT SELECT ON TABLE db1.indexed_table TO USER index_both");
            executeGrant("GRANT SELECT ON TABLE db1.`indexed_table$global_index` TO USER index_both");
            executeGrant("GRANT SELECT ON TABLE db1.`indexed_table$global_index` TO USER index_virtual_only");
            executeGrant("GRANT SELECT ON ALL TABLES IN DATABASE db1 TO USER index_all_tables");
        } finally {
            connectContext.setCurrentCatalog(previousCatalog);
        }
    }

    @BeforeEach
    public void resetInvocations() {
        clearInvocations(metadata);
    }

    @AfterEach
    public void restoreContext() {
        connectContext.setThreadLocalInfo();
    }

    @AfterAll
    public static void closeCatalog() throws Exception {
        if (nativeCatalog != null) {
            nativeCatalog.close();
        }
    }

    @Test
    public void testInternalIndexUsesCallerVirtualTablePrivilege() throws Exception {
        for (String user : List.of("index_both", "index_all_tables")) {
            clearInvocations(metadata);
            ConnectContext caller = caller(user);
            AtomicInteger executions = mockIndexExecution(caller);

            assertContains(plan(QUERY, caller), "PREDICATES:");

            assertEquals(1, executions.get());
            assertNotNull(lastScanParams().getConnectorIndexResult());
            assertSame(caller, ConnectContext.get());
        }
    }

    @Test
    public void testBaseTableOnlyPrivilegeFallsBackBeforeIndexExecution() throws Exception {
        ConnectContext caller = caller("index_base_only");
        AtomicInteger executions = mockIndexExecution(caller);

        assertContains(plan(QUERY, caller), "PREDICATES:");

        verify(metadata).evaluateIndex(any(Table.class), any(IndexCondition.class), any(TvrVersionRange.class));
        verify(metadata, never()).getIndexShards(any(Table.class), anyLong(), anyMap());
        assertEquals(0, executions.get());
        assertNull(lastScanParams().getConnectorIndexResult());
        assertSame(caller, ConnectContext.get());
    }

    @Test
    public void testVirtualTablePrivilegeDoesNotGrantBaseTableAccess() throws Exception {
        ConnectContext caller = caller("index_virtual_only");
        AtomicInteger executions = mockIndexExecution(caller);

        ErrorReportException error = assertThrows(ErrorReportException.class, () -> plan(QUERY, caller));

        assertTrue(error.getMessage().contains("Access denied"), error.getMessage());
        verify(metadata, never()).evaluateIndex(any(Table.class), any(IndexCondition.class), any(TvrVersionRange.class));
        verify(metadata, never()).getRemoteFiles(any(Table.class), any(GetRemoteFilesParams.class));
        assertEquals(0, executions.get());
    }

    @Test
    public void testDirectVirtualTableQueryKeepsItsOwnPrivilegeCheck() throws Exception {
        String sql = "SELECT to_base64(index_result) FROM " + INDEX_TABLE
                + " WHERE args = '" + request.toTransportString() + "'";
        ConnectContext caller = caller("index_base_only");
        ErrorReportException error = assertThrows(ErrorReportException.class, () -> plan(sql, caller));
        assertTrue(error.getMessage().contains("Access denied"), error.getMessage());
        verify(metadata, never()).getIndexShards(any(Table.class), anyLong(), anyMap());

        assertContains(plan(sql, caller("index_both")), "TABLE: indexed_table$global_index", "SHARDS: 1");
    }

    @Test
    public void testInternalIndexDoesNotInheritCallerAuthorizationBypass() throws Exception {
        ConnectContext caller = caller("index_base_only");
        caller.setBypassAuthorizerCheck(true);
        AtomicInteger executions = mockIndexExecution(caller);

        SemanticException error = assertThrows(SemanticException.class,
                () -> new PaimonGlobalIndexService(table, request).evaluate());

        assertNotNull(error.getCause());
        assertTrue(error.getCause().getMessage().contains("Access denied"), error.getCause().getMessage());
        assertEquals(0, executions.get());
        assertTrue(caller.isBypassAuthorizerCheck());
        assertSame(caller, ConnectContext.get());
    }

    private static ConnectContext caller(String user) throws Exception {
        UserIdentity identity = new UserIdentity(user, "%");
        ConnectContext context = UtFrameUtils.initCtxForNewPrivilege(identity);
        // The test helper defaults to ROOT_ROLE_ID even for a non-root identity.
        context.setCurrentRoleIds(identity);
        context.getSessionVariable().setPaimonGlobalIndexScanStage(2);
        context.setStartTime();
        return context;
    }

    private static String plan(String sql, ConnectContext context) {
        return StatementPlanner.plan(SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable()), context)
                .getExplainString(TExplainLevel.NORMAL);
    }

    private static void executeGrant(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
    }

    private static GetRemoteFilesParams lastScanParams() {
        ArgumentCaptor<GetRemoteFilesParams> params = ArgumentCaptor.forClass(GetRemoteFilesParams.class);
        verify(metadata, atLeastOnce()).getRemoteFiles(any(Table.class), params.capture());
        return params.getValue();
    }

    private static AtomicInteger mockIndexExecution(ConnectContext caller) {
        AtomicInteger executions = new AtomicInteger();
        // Keep the production service, SimpleExecutor, planner, authorization and real BTree
        // discovery. Only replace BE execution, returning the matching row's serialized bitmap.
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                assertSame(caller, context.getParent());
                assertEquals(caller.getCurrentUserIdentity(), context.getCurrentUserIdentity());
                assertEquals(caller.getCurrentRoleIds(), context.getCurrentRoleIds());
                assertFalse(context.isBypassAuthorizerCheck());
                assertContains(plan.getExplainString(TExplainLevel.NORMAL), "TABLE: indexed_table$global_index", "SHARDS: 1");
                executions.incrementAndGet();
                TResultBatch batch = new TResultBatch();
                batch.addToRows(ByteBuffer.wrap(("{\"data\":[\"" + encodedBitmap + "\"]}")
                        .getBytes(StandardCharsets.UTF_8)));
                return Pair.create(List.of(batch), new Status());
            }
        };
        return executions;
    }
}
