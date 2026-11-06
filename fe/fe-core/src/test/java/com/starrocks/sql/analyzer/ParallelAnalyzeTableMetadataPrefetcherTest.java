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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.PaimonView;
import com.starrocks.catalog.Table;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import mockit.Delegate;
import mockit.Expectations;
import mockit.FullVerifications;
import mockit.Injectable;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ParallelAnalyzeTableMetadataPrefetcherTest {
    @Test
    void testDoesNothingWhenDisabled(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        session.getSessionVariable().setEnableParallelAnalyzeTableMetadata(false);
        StatementBase statement = parse("select * from external.db.t1", session);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesUniqueTablesAndExcludesCteReferences(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("with cte as (select * from external.db.t1) " +
                "select * from cte join external.db.t1 duplicate_table on true " +
                "join external.db.t2 on true", session);

        expectPrefetch(metadataMgr, session, "external", "db", "t1", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "t2", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testUsesSessionDefaultsAndSkipsInternalCatalog(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select * from t1 join default_catalog.db.internal_table on true", session);

        expectPrefetch(metadataMgr, session, "external", "db", "t1", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesTablesInSelectListScalarSubqueries(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select (select count(*) from external.db.t1), " +
                "(select count(*) from external.db.t2)", session);

        expectPrefetch(metadataMgr, session, "external", "db", "t1", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "t2", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesSourceTablesInUnanalyzedDmlParsedFromSql(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        expectPrefetch(metadataMgr, session, "external", "db", "insert_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "update_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "delete_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "merge_source", 1);

        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("insert into default_catalog.db.target select * from external.db.insert_source", session),
                session, metadataMgr));
        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("update default_catalog.db.target set value = source.value " +
                        "from external.db.update_source source where true", session),
                session, metadataMgr));
        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("delete from default_catalog.db.target using external.db.delete_source source where true", session),
                session, metadataMgr));
        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("merge into default_catalog.db.target target using external.db.merge_source source " +
                        "on target.id = source.id when matched then update set value = source.value", session),
                session, metadataMgr));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesDmlCteSourceWithoutPrefetchingCteReference(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        expectPrefetch(metadataMgr, session, "external", "db", "update_cte_source", 1);

        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("with update_cte as (select * from external.db.update_cte_source) " +
                        "update default_catalog.db.target set value = source.value " +
                        "from update_cte source where true", session),
                session, metadataMgr));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesTablesInUnanalyzedDmlExpressions(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        expectPrefetch(metadataMgr, session, "external", "db", "update_assignment_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "update_predicate_source1", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "update_predicate_source2", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "delete_predicate_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "merge_condition_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "merge_when_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "merge_assignment_source", 1);

        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("update default_catalog.db.target set value = " +
                        "(select value from external.db.update_assignment_source) " +
                        "where a in (select a from external.db.update_predicate_source1) " +
                        "and b in (select b from external.db.update_predicate_source2)", session),
                session, metadataMgr));
        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("delete from default_catalog.db.target where a in " +
                        "(select a from external.db.delete_predicate_source)", session),
                session, metadataMgr));
        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("merge into default_catalog.db.target target using (select 1 as id) source " +
                        "on target.id = source.id and target.id in " +
                        "(select id from external.db.merge_condition_source) " +
                        "when matched and target.id in (select id from external.db.merge_when_source) " +
                        "then update set value = " +
                        "(select max(value) from external.db.merge_assignment_source)", session),
                session, metadataMgr));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchesTablesBelowPivotRelations(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        expectPrefetch(metadataMgr, session, "external", "db", "pivot_source1", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "pivot_source2", 1);

        Assertions.assertDoesNotThrow(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("select * from external.db.pivot_source1 " +
                        "pivot (sum(value) for category in ('a')) union all " +
                        "select * from external.db.pivot_source2 " +
                        "pivot (sum(value) for category in ('b'))", session),
                session, metadataMgr));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testLogsAndDefersPrefetchFailuresToNormalAnalysis(@Injectable MetadataMgr metadataMgr) throws Exception {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select * from external.db.failing_table " +
                "join external.db.other_table on true", session);

        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), "external", "db",
                        "failing_table", session);
                result = new RuntimeException("prefetch failed");
                times = 1;

                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), "external", "db",
                        "other_table", session);
                times = 1;
            }
        };

        MessageCollector logs = capturePrefetcherLogs(() ->
                ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr));

        Assertions.assertTrue(logs.messages.stream().anyMatch(message ->
                message.contains("query [" + session.getQueryId() + "], table [external.db.failing_table]")));
        Assertions.assertTrue(logs.throwables.stream().anyMatch(throwable ->
                throwable instanceof RuntimeException && "prefetch failed".equals(throwable.getMessage())));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBindsInitiatingSession(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select * from external.db.t1", session);
        AtomicReference<ConnectContext> workerContext = new AtomicReference<>();

        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), "external", "db", "t1",
                        session);
                result = new Delegate<Table>() {
                    Table prefetchTableMetadataForAnalyze(String queryId, String catalogName, String databaseName,
                                                         String tableName, ConnectContext context) {
                        workerContext.set(ConnectContext.get());
                        return null;
                    }
                };
            }
        };

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        Assertions.assertSame(session, workerContext.get());
    }

    @Test
    void testExpandsMixedNestedViewsAndDeduplicates(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select * from v join v duplicate_view on true join shared on true", session);
        List<String> originalNames = AnalyzerUtils.collectTableRelations(statement).stream()
                .map(relation -> relation.getName().toString()).collect(Collectors.toList());
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "v",
                view("v", "select * from nested join shared on true"));
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "nested",
                new IcebergView(2, "external", "db", "nested", List.of(),
                        "select * from external.db.shared join paimon_view on true", "paimon", "view_db", null, Map.of()));
        expectResolvedPrefetch(metadataMgr, session, "paimon", "view_db", "paimon_view",
                new PaimonView(3, "paimon", "view_db", "paimon_view", List.of(),
                        "select * from external.db.shared join leaf on true"));
        expectPrefetch(metadataMgr, session, "external", "db", "shared", 1);
        expectPrefetch(metadataMgr, session, "paimon", "view_db", "leaf", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

        Assertions.assertEquals(originalNames, AnalyzerUtils.collectTableRelations(statement).stream()
                .map(relation -> relation.getName().toString()).collect(Collectors.toList()));
        Assertions.assertTrue(AnalyzerUtils.collectTableRelations(statement).stream()
                .allMatch(relation -> relation.getTable() == null));
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testHiveViewDependenciesRespectNestedCteScopes(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "caller_db");
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "v", view("v",
                "with shadowed as (select * from cte_source) " +
                        "select (select count(*) from scalar_source) from shadowed " +
                        "join external.db.shadowed physical_table on true " +
                        "join (with nested_cte as (select * from nested_source) " +
                        "select * from nested_cte) nested_query on true"));
        expectPrefetch(metadataMgr, session, "external", "db", "cte_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "shadowed", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "scalar_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "nested_source", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(parse("select * from external.db.v", session), session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testIcebergViewDefaultsAndExplicitQualification(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "caller_db");
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "v",
                new IcebergView(1, "external", "db", "v", List.of(),
                        "with shadowed as (select * from cte_source) " +
                                "select (select count(*) from other.other_db.scalar_source) from shadowed " +
                                "join external.db.shadowed physical_table on true " +
                                "join default_catalog.db.internal_table on true " +
                                "join (with nested_cte as (select * from nested_source) " +
                                "select * from nested_cte) nested_query on true", "defaults", "default_db", null, Map.of()));
        expectPrefetch(metadataMgr, session, "defaults", "default_db", "cte_source", 1);
        expectPrefetch(metadataMgr, session, "defaults", "default_db", "nested_source", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "shadowed", 1);
        expectPrefetch(metadataMgr, session, "other", "other_db", "scalar_source", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(parse("select * from external.db.v", session), session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testCyclicViewsTerminateWithOneWorker(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        session.getSessionVariable().setPrepareMetadataPoolSize(1);
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "a", view("a", "select * from b"));
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "b",
                view("b", "select * from a join leaf on true"));
        expectPrefetch(metadataMgr, session, "external", "db", "leaf", 1);

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(parse("select * from a", session), session, metadataMgr);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testViewParsingFailureRestoresContextAndContinuesOtherBranches(@Injectable MetadataMgr metadataMgr)
            throws Exception {
        ConnectContext session = newSession("external", "db");
        ConnectContext previousContext = newSession("other", "other_db");
        Thread coordinator = Thread.currentThread();
        HiveView broken = new HiveView(1, "external", "db", "broken", List.of(), "select (", HiveView.Type.Trino) {
            @Override
            public QueryStatement getQueryStatement() {
                Assertions.assertSame(coordinator, Thread.currentThread());
                Assertions.assertSame(session, ConnectContext.get());
                throw new IllegalArgumentException("invalid view definition");
            }
        };
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "broken", broken);
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "healthy", view("healthy", "select * from leaf"));
        expectPrefetch(metadataMgr, session, "external", "db", "missing", 1);
        expectPrefetch(metadataMgr, session, "external", "db", "leaf", 1);
        ConnectContext originalContext = ConnectContext.get();
        previousContext.setThreadLocalInfo();
        try {
            MessageCollector logs = capturePrefetcherLogs(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                    parse("select * from broken join healthy on true join missing on true", session), session, metadataMgr));
            Assertions.assertSame(previousContext, ConnectContext.get());
            Assertions.assertTrue(logs.messages.stream().anyMatch(message ->
                    message.contains("table [external.db.broken]")));
            Assertions.assertTrue(logs.throwables.stream().anyMatch(throwable ->
                    "invalid view definition".equals(throwable.getMessage())));
        } finally {
            if (originalContext == null) {
                ConnectContext.remove();
            } else {
                originalContext.setThreadLocalInfo();
            }
        }
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testTrinoViewRestoresDialectAndDefersMalformedDefinition(@Injectable MetadataMgr metadataMgr) throws Exception {
        ConnectContext session = newSession("external", "db");
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "trino_view",
                new HiveView(1, "external", "db", "trino_view", List.of(), "select * from leaf", HiveView.Type.Trino));
        expectResolvedPrefetch(metadataMgr, session, "external", "db", "broken",
                new HiveView(2, "external", "db", "broken", List.of(), "select (", HiveView.Type.Trino));
        expectPrefetch(metadataMgr, session, "external", "db", "leaf", 1);
        String dialect = session.getSessionVariable().getSqlDialect();

        MessageCollector logs = capturePrefetcherLogs(() -> ParallelAnalyzeTableMetadataPrefetcher.prefetch(
                parse("select * from trino_view join broken on true", session), session, metadataMgr));

        Assertions.assertEquals(dialect, session.getSessionVariable().getSqlDialect());
        Assertions.assertTrue(logs.messages.stream().anyMatch(message -> message.contains("table [external.db.broken]")));
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testSingleViewPrefetchesFiftyTablesConcurrentlyAndAnalysisReusesQueryCache(
            @Injectable LocalMetastore localMetastore, @Injectable TemporaryTableMgr temporaryTableMgr,
            @Injectable ConnectorMgr connectorMgr, @Injectable ConnectorTblMetaInfoMgr tableMetaInfoMgr,
            @Injectable CatalogConnector connector, @Injectable IHiveMetastore backend,
            @Injectable RemoteFileOperations fileOps) {
        ConnectContext session = newSession("external", "db");
        String viewSql = IntStream.range(0, 50).mapToObj(i -> "select id from t" + i)
                .collect(Collectors.joining(" union all "));
        HiveView rootView = view("all_events", viewSql);
        CountDownLatch firstTwoLoads = new CountDownLatch(2);
        AtomicBoolean overlapped = new AtomicBoolean();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Map<String, AtomicInteger> loads = new ConcurrentHashMap<>();
        Map<String, Table> loadedTables = new ConcurrentHashMap<>();
        new Expectations() {
            {
                backend.getTable("db", anyString);
                result = new Delegate<Table>() {
                    Table load(String dbName, String tableName) throws InterruptedException {
                        Assertions.assertSame(session, ConnectContext.get());
                        loads.computeIfAbsent(tableName, key -> new AtomicInteger()).incrementAndGet();
                        if (tableName.equals("all_events")) {
                            return rootView;
                        }
                        int running = active.incrementAndGet();
                        peak.accumulateAndGet(running, Math::max);
                        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, "Hive.loadTable")) {
                            firstTwoLoads.countDown();
                            overlapped.compareAndSet(false, firstTwoLoads.await(5, TimeUnit.SECONDS));
                            Table table = new Table(1, tableName, Table.TableType.HIVE, List.of());
                            loadedTables.put(tableName, table);
                            return table;
                        } finally {
                            active.decrementAndGet();
                        }
                    }
                };
            }
        };
        CachingHiveMetastore queryCache = CachingHiveMetastore.createQueryLevelInstance(backend, 100);
        HiveMetadata metadata = new HiveMetadata("external", null,
                new HiveMetastoreOperations(queryCache, false, new Configuration(),
                        MetastoreType.HMS, "external"), fileOps, null, Optional.empty(), Runnable::run,
                new ConnectorProperties(ConnectorType.HIVE));
        CatalogConnectorMetadata routedMetadata = new CatalogConnectorMetadata(
                metadata, new InformationSchemaMetadata("external"), null);
        new Expectations() {
            {
                connectorMgr.getConnector("external");
                result = connector;
                connector.getMetadata();
                result = routedMetadata;
                times = 1;
            }
        };
        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, connectorMgr, tableMetaInfoMgr);
        ConnectContext previousContext = ConnectContext.get();
        Tracers previousTracers = Tracers.get();
        Tracers.set(new Tracers());
        session.setThreadLocalInfo();
        Tracers.register();
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.EXTERNAL, true, false);
        try {
            ParallelAnalyzeTableMetadataPrefetcher.prefetch(parse("select * from all_events", session), session, metadataMgr);
            Assertions.assertTrue(overlapped.get(), "one root view must allow overlapping base-table loads");
            Assertions.assertEquals(2, peak.get(), "concurrency must respect the configured pool size");
            Assertions.assertEquals(51, loads.size());
            Assertions.assertSame(rootView, metadataMgr.getTable(session, "external", "db", "all_events"));
            for (int i = 0; i < 50; i++) {
                Assertions.assertSame(loadedTables.get("t" + i), metadataMgr.getTable(session, "external", "db", "t" + i));
            }
            Assertions.assertTrue(loads.values().stream().allMatch(count -> count.get() == 1),
                    "later analysis lookups must reuse prefetched tables with catalog caching disabled");
            String trace = Tracers.printScopeTimer();
            Assertions.assertTrue(trace.contains("EXTERNAL.parallel_analyze_table_metadata[1]"), trace);
            Assertions.assertTrue(trace.contains("EXTERNAL.expand_view_dependencies[1]"), trace);
            Assertions.assertTrue(trace.contains("Hive.loadTable[50]"), trace);
        } finally {
            metadataMgr.removeQueryMetadata();
            Tracers.close();
            Tracers.set(previousTracers);
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testSlowSiblingDoesNotDelayNestedViewDependencies(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        String queryId = session.getQueryId().toString();
        CountDownLatch leafStarted = new CountDownLatch(1);
        AtomicBoolean leafStartedWhileSlowBlocked = new AtomicBoolean();
        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(queryId, "external", "db", "slow", session);
                result = new Delegate<Table>() {
                    Table prefetchTableMetadataForAnalyze(String queryId, String catalogName, String databaseName,
                                                          String tableName, ConnectContext context)
                            throws InterruptedException {
                        // Batch expansion would deadlock here: leaf is only submitted after slow returns.
                        leafStartedWhileSlowBlocked.set(leafStarted.await(5, TimeUnit.SECONDS));
                        return null;
                    }
                };
                times = 1;

                metadataMgr.prefetchTableMetadataForAnalyze(queryId, "external", "db", "v", session);
                result = view("v", "select * from leaf");
                times = 1;

                metadataMgr.prefetchTableMetadataForAnalyze(queryId, "external", "db", "leaf", session);
                result = new Delegate<Table>() {
                    Table prefetchTableMetadataForAnalyze(String queryId, String catalogName, String databaseName,
                                                          String tableName, ConnectContext context) {
                        leafStarted.countDown();
                        return null;
                    }
                };
                times = 1;
            }
        };

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(parse("select * from slow join v on true", session), session,
                metadataMgr);

        Assertions.assertTrue(leafStartedWhileSlowBlocked.get());
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testInterruptedCoordinatorWaitsForRunningLookupsAndSkipsQueuedOnes(@Injectable MetadataMgr metadataMgr)
            throws Exception {
        ConnectContext session = newSession("external", "db");
        session.getSessionVariable().setPrepareMetadataPoolSize(1);
        String queryId = session.getQueryId().toString();
        CountDownLatch runningStarted = new CountDownLatch(1);
        CountDownLatch releaseRunning = new CountDownLatch(1);
        AtomicBoolean runningFinished = new AtomicBoolean();
        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(queryId, "external", "db", "running", session);
                result = new Delegate<Table>() {
                    Table prefetchTableMetadataForAnalyze(String queryId, String catalogName, String databaseName,
                                                          String tableName, ConnectContext context) {
                        runningStarted.countDown();
                        try {
                            releaseRunning.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        runningFinished.set(true);
                        return null;
                    }
                };
                times = 1;
            }
        };
        StatementBase statement = parse("select * from running join queued on true", session);
        CountDownLatch returned = new CountDownLatch(1);
        AtomicBoolean finishedBeforeReturn = new AtomicBoolean();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        Thread coordinator = new Thread(() -> {
            ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);
            finishedBeforeReturn.set(runningFinished.get());
            interruptPreserved.set(Thread.currentThread().isInterrupted());
            returned.countDown();
        });

        coordinator.start();
        Assertions.assertTrue(runningStarted.await(5, TimeUnit.SECONDS));
        coordinator.interrupt();
        boolean returnedWhileLookupRunning = returned.await(200, TimeUnit.MILLISECONDS);
        releaseRunning.countDown();
        coordinator.join(TimeUnit.SECONDS.toMillis(5));

        Assertions.assertFalse(returnedWhileLookupRunning);
        Assertions.assertTrue(finishedBeforeReturn.get());
        Assertions.assertTrue(interruptPreserved.get());
        // The queued lookup must never reach MetadataMgr, or it would re-register query metadata after cleanup.
        new FullVerifications(metadataMgr) {
        };
    }

    @ParameterizedTest
    @CsvSource({
            "TrInO, Hive, true, select * from `leaf`, true",
            "TrInO, Hive, false, select * from leaf, true",
            "TrInO, Hive, true, select (, false",
            "starrocks, Trino, false, select * from leaf, true",
            "starrocks, Trino, true, select (, false"
    })
    void testTrinoViewParsingPreservesAliasCaseSetting(String dialect, HiveView.Type viewType,
                                                      boolean aliasCaseInsensitive, String viewSql,
                                                      boolean hasDependencies, @Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        session.getSessionVariable().setSqlDialect(dialect);
        ConnectContext previousContext = ConnectContext.get();
        session.setThreadLocalInfo();
        try {
            StatementBase statement = parse("with cte as (select a.id from external.db.t A) " +
                    "select * from cte join external.db.v on true", session);
            session.setRelationAliasCaseInSensitive(aliasCaseInsensitive);
            expectPrefetch(metadataMgr, session, "external", "db", "t", 1);
            expectResolvedPrefetch(metadataMgr, session, "external", "db", "v",
                    new HiveView(1, "external", "db", "v", List.of(), viewSql, viewType));
            if (hasDependencies) {
                expectPrefetch(metadataMgr, session, "external", "db", "leaf", 1);
            }

            ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);

            Assertions.assertEquals(aliasCaseInsensitive, session.isRelationAliasCaseInsensitive());
            Assertions.assertSame(session, ConnectContext.get());
            new FullVerifications(metadataMgr) {
            };
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private static HiveView view(String name, String sql) {
        return new HiveView(1, "external", "db", name, List.of(), sql, HiveView.Type.Hive);
    }

    private static void expectResolvedPrefetch(MetadataMgr metadataMgr, ConnectContext session,
                                               String catalog, String database, String table, Table resolvedTable) {
        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), catalog, database, table, session);
                result = resolvedTable;
                times = 1;
            }
        };
    }

    private static ConnectContext newSession(String catalog, String database) {
        ConnectContext session = new ConnectContext();
        session.setCurrentCatalog(catalog);
        session.setDatabase(database);
        session.setQueryId(UUID.randomUUID());
        session.getSessionVariable().setEnableParallelAnalyzeTableMetadata(true);
        session.getSessionVariable().setPrepareMetadataPoolSize(2);
        return session;
    }

    private static StatementBase parse(String sql, ConnectContext session) {
        return SqlParser.parse(sql, session.getSessionVariable()).get(0);
    }

    private static void expectPrefetch(MetadataMgr metadataMgr, ConnectContext session,
                                       String catalog, String database, String table, int invocationCount) {
        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), catalog, database, table,
                        session);
                this.times = invocationCount;
            }
        };
    }

    private static MessageCollector capturePrefetcherLogs(ThrowingRunnable body) throws Exception {
        MessageCollector collector = new MessageCollector();
        Logger logger = (Logger) LogManager.getLogger(ParallelAnalyzeTableMetadataPrefetcher.class);
        Level level = logger.getLevel();
        collector.start();
        logger.addAppender(collector);
        logger.setLevel(Level.WARN);
        try {
            body.run();
        } finally {
            logger.setLevel(level);
            logger.removeAppender(collector);
            collector.stop();
        }
        return collector;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static class MessageCollector extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final List<Throwable> throwables = new CopyOnWriteArrayList<>();

        MessageCollector() {
            super("parallel-analyze-table-metadata-prefetcher-message-collector", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
            if (event.getThrown() != null) {
                throwables.add(event.getThrown());
            }
        }
    }
}
