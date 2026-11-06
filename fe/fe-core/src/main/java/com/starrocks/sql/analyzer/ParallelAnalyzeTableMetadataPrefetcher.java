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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.ConnectorView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * Warms connector table metadata before analysis resolves the query tree.
 *
 * <p>Connector metadata is obtained with the owning query id, which preserves query-scoped metadata caches.
 * Workers temporarily bind the initiating context while loading metadata so connector code that reads
 * session-scoped cache policies observes the same settings as normal analysis. Connector view dependencies
 * are discovered as soon as each view resolves, using the same view parsing and qualification as analysis,
 * so a slow lookup only delays its own dependencies.</p>
 */
public class ParallelAnalyzeTableMetadataPrefetcher {
    private static final Logger LOG = LogManager.getLogger(ParallelAnalyzeTableMetadataPrefetcher.class);

    private ParallelAnalyzeTableMetadataPrefetcher() {
    }

    public static void prefetch(StatementBase statement, ConnectContext session) {
        if (!session.getSessionVariable().enableParallelAnalyzeTableMetadata()) {
            return;
        }
        prefetch(statement, session, GlobalStateMgr.getCurrentState().getMetadataMgr());
    }

    @VisibleForTesting
    static void prefetch(StatementBase statement, ConnectContext session, MetadataMgr metadataMgr) {
        if (!session.getSessionVariable().enableParallelAnalyzeTableMetadata()) {
            return;
        }

        UUID queryId = session.getQueryId();
        if (queryId == null) {
            return;
        }

        Set<TableKey> tableKeys = collectTableKeys(statement, session);
        if (tableKeys.isEmpty()) {
            return;
        }

        // A single root view can discover many base tables; do not cap concurrency by the root count.
        int poolSize = Math.max(1, session.getSessionVariable().getPrepareMetadataPoolSize());
        String queryIdString = queryId.toString();
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize,
                new ThreadFactoryBuilder().setNameFormat(
                        String.format("analyze-table-metadata-%s-%%d", queryIdString)).build());
        Tracers ownerTracers = Tracers.get();
        AtomicBoolean cancelled = new AtomicBoolean();

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "EXTERNAL.parallel_analyze_table_metadata")) {
            // Workers only load metadata; this thread owns the visited set, view parsing and tracer merging.
            BlockingQueue<PrefetchResult> completed = new LinkedBlockingQueue<>();
            Set<TableKey> visited = new LinkedHashSet<>(tableKeys);
            int pending = 0;
            for (TableKey tableKey : tableKeys) {
                submit(executorService, completed, cancelled, metadataMgr, queryIdString, tableKey, session,
                        ownerTracers);
                pending++;
            }
            while (pending > 0) {
                PrefetchResult result;
                try {
                    result = completed.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                pending--;
                ownerTracers.mergeFrom(result.tracers);
                if (result.failure != null) {
                    logPrefetchFailure(queryIdString, result.tableKey, result.failure);
                    continue;
                }
                if (!(result.table instanceof ConnectorView)) {
                    continue;
                }
                Set<TableKey> dependencies;
                try {
                    dependencies = collectViewDependencies((ConnectorView) result.table, session);
                } catch (RuntimeException e) {
                    logPrefetchFailure(queryIdString, result.tableKey, e);
                    continue;
                }
                for (TableKey dependency : dependencies) {
                    if (visited.add(dependency)) {
                        submit(executorService, completed, cancelled, metadataMgr, queryIdString, dependency,
                                session, ownerTracers);
                        pending++;
                    }
                }
            }
        } finally {
            cancelled.set(true);
            executorService.shutdown();
            Uninterruptibles.awaitTerminationUninterruptibly(executorService);
        }
    }

    private static void submit(ExecutorService executorService, BlockingQueue<PrefetchResult> completed,
                               AtomicBoolean cancelled, MetadataMgr metadataMgr, String queryId, TableKey tableKey,
                               ConnectContext session, Tracers ownerTracers) {
        Tracers forked = ownerTracers.fork(true);
        // whenComplete also runs for Errors, so every submitted lookup reports back exactly once.
        CompletableFuture.supplyAsync(() -> cancelled.get() ? null :
                        prefetchTable(metadataMgr, queryId, tableKey, session, forked),
                executorService).whenComplete((table, throwable) ->
                completed.add(new PrefetchResult(tableKey, forked, table, throwable)));
    }

    private static Table prefetchTable(MetadataMgr metadataMgr, String queryId, TableKey tableKey,
                                      ConnectContext session, Tracers forkedTracers) {
        Tracers previousTracers = Tracers.get();
        ConnectContext previousContext = ConnectContext.get();
        Tracers.set(forkedTracers);
        session.setThreadLocalInfo();
        try {
            return metadataMgr.prefetchTableMetadataForAnalyze(queryId, tableKey.catalog, tableKey.database, tableKey.table,
                    session);
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
            Tracers.set(previousTracers);
        }
    }

    private static void logPrefetchFailure(String queryId, TableKey tableKey, Throwable throwable) {
        if (throwable != null) {
            Throwable cause = throwable.getCause() == null ? throwable : throwable.getCause();
            LOG.warn("Failed to prefetch metadata for query [{}], table [{}.{}.{}]; " +
                            "analysis will retry the metadata lookup",
                    queryId, tableKey.catalog, tableKey.database, tableKey.table, cause);
        }
    }

    private static Set<TableKey> collectViewDependencies(ConnectorView view, ConnectContext session) {
        // Bind the session so the view body is parsed with the session's SQL dialect, as during analysis.
        ConnectContext previousContext = ConnectContext.get();
        boolean relationAliasCaseInsensitive = session.isRelationAliasCaseInsensitive();
        session.setThreadLocalInfo();
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "EXTERNAL.expand_view_dependencies")) {
            return collectTableKeys(view.getQueryStatement(), session);
        } finally {
            // Trino parsing and dialect fallback change this flag; speculative parsing must preserve it.
            session.setRelationAliasCaseInSensitive(relationAliasCaseInsensitive);
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private static Set<TableKey> collectTableKeys(StatementBase statement, ConnectContext session) {
        Set<TableKey> tableKeys = new LinkedHashSet<>();
        for (TableRelation relation : AnalyzerUtils.collectNonCTETableRelations(statement)) {
            TableName tableName = relation.getName();
            String catalog = Strings.isNullOrEmpty(tableName.getCatalog()) ? session.getCurrentCatalog() :
                    tableName.getCatalog();
            String database = Strings.isNullOrEmpty(tableName.getDb()) ? session.getDatabase() : tableName.getDb();
            String table = tableName.getTbl();
            if (Strings.isNullOrEmpty(catalog) || Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(table)
                    || CatalogMgr.isInternalCatalog(catalog)) {
                continue;
            }
            tableKeys.add(new TableKey(catalog, database, table));
        }
        return tableKeys;
    }

    private record TableKey(String catalog, String database, String table) {
    }

    private record PrefetchResult(TableKey tableKey, Tracers tracers, Table table, Throwable failure) {
    }
}
