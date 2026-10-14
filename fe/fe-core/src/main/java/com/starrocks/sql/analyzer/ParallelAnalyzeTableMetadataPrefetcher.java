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
import com.starrocks.catalog.TableName;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * Warms connector table metadata before analysis resolves the query tree.
 *
 * <p>Connector metadata is obtained with the owning query id, which preserves query-scoped metadata caches.
 * Workers temporarily bind the initiating context while loading metadata so connector code that reads
 * session-scoped cache policies observes the same settings as normal analysis.</p>
 */
public class ParallelAnalyzeTableMetadataPrefetcher {
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

        List<TableKey> tableKeys = collectTableKeys(statement, session);
        if (tableKeys.isEmpty()) {
            return;
        }

        int poolSize = Math.max(1,
                Math.min(session.getSessionVariable().getPrepareMetadataPoolSize(), tableKeys.size()));
        String queryIdString = queryId.toString();
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize,
                new ThreadFactoryBuilder().setNameFormat(
                        String.format("analyze-table-metadata-%s-%%d", queryIdString)).build());
        Tracers ownerTracers = Tracers.get();

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "EXTERNAL.parallel_analyze_table_metadata")) {
            List<Tracers> forks = new ArrayList<>(tableKeys.size());
            CompletableFuture<?>[] futures = new CompletableFuture[tableKeys.size()];
            for (int i = 0; i < tableKeys.size(); i++) {
                TableKey tableKey = tableKeys.get(i);
                Tracers forked = ownerTracers.fork(true);
                forks.add(forked);
                futures[i] = CompletableFuture.runAsync(() -> prefetchTable(metadataMgr, queryIdString, tableKey,
                        session, forked), executorService).handle((ignoredResult, throwable) -> null);
            }
            CompletableFuture.allOf(futures).join();
            for (Tracers fork : forks) {
                ownerTracers.mergeFrom(fork);
            }
        } finally {
            executorService.shutdown();
        }
    }

    private static void prefetchTable(MetadataMgr metadataMgr, String queryId, TableKey tableKey,
                                      ConnectContext session, Tracers forkedTracers) {
        Tracers previousTracers = Tracers.get();
        ConnectContext previousContext = ConnectContext.get();
        Tracers.set(forkedTracers);
        session.setThreadLocalInfo();
        try {
            metadataMgr.prefetchTableMetadataForAnalyze(queryId, tableKey.catalog, tableKey.database, tableKey.table,
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

    private static List<TableKey> collectTableKeys(StatementBase statement, ConnectContext session) {
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
        return new ArrayList<>(tableKeys);
    }

    private record TableKey(String catalog, String database, String table) {
    }
}
