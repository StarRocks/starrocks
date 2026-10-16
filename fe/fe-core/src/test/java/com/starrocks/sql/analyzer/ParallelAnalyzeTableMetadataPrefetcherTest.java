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

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import mockit.Delegate;
import mockit.Expectations;
import mockit.FullVerifications;
import mockit.Injectable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

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
    void testAcceptsUnanalyzedDmlParsedFromSql(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        expectPrefetch(metadataMgr, session, "external", "db", "insert_source", 1);

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

        List<String> messages = capturePrefetcherLogs(() ->
                ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr));

        Assertions.assertTrue(messages.stream().anyMatch(message ->
                message.contains("query [" + session.getQueryId() + "], table [external.db.failing_table]")));

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBindsInitiatingSession(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession("external", "db");
        StatementBase statement = parse("select * from external.db.t1", session);

        new Expectations() {
            {
                metadataMgr.prefetchTableMetadataForAnalyze(session.getQueryId().toString(), "external", "db", "t1",
                        session);
                result = new Delegate<Void>() {
                    void prefetchTableMetadataForAnalyze(String queryId, String catalogName, String databaseName,
                                                         String tableName, ConnectContext context) {
                        Assertions.assertSame(session, ConnectContext.get());
                    }
                };
            }
        };

        ParallelAnalyzeTableMetadataPrefetcher.prefetch(statement, session, metadataMgr);
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

    private static List<String> capturePrefetcherLogs(ThrowingRunnable body) throws Exception {
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
        return collector.messages;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static class MessageCollector extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        MessageCollector() {
            super("parallel-analyze-table-metadata-prefetcher-message-collector", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
