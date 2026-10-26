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

package com.starrocks.common.proc;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.ShowStmtAnalyzer;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExternalProcContextTest {
    private static final String CATALOG = "session_catalog";
    private ConnectContext expectedContext;
    private StarRocksConnectorException metadataFailure;

    @BeforeEach
    void setUp() {
        ConnectorMetadata metadata = new ConnectorMetadata() {
            @Override
            public List<String> listDbNames(ConnectContext context) {
                assertContext(context);
                return List.of("db");
            }

            @Override
            public Database getDb(ConnectContext context, String dbName) {
                assertContext(context);
                assertEquals("db", dbName);
                if (metadataFailure != null) {
                    throw metadataFailure;
                }
                return new Database(1, dbName);
            }

            @Override
            public List<String> listTableNames(ConnectContext context, String dbName) {
                assertContext(context);
                assertEquals("db", dbName);
                return List.of("tbl");
            }

            @Override
            public Table getTable(ConnectContext context, String dbName, String tableName) {
                assertContext(context);
                assertEquals("db", dbName);
                assertEquals("tbl", tableName);
                return new Table(2, tableName, Table.TableType.HIVE,
                        List.of(new Column("id", IntegerType.INT)));
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
                assertEquals(CATALOG, catalogName);
                return Optional.of(metadata);
            }
        };
        new MockUp<CatalogMgr>() {
            @Mock
            public boolean catalogExists(String catalogName) {
                return CATALOG.equals(catalogName);
            }
        };
    }

    @Test
    void describePreservesEachUsersSession() {
        for (String user : List.of("alice", "bob")) {
            ConnectContext context = session(user);
            try (var ignored = context.bindScope()) {
                DescribeStmt statement = describe(context);
                List<List<String>> rows = ShowExecutor.execute(statement, context).getResultRows();
                assertEquals(1, rows.size());
                assertEquals("id", rows.get(0).get(0));
                assertEquals("INT", rows.get(0).get(1));
                assertEquals(user + "-token", context.getAuthToken());
            }
        }
    }

    @Test
    void reusedDirectoriesResolveTheCurrentSessionForEveryOperation() throws Exception {
        ExternalDbsProcDir databases = new ExternalDbsProcDir(CATALOG);
        ExternalTablesProcDir tables = new ExternalTablesProcDir(CATALOG, "db");
        boolean checkDbState = Config.enable_check_db_state;
        Config.enable_check_db_state = true;
        try {
            for (String user : List.of("alice", "bob")) {
                ConnectContext context = session(user);
                try (var ignored = context.bindScope()) {
                    assertEquals(List.of(List.of("db")), databases.fetchResult().getRows());
                    assertEquals(List.of(List.of("tbl")), databases.lookup("db").fetchResult().getRows());
                    assertEquals(List.of(List.of("tbl")), tables.fetchResult().getRows());
                    ProcDirInterface table = (ProcDirInterface) tables.lookup("tbl");
                    assertEquals("id", table.lookup("schema").fetchResult().getRows().get(0).get(0));
                }
            }
        } finally {
            Config.enable_check_db_state = checkDbState;
        }
    }

    @Test
    void connectorFailuresStillReachTheCaller() {
        ConnectContext context = session("alice");
        metadataFailure = new StarRocksConnectorException("metadata access denied");
        try (var ignored = context.bindScope()) {
            DescribeStmt statement = describe(context);
            assertSame(metadataFailure, assertThrows(StarRocksConnectorException.class,
                    () -> ShowExecutor.execute(statement, context)));
        }
    }

    @Test
    void sessionlessCallsUseAnonymousContextsWithoutRetainingThePreviousUser() throws Exception {
        ConnectContext previous = ConnectContext.get();
        boolean checkDbState = Config.enable_check_db_state;
        Config.enable_check_db_state = true;
        ConnectContext.remove();
        try {
            ExternalDbsProcDir databases = new ExternalDbsProcDir(CATALOG);
            ExternalTablesProcDir tables = new ExternalTablesProcDir(CATALOG, "db");
            for (String user : List.of("alice", "bob")) {
                ConnectContext context = session(user);
                try (var ignored = context.bindScope()) {
                    databases.fetchResult();
                    tables.lookup("tbl");
                }
                expectedContext = null;
                assertEquals(List.of(List.of("db")), databases.fetchResult().getRows());
                assertEquals(List.of(List.of("tbl")), databases.lookup("db").fetchResult().getRows());
                assertEquals(List.of(List.of("tbl")), tables.fetchResult().getRows());
                ProcDirInterface table = (ProcDirInterface) tables.lookup("tbl");
                assertEquals("id", table.lookup("schema").fetchResult().getRows().get(0).get(0));
                ProcNodeInterface schema = ProcService.getInstance().open("/catalog/" + CATALOG + "/db/tbl/schema");
                assertEquals("id", schema.fetchResult().getRows().get(0).get(0));
                assertNull(ConnectContext.get(), "Anonymous fallback must not bind a thread-local session");
            }
        } finally {
            Config.enable_check_db_state = checkDbState;
            ConnectContext.set(previous);
        }
    }

    private void assertContext(ConnectContext context) {
        assertNotNull(context, "Connectors may dereference the context even for sessionless Proc calls");
        if (expectedContext == null) {
            assertNull(context.getCurrentUserIdentity());
            assertNull(context.getAuthToken());
        } else {
            assertSame(expectedContext, context);
        }
    }

    private ConnectContext session(String user) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setAuthToken(user + "-token");
        expectedContext = context;
        return context;
    }

    private DescribeStmt describe(ConnectContext context) {
        DescribeStmt statement = (DescribeStmt) SqlParser.parseSingleStatement(
                "DESCRIBE " + CATALOG + ".db.tbl", context.getSessionVariable().getSqlMode());
        ShowStmtAnalyzer.analyze(statement, context);
        return statement;
    }
}
