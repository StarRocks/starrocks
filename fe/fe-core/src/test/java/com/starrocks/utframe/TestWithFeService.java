// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.utframe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.Backend;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;


/**
 * This is the base class for unit class that wants to start a FE service.
 * Concrete test class must be derived class of {@link TestWithFeService}.
 *
 * This class use {@link TestInstance} in JUnit5 to do initialization and cleanup stuff. Unlike
 * deprecated legacy combination-based implementation {@link UtFrameUtils}, we use an
 * inherit-manner,
 * thus we could wrap common logic in this base class. It's more easy to use.
 * Note:
 * Unit-test method in derived classes must use the JUnit5 {@link org.junit.jupiter.api.Test}
 * annotation, rather than the old JUnit4 {@link org.junit.Test} or others.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestWithFeService {

    protected String starrocksHome;
    protected String runningDir =
            "fe/mocked/" + getClass().getSimpleName() + "/" + UUID.randomUUID() + "/";
    protected ConnectContext connectContext;

    @BeforeAll
    public final void beforeAll() throws Exception {
        beforeCreatingConnectContext();
        connectContext = createDefaultCtx();
        beforeCluster();
        createStarrocksCluster();
        runBeforeAll();
    }

    protected void beforeCluster() {
    }

    @AfterAll
    public final void afterAll() throws Exception {
        runAfterAll();
        GlobalStateMgr.getCurrentState().clear();
        cleanStarrocksFeDir();
    }

    @BeforeEach
    public final void beforeEach() throws Exception {
        runBeforeEach();
    }

    @AfterEach
    public final void afterEach() throws Exception {
        runAfterEach();
    }
    protected void beforeCreatingConnectContext() throws Exception {

    }

    protected void runBeforeAll() throws Exception {
    }

    protected void runAfterAll() throws Exception {
    }

    protected void runBeforeEach() throws Exception {
    }

    protected void runAfterEach() throws Exception {

    }

    // Help to create a mocked ConnectContext.
    protected ConnectContext createDefaultCtx() throws IOException {
        return createCtx(UserIdentity.ROOT, "127.0.0.1");
    }

    protected <T extends StatementBase> T createStmt(String showSql)
            throws Exception {
        return (T) parseAndAnalyzeStmt(showSql, connectContext);
    }

    protected ConnectContext createCtx(UserIdentity user, String host) throws IOException {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getUser());
        ctx.setRemoteIP(host);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    protected StatementBase parseAndAnalyzeStmt(String originStmt) throws Exception {
        return parseAndAnalyzeStmt(originStmt, connectContext);
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    protected StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        System.out.println("begin to parse stmt: " + originStmt);
        List<StatementBase> statementBases = SqlParser.parse(originStmt, ctx.getSessionVariable());
        StatementBase firstStatement = statementBases.get(0);
        Analyzer.analyze(firstStatement, ctx);
        firstStatement.setOrigStmt(new OriginStatement(originStmt, 0));
        return firstStatement;
    }

    protected void createStarrocksCluster() {
        UtFrameUtils.createMinStarRocksCluster(true);
    }

    protected void cleanStarrocksFeDir() {
        try {
            cleanDir(starrocksHome + "/" + runningDir);
            cleanDir(Config.plugin_dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createDatabase(String db) throws Exception {
        GlobalStateMgr.getCurrentState().getMetadata().createDb(db);
    }

    public void useDatabase(String dbName) {
        connectContext.setDatabase(
                ClusterNamespace.getFullName(dbName));
    }

    public void createTable(String sql) throws Exception {
        try {
            createTables(sql);
        } catch (ConcurrentModificationException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void dropTable(String table, boolean force) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) parseAndAnalyzeStmt(
                "drop table " + table + (force ? " force" : "") + ";", connectContext);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
    }

    public void createTables(String... sqls) throws Exception {
        for (String sql : sqls) {
            CreateTableStmt stmt = (CreateTableStmt) parseAndAnalyzeStmt(sql);
            GlobalStateMgr.getCurrentState().createTable(stmt);
        }
        updateReplicaPathHash();
    }

    private void updateReplicaPathHash() {
        com.google.common.collect.Table<Long, Long, Replica> replicaMetaTable =
                GlobalStateMgr.getCurrentInvertedIndex()
                        .getReplicaMetaTable();
        for (com.google.common.collect.Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
            if (be == null) {
                continue;
            }
            Replica replica = cell.getValue();
            TabletMeta tabletMeta =
                    GlobalStateMgr.getCurrentInvertedIndex().getTabletMeta(cell.getRowKey());
            ImmutableMap<String, DiskInfo> diskMap = be.getDisks();
            for (DiskInfo diskInfo : diskMap.values()) {
                if (diskInfo.getStorageMedium() == tabletMeta.getStorageMedium()) {
                    replica.setPathHash(diskInfo.getPathHash());
                    break;
                }
            }
        }
    }

    // clear the specified dir
    private void cleanDir(String dir) throws IOException {
        File localDir = new File(dir);
        if (localDir.exists()) {
            Files.walk(Paths.get(dir))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(file -> {
                        System.out.println("DELETE FE SERVER DIR: " + file.getAbsolutePath());
                        file.delete();
                    });
        } else {
            System.out.println("No need clean DIR: " + dir);
        }
    }
}
