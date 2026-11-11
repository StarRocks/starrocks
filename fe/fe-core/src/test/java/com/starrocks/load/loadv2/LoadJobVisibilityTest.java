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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.GetLoadInfoAction;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.Load;
import com.starrocks.load.LoadJobVisibility;
import com.starrocks.load.LoadJobVisibility.Access;
import com.starrocks.load.LoadJobVisibility.VisibleTask;
import com.starrocks.load.batchwrite.MergeCommitTask;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TTrackingLoadInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.load.LoadJobVisibility.Access.FULL;
import static com.starrocks.load.LoadJobVisibility.Access.METADATA;
import static com.starrocks.load.LoadJobVisibility.Access.NONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How much of a load job a user may see: the rule in {@link LoadJobVisibility}, and the information_schema RPCs, SHOW
 * statements, SHOW PROC nodes and the load info API that apply it.
 */
public class LoadJobVisibilityTest {
    private static final String DB = "load_visibility_db";
    private static final long DROPPED_TABLE_ID = 987654321L;
    private static final String TRACKING_URL = "http://127.0.0.1:8040/api/_load_error_log?file=error_log_x";
    // A rejected value, as an error message can quote it.
    private static final String ROW_VALUE = "ssn 123-45-6789";

    private static ConnectContext ctx;
    private static long dbId;
    private static long openTableId;
    private static long secretTableId;
    private static long mvId;

    // none holds no grant, reader may read t_open, writer may only insert into t_secret, both may read both tables,
    // mixed may read t_open and insert into t_secret, loader may read and insert into t_secret, mvReader may only
    // read the materialized view mv_open, and operator holds OPERATE alone.
    private static UserIdentity none;
    private static UserIdentity reader;
    private static UserIdentity writer;
    private static UserIdentity both;
    private static UserIdentity mixed;
    private static UserIdentity loader;
    private static UserIdentity mvReader;
    private static UserIdentity operator;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE t_open (k INT, v VARCHAR(3)) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                .withTable("CREATE TABLE t_secret (k INT, v VARCHAR(3)) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv_open DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "REFRESH DEFERRED MANUAL PROPERTIES ('replication_num' = '1') AS SELECT k, v FROM t_open");

        none = createUser(starRocksAssert, "vis_none");
        reader = createUser(starRocksAssert, "vis_reader");
        writer = createUser(starRocksAssert, "vis_writer");
        both = createUser(starRocksAssert, "vis_both");
        mixed = createUser(starRocksAssert, "vis_mixed");
        loader = createUser(starRocksAssert, "vis_loader");
        mvReader = createUser(starRocksAssert, "vis_mv_reader");
        operator = createUser(starRocksAssert, "vis_operator");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO USER vis_reader");
        starRocksAssert.ddl("GRANT INSERT ON TABLE " + DB + ".t_secret TO USER vis_writer");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO USER vis_both");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_secret TO USER vis_both");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO USER vis_mixed");
        starRocksAssert.ddl("GRANT INSERT ON TABLE " + DB + ".t_secret TO USER vis_mixed");
        starRocksAssert.ddl("GRANT SELECT, INSERT ON TABLE " + DB + ".t_secret TO USER vis_loader");
        starRocksAssert.ddl("GRANT SELECT ON MATERIALIZED VIEW " + DB + ".mv_open TO USER vis_mv_reader");
        starRocksAssert.ddl("GRANT OPERATE ON SYSTEM TO USER vis_operator");

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        dbId = metastore.getDb(DB).getId();
        openTableId = metastore.getTable(DB, "t_open").getId();
        secretTableId = metastore.getTable(DB, "t_secret").getId();
        mvId = metastore.getTable(DB, "mv_open").getId();
    }

    @AfterEach
    public void restoreRoot() {
        switchTo(UserIdentity.ROOT);
    }

    @Test
    public void testLoadJobNeedsPrivilegeOnEveryTargetTable(@Injectable LoadJob multiTableJob,
                                                             @Injectable LoadJob unanalyzedJob) throws Exception {
        new Expectations() {
            {
                multiTableJob.getDbId();
                minTimes = 0;
                result = dbId;
                multiTableJob.getTableIds();
                minTimes = 0;
                result = Sets.newHashSet(openTableId, secretTableId);

                // A job whose target tables are not known, e.g. a broker load whose statement failed to analyze.
                unanalyzedJob.getDbId();
                minTimes = 0;
                result = dbId;
                unanalyzedJob.getTableIds();
                minTimes = 0;
                result = Sets.newHashSet();
            }
        };
        LoadJob onOpen = new InsertLoadJob("on_open", dbId, openTableId, System.currentTimeMillis(), "", "", null);
        LoadJob onSecret = new InsertLoadJob("on_secret", dbId, secretTableId, System.currentTimeMillis(), "", "", null);
        LoadJob onDropped = new InsertLoadJob("on_dropped", dbId, DROPPED_TABLE_ID, System.currentTimeMillis(), "", "",
                null);

        assertEquals(FULL, access(reader, onOpen));
        assertEquals(NONE, access(reader, onSecret));
        assertEquals(NONE, access(none, onOpen));
        // A privilege other than SELECT shows the job without its rows.
        assertEquals(METADATA, access(writer, onSecret));
        // OPERATE shows any job without its rows.
        assertEquals(METADATA, access(operator, onOpen));

        // The least access over the target tables counts.
        assertEquals(NONE, access(reader, multiTableJob));
        assertEquals(NONE, access(writer, multiTableJob));
        assertEquals(METADATA, access(mixed, multiTableJob));
        assertEquals(FULL, access(both, multiTableJob));

        // A materialized view, the target of its refresh job, is authorized as a materialized view.
        LoadJob onMv = new InsertLoadJob("on_mv", dbId, mvId, System.currentTimeMillis(), "", "", null);
        assertEquals(NONE, access(reader, onMv));
        assertEquals(FULL, access(mvReader, onMv));

        // A target table that cannot be resolved is shown in full to root alone, and without its rows to OPERATE.
        assertEquals(NONE, access(both, onDropped));
        assertEquals(NONE, access(both, unanalyzedJob));
        assertEquals(FULL, access(UserIdentity.ROOT, onDropped));
        assertEquals(FULL, access(UserIdentity.ROOT, unanalyzedJob));
        assertEquals(METADATA, access(operator, onDropped));
        assertEquals(METADATA, access(operator, unanalyzedJob));

        // A caller without an identity sees nothing.
        assertNull(LoadJobVisibility.callerOf(new TGetLoadsParams()).getCurrentUserIdentity());
        assertEquals(NONE, LoadJobVisibility.accessOf(LoadJobVisibility.callerOf(new TGetLoadsParams()), onOpen));
        assertEquals(NONE, LoadJobVisibility.accessOf(null, onOpen));
    }

    @Test
    public void testRoutineAndStreamLoadTasks(@Injectable StreamLoadTask openTask,
                                              @Injectable StreamLoadTask secretTask,
                                              @Injectable StreamLoadMultiStmtTask multiStmtTask,
                                              @Injectable MergeCommitTask mergeCommitTask,
                                              @Injectable MergeCommitTask notStartedMergeCommitTask) {
        new Expectations() {
            {
                openTask.getDBId();
                minTimes = 0;
                result = dbId;
                openTask.getTableId();
                minTimes = 0;
                result = openTableId;

                secretTask.getDBId();
                minTimes = 0;
                result = dbId;
                secretTask.getTableId();
                minTimes = 0;
                result = secretTableId;

                multiStmtTask.getTasks();
                minTimes = 0;
                result = Lists.newArrayList(openTask, secretTask);

                mergeCommitTask.getDBId();
                minTimes = 0;
                result = dbId;
                mergeCommitTask.getTableId();
                minTimes = 0;
                result = openTableId;

                // A merge commit task learns its table id once it runs.
                notStartedMergeCommitTask.getDBId();
                minTimes = 0;
                result = dbId;
                notStartedMergeCommitTask.getTableId();
                minTimes = 0;
                result = -1L;
            }
        };
        RoutineLoadJob routineJob = new KafkaRoutineLoadJob(1L, "routine_on_secret", dbId, secretTableId,
                "127.0.0.1:9092", "topic");
        assertEquals(METADATA, LoadJobVisibility.accessOf(caller(writer), routineJob));
        assertEquals(FULL, LoadJobVisibility.accessOf(caller(both), routineJob));
        assertEquals(NONE, LoadJobVisibility.accessOf(caller(reader), routineJob));

        assertEquals(List.of(new VisibleTask(openTask, FULL)), visibleTasks(reader, openTask));
        assertTrue(visibleTasks(reader, secretTask).isEmpty());
        assertEquals(List.of(new VisibleTask(secretTask, METADATA)), visibleTasks(writer, secretTask));

        // Only the statements of a multi-statement transaction the caller may see are kept, and they are shown in
        // full only if every statement is: a failed transaction gives each statement the error of the one that
        // failed, with its error-log URL.
        assertEquals(List.of(new VisibleTask(openTask, METADATA)), visibleTasks(reader, multiStmtTask));
        assertEquals(List.of(new VisibleTask(openTask, METADATA), new VisibleTask(secretTask, METADATA)),
                visibleTasks(mixed, multiStmtTask));
        assertEquals(List.of(new VisibleTask(openTask, FULL), new VisibleTask(secretTask, FULL)),
                visibleTasks(both, multiStmtTask));
        assertTrue(visibleTasks(none, multiStmtTask).isEmpty());

        assertEquals(List.of(new VisibleTask(mergeCommitTask, FULL)), visibleTasks(reader, mergeCommitTask));
        assertTrue(visibleTasks(writer, mergeCommitTask).isEmpty());
        assertTrue(visibleTasks(both, notStartedMergeCommitTask).isEmpty());
        assertEquals(List.of(new VisibleTask(notStartedMergeCommitTask, FULL)),
                visibleTasks(UserIdentity.ROOT, notStartedMergeCommitTask));
        assertEquals(List.of(new VisibleTask(notStartedMergeCommitTask, METADATA)),
                visibleTasks(operator, notStartedMergeCommitTask));
    }

    @Test
    public void testFailedTransactionErrorNeedsEveryStatement() throws Exception {
        // A multi-statement transaction on t_open and t_secret whose t_secret statement failed: the rollback gives
        // every statement the transaction's error (cancelCoordinatorOnly), which names the t_secret error log.
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb(DB);
        StreamLoadMultiStmtTask txn = new StreamLoadMultiStmtTask(GlobalStateMgr.getCurrentState().getNextId(), db,
                "multi_stmt_failed", "", "", 100000L, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        String reason = "commit failed: abnormal data more than max filter rate, tracking_url: " + TRACKING_URL;
        Map<String, StreamLoadTask> statements = Deencapsulation.getField(txn, "taskMaps");
        for (String name : List.of("t_open", "t_secret")) {
            StreamLoadTask statement = new StreamLoadTask(GlobalStateMgr.getCurrentState().getNextId(), db,
                    (OlapTable) metastore.getTable(DB, name), txn.getLabel(), "", "", 100000L, 1, 0,
                    System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
            // What the rollback of the transaction (tryRollbackNow) does to each statement.
            statement.cancelCoordinatorOnly(reason);
            statements.put(name, statement);
        }
        StreamLoadTask open = statements.get("t_open");
        StreamLoadTask secret = statements.get("t_secret");
        StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        streamLoadMgr.addLoadTask(txn, false);
        try {
            // reader may read t_open alone: its statement without the error, in every view, looked up either way.
            assertEquals(List.of(new VisibleTask(open, METADATA)), visibleTasks(reader, txn));
            TGetLoadsParams byId = request(reader);
            byId.setJob_id(open.getId());
            for (List<TStreamLoadInfo> rows : List.of(streamLoads(reader, txn.getLabel()),
                    new FrontendServiceImpl(null).getStreamLoads(byId).getLoads())) {
                assertEquals(List.of(open.getId()), rows.stream().map(TStreamLoadInfo::getId)
                        .collect(Collectors.toList()));
                assertFalse(rows.get(0).isSetError_msg());
            }
            for (List<TLoadInfo> rows : List.of(List.of(loadsRow(reader, txn.getLabel())),
                    LoadsSystemTable.query(byId).getLoads())) {
                assertEquals(List.of(open.getId()), rows.stream().map(TLoadInfo::getJob_id)
                        .collect(Collectors.toList()));
                assertFalse(rows.get(0).isSetError_msg());
            }
            int errorMsg = ShowStreamLoadStmt.getTitleNames().indexOf("ErrorMsg");
            assertNull(row(showRows(reader, "SHOW ALL STREAM LOAD FROM " + DB), 0, txn.getLabel()).get(errorMsg));
            byId.setJob_id(secret.getId());
            assertTrue(new FrontendServiceImpl(null).getStreamLoads(byId).getLoads().isEmpty());

            // both may read both tables: every statement in full.
            assertEquals(Sets.newHashSet(new VisibleTask(open, FULL), new VisibleTask(secret, FULL)),
                    Sets.newHashSet(visibleTasks(both, txn)));
            List<TStreamLoadInfo> rows = streamLoads(both, txn.getLabel());
            assertEquals(2, rows.size());
            assertTrue(rows.stream().allMatch(info -> reason.equals(info.getError_msg())), rows.toString());

            // With the rule off, a statement looked up by its job id is shown in full to anyone, as before the rule.
            Config.setMutableConfig("enable_new_load_job_privilege_check", "false", false, "");
            try {
                for (StreamLoadTask statement : List.of(open, secret)) {
                    TGetLoadsParams request = request(none);
                    request.setJob_id(statement.getId());
                    List<TStreamLoadInfo> streamRows = new FrontendServiceImpl(null).getStreamLoads(request)
                            .getLoads();
                    assertEquals(List.of(statement.getId()), streamRows.stream().map(TStreamLoadInfo::getId)
                            .collect(Collectors.toList()));
                    assertEquals(reason, streamRows.get(0).getError_msg());
                    List<TLoadInfo> loadsRows = LoadsSystemTable.query(request).getLoads();
                    assertEquals(List.of(statement.getId()), loadsRows.stream().map(TLoadInfo::getJob_id)
                            .collect(Collectors.toList()));
                    assertEquals(reason, loadsRows.get(0).getError_msg());
                }
            } finally {
                Config.setMutableConfig("enable_new_load_job_privilege_check", "true", false, "");
            }
        } finally {
            Map<String, AbstractStreamLoadTask> byLabel = Deencapsulation.getField(streamLoadMgr, "idToStreamLoadTask");
            byLabel.remove(txn.getLabel());
            Map<Long, Map<String, AbstractStreamLoadTask>> byDb =
                    Deencapsulation.getField(streamLoadMgr, "dbToLabelToStreamLoadTask");
            byDb.get(dbId).remove(txn.getLabel());
        }
    }

    @Test
    public void testEphemeralCaller() throws Exception {
        // A user authenticated through a security integration exists only in its session, and its privileges are the
        // roles that session activated. The caller is built on an RPC thread, which has no session.
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.ddl("CREATE ROLE vis_open_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO ROLE vis_open_role");
        long roleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("vis_open_role");
        TUserIdentity userIdent = UserIdentityUtils.toThrift(new UserIdentity("vis_ephemeral", "%", false, true));
        TUserRoles roles = new TUserRoles();
        roles.setRole_id_list(List.of(roleId));
        userIdent.setCurrent_role_ids(roles);
        TGetLoadsParams request = new TGetLoadsParams();
        request.setCurrent_user_ident(userIdent);
        LoadJob onOpen = new InsertLoadJob("on_open", dbId, openTableId, System.currentTimeMillis(), "", "", null);
        LoadJob onSecret = new InsertLoadJob("on_secret", dbId, secretTableId, System.currentTimeMillis(), "", "", null);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Access> accesses = executor.submit(() -> {
                ConnectContext caller = LoadJobVisibility.callerOf(request);
                return List.of(LoadJobVisibility.accessOf(caller, onOpen), LoadJobVisibility.accessOf(caller, onSecret));
            }).get(30, TimeUnit.SECONDS);
            assertEquals(List.of(FULL, NONE), accesses);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testBulkLoadJobKeepsTargetTablesWhenReplayed() {
        // A replayed or reloaded broker load is not analyzed again, so its target tables must come from the
        // persisted job rather than from the file groups built at analysis.
        BrokerLoadJob job = new BrokerLoadJob();
        job.dbId = dbId;
        job.tableIds = Sets.newHashSet(openTableId);
        LoadJob replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, LoadJob.class), LoadJob.class);

        assertEquals(Sets.newHashSet(openTableId), replayed.getTableIds());
        assertEquals(FULL, access(reader, replayed));
        assertEquals(NONE, access(writer, replayed));
    }

    @Test
    public void testTemporaryTableIsShownInFullToRootAlone() throws Exception {
        // A temporary table is registered by id alone, so its name leads to no table, or to a permanent table of the
        // same name, and neither may stand in for it.
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx).useDatabase(DB);
        starRocksAssert.withTemporaryTable("CREATE TEMPORARY TABLE t_open (k INT, v VARCHAR(3)) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTemporaryTable("CREATE TEMPORARY TABLE t_tmp (k INT, v VARCHAR(3)) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            LoadJob onTmpOpen = new InsertLoadJob("on_tmp_open", dbId, temporaryTableId(db, "t_open"),
                    System.currentTimeMillis(), "", "", null);
            LoadJob onTmp = new InsertLoadJob("on_tmp", dbId, temporaryTableId(db, "t_tmp"),
                    System.currentTimeMillis(), "", "", null);

            assertEquals(NONE, access(reader, onTmpOpen));
            assertEquals(NONE, access(none, onTmp));
            assertEquals(FULL, access(UserIdentity.ROOT, onTmpOpen));
            assertEquals(FULL, access(UserIdentity.ROOT, onTmp));
            assertEquals(METADATA, access(operator, onTmp));
        } finally {
            starRocksAssert.dropTemporaryTable("t_open", true);
            starRocksAssert.dropTemporaryTable("t_tmp", true);
        }
    }

    @Test
    public void testTableHalfwayThroughDropIsNotAuthorized() throws Exception {
        // DROP removes the name of a table before its id. In between, the name leads nowhere and the check lets the
        // missing table through, while the id still finds it.
        LoadJob onOpen = new InsertLoadJob("on_open", dbId, openTableId, System.currentTimeMillis(), "", "", null);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Map<String, Table> nameToTable = Deencapsulation.getField(db, "nameToTable");
        Table table = nameToTable.remove("t_open");
        try {
            assertEquals(NONE, access(none, onOpen));
            assertEquals(NONE, access(reader, onOpen));
            assertEquals(FULL, access(UserIdentity.ROOT, onOpen));
            assertEquals(METADATA, access(operator, onOpen));
        } finally {
            nameToTable.put("t_open", table);
        }
    }

    @Test
    public void testCheckWaitsForARunningDrop() throws Exception {
        // DROP and RENAME change the names in a database under its write lock; the check must not run meanwhile.
        LoadJob onOpen = new InsertLoadJob("on_open", dbId, openTableId, System.currentTimeMillis(), "", "", null);
        ConnectContext readerCaller = caller(reader);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Locker locker = new Locker();
        locker.lockDatabase(dbId, LockType.WRITE);
        Future<Access> access;
        try {
            access = executor.submit(() -> LoadJobVisibility.accessOf(readerCaller, onOpen));
            Thread.sleep(300);
            assertFalse(access.isDone());
        } finally {
            locker.unLockDatabase(dbId, LockType.WRITE);
        }
        try {
            assertEquals(FULL, access.get(30, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testLoadJobSurfaces() throws Exception {
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        InsertLoadJob openJob = new InsertLoadJob("insert_on_open", dbId, openTableId, System.currentTimeMillis(), "",
                TRACKING_URL, null);
        InsertLoadJob secretJob = new InsertLoadJob("insert_on_secret", dbId, secretTableId,
                System.currentTimeMillis(), ROW_VALUE, TRACKING_URL, null);
        loadMgr.addLoadJob(openJob);
        loadMgr.addLoadJob(secretJob);

        // information_schema.loads
        assertEquals(List.of("insert_on_open"), loadsLabels(reader));
        assertEquals(List.of("insert_on_secret"), loadsLabels(writer));
        assertEquals(List.of("insert_on_open", "insert_on_secret"), loadsLabels(operator));
        TLoadInfo redacted = loadsRow(writer, "insert_on_secret");
        assertEquals("type:LOAD_RUN_FAIL", redacted.getError_msg());
        assertFalse(redacted.isSetUrl());
        assertFalse(redacted.isSetTracking_sql());
        assertFalse(redacted.isSetProperties());
        assertEquals(redacted, loadsRow(operator, "insert_on_secret"));
        TLoadInfo full = loadsRow(both, "insert_on_secret");
        assertEquals("type:LOAD_RUN_FAIL; msg:" + ROW_VALUE, full.getError_msg());
        assertEquals(TRACKING_URL, full.getUrl());
        assertTrue(full.isSetTracking_sql());

        // information_schema.load_tracking_logs: BE gets the error-log URL only with SELECT on the target tables.
        assertTrue(trackingLoads(reader, secretJob.getId()).isEmpty());
        List<TTrackingLoadInfo> trackingLoads = trackingLoads(writer, secretJob.getId());
        assertEquals(1, trackingLoads.size());
        assertFalse(trackingLoads.get(0).isSetUrls());
        trackingLoads = trackingLoads(both, secretJob.getId());
        assertEquals(1, trackingLoads.size());
        assertEquals(List.of(TRACKING_URL), trackingLoads.get(0).getUrls());

        // SHOW LOAD
        String showLoad = "SHOW LOAD FROM " + DB;
        assertEquals(List.of("insert_on_open"), labels(showRows(reader, showLoad), 1, "insert_on_"));
        assertEquals(List.of("insert_on_secret"), labels(showRows(writer, showLoad), 1, "insert_on_"));
        assertTrue(labels(showRows(none, showLoad), 1, "insert_on_").isEmpty());
        assertEquals(List.of("insert_on_open", "insert_on_secret"),
                labels(showRows(UserIdentity.ROOT, showLoad), 1, "insert_on_"));
        List<String> redactedRow = row(showRows(writer, showLoad), 1, "insert_on_secret");
        assertEquals("type:LOAD_RUN_FAIL", redactedRow.get(LoadProcDir.TITLE_NAMES.indexOf("ErrorMsg")));
        assertEquals(FeConstants.NULL_STRING, redactedRow.get(LoadProcDir.TITLE_NAMES.indexOf("TrackingSQL")));
        List<String> fullRow = row(showRows(both, showLoad), 1, "insert_on_secret");
        assertEquals("type:LOAD_RUN_FAIL; msg:" + ROW_VALUE,
                fullRow.get(LoadProcDir.TITLE_NAMES.indexOf("ErrorMsg")));

        // SHOW PROC, which needs OPERATE
        String showProc = "SHOW PROC '/jobs/" + dbId + "/load'";
        assertEquals(redactedRow, row(showRows(operator, showProc), 1, "insert_on_secret"));
        assertEquals(fullRow, row(showRows(UserIdentity.ROOT, showProc), 1, "insert_on_secret"));
    }

    @Test
    public void testStreamAndRoutineLoadSurfaces() throws Exception {
        TransactionResult resp = new TransactionResult();
        GlobalStateMgr.getCurrentState().getStreamLoadMgr().beginLoadTaskFromBackend(DB, "t_secret",
                "stream_on_secret", null, "", "", 100000, resp, false, WarehouseManager.DEFAULT_RESOURCE, 10002);
        AbstractStreamLoadTask streamTask =
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().getTaskByName("stream_on_secret").get(0);
        Deencapsulation.setField(streamTask, "errorMsg", ROW_VALUE);
        Deencapsulation.setField(streamTask, "trackingUrl", TRACKING_URL);
        long routineJobId = GlobalStateMgr.getCurrentState().getNextId();
        KafkaRoutineLoadJob routineJob = new KafkaRoutineLoadJob(routineJobId, "routine_on_secret", dbId,
                secretTableId, "127.0.0.1:9092", "topic");
        // setOtherMsg would stamp the message with the time.
        Deencapsulation.setField(routineJob, "otherMsg", ROW_VALUE);
        Queue<String> errorLogUrls = Deencapsulation.getField(routineJob, "errorLogUrls");
        errorLogUrls.add(TRACKING_URL);
        GlobalStateMgr.getCurrentState().getRoutineLoadMgr().replayCreateRoutineLoadJob(routineJob);
        FrontendServiceImpl service = new FrontendServiceImpl(null);

        // information_schema.stream_loads
        TGetLoadsParams streamRequest = request(reader);
        streamRequest.setLabel("stream_on_secret");
        assertTrue(service.getStreamLoads(streamRequest).getLoads().isEmpty());
        streamRequest = request(writer);
        streamRequest.setLabel("stream_on_secret");
        List<TStreamLoadInfo> streamLoads = service.getStreamLoads(streamRequest).getLoads();
        assertEquals(List.of("stream_on_secret"), streamLoads.stream().map(TStreamLoadInfo::getLabel)
                .collect(Collectors.toList()));
        assertFalse(streamLoads.get(0).isSetError_msg());
        assertFalse(streamLoads.get(0).isSetTracking_url());
        streamRequest = request(both);
        streamRequest.setLabel("stream_on_secret");
        streamLoads = service.getStreamLoads(streamRequest).getLoads();
        assertEquals(ROW_VALUE, streamLoads.get(0).getError_msg());
        assertEquals(TRACKING_URL, streamLoads.get(0).getTracking_url());

        // information_schema.loads, which lists stream loads too: their error has no type to keep.
        TGetLoadsParams loadsRequest = request(writer);
        loadsRequest.setLabel("stream_on_secret");
        TLoadInfo streamLoad = LoadsSystemTable.query(loadsRequest).getLoads().get(0);
        assertFalse(streamLoad.isSetError_msg());
        assertFalse(streamLoad.isSetUrl());

        // SHOW STREAM LOAD, and SHOW PROC, which needs OPERATE
        String showStreamLoad = "SHOW ALL STREAM LOAD FROM " + DB;
        int errorMsg = ShowStreamLoadStmt.getTitleNames().indexOf("ErrorMsg");
        assertTrue(labels(showRows(reader, showStreamLoad), 0, "stream_on_").isEmpty());
        assertNull(row(showRows(writer, showStreamLoad), 0, "stream_on_secret").get(errorMsg));
        assertNull(row(showRows(operator, showStreamLoad), 0, "stream_on_secret").get(errorMsg));
        // SELECT alone, without INSERT, shows the task in full.
        assertEquals(ROW_VALUE, row(showRows(both, showStreamLoad), 0, "stream_on_secret").get(errorMsg));
        String showStreamProc = "SHOW PROC '/stream_loads/stream_on_secret'";
        assertNull(row(showRows(operator, showStreamProc), 0, "stream_on_secret").get(errorMsg));
        assertEquals(ROW_VALUE, row(showRows(UserIdentity.ROOT, showStreamProc), 0, "stream_on_secret")
                .get(errorMsg));

        // information_schema.routine_load_jobs, looked up by id and by database
        TGetLoadsParams routineRequest = request(reader);
        routineRequest.setJob_id(routineJobId);
        assertTrue(service.getRoutineLoadJobs(routineRequest).getLoads().isEmpty());
        routineRequest = request(writer);
        routineRequest.setJob_id(routineJobId);
        List<TRoutineLoadJobInfo> routineLoads = service.getRoutineLoadJobs(routineRequest).getLoads();
        assertEquals(1, routineLoads.size());
        assertFalse(routineLoads.get(0).isSetError_log_urls());
        assertFalse(routineLoads.get(0).isSetOther_msg());
        routineRequest = request(both);
        routineRequest.setJob_id(routineJobId);
        routineLoads = service.getRoutineLoadJobs(routineRequest).getLoads();
        assertEquals(TRACKING_URL, routineLoads.get(0).getError_log_urls());
        assertEquals(ROW_VALUE, routineLoads.get(0).getOther_msg());
        routineRequest = request(reader);
        routineRequest.setDb(DB);
        assertTrue(service.getRoutineLoadJobs(routineRequest).getLoads().isEmpty());
        routineRequest = request(writer);
        routineRequest.setDb(DB);
        assertEquals(List.of("routine_on_secret"), service.getRoutineLoadJobs(routineRequest).getLoads().stream()
                .map(TRoutineLoadJobInfo::getName).filter(name -> name.startsWith("routine_on_"))
                .collect(Collectors.toList()));

        // information_schema.load_tracking_logs
        assertFalse(trackingLoads(writer, routineJobId).get(0).isSetUrls());
        assertEquals(List.of(TRACKING_URL), trackingLoads(both, routineJobId).get(0).getUrls());

        // SHOW PROC '/routine_loads/name'
        int otherMsg = ShowRoutineLoadStmt.getTitleNames().indexOf("OtherMsg");
        String showRoutineProc = "SHOW PROC '/routine_loads/routine_on_secret'";
        assertNull(row(showRows(operator, showRoutineProc), 1, "routine_on_secret").get(otherMsg));
        assertEquals(ROW_VALUE, row(showRows(UserIdentity.ROOT, showRoutineProc), 1, "routine_on_secret")
                .get(otherMsg));
    }

    @Test
    public void testShowRoutineLoad() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(GlobalStateMgr.getCurrentState().getNextId(),
                "show_routine_on_secret", dbId, secretTableId, "127.0.0.1:9092", "topic");
        // setOtherMsg would stamp the message with the time.
        Deencapsulation.setField(job, "otherMsg", ROW_VALUE);
        GlobalStateMgr.getCurrentState().getRoutineLoadMgr().replayCreateRoutineLoadJob(job);
        String showRoutineLoad = "SHOW ALL ROUTINE LOAD FROM " + DB;
        int otherMsg = ShowRoutineLoadStmt.getTitleNames().indexOf("OtherMsg");
        assertTrue(labels(showRows(reader, showRoutineLoad), 1, "show_routine_").isEmpty());
        assertNull(row(showRows(writer, showRoutineLoad), 1, "show_routine_on_secret").get(otherMsg));
        assertNull(row(showRows(operator, showRoutineLoad), 1, "show_routine_on_secret").get(otherMsg));
        // SELECT alone, without INSERT, shows the job in full.
        assertEquals(ROW_VALUE, row(showRows(both, showRoutineLoad), 1, "show_routine_on_secret").get(otherMsg));

        // Halfway through DROP the name leads nowhere, so a check by name lets the table through, while the job
        // still finds it by id.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Map<String, Table> nameToTable = Deencapsulation.getField(db, "nameToTable");
        Table table = nameToTable.remove("t_secret");
        try {
            assertTrue(labels(showRows(none, showRoutineLoad), 1, "show_routine_").isEmpty());
            assertNull(row(showRows(operator, showRoutineLoad), 1, "show_routine_on_secret").get(otherMsg));
        } finally {
            nameToTable.put("t_secret", table);
        }
    }

    @Test
    public void testLoadInfoApi() throws Exception {
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        loadMgr.addLoadJob(new InsertLoadJob("info_on_open", dbId, openTableId, System.currentTimeMillis(), "",
                TRACKING_URL, null));
        loadMgr.addLoadJob(new InsertLoadJob("info_on_secret", dbId, secretTableId, System.currentTimeMillis(),
                ROW_VALUE, TRACKING_URL, null));
        String[] label = {""};
        new MockUp<BaseRequest>() {
            @Mock
            public String getSingleParameter(String key) {
                return "db".equals(key) ? DB : label[0];
            }
        };
        List<RestBaseResult> results = Lists.newArrayList();
        new MockUp<RestBaseAction>() {
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response, RestBaseResult result) {
                results.add(result);
            }
        };
        GetLoadInfoAction action = new GetLoadInfoAction(null, false);
        BaseRequest request = new BaseRequest(null, null);
        ctx.setThreadLocalInfo();

        // writer may insert into t_secret, which passes the database-level check, but holds nothing on t_open.
        switchTo(writer);
        label[0] = "info_on_open";
        assertThrows(AccessDeniedException.class, () -> action.executeWithoutPassword(request, new BaseResponse()));
        assertTrue(results.isEmpty());
        // Without SELECT on t_secret, writer gets neither the error-log URL nor the fail message.
        label[0] = "info_on_secret";
        action.executeWithoutPassword(request, new BaseResponse());
        Load.JobInfo info = Deencapsulation.getField(results.get(0), "jobInfo");
        assertEquals("", info.trackingUrl);
        assertEquals("type:LOAD_RUN_FAIL", info.failMsg);

        switchTo(loader);
        action.executeWithoutPassword(request, new BaseResponse());
        info = Deencapsulation.getField(results.get(1), "jobInfo");
        assertEquals(TRACKING_URL, info.trackingUrl);
        assertEquals(ROW_VALUE, info.failMsg);
    }

    @Test
    public void testRuleOffRestoresTheEarlierChecks() throws Exception {
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        InsertLoadJob secretJob = new InsertLoadJob("legacy_on_secret", dbId, secretTableId,
                System.currentTimeMillis(), ROW_VALUE, TRACKING_URL, null);
        loadMgr.addLoadJob(secretJob);
        TransactionResult resp = new TransactionResult();
        GlobalStateMgr.getCurrentState().getStreamLoadMgr().beginLoadTaskFromBackend(DB, "t_secret",
                "legacy_stream_on_secret", null, "", "", 100000, resp, false, WarehouseManager.DEFAULT_RESOURCE, 10002);
        AbstractStreamLoadTask streamTask =
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().getTaskByName("legacy_stream_on_secret").get(0);
        Deencapsulation.setField(streamTask, "errorMsg", ROW_VALUE);
        KafkaRoutineLoadJob routineJob = new KafkaRoutineLoadJob(GlobalStateMgr.getCurrentState().getNextId(),
                "legacy_routine_on_secret", dbId, secretTableId, "127.0.0.1:9092", "topic");
        // setOtherMsg would stamp the message with the time.
        Deencapsulation.setField(routineJob, "otherMsg", ROW_VALUE);
        GlobalStateMgr.getCurrentState().getRoutineLoadMgr().replayCreateRoutineLoadJob(routineJob);
        String showLoad = "SHOW LOAD FROM " + DB;
        String showStreamLoad = "SHOW ALL STREAM LOAD FROM " + DB;
        String showRoutineLoad = "SHOW ALL ROUTINE LOAD FROM " + DB;
        int streamErrorMsg = ShowStreamLoadStmt.getTitleNames().indexOf("ErrorMsg");
        int otherMsg = ShowRoutineLoadStmt.getTitleNames().indexOf("OtherMsg");

        // The switch is mutable: it takes effect at once, as ADMIN SET FRONTEND CONFIG sets it.
        Config.setMutableConfig("enable_new_load_job_privilege_check", "false", false, "");
        try {
            assertFalse(LoadJobVisibility.isEnabled());
            // The information_schema views, SHOW LOAD and SHOW PROC check nothing, and show every field.
            assertEquals(TRACKING_URL, loadsRow(none, "legacy_on_secret").getUrl());
            assertEquals(List.of(TRACKING_URL), trackingLoads(none, secretJob.getId()).get(0).getUrls());
            assertEquals(ROW_VALUE, streamLoads(none, "legacy_stream_on_secret").get(0).getError_msg());
            assertEquals(ROW_VALUE, routineLoadJobs(none, routineJob.getId()).get(0).getOther_msg());
            assertEquals(List.of("legacy_on_secret"), labels(showRows(none, showLoad), 1, "legacy_on_"));
            assertEquals(ROW_VALUE, row(showRows(operator, "SHOW PROC '/routine_loads/legacy_routine_on_secret'"), 1,
                    "legacy_routine_on_secret").get(otherMsg));
            // SHOW STREAM LOAD and SHOW ROUTINE LOAD check INSERT on the target table again: SELECT alone or OPERATE
            // does not pass, INSERT does and shows every field.
            assertEquals(ROW_VALUE,
                    row(showRows(writer, showStreamLoad), 0, "legacy_stream_on_secret").get(streamErrorMsg));
            assertTrue(labels(showRows(both, showStreamLoad), 0, "legacy_").isEmpty());
            assertTrue(labels(showRows(operator, showStreamLoad), 0, "legacy_").isEmpty());
            assertEquals(ROW_VALUE,
                    row(showRows(writer, showRoutineLoad), 1, "legacy_routine_on_secret").get(otherMsg));
            assertTrue(labels(showRows(both, showRoutineLoad), 1, "legacy_").isEmpty());
            assertTrue(labels(showRows(operator, showRoutineLoad), 1, "legacy_").isEmpty());
        } finally {
            Config.setMutableConfig("enable_new_load_job_privilege_check", "true", false, "");
        }

        // Turned back on, the rule applies again at once.
        assertTrue(LoadJobVisibility.isEnabled());
        TGetLoadsParams loadsRequest = request(none);
        loadsRequest.setLabel("legacy_on_secret");
        assertTrue(LoadsSystemTable.query(loadsRequest).getLoads().isEmpty());
        assertTrue(trackingLoads(none, secretJob.getId()).isEmpty());
        assertTrue(streamLoads(none, "legacy_stream_on_secret").isEmpty());
        assertTrue(routineLoadJobs(none, routineJob.getId()).isEmpty());
        assertTrue(labels(showRows(none, showLoad), 1, "legacy_on_").isEmpty());
        assertNull(row(showRows(operator, "SHOW PROC '/routine_loads/legacy_routine_on_secret'"), 1,
                "legacy_routine_on_secret").get(otherMsg));
        assertNull(row(showRows(writer, showStreamLoad), 0, "legacy_stream_on_secret").get(streamErrorMsg));
        assertEquals(ROW_VALUE, row(showRows(both, showStreamLoad), 0, "legacy_stream_on_secret").get(streamErrorMsg));
        assertNull(row(showRows(operator, showRoutineLoad), 1, "legacy_routine_on_secret").get(otherMsg));
    }

    @Test
    public void testLoadInfoApiWithRuleOff() throws Exception {
        GlobalStateMgr.getCurrentState().getLoadMgr().addLoadJob(new InsertLoadJob("legacy_info_on_open", dbId,
                openTableId, System.currentTimeMillis(), ROW_VALUE, TRACKING_URL, null));
        new MockUp<BaseRequest>() {
            @Mock
            public String getSingleParameter(String key) {
                return "db".equals(key) ? DB : "legacy_info_on_open";
            }
        };
        List<RestBaseResult> results = Lists.newArrayList();
        new MockUp<RestBaseAction>() {
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response, RestBaseResult result) {
                results.add(result);
            }
        };
        GetLoadInfoAction action = new GetLoadInfoAction(null, false);
        BaseRequest request = new BaseRequest(null, null);
        ctx.setThreadLocalInfo();
        // writer may insert into t_secret, which passes the database-level check, but holds nothing on t_open.
        switchTo(writer);

        Config.setMutableConfig("enable_new_load_job_privilege_check", "false", false, "");
        try {
            // The database-level check is the only one, as before the rule: the job and its URL come back in full.
            action.executeWithoutPassword(request, new BaseResponse());
            Load.JobInfo info = Deencapsulation.getField(results.get(0), "jobInfo");
            assertEquals(TRACKING_URL, info.trackingUrl);
            assertEquals(ROW_VALUE, info.failMsg);
        } finally {
            Config.setMutableConfig("enable_new_load_job_privilege_check", "true", false, "");
        }

        assertThrows(AccessDeniedException.class, () -> action.executeWithoutPassword(request, new BaseResponse()));
        assertEquals(1, results.size());
    }

    private static UserIdentity createUser(StarRocksAssert starRocksAssert, String name) throws Exception {
        starRocksAssert.withUser(name);
        return UserIdentity.createAnalyzedUserIdentWithIp(name, "%");
    }

    private static long temporaryTableId(Database db, String name) {
        return db.getTemporaryTables().stream().filter(table -> table.getName().equals(name)).findFirst()
                .orElseThrow().getId();
    }

    private static TGetLoadsParams request(UserIdentity user) {
        TGetLoadsParams request = new TGetLoadsParams();
        request.setCurrent_user_ident(UserIdentityUtils.toThrift(user));
        return request;
    }

    private static ConnectContext caller(UserIdentity user) {
        return LoadJobVisibility.callerOf(request(user));
    }

    private static Access access(UserIdentity user, LoadJob job) {
        return LoadJobVisibility.accessOf(caller(user), job);
    }

    private static List<VisibleTask> visibleTasks(UserIdentity user, AbstractStreamLoadTask task) {
        return LoadJobVisibility.visibleTasks(caller(user), task);
    }

    private static List<String> loadsLabels(UserIdentity user) {
        TGetLoadsParams request = request(user);
        request.setDb(DB);
        return LoadsSystemTable.query(request).getLoads().stream().map(TLoadInfo::getLabel)
                .filter(label -> label.startsWith("insert_on_")).sorted().collect(Collectors.toList());
    }

    private static TLoadInfo loadsRow(UserIdentity user, String label) {
        TGetLoadsParams request = request(user);
        request.setLabel(label);
        List<TLoadInfo> loads = LoadsSystemTable.query(request).getLoads();
        assertEquals(1, loads.size(), loads.toString());
        return loads.get(0);
    }

    private static List<TTrackingLoadInfo> trackingLoads(UserIdentity user, long jobId) throws Exception {
        TGetLoadsParams request = request(user);
        request.setJob_id(jobId);
        return new FrontendServiceImpl(null).getTrackingLoads(request).getTrackingLoads();
    }

    private static List<TStreamLoadInfo> streamLoads(UserIdentity user, String label) throws Exception {
        TGetLoadsParams request = request(user);
        request.setLabel(label);
        return new FrontendServiceImpl(null).getStreamLoads(request).getLoads();
    }

    private static List<TRoutineLoadJobInfo> routineLoadJobs(UserIdentity user, long jobId) throws Exception {
        TGetLoadsParams request = request(user);
        request.setJob_id(jobId);
        return new FrontendServiceImpl(null).getRoutineLoadJobs(request).getLoads();
    }

    private static void switchTo(UserIdentity user) {
        ctx.setCurrentUserIdentity(user);
        try {
            ctx.setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(user));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        ctx.setQualifiedUser(user.getUser());
    }

    private static List<List<String>> showRows(UserIdentity user, String sql) throws Exception {
        switchTo(user);
        ctx.setThreadLocalInfo();
        ShowResultSet resultSet = ShowExecutor.execute((ShowStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        return resultSet.getResultRows();
    }

    private static List<String> labels(List<List<String>> rows, int labelColumn, String prefix) {
        return rows.stream().map(row -> row.get(labelColumn)).filter(label -> label.startsWith(prefix)).sorted()
                .collect(Collectors.toList());
    }

    private static List<String> row(List<List<String>> rows, int labelColumn, String label) {
        List<List<String>> matched = rows.stream().filter(row -> label.equals(row.get(labelColumn)))
                .collect(Collectors.toList());
        assertEquals(1, matched.size(), rows.toString());
        return matched.get(0);
    }
}
