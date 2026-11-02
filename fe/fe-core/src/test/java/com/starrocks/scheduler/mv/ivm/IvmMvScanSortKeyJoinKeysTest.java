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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.lake.bookmark.AlreadyAtLatestException;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The extra sort-key equalities {@code IvmDeltaAggregateRule} puts on the state-merge join.
 */
public class IvmMvScanSortKeyJoinKeysTest {
    private static final String DB = "db_ivm_mv_scan_sort_key_join_keys";
    private static final String BASE_DDL = "CREATE TABLE sk_fact (dt DATE NOT NULL, a INT NOT NULL, " +
            "b INT NOT NULL, c INT NOT NULL, v BIGINT) DUPLICATE KEY(dt, a) PARTITION BY RANGE(dt) (" +
            "PARTITION p1 VALUES LESS THAN ('2024-02-01'), PARTITION p2 VALUES LESS THAN ('2024-03-01')) " +
            "DISTRIBUTED BY HASH(a) BUCKETS 1 PROPERTIES ('replication_num' = '1')";
    private static final String QUERY =
            "SELECT dt, a, b, c, SUM(v) AS total FROM sk_fact GROUP BY dt, a, b, c";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private final List<String> createdMvs = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        starRocksAssert.withTable(BASE_DDL);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
    }

    @AfterEach
    public void after() throws Exception {
        for (String mv : createdMvs) {
            starRocksAssert.dropMaterializedView(mv);
        }
        createdMvs.clear();
    }

    @Test
    public void testSortKeyPrefixBecomesAJoinKey() throws Exception {
        String plan = refreshPlanWithDelta("mv_sorted", "ORDER BY (b)");
        assertJoinsOn(plan, "b");
    }

    @Test
    public void testWithoutOrderByTheRowIdIsTheOnlyJoinKey() throws Exception {
        String plan = refreshPlanWithDelta("mv_unsorted", "");
        Assertions.assertFalse(plan.contains("<=>"),
                "the sort key is __ROW_ID__, which the join already carries:\n" + plan);
    }

    @Test
    public void testThePartitionColumnIsJoinedLikeAnyOther() throws Exception {
        // A partition holds its partition KEY constant, not the column behind it: PARTITION BY
        // date_trunc('day', dt) leaves dt ranging over the whole day, so it filters like any other column.
        String plan = refreshPlanWithDelta("mv_part_first", "ORDER BY (dt, b)");
        assertJoinsOn(plan, "dt");
        assertJoinsOn(plan, "b");
    }

    @Test
    public void testASortKeyNamingASecondAliasOfAGroupKeyIsJoined() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_aliased PARTITION BY dt " +
                "DISTRIBUTED BY HASH(x) BUCKETS 1 ORDER BY (y) REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL') AS " +
                "SELECT dt, a AS x, a AS y, SUM(v) AS total FROM sk_fact GROUP BY dt, a");
        createdMvs.add("mv_aliased");
        MaterializedView mv = (MaterializedView) db.getTable("mv_aliased");
        seedBaseline(mv);
        bumpVersion("p2");
        String plan = explainRefresh(mv);
        // Both x and y are the group key a; a sort key naming the second one still has to find it.
        // The two sides carry different names here -- the mv column y against the group key a.
        assertJoinsOn(plan, "a", "y");
    }

    @Test
    public void testAtMostTwoSortColumnsBecomeJoinKeys() throws Exception {
        String plan = refreshPlanWithDelta("mv_three_sorted", "ORDER BY (a, b, c)");
        assertJoinsOn(plan, "a");
        assertJoinsOn(plan, "b");
        assertDoesNotJoinOn(plan, "c", "past the second column the filter is always true");
    }

    @Test
    public void testTheSessionVariableTurnsItOff() throws Exception {
        String plan = refreshPlanWithDelta("mv_switched_off", "ORDER BY (b)",
                ", 'session.enable_ivm_mv_scan_sort_key_join_keys' = 'false'");
        Assertions.assertFalse(plan.contains("<=>"),
                "the switch left a sort-key join key behind:\n" + plan);
    }

    @Test
    public void testAggregateOutputSortKeyIsNotJoined() throws Exception {
        String plan = refreshPlanWithDelta("mv_sorted_by_agg", "ORDER BY (total, b)");
        Assertions.assertFalse(plan.contains("<=>"),
                "the mv holds the merged total and the delta a partial one, so equating them would drop "
                        + "the row -- and every later sort column is clustered only within it:\n" + plan);
    }

    private String refreshPlanWithDelta(String name, String orderBy) throws Exception {
        return refreshPlanWithDelta(name, orderBy, "");
    }

    private String refreshPlanWithDelta(String name, String orderBy, String extraProperties) throws Exception {
        MaterializedView mv = createMv(name, orderBy, extraProperties);
        seedBaseline(mv);
        bumpVersion("p2");
        String plan = explainRefresh(mv);
        Assertions.assertTrue(plan.contains("MaterializedView: true"),
                "the state-merge join is gone, so there is no join key to assert on:\n" + plan);
        return plan;
    }

    private MaterializedView createMv(String name, String orderBy, String extraProperties) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " PARTITION BY dt " +
                "DISTRIBUTED BY HASH(a) BUCKETS 1 " + orderBy + " REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL'" + extraProperties + ") AS " + QUERY);
        createdMvs.add(name);
        return (MaterializedView) db.getTable(name);
    }

    private static void assertJoinsOn(String plan, String column) {
        assertJoinsOn(plan, column, column);
    }

    private static void assertJoinsOn(String plan, String deltaColumn, String mvColumn) {
        Assertions.assertTrue(joinsOn(plan, deltaColumn, mvColumn),
                "expected a null-safe join key pairing " + deltaColumn + " with " + mvColumn + ":\n" + plan);
    }

    private static void assertDoesNotJoinOn(String plan, String column, String why) {
        Assertions.assertFalse(joinsOn(plan, column, column),
                "unexpected join key on " + column + " -- " + why + ":\n" + plan);
    }

    private static boolean joinsOn(String plan, String deltaColumn, String mvColumn) {
        return Pattern.compile("equal join conjunct: \\d+: (" + deltaColumn + " <=> \\d+: " + mvColumn
                + "|" + mvColumn + " <=> \\d+: " + deltaColumn + ")").matcher(plan).find();
    }

    /** Pin the MV's consumed position on the base at the table's current state. */
    private static void seedBaseline(MaterializedView mv) throws Exception {
        OlapTable table = (OlapTable) db.getTable("sk_fact");
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        long bookmarkId;
        try {
            bookmarkId = bookmarkManager.create(db.getId(), table.getId(), BookmarkHolder.forMv(mv.getMvId()))
                    .getBookmarkId();
        } catch (AlreadyAtLatestException e) {
            bookmarkId = e.getBookmarkId();
        }
        BaseTableInfo baseTableInfo = mv.getBaseTableInfos().stream()
                .filter(info -> info.matchTable(table))
                .findFirst()
                .orElseThrow();
        mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap()
                .put(baseTableInfo, TvrTableSnapshot.of(bookmarkId));
    }

    private static void bumpVersion(String partitionName) {
        OlapTable table = (OlapTable) db.getTable("sk_fact");
        PhysicalPartition partition = table.getPartition(partitionName).getDefaultPhysicalPartition();
        partition.setVisibleVersion(partition.getVisibleVersion() + 1, System.currentTimeMillis());
        partition.setDataVersion(partition.getDataVersion() + 1);
    }

    private static String explainRefresh(MaterializedView mv) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertNotNull(task);
        StatementBase statement = MVTestBase.getAnalyzedPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName(), connectContext);
        return taskManager.getMVRefreshExplain(task, new ExecuteOption(70, false, new HashMap<>()), statement);
    }
}
