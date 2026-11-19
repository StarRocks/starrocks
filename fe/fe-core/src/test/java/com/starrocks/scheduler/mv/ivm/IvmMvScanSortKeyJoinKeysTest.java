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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The extra sort-key equalities {@code IvmDeltaAggregateRule} puts on the state-merge join.
 */
public class IvmMvScanSortKeyJoinKeysTest extends MVIVMIcebergTestBase {
    private static final String BASE_TABLE = "`iceberg0`.`partitioned_db`.`t2`";
    // t2's four days fall in one month: a single mv partition keeps the join in the mv's buckets.
    private static final String MONTHLY_PARTITIONS = "PARTITION BY date_trunc('month', date)";
    private static final String DAILY_PARTITIONS = "PARTITION BY date";
    // These mvs hold no rows, so the join would broadcast them; a production mv is far past that limit.
    private static final String NO_BROADCAST = ", 'session.broadcast_row_limit' = '0'";
    private static final String QUERY =
            "SELECT date, id, data, COUNT(*) AS total FROM " + BASE_TABLE + " GROUP BY date, id, data";

    private final List<String> createdMvs = new ArrayList<>();

    @AfterEach
    public void after() throws Exception {
        for (String mv : createdMvs) {
            starRocksAssert.dropMaterializedView(mv);
        }
        createdMvs.clear();
    }

    @Test
    public void testSortKeyPrefixBecomesAJoinKey() throws Exception {
        String plan = refreshPlanWithDelta("mv_sorted", "ORDER BY (data)");
        assertJoinsOn(plan, "data");
    }

    @Test
    public void testWithoutOrderByTheRowIdIsTheOnlyJoinKey() throws Exception {
        String plan = refreshPlanWithDelta("mv_unsorted", "");
        Assertions.assertFalse(plan.contains("<=>"),
                "the sort key is __ROW_ID__, which the join already carries:\n" + plan);
    }

    @Test
    public void testThePartitionColumnIsJoinedLikeAnyOther() throws Exception {
        // A partition holds its partition KEY constant, not the column behind it: date_trunc('month', date)
        // leaves date ranging over the whole month, so it filters like any other column.
        String plan = refreshPlanWithDelta("mv_part_first", "ORDER BY (date, data)");
        assertJoinsOn(plan, "date");
        assertJoinsOn(plan, "data");
    }

    @Test
    public void testASortKeyNamingASecondAliasOfAGroupKeyIsJoined() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_aliased " + MONTHLY_PARTITIONS +
                " DISTRIBUTED BY HASH(x) BUCKETS 1 ORDER BY (y) REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL') AS " +
                "SELECT date, id AS x, id AS y, COUNT(*) AS total FROM " + BASE_TABLE + " GROUP BY date, id");
        createdMvs.add("mv_aliased");
        MaterializedView mv = getMv("mv_aliased");
        seedTvrBaselineAtVersionZero(mv);
        String plan = explainRefresh(mv);
        // x and y are both the group key id; the sort key names the second, so the mv column y pairs with id.
        assertJoinsOn(plan, "id", "y");
    }

    @Test
    public void testAtMostTwoSortColumnsBecomeJoinKeys() throws Exception {
        String plan = refreshPlanWithDelta("mv_three_sorted", "ORDER BY (id, data, date)");
        assertJoinsOn(plan, "id");
        assertJoinsOn(plan, "data");
        assertDoesNotJoinOn(plan, "date", "past the second column the filter is always true");
    }

    @Test
    public void testTheSessionVariableTurnsItOff() throws Exception {
        String plan = refreshPlanWithDelta("mv_switched_off", "ORDER BY (data)",
                ", 'session.enable_ivm_mv_scan_sort_key_join_keys' = 'false'");
        Assertions.assertFalse(plan.contains("<=>"),
                "the switch left a sort-key join key behind:\n" + plan);
    }

    @Test
    public void testAggregateOutputSortKeyIsNotJoined() throws Exception {
        String plan = refreshPlanWithDelta("mv_sorted_by_agg", "ORDER BY (total, data)");
        Assertions.assertFalse(plan.contains("<=>"),
                "the mv holds the merged total and the delta a partial one, so equating them would drop "
                        + "the row -- and every later sort column is clustered only within it:\n" + plan);
    }

    @Test
    public void testAShuffledJoinKeepsOnlyTheRowIdKey() throws Exception {
        String plan = refreshPlanWithDelta("mv_daily", DAILY_PARTITIONS, "ORDER BY (data)", NO_BROADCAST);
        Assertions.assertTrue(plan.contains("(PARTITIONED)"),
                "a mv in four partitions outside any colocate group shuffles the join:\n" + plan);
        Assertions.assertFalse(plan.contains("<=>"),
                "a second shuffle key would keep the row-id runtime filter out of the mv scan:\n" + plan);
    }

    @Test
    public void testAColocatedMvKeepsTheSortKeyAcrossPartitions() throws Exception {
        String plan = refreshPlanWithDelta("mv_daily_colocated", DAILY_PARTITIONS, "ORDER BY (data)",
                ", 'colocate_with' = 'sk_colocate_group'" + NO_BROADCAST);
        Assertions.assertFalse(plan.contains("(PARTITIONED)"), "colocation keeps the mv in its buckets:\n" + plan);
        assertJoinsOn(plan, "data");
    }

    private String refreshPlanWithDelta(String name, String orderBy) throws Exception {
        return refreshPlanWithDelta(name, orderBy, "");
    }

    private String refreshPlanWithDelta(String name, String orderBy, String extraProperties) throws Exception {
        return refreshPlanWithDelta(name, MONTHLY_PARTITIONS, orderBy, extraProperties);
    }

    private String refreshPlanWithDelta(String name, String partitionBy, String orderBy, String extraProperties)
            throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " " + partitionBy +
                " DISTRIBUTED BY HASH(id) BUCKETS 1 " + orderBy + " REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL'" + extraProperties + ") AS " + QUERY);
        createdMvs.add(name);
        MaterializedView mv = getMv(name);
        seedTvrBaselineAtVersionZero(mv);
        String plan = explainRefresh(mv);
        Assertions.assertTrue(plan.contains("MaterializedView: true"),
                "the state-merge join is gone, so there is no join key to assert on:\n" + plan);
        return plan;
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

    private static String explainRefresh(MaterializedView mv) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertNotNull(task);
        StatementBase statement = getAnalyzedPlan("EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName(), connectContext);
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.NORMAL.value(), false, new HashMap<>());
        return taskManager.getMVRefreshExplain(task, option, statement);
    }
}
