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


package com.starrocks.sql.optimizer;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest {
    private static final String DEFAULT_CREATE_TABLE_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `table_statistic_v1` (\n"
            + "  `table_id` bigint NOT NULL,\n"
            + "  `column_name` varchar(65530) NOT NULL,\n"
            + "  `db_id` bigint NOT NULL,\n"
            + "  `table_name` varchar(65530) NOT NULL,\n"
            + "  `db_name` varchar(65530) NOT NULL,\n"
            + "  `row_count` bigint NOT NULL,\n"
            + "  `data_size` bigint NOT NULL,\n"
            + "  `distinct_count` bigint NOT NULL,\n"
            + "  `null_count` bigint NOT NULL,\n"
            + "  `max` varchar(65530) NOT NULL,\n"
            + "  `min` varchar(65530) NOT NULL,\n"
            + "  `update_time` datetime NOT NULL\n"
            + "  )\n"
            + "ENGINE=OLAP\n"
            + "UNIQUE KEY(`table_id`,  `column_name`, `db_id`)\n"
            + "DISTRIBUTED BY HASH(`table_id`, `column_name`, `db_id`) BUCKETS 2\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"1\",\n"
            + "\"in_memory\" = \"false\",\n"
            + "\"storage_format\" = \"V2\"\n"
            + ");";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    protected static void setTableStatistics(OlapTable table, long rowCount) {
        for (Partition partition : table.getAllPartitions()) {
            partition.getBaseIndex().setRowCount(rowCount);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void extractConjuncts() {
        ScalarOperator root =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        ConstantOperator.createBoolean(false),
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                ConstantOperator.createBoolean(true),
                                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                        ConstantOperator.createInt(1),
                                        ConstantOperator.createInt(2))));

        List<ScalarOperator> list = Utils.extractConjuncts(root);

        assertEquals(4, list.size());
        assertFalse(((ConstantOperator) list.get(0)).getBoolean());
        assertTrue(((ConstantOperator) list.get(1)).getBoolean());
        assertEquals(1, ((ConstantOperator) list.get(2)).getInt());
        assertEquals(2, ((ConstantOperator) list.get(3)).getInt());
    }

    @Test
    public void extractColumnRef() {
        ScalarOperator root =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        ConstantOperator.createBoolean(false),
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                        new ColumnRefOperator(3, Type.INT, "hello", true),
                                        ConstantOperator.createInt(1)),
                                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                        new ColumnRefOperator(1, Type.INT, "name", true),
                                        new ColumnRefOperator(2, Type.INT, "age", true))));

        List<ColumnRefOperator> list = Utils.extractColumnRef(root);

        assertEquals(3, list.size());
        assertTrue(list.get(0).isVariable());
        assertTrue(list.get(1).isVariable());
        assertTrue(list.get(2).isVariable());
        assertEquals(3, list.get(0).getId());
        assertEquals(1, list.get(1).getId());
        assertEquals(2, list.get(2).getId());
    }

    @Test
    public void compoundAnd1() {
        ScalarOperator tree1 = Utils.compoundAnd(ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.createInt(3),
                ConstantOperator.createInt(4),
                ConstantOperator.createInt(5));

        assertEquals(CompoundPredicateOperator.CompoundType.AND, ((CompoundPredicateOperator) tree1).getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) tree1.getChild(0)).getCompoundType());
        assertEquals(5, ((ConstantOperator) tree1.getChild(1)).getInt());

        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) tree1.getChild(0).getChild(0)).getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) tree1.getChild(0).getChild(1)).getCompoundType());

        assertEquals(1, ((ConstantOperator) tree1.getChild(0).getChild(0).getChild(0)).getInt());
        assertEquals(2, ((ConstantOperator) tree1.getChild(0).getChild(0).getChild(1)).getInt());

        assertEquals(3, ((ConstantOperator) tree1.getChild(0).getChild(1).getChild(0)).getInt());
        assertEquals(4, ((ConstantOperator) tree1.getChild(0).getChild(1).getChild(1)).getInt());
    }

    @Test
    public void compoundAnd2() {
        ScalarOperator tree1 = Utils.compoundAnd(ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.createInt(3),
                ConstantOperator.createInt(4));

        assertEquals(CompoundPredicateOperator.CompoundType.AND, ((CompoundPredicateOperator) tree1).getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) tree1.getChild(0)).getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) tree1.getChild(1)).getCompoundType());

        assertEquals(1, ((ConstantOperator) tree1.getChild(0).getChild(0)).getInt());
        assertEquals(2, ((ConstantOperator) tree1.getChild(0).getChild(1)).getInt());

        assertEquals(3, ((ConstantOperator) tree1.getChild(1).getChild(0)).getInt());
        assertEquals(4, ((ConstantOperator) tree1.getChild(1).getChild(1)).getInt());
    }

    @Test
    public void compoundAnd3() {
        ScalarOperator tree1 = Utils.compoundAnd(ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.createInt(3),
                ConstantOperator.createInt(4),
                ConstantOperator.createInt(5),
                ConstantOperator.createInt(6));

        assertEquals(CompoundPredicateOperator.CompoundType.AND, ((CompoundPredicateOperator) tree1).getCompoundType());
        CompoundPredicateOperator leftChild = (CompoundPredicateOperator) tree1.getChild(0);
        CompoundPredicateOperator rightChild = (CompoundPredicateOperator) tree1.getChild(1);

        assertEquals(CompoundPredicateOperator.CompoundType.AND, leftChild.getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND, rightChild.getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) leftChild.getChild(0)).getCompoundType());
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) leftChild.getChild(1)).getCompoundType());

        assertEquals(1, ((ConstantOperator) leftChild.getChild(0).getChild(0)).getInt());
        assertEquals(2, ((ConstantOperator) leftChild.getChild(0).getChild(1)).getInt());

        assertEquals(3, ((ConstantOperator) leftChild.getChild(1).getChild(0)).getInt());
        assertEquals(4, ((ConstantOperator) leftChild.getChild(1).getChild(1)).getInt());

        assertEquals(5, ((ConstantOperator) rightChild.getChild(0)).getInt());
        assertEquals(6, ((ConstantOperator) rightChild.getChild(1)).getInt());
    }

    @Test
    public void unknownStats1() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10);
        GlobalStateMgr.getCurrentStatisticStorage().addColumnStatistic(t0, "v1",
                new ColumnStatistic(1, 1, 0, 1, 1));

        Map<ColumnRefOperator, Column> columnRefMap = new HashMap<>();
        columnRefMap.put(new ColumnRefOperator(1, Type.BIGINT, "v1", true),
                t0.getBaseColumn("v1"));
        columnRefMap.put(new ColumnRefOperator(2, Type.BIGINT, "v2", true),
                t0.getBaseColumn("v2"));
        columnRefMap.put(new ColumnRefOperator(3, Type.BIGINT, "v3", true),
                t0.getBaseColumn("v3"));

        OptExpression opt =
                new OptExpression(new LogicalOlapScanOperator(t0, columnRefMap, Maps.newHashMap(), null, -1, null));
        Assert.assertTrue(Utils.hasUnknownColumnsStats(opt));

        GlobalStateMgr.getCurrentStatisticStorage().addColumnStatistic(t0, "v2",
                new ColumnStatistic(1, 1, 0, 1, 1));
        GlobalStateMgr.getCurrentStatisticStorage().addColumnStatistic(t0, "v3",
                new ColumnStatistic(1, 1, 0, 1, 1));
        opt = new OptExpression(new LogicalOlapScanOperator(t0, columnRefMap, Maps.newHashMap(), null, -1, null));
        Assert.assertFalse(Utils.hasUnknownColumnsStats(opt));
    }

    @Test
    public void unknownStats2() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        OptExpression opt =
                new OptExpression(
                        new LogicalOlapScanOperator(t1, Maps.newHashMap(), Maps.newHashMap(), null, -1, null));
        Assert.assertFalse(Utils.hasUnknownColumnsStats(opt));
    }

    @Test
    public void testCountJoinNode() {
        OptExpression root = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null),
                OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null),
                        OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null)),
                        OptExpression.create(new LogicalValuesOperator(Lists.newArrayList(), Lists.newArrayList()))),
                OptExpression.create(new LogicalValuesOperator(Lists.newArrayList(), Lists.newArrayList())));

        assertEquals(1, Utils.countJoinNodeSize(root, JoinOperator.semiAntiJoinSet()));


        //      outer join (left child semi join node = 1, right child semi join node = 3) => result is 3
        //      /         \
        //  semi join      semi join (left child semi join node = 1, right child semi join node = 1)
        //                   /    \            => result is 1 + 1 + 1 = 3
        //           outer join   semi join
        //             /      \
        //         semi join   node
        root = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null),
                OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null)),
                OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                        OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null),
                                OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null),
                                        OptExpression.create(
                                                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null)),
                                        OptExpression.create(
                                                new LogicalValuesOperator(Lists.newArrayList(), Lists.newArrayList()))),
                                OptExpression.create(
                                        new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null)))));

        assertEquals(3, Utils.countJoinNodeSize(root, JoinOperator.semiAntiJoinSet()));

        //      semi join (left child semi join node = 0, right child semi join node = 3) => result is 0 + 3 + 1 = 4
        //      /         \
        //  inner join   semi join (left child semi join node = 2, right child semi join node = 0)
        //                /    \            => result is 2 + 0 + 1 = 3
        //        semi join    node
        //         /      \
        //   semi join  node
        root = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null),
                OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null)),
                OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                        OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null),
                                OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null),
                                        OptExpression.create(
                                                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null)),
                                        OptExpression.create(
                                                new LogicalValuesOperator(Lists.newArrayList(), Lists.newArrayList()))),
                                OptExpression.create(
                                        new LogicalValuesOperator(Lists.newArrayList(), Lists.newArrayList())))));

        assertEquals(4, Utils.countJoinNodeSize(root, JoinOperator.semiAntiJoinSet()));
    }
}