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

package com.starrocks.statistic.base;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.hyper.ConstQueryJob;
import com.starrocks.statistic.hyper.HyperQueryJob;
import com.starrocks.statistic.hyper.HyperStatisticSQLs;
import com.starrocks.utframe.StarRocksAssert;
import org.apache.velocity.VelocityContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SampleInfoTest extends DistributedEnvPlanTestBase {

    private static Database db;

    private static Table table;

    private static PartitionSampler sampler;

    private static long pid;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table t_struct(c0 INT, " +
                "c1 date," +
                "c2 varchar(255)," +
                "c3 decimal(10, 2)," +
                "c4 struct<a int, b array<struct<a int, b int>>>," +
                "c5 struct<a int, b int>," +
                "c6 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t_struct");
        pid = table.getPartition("t_struct").getId();
        sampler = PartitionSampler.create(table, List.of(pid), Maps.newHashMap());
    }

    @Test
    public void generateComplexTypeColumnTask() {
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());

        List<HyperQueryJob> job =
                HyperQueryJob.createFullQueryJobs(connectContext, db, table, columnNames, columnTypes, List.of(pid), 1);
        Assert.assertEquals(2, job.size());
        Assert.assertTrue(job.get(1) instanceof ConstQueryJob);
    }

    @Test
    public void generatePrimitiveTypeColumnTask() {
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());

        ColumnClassifier cc = ColumnClassifier.of(columnNames, columnTypes, table);
        ColumnStats columnStat = cc.getColumnStats().stream().filter(c -> c instanceof PrimitiveTypeColumnStats)
                .findAny().orElse(null);

        VelocityContext context = HyperStatisticSQLs.buildBaseContext(db, table, table.getPartition(pid), columnStat);
        context.put("dataSize", columnStat.getFullDateSize());
        context.put("countNullFunction", columnStat.getFullNullCount());
        context.put("hllFunction", columnStat.getNDV());
        context.put("maxFunction", columnStat.getMax());
        context.put("minFunction", columnStat.getMin());
        String sql = HyperStatisticSQLs.build(context, HyperStatisticSQLs.BATCH_FULL_STATISTIC_TEMPLATE);
        assertContains(sql, "hex(hll_serialize(IFNULL(hll_raw(`c0`)");
        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        Assert.assertTrue(stmt.get(0) instanceof QueryStatement);
    }

    @Test
    public void generateSubFieldTypeColumnTask() {
        List<String> columnNames = Lists.newArrayList("c1", "c4.b", "c6.c.b");
        List<Type> columnTypes = Lists.newArrayList(Type.DATE, new ArrayType(Type.ANY_STRUCT), Type.INT);

        ColumnClassifier cc = ColumnClassifier.of(columnNames, columnTypes, table);
        List<ColumnStats> columnStat = cc.getColumnStats().stream().filter(c -> c instanceof SubFieldColumnStats)
                .collect(Collectors.toList());
        String sql = HyperStatisticSQLs.buildSampleSQL(db, table, table.getPartition(pid), columnStat, sampler,
                HyperStatisticSQLs.BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE);
        Assert.assertEquals(1, columnStat.size());
        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        Assert.assertTrue(stmt.get(0) instanceof QueryStatement);
    }

    @AfterClass
    public static void afterClass() {
        FeConstants.runningUnitTest = false;
    }
}