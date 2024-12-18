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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;


public class StatisticsMetaMgrTest extends PlanTestBase  {
    @Test
    public void alterMetaTable() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        StatisticsMetaManager m = new StatisticsMetaManager();
        String tblName = "test_alter_name";
        m.createStatisticsTablesForTest();
        TableName tableName = new TableName(STATISTICS_DB_NAME, tblName);
        Map<String, String> properties = Maps.newHashMap();

        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
        CreateTableStmt stmt = new CreateTableStmt(false, false,
                tableName,
                ImmutableList.of(
                        new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                        new ColumnDef("partition_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                        new ColumnDef("column_name", new TypeDef(ScalarType.createVarcharType(65530))),
                        new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                        new ColumnDef("table_name", new TypeDef(ScalarType.createVarcharType(65530)))),
                EngineType.defaultEngine().name(),
                new KeysDesc(KeysType.UNIQUE_KEYS, ImmutableList.of("table_id", "partition_id", "column_name")),
                null,
                new HashDistributionDesc(10, ImmutableList.of("table_id", "partition_id", "column_name")),
                properties,
                null,
                "");
        Analyzer.analyze(stmt, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        Table table = globalStateMgr.getLocalMetastore().getTable("_statistics_", tblName);
        m.alterFullStatisticsTable(connectContext, table);
        Assert.assertTrue(m.checkTableCompatible(tblName));
        Assert.assertTrue(m.alterTable(FULL_STATISTICS_TABLE_NAME));
    }
}
