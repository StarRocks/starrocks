// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/utframe/StarRocksAssert.java

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

import com.google.common.base.Preconditions;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class StarRocksAssert {

    private ConnectContext ctx;

    public StarRocksAssert() throws IOException {
        this(UtFrameUtils.createDefaultCtx());
    }

    public StarRocksAssert(ConnectContext ctx) {
        Config.tablet_create_timeout_second = 60;
        this.ctx = ctx;
        this.ctx.setQueryId(UUIDUtil.genUUID());
    }

    public ConnectContext getCtx() {
        return this.ctx;
    }

    public StarRocksAssert withEnableMV() {
        Config.enable_materialized_view = true;
        return this;
    }

    public StarRocksAssert withDatabase(String dbName) throws Exception {
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database " + dbName + ";", ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        return this;
    }

    public StarRocksAssert withDatabaseWithoutAnalyze(String dbName) throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, dbName);
        dbStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        Catalog.getCurrentCatalog().createDb(dbStmt);
        return this;
    }

    public boolean databaseExist(String dbName) {
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:" + dbName);
        return db != null;
    }

    public StarRocksAssert useDatabase(String dbName) {
        ctx.setDatabase(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        return this;
    }

    public StarRocksAssert withoutUseDatabase() {
        ctx.setDatabase("");
        return this;
    }

    public StarRocksAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        return this;
    }

    public StarRocksAssert withView(String sql) throws Exception {
        CreateViewStmt createTableStmt = (CreateViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createView(createTableStmt);
        return this;
    }

    public StarRocksAssert dropView(String viewName) throws Exception {
        DropTableStmt dropViewStmt =
                (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt("drop view " + viewName + ";", ctx);
        Catalog.getCurrentCatalog().dropTable(dropViewStmt);
        return this;
    }

    public StarRocksAssert dropDatabase(String dbName) throws Exception {
        DropDbStmt dropDbStmt =
                (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt("drop database " + dbName + ";", ctx);
        Catalog.getCurrentCatalog().dropDb(dropDbStmt);
        return this;
    }

    public StarRocksAssert dropTable(String tableName) throws Exception {
        DropTableStmt dropTableStmt =
                (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt("drop table " + tableName + ";", ctx);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
        return this;
    }

    // Add materialized view to the schema
    public StarRocksAssert withMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStmt createMaterializedViewStmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createMaterializedView(createMaterializedViewStmt);
        checkAlterJob();
        return this;
    }

    // Add rollup
    public StarRocksAssert withRollup(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().alterTable(alterTableStmt);
        checkAlterJob();
        return this;
    }

    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getJobState().isFinalState()) {
                continue;
            }
            Database database = Catalog.getCurrentCatalog().getDb(alterJobV2.getDbId());
            Table table = database.getTable(alterJobV2.getTableId());
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable olapTable = (OlapTable) table;
            int retry = 0;
            while (olapTable.getState() != OlapTable.OlapTableState.NORMAL && retry++ < 6000) {
                Thread.sleep(10);
            }
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    public QueryAssert query(String sql) {
        return new QueryAssert(ctx, sql);
    }

    public class QueryAssert {
        private final ConnectContext connectContext;
        private final String sql;

        public QueryAssert(ConnectContext connectContext, String sql) {
            this.connectContext = connectContext;
            this.sql = sql;
        }

        public void explainContains(String... keywords) throws Exception {
            String plan = explainQuery();
            Assert.assertTrue(plan, Stream.of(keywords).allMatch(plan::contains));
        }

        public void explainContains(String keywords, int count) throws Exception {
            Assert.assertEquals(StringUtils.countMatches(explainQuery(), keywords), count);
        }

        public void explainWithout(String s) throws Exception {
            Assert.assertFalse(explainQuery().contains(s));
        }

        public String explainQuery() throws Exception {
            return UtFrameUtils.getFragmentPlan(connectContext, sql);
        }

        public void analysisError(String keywords) {
            try {
                explainQuery();
            } catch (AnalysisException | StarRocksPlannerException analysisException) {
                Assert.assertTrue(Stream.of(keywords).allMatch(analysisException.getMessage()::contains));
                return;
            } catch (Exception ex) {
                Assert.fail();
            }
            Assert.fail();
        }
    }
}
