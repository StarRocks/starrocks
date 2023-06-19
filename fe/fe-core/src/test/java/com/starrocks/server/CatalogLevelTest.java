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


package com.starrocks.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.iceberg.hive.HiveTableOperations;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CatalogLevelTest {
    HiveMetaClient metaClient = new HiveMetastoreTest.MockedHiveMetaClient();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testQueryExternalCatalogInDefaultCatalog(@Mocked MetadataMgr metadataMgr) throws Exception {
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\"," +
                " \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);

        Table hmsTable = metaClient.getTable("hive_db", "hive_table");
        com.starrocks.catalog.Table hiveTable = HiveMetastoreApiConverter.toHiveTable(hmsTable, "hive_catalog");
        GlobalStateMgr.getCurrentState().setMetadataMgr(metadataMgr);
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("hive_catalog", "hive_db");
                result = new Database(111, "hive_db");
                minTimes = 0;

                metadataMgr.getTable("hive_catalog", "hive_db", "hive_table");
                result = hiveTable;
            }
        };
        String sql1 = "select col1 from hive_catalog.hive_db.hive_table";

        AnalyzeTestUtil.analyzeSuccess(sql1);

        String sql = "create user u1";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().createUser(createUserStmt);

        AnalyzeTestUtil.getConnectContext().setCurrentUserIdentity(new UserIdentity("u1", "%"));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(
                AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db"));

        String grantSql = "grant all on CATALOG hive_catalog to u1";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql,
                AnalyzeTestUtil.getConnectContext());
        DDLStmtExecutor.execute(grantPrivilegeStmt,  AnalyzeTestUtil.getConnectContext());
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(
                AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db"));

        grantSql = "grant ALL on DATABASE hive_catalog.hive_db to u1";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql,
                AnalyzeTestUtil.getConnectContext());
        DDLStmtExecutor.execute(grantPrivilegeStmt,  AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(
                AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db"));
    }

    @Test
    public void testQueryIcebergCatalog(@Mocked MetadataMgr metadataMgr,
                                        @Mocked HiveTableOperations hiveTableOperations) throws Exception {
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\"," +
                " \"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\" = \"hive\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        Configuration conf = new Configuration();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://127.0.0.1:9083");

        org.apache.iceberg.Table tbl = new org.apache.iceberg.BaseTable(hiveTableOperations, "iceberg_table");
        com.starrocks.catalog.Table icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "iceberg_db", "iceberg_table",
                Lists.newArrayList(new Column("col1", Type.LARGEINT)), tbl, Maps.newHashMap());

        GlobalStateMgr.getCurrentState().setMetadataMgr(metadataMgr);
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("iceberg_catalog", "iceberg_db");
                result = new Database(111, "iceberg_db");
                minTimes = 0;

                metadataMgr.getTable("iceberg_catalog", "iceberg_db", "iceberg_table");
                result = icebergTable;
            }
        };
        String sql1 = "select col1 from iceberg_catalog.iceberg_db.iceberg_table";

        AnalyzeTestUtil.analyzeSuccess(sql1);
    }
}
