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

package com.starrocks.catalog.system.sys;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SysObjectDependenciesTest {

    private StarRocksAssert starRocksAssert;

    private ConnectContext connectContext;

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();


    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String createUserSql = "create user if not exists test_mv";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createUserSql, connectContext), connectContext);
    }

    @Test
    public void testObjectDependencies() throws Exception {
        starRocksAssert.withDatabase("test")
                .useDatabase("test")
                .withTable("CREATE TABLE test.test_mv_base_table\n" +
                        "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n" + ")\n"
                        + "PARTITION BY RANGE(k1)\n" +
                        "(\n" + "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n"
                        + "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" + ")\n"
                        + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" + "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.mv_1\n"
                        + "PARTITION BY k1\n" + "distributed by hash(k2) buckets 3\n"
                        + "refresh async\n"
                        + "as select k1, k2, sum(v1) as total from test_mv_base_table group by k1, k2;")
                .withMaterializedView("create materialized view test.mv_2\n"
                        + "PARTITION BY date_trunc('month', k1)\n"
                        + "distributed by hash(k2) buckets 3\n"
                        + "refresh async\n"
                        + "as select k1, k2, sum(v1) as total from test_mv_base_table group by k1, k2;");


        String grantSql2 = "GRANT ALL ON TABLE test.test_mv_base_table TO USER `test_mv`@`%`;";
        String grantSql = "GRANT ALL ON MATERIALIZED VIEW test.mv_1 TO USER `test_mv`@`%`;";
        String grantSql1 = "GRANT ALL ON MATERIALIZED VIEW test.mv_2 TO USER `test_mv`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql2, connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql1, connectContext), connectContext);

        TObjectDependencyReq dependencyReq = buildRequest();

        TObjectDependencyRes objectDependencyRes = SysObjectDependencies.listObjectDependencies(dependencyReq);
        Assert.assertNotNull(objectDependencyRes);
        Assert.assertEquals(2, objectDependencyRes.getItemsSize());
        Assert.assertEquals("OLAP", objectDependencyRes.getItems().get(0).getRef_object_type());
    }


    @Test
    public void testUnknownCatalogObjectDependencies() throws Exception {
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());
        starRocksAssert = new StarRocksAssert(connectContext);


        String mvName = "test.iceberg_mv";

        starRocksAssert.withDatabase("test")
                .useDatabase("test")
                .withMaterializedView("create materialized view " + mvName + " " +
                        "partition by str2date(d,'%Y-%m-%d') " +
                        "distributed by hash(a) " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\n" +
                        "'replication_num' = '1'" +
                        ") " +
                        "as select a, b, d, bitmap_union(to_bitmap(t1.c))" +
                        " from iceberg0.partitioned_db.part_tbl1 as t1 " +
                        " group by a, b, d;");

        String grantSql1 = "GRANT ALL ON MATERIALIZED VIEW test.iceberg_mv TO USER `test_mv`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql1, connectContext), connectContext);


        TObjectDependencyReq dependencyReq = buildRequest();


        TObjectDependencyRes objectDependencyRes = SysObjectDependencies.listObjectDependencies(dependencyReq);

        Assert.assertEquals("ICEBERG", objectDependencyRes.getItems().get(0).getRef_object_type());
    }


    private static TObjectDependencyReq buildRequest() {
        TObjectDependencyReq dependencyReq = new TObjectDependencyReq();
        TAuthInfo tAuthInfo = new TAuthInfo();

        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("test_mv");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        tAuthInfo.setCurrent_user_ident(userIdentity);
        tAuthInfo.setUser("root");
        tAuthInfo.setUser_ip("127.0.0.1");
        dependencyReq.setAuth_info(tAuthInfo);
        return dependencyReq;
    }


}
