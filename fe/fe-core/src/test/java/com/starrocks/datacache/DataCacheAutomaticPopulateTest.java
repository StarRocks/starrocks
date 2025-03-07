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

package com.starrocks.datacache;

import com.starrocks.analysis.Expr;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DataCacheAutomaticPopulateTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Before
    public void resetSessionVariable() {
        connectContext.setStatisticsContext(false);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableScanDataCache(true);
        sessionVariable.setEnablePopulateDataCache(true);
        sessionVariable.setDataCachePopulateMode(DataCachePopulateMode.AUTO.modeName());
        // just for mock
        connectContext.setExecutor(new StmtExecutor(connectContext, new QueryStatement(new QueryRelation() {
            @Override
            public List<Expr> getOutputExpression() {
                return List.of();
            }
        })));
    }


    @Test
    public void testCompatibleWithOldParameter() throws Exception {
        String sql = "select age from hive0.datacache_db.multi_partition_table where l_shipdate>='1998-01-03' and l_orderkey=1";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: true}");

        connectContext.getSessionVariable().setEnablePopulateDataCache(false);
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");
    }

    @Test
    public void testAlwaysMode() throws Exception {
        String sql = "select * from hive0.datacache_db.multi_partition_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");

        connectContext.getSessionVariable().setDataCachePopulateMode(DataCachePopulateMode.ALWAYS.modeName());

        sql = "select * from hive0.datacache_db.multi_partition_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: true}");
    }

    @Test
    public void testAllColumnsScan() throws Exception {
        // two columns, not populate
        String sql = "select * from hive0.datacache_db.multi_partition_table where l_shipdate>='1998-01-03' and l_orderkey=1";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");

        // specific one column, should not ignore it
        sql = "select age from hive0.datacache_db.multi_partition_table where l_shipdate>='1998-01-03' and l_orderkey=1";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: true}");
    }

    @Test
    public void testAllPartitionScan() throws Exception {
        // all partitions scan
        String sql = "select age from hive0.datacache_db.multi_partition_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");
    }

    @Test
    public void testOneColumnOnePartitionScan() throws Exception {
        // normal_table has only one column, should not ignore it
        String sql = "select * from hive0.datacache_db.normal_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: true}");
    }

    @Test
    public void testNoneQueryStatement() throws Exception {
        connectContext.setExecutor(new StmtExecutor(connectContext, new InsertStmt(null, NodePosition.ZERO)));
        String sql = "select * from hive0.datacache_db.normal_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");
    }

    @Test
    public void testStatisticsCollectSQL() throws Exception {
        connectContext.setStatisticsContext(true);
        String sql = "select * from hive0.datacache_db.normal_table";
        assertVerbosePlanContains(sql, "dataCacheOptions={populate: false}");
    }

    @Test
    public void testDisableDataCache() throws Exception {
        connectContext.getSessionVariable().setEnableScanDataCache(false);
        String sql = "select * from hive0.datacache_db.normal_table";
        Assert.assertFalse(getVerboseExplain(sql).contains("dataCacheOptions"));
    }
}
