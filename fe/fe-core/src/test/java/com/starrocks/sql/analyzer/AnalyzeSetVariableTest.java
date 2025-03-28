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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SetExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.uber.m3.util.ImmutableMap;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSetUserVariableFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.connectContext;
import static org.assertj.core.api.Assertions.assertThat;

public class AnalyzeSetVariableTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
    }

    @Test
    public void testSetVariable() {
        String sql = "set query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set `query_timeout` = 10";
        analyzeSuccess(sql);
        sql = "set \"query_timeout\" = 10";
        analyzeFail(sql);
        sql = "set GLOBAL query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set SESSION query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set LOCAL query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set tablet_internal_parallel_mode = auto";
        analyzeSuccess(sql);
        sql = "set tablet_internal_parallel_mode = force_split";
        analyzeSuccess(sql);
        sql = "set tablet_internal_parallel_mode = force";
        analyzeFail(sql);
    }

    @Test
    public void testUserVariable() throws Exception {
        String sql = "set @var1 = 1";
        analyzeSuccess(sql);
        sql = "set @`var1` = 1";
        analyzeSuccess(sql);
        sql = "set @'var1' = 1";
        analyzeSuccess(sql);
        sql = "set @\"var1\" = 1";
        analyzeSuccess(sql);

        sql = "set @varvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv123 = 1";
        analyzeFail(sql, "User variable name 'varvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv123' is illegal");

        sql = "set @var = NULL";
        analyzeSuccess(sql);

        sql = "set @var = 1 + 2";
        SetStmt setStmt = (SetStmt) analyzeSuccess(sql);
        ConnectContext ctx = starRocksAssert.getCtx();
        UserVariable userVariable = (UserVariable) setStmt.getSetListItems().get(0);
        SetExecutor executor = new SetExecutor(ctx, setStmt);
        executor.execute();
        Assert.assertNotNull(userVariable.getEvaluatedExpression());
        Assert.assertEquals("3", ((LiteralExpr) userVariable.getEvaluatedExpression()).getStringValue());

        sql = "set @var = abs(1.2)";
        setStmt = (SetStmt) analyzeSuccess(sql);
        userVariable = (UserVariable) setStmt.getSetListItems().get(0);
        SetStmtAnalyzer.calcuteUserVariable(userVariable);
        Assert.assertTrue(userVariable.getUnevaluatedExpression() instanceof Subquery);

        sql = "set @var =JSON_ARRAY(1, 2, 3)";
        setStmt = (SetStmt) analyzeSuccess(sql);
        userVariable = (UserVariable) setStmt.getSetListItems().get(0);
        SetStmtAnalyzer.calcuteUserVariable(userVariable);
        Assert.assertTrue(userVariable.getUnevaluatedExpression() instanceof Subquery);

        sql = "set @var = (select 1)";
        analyzeSuccess(sql);

        sql = "set @var = (select v1 from test.t0)";
        analyzeSuccess(sql);

        sql = "set @var = (select sum(v1) from test.t0)";
        analyzeSuccess(sql);

        sql = "set @var = (select sum(v1) from test.t0 group by v2)";
        setStmt = (SetStmt) analyzeSuccess(sql);
        SetStmtAnalyzer.calcuteUserVariable((UserVariable) setStmt.getSetListItems().get(0));
        Assert.assertTrue(((UserVariable) setStmt.getSetListItems().get(0)).getUnevaluatedExpression().getType().isIntegerType());

        sql = "set @var1 = 1, @var2 = 2";
        setStmt = (SetStmt) analyzeSuccess(sql);
        Assert.assertEquals(2, setStmt.getSetListItems().size());

        sql = "set @var = [1,2,3]";
        setStmt = (SetStmt) analyzeSuccess(sql);
        Assert.assertEquals(1, setStmt.getSetListItems().size());

        sql = "set @var = to_binary('abab', 'hex')";
        analyzeSetUserVariableFail(sql, "Can't set variable with type VARBINARY");

        sql = "set @var = [bitmap_empty(), bitmap_empty(), bitmap_empty()]";
        analyzeSetUserVariableFail(sql, "Can't set variable with type ARRAY<BITMAP>");

        sql = "set @var = bitmap_empty()";
        analyzeSetUserVariableFail(sql, "Can't set variable with type BITMAP");

        sql = "set @var = (select bitmap_empty())";
        analyzeSetUserVariableFail(sql, "Can't set variable with type BITMAP");

        sql = "set @var = hll_empty()";
        analyzeSetUserVariableFail(sql, "Can't set variable with type HLL");

        sql = "set @var = percentile_empty()";
        analyzeSetUserVariableFail(sql, "Can't set variable with type PERCENTILE");

        sql = "set @var=foo";
        analyzeSetUserVariableFail(sql, "Column 'foo' cannot be resolved");
    }

    @Test
    public void testSystemVariable() {
        String sql = "set @@query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@GLOBAL.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@SESSION.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@LOCAL.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@event_scheduler = ON";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetNames() {
        String sql = "SET NAMES 'utf8mb4' COLLATE 'bogus'";
        analyzeSuccess(sql);
        sql = "SET NAMES 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "SET NAMES default";
        analyzeSuccess(sql);
        sql = "SET CHARSET 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "SET CHAR SET 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "show character set where charset = 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "SET CHARACTER SET utf8";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetPassword() {
        String sql = "SET PASSWORD FOR 'testUser' = PASSWORD('testPass')";
        SetStmt setStmt = (SetStmt) analyzeSuccess(sql);
        SetPassVar setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        String password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);

        sql = "SET PASSWORD = PASSWORD('testPass')";
        setStmt = (SetStmt) analyzeSuccess(sql);
        setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);

        sql = "SET PASSWORD = '*88EEBA7D913688E7278E2AD071FDB5E76D76D34B'";
        setStmt = (SetStmt) analyzeSuccess(sql);
        setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);
    }

    @Test
    public void testSetResourceGroupName() {
        String rg1Name = "rg1";
        TWorkGroup rg1 = new TWorkGroup();
        ResourceGroupMgr mgr = GlobalStateMgr.getCurrentState().getResourceGroupMgr();
        new Expectations(mgr) {
            {
                mgr.chooseResourceGroupByName(rg1Name);
                result = rg1;
            }

            {
                mgr.chooseResourceGroupByName(anyString);
                result = null;
            }
        };

        String sql;

        sql = String.format("SET resource_group = %s", rg1Name);
        analyzeSuccess(sql);

        sql = "SET resource_group = not_exist_rg";
        analyzeFail(sql, "resource group not exists");

        sql = "SET resource_group = ''";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetResourceGroupID() {
        long rg1ID = 1;
        TWorkGroup rg1 = new TWorkGroup();
        rg1.setId(rg1ID);
        ResourceGroupMgr mgr = GlobalStateMgr.getCurrentState().getResourceGroupMgr();
        new Expectations(mgr) {
            {
                mgr.chooseResourceGroupByID(rg1ID);
                result = rg1;
            }

            {
                mgr.chooseResourceGroupByID(anyLong);
                result = null;
            }
        };

        String sql;

        sql = String.format("SET resource_group_id = %s", rg1ID);
        analyzeSuccess(sql);

        sql = "SET resource_group_id = 2";
        analyzeFail(sql, "resource group not exists");

        sql = "SET resource_group_id = 0";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetTran() {
        String sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        analyzeSuccess(sql);

        sql = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        analyzeSuccess(sql);

        sql = "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetAdaptiveDopMaxBlockRowsPerDriverSeq() {
        String sql;

        sql = "SET runtime_adaptive_dop_max_block_rows_per_driver_seq = 0";
        analyzeFail(sql);

        sql = "SET runtime_adaptive_dop_max_block_rows_per_driver_seq = -1";
        analyzeFail(sql);

        sql = "SET runtime_adaptive_dop_max_block_rows_per_driver_seq = 1";
        analyzeSuccess(sql);
    }

    @Test
    public void testComputationFragmentSchedulingPolicy() {
        String sql;

        sql = "SET computation_fragment_scheduling_policy = compute_nodes_only";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = all_nodes";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = ALL_NODES";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = All_nodes";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = 'all_nodes'";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = \"all_nodes\"";
        analyzeSuccess(sql);

        sql = "SET computation_fragment_scheduling_policy = compute_nodes";
        analyzeFail(sql);
    }

    @Test
    public void testSetAnnParams() {
        SessionVariable sv = connectContext.getSessionVariable();
        String sql;

        sql = "set ann_params='invalid-format'";
        analyzeFail(sql,
                "Unsupported ann_params: invalid-format, " +
                        "It should be a Dict JSON string, each key and value of which is string");

        sql = "set ann_params='{\"Efsearch\": [1,2,3]}'";
        analyzeFail(sql,
                "Unsupported ann_params: {\"Efsearch\": [1,2,3]}, " +
                        "It should be a Dict JSON string, each key and value of which is string");

        sql = "set ann_params='{\"invalid-key\":\"abc\"}'";
        analyzeFail(sql, "Unknown index param: `INVALID-KEY");

        sql = "set ann_params='{\"Efsearch\": 0}'";
        analyzeFail(sql, "Value of `EFSEARCH` must be >= 1");

        sql = "set ann_params='{}'";
        analyzeSuccess(sql);
        sv.setAnnParams("{}");
        assertThat(connectContext.getSessionVariable().getAnnParams()).isEmpty();

        sql = "set ann_params=''";
        analyzeSuccess(sql);
        sv.setAnnParams("");
        assertThat(connectContext.getSessionVariable().getAnnParams()).isEmpty();

        sql = "set ann_params='{\"Efsearch\": 1}'";
        analyzeSuccess(sql);
        sv.setAnnParams("{\"Efsearch\": 1}");
        assertThat(connectContext.getSessionVariable().getAnnParams()).containsExactlyEntriesOf(ImmutableMap.of("Efsearch", "1"));
    }
}
