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

package com.starrocks.qe;

import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class StmtExecutorTest {

    @Test
    public void testIsForwardToLeader(@Mocked GlobalStateMgr state) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.isInTransferringToLeader();
                times = 1;
                result = true;

                state.getSqlParser();
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                times = 2;
                result = false;
                result = true;
            }
        };

        Assertions.assertFalse(new StmtExecutor(new ConnectContext(),
                SqlParser.parseSingleStatement("show frontends", SqlModeHelper.MODE_DEFAULT)).isForwardToLeader());
    }

    @Test
    public void testForwardExplicitTxnSelectOnFollower(@Mocked GlobalStateMgr state,
                                                       @Mocked ConnectContext ctx) {
        StatementBase stmt;
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                minTimes = 0;
                result = false;

                state.isInTransferringToLeader();
                minTimes = 0;
                result = false;

                ctx.getSerializer();
                minTimes = 0;
                result = serializer;

                ctx.getTxnId();
                minTimes = 0;
                result = 1L;

                ctx.isQueryStmt((StatementBase) any);
                minTimes = 0;
                result = true;
            }
        };

        // Parse after expectations to ensure GlobalStateMgr.getSqlParser() is properly mocked
        stmt = SqlParser.parseSingleStatement("select 1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        Assertions.assertTrue(executor.isForwardToLeader());
    }

    @Test
    public void testExecType() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Query", executor.getExecType());
        Assertions.assertFalse(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("insert into t1 select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("create table t1 as select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("update t1 set k1 = 1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Update", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("delete from t1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Delete", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
    }

    @Test
    public void buildTopLevelProfile_createsProfileWithCorrectSummaryInfo() {
        new MockUp<WarehouseManager>() {
            @Mock
            public String getWarehouseComputeResourceName(ComputeResource computeResource) {
                return "default_warehouse";
            }
        };

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
        RuntimeProfile profile = Deencapsulation.invoke(executor, "buildTopLevelProfile");

        Assertions.assertNotNull(profile);
        Assertions.assertEquals("Query", profile.getName());
        RuntimeProfile summaryProfile = profile.getChild("Summary");
        Assertions.assertNotNull(summaryProfile);
        Assertions.assertEquals("Running", summaryProfile.getInfoString(ProfileManager.QUERY_STATE));
        Assertions.assertEquals("default_warehouse", summaryProfile.getInfoString(ProfileManager.WAREHOUSE_CNGROUP));
    }

    @Test
    public void testExecTimeout() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        {
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("analyze table t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(Config.statistic_collect_query_timeout, executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("create table t2 as select * from t1",
                    SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
        }
    }

    @Test
    public void testCollectAndCheckPlanScanLimits() throws Exception {
        // mock resource group limits
        TWorkGroup group = new TWorkGroup();
        group.setPlan_scan_rows_limit(80L);
        group.setPlan_scan_partitions_limit(3L);
        group.setPlan_scan_tablets_limit(2L);

        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext connect,
                                                   ResourceGroupClassifier.QueryType queryType) {
                return group;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ctx.setAuditEventBuilder(new AuditEvent.AuditEventBuilder());

        ExecPlan execPlan = new ExecPlan();

        OlapScanNode olapScan = Mockito.mock(OlapScanNode.class);
        when(olapScan.getCardinality()).thenReturn(100L);
        when(olapScan.getSelectedPartitionNum()).thenReturn(2L);
        when(olapScan.getScanTabletIds()).thenReturn(List.of(1L, 2L, 3L));

        HdfsScanNode hdfsScan = Mockito.mock(HdfsScanNode.class);
        HDFSScanNodePredicates preds = Mockito.mock(HDFSScanNodePredicates.class);
        when(hdfsScan.getCardinality()).thenReturn(50L);
        when(hdfsScan.getScanNodePredicates()).thenReturn(preds);
        when(preds.getSelectedPartitionIds()).thenReturn(Set.of(1L, 2L, 3L, 4L));

        PaimonScanNode paimonScan = Mockito.mock(PaimonScanNode.class);
        when(paimonScan.getCardinality()).thenReturn(60L);
        when(paimonScan.getSelectedPartitionNum()).thenReturn(1L);

        HudiScanNode hudiScan = Mockito.mock(HudiScanNode.class);
        when(hudiScan.getCardinality()).thenReturn(70L);
        when(hudiScan.getSelectedPartitionNum()).thenReturn(2L);

        execPlan.getScanNodes().addAll(List.of(olapScan, hdfsScan, paimonScan, hudiScan));

        StmtExecutor executor = new StmtExecutor(ctx, Mockito.mock(com.starrocks.sql.ast.StatementBase.class));

        Method m = StmtExecutor.class.getDeclaredMethod(
                "collectAndCheckPlanScanLimits", ExecPlan.class, ConnectContext.class);
        m.setAccessible(true);

        assertThatThrownBy(() -> m.invoke(executor, execPlan, ctx))
                .hasCauseInstanceOf(DdlException.class);
    }
}
