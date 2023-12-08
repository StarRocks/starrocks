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

package com.starrocks.sql.plan;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.StatementBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class SetVarTest extends PlanTestBase {

    @Test
    public void testInsertStmt() throws Exception {
        SessionVariable variable = starRocksAssert.getCtx().getSessionVariable();
        int queryTimeout = variable.getQueryTimeoutS();

        // prepare table
        starRocksAssert.withTable("create table tbl (c1 int) properties('replication_num'='1')");
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                SessionVariable variables = execPlan.getConnectContext().getSessionVariable();
                Assert.assertEquals(10, variables.getQueryTimeoutS());
            }
        };

        new MockUp<DDLStmtExecutor>() {
            @Mock
            public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
                SessionVariable variables = context.getSessionVariable();
                Assert.assertFalse(variables.getEnableAdaptiveSinkDop());
                return null;
            }
        };

        // insert
        {
            String sql = "insert /*+set_var(query_timeout=10) */ into tbl values(1) ";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // update
        {
            String sql = "update /*+set_var(query_timeout=10) */ tbl set c1 = 2 where c1 = 1";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // delete
        {
            String sql = "delete /*+set_var(query_timeout=10) */ from tbl where c1 = 1";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // load
        {
            boolean enableAdaptiveSinkDop = variable.getEnableAdaptiveSinkDop();
            String sql = "LOAD /*+set_var(enable_adaptive_sink_dop=false)*/ "
                          + "LABEL label0 (DATA INFILE('/path1/file') INTO TABLE tbl)";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(enableAdaptiveSinkDop, variable.getEnableAdaptiveSinkDop());
        }

        starRocksAssert.dropTable("tbl");
    }
}
