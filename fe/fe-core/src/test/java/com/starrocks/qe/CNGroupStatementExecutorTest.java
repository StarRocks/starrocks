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

import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class CNGroupStatementExecutorTest {

    @Test
    public void testCnGroupStatement() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        String cnGroupNotImplementedMsg = "CnGroup is not implemented";
        {
            StatementBase stmt =
                    SqlParser.parseSingleStatement("ALTER WAREHOUSE default_warehouse ADD CNGROUP cngroup1",
                            SqlModeHelper.MODE_DEFAULT);
            Assert.assertTrue(stmt instanceof CreateCnGroupStmt);
            ctx.setQueryId(UUIDUtil.genUUID());
            DdlException exception = Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt, ctx));
            Assert.assertEquals(cnGroupNotImplementedMsg, exception.getMessage());
        }
        {
            StatementBase stmt =
                    SqlParser.parseSingleStatement("ALTER WAREHOUSE default_warehouse DROP CNGROUP cngroup1",
                            SqlModeHelper.MODE_DEFAULT);
            Assert.assertTrue(stmt instanceof DropCnGroupStmt);
            ctx.setQueryId(UUIDUtil.genUUID());
            DdlException exception = Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt, ctx));
            Assert.assertEquals(cnGroupNotImplementedMsg, exception.getMessage());
        }
        {
            StatementBase stmt =
                    SqlParser.parseSingleStatement("ALTER WAREHOUSE default_warehouse ENABLE CNGROUP cngroup1",
                            SqlModeHelper.MODE_DEFAULT);
            Assert.assertTrue(stmt instanceof EnableDisableCnGroupStmt);
            ctx.setQueryId(UUIDUtil.genUUID());
            DdlException exception = Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt, ctx));
            Assert.assertEquals(cnGroupNotImplementedMsg, exception.getMessage());
        }
        {
            StatementBase stmt =
                    SqlParser.parseSingleStatement("ALTER WAREHOUSE default_warehouse DISABLE CNGROUP cngroup1",
                            SqlModeHelper.MODE_DEFAULT);
            Assert.assertTrue(stmt instanceof EnableDisableCnGroupStmt);
            ctx.setQueryId(UUIDUtil.genUUID());
            DdlException exception = Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt, ctx));
            Assert.assertEquals(cnGroupNotImplementedMsg, exception.getMessage());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement(
                    "ALTER WAREHOUSE default_warehouse MODIFY CNGROUP cngroup1 SET ('location' = 'b')",
                    SqlModeHelper.MODE_DEFAULT);
            Assert.assertTrue(stmt instanceof AlterCnGroupStmt);
            ctx.setQueryId(UUIDUtil.genUUID());
            DdlException exception = Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt, ctx));
            Assert.assertEquals(cnGroupNotImplementedMsg, exception.getMessage());
        }
    }
}
