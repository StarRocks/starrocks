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

import com.starrocks.common.ErrorCode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import org.junit.Assert;
import org.junit.Test;

public class WarehouseAnalyzerTest {

    @Test
    public void testCNGroupStatement() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        {
            CreateCnGroupStmt stmt = new CreateCnGroupStmt(true, "", "cngroup", "", null);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_WAREHOUSE_NAME, context.getState().getErrorCode());
        }
        {
            CreateCnGroupStmt stmt = new CreateCnGroupStmt(true, "default_warehouse", "", "", null);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, context.getState().getErrorCode());
        }
        {
            DropCnGroupStmt stmt = new DropCnGroupStmt(true, "", "cngroup", true);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_WAREHOUSE_NAME, context.getState().getErrorCode());
        }
        {
            DropCnGroupStmt stmt = new DropCnGroupStmt(true, "default_warehouse", "", true);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, context.getState().getErrorCode());
        }
        {
            EnableDisableCnGroupStmt stmt = new EnableDisableCnGroupStmt("", "cngroup", true);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_WAREHOUSE_NAME, context.getState().getErrorCode());
        }
        {
            EnableDisableCnGroupStmt stmt = new EnableDisableCnGroupStmt("default_warehouse", "", true);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, context.getState().getErrorCode());
        }
        {
            AlterCnGroupStmt stmt = new AlterCnGroupStmt("", "cngroup", null);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_WAREHOUSE_NAME, context.getState().getErrorCode());
        }
        {
            AlterCnGroupStmt stmt = new AlterCnGroupStmt("default_warehouse", "", null);
            Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, null));
            Assert.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, context.getState().getErrorCode());
        }
    }
}
