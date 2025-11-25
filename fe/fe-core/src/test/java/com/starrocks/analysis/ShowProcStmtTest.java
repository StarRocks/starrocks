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

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class ShowProcStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testShowProc() {
        ShowProcStmt stmt = new ShowProcStmt("/dbs");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("/dbs", stmt.getPath());
    }

    @Test
    public void testShowProcSql() {
        String sql = "show proc '/dbs/10001'";
        ShowProcStmt stmt =
                (ShowProcStmt)com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        Assertions.assertEquals("/dbs/10001", stmt.getPath());
    }
}
