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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class ReserveLeadingInlineCommentTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");

        String createTblStmtStr = "create table db.tb2(kk1 int, kk2 varchar(200)) "
                + "DUPLICATE KEY(kk1) distributed by hash(kk1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void test() {
        String sqlFmt = "{COMMENT} select count(1) from db.tb2";
        String[] comments = new String[] {
                "/*abc*/",
                "   /*abcdef */",
                "/*\n\n\n\rabcdef\n\n\r*/",
                "/* celostar_context: {extrainfo} */"
        };
        for (String comment : comments) {
            String sql = sqlFmt.replace("{COMMENT}", comment);
            ConnectContext connectContext = starRocksAssert.getCtx();
            StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, connectContext);
            String newSql = AstToSQLBuilder.toSQLOrDefault(stmt, stmt.getOrigStmt().originStmt);
            Assert.assertTrue(newSql.contains(comment.trim()));
        }
    }
}
