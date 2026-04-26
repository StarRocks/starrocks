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

package com.starrocks.sql.parser;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AnalyzeProfileStmt;
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

public class AnalyzeProfileParserTest {

    @Test
    public void testParseAnalyzeProfileWithStringQueryId() {
        String sql = "ANALYZE PROFILE FROM 'test-query-id'";
        SessionVariable sessionVariable = new SessionVariable();
        StatementBase stmt = SqlParser.parseOneWithStarRocksDialect(sql, sessionVariable);

        Assertions.assertTrue(stmt instanceof AnalyzeProfileStmt);
        AnalyzeProfileStmt analyzeStmt = (AnalyzeProfileStmt) stmt;
        Assertions.assertEquals("test-query-id", analyzeStmt.getQueryId());
        Assertions.assertEquals(Collections.emptyList(), analyzeStmt.getPlanNodeIds());
    }

    @Test
    public void testParseAnalyzeProfileWithStringQueryIdAndPlanNodes() {
        String sql = "ANALYZE PROFILE FROM 'test-query-id', 1, 2, 3";
        SessionVariable sessionVariable = new SessionVariable();
        StatementBase stmt = SqlParser.parseOneWithStarRocksDialect(sql, sessionVariable);

        Assertions.assertTrue(stmt instanceof AnalyzeProfileStmt);
        AnalyzeProfileStmt analyzeStmt = (AnalyzeProfileStmt) stmt;
        Assertions.assertEquals("test-query-id", analyzeStmt.getQueryId());
        Assertions.assertEquals(Arrays.asList(1, 2, 3), analyzeStmt.getPlanNodeIds());
    }

    @Test
    public void testParseAnalyzeProfileWithLastQueryId() {
        ConnectContext context = ConnectContext.build();
        context.setThreadLocalInfo();
        context.setLastQueryId(UUID.randomUUID());
        String lastQueryId = context.getLastQueryId().toString();

        String sql = "ANALYZE PROFILE FROM LAST_QUERY_ID()";
        SessionVariable sessionVariable = new SessionVariable();
        StatementBase stmt = SqlParser.parseOneWithStarRocksDialect(sql, sessionVariable);

        Assertions.assertTrue(stmt instanceof AnalyzeProfileStmt);
        AnalyzeProfileStmt analyzeStmt = (AnalyzeProfileStmt) stmt;
        Assertions.assertEquals(lastQueryId, analyzeStmt.getQueryId());
        Assertions.assertEquals(Collections.emptyList(), analyzeStmt.getPlanNodeIds());
    }

    @Test
    public void testParseAnalyzeProfileWithLastQueryIdAndPlanNodes() {
        ConnectContext context = ConnectContext.build();
        context.setThreadLocalInfo();
        context.setLastQueryId(UUID.randomUUID());
        String lastQueryId = context.getLastQueryId().toString();
        
        String sql = "ANALYZE PROFILE FROM LAST_QUERY_ID(), 0, 1";
        SessionVariable sessionVariable = new SessionVariable();
        StatementBase stmt = SqlParser.parseOneWithStarRocksDialect(sql, sessionVariable);

        Assertions.assertTrue(stmt instanceof AnalyzeProfileStmt);
        AnalyzeProfileStmt analyzeStmt = (AnalyzeProfileStmt) stmt;
        Assertions.assertEquals(lastQueryId, analyzeStmt.getQueryId());
        Assertions.assertEquals(Arrays.asList(0, 1), analyzeStmt.getPlanNodeIds());
    }
}
