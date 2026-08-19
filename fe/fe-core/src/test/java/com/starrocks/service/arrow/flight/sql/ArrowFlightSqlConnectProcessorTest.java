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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArrowFlightSqlConnectProcessorTest {
    @Test
    public void testParseMarksRelationsForPolicyRewrite() throws Exception {
        ArrowFlightSqlConnectContext context = new ArrowFlightSqlConnectContext("token");
        ArrowFlightSqlConnectProcessor processor = new ArrowFlightSqlConnectProcessor(
                context,
                "SELECT * FROM db1.tbl1 JOIN db1.tbl2 ON tbl1.id = tbl2.id");
        StatementBase parsedStatement = processor.parse(
                "SELECT * FROM db1.tbl1 JOIN db1.tbl2 ON tbl1.id = tbl2.id",
                context.getSessionVariable());

        Map<?, Relation> relations = AnalyzerUtils.collectAllTableAndViewRelations(parsedStatement);
        assertFalse(relations.isEmpty());
        assertTrue(relations.values().stream().allMatch(Relation::isNeedRewrittenByPolicy));
    }
}
