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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AnalyzeProfileStmtTest {

    @Test
    public void testAnalyzeProfileWithStringQueryId() {
        String queryId = "test-query-id";
        List<Integer> planNodeIds = Arrays.asList(1, 2, 3);
        AnalyzeProfileStmt stmt = new AnalyzeProfileStmt(queryId, planNodeIds, NodePosition.ZERO);

        Assertions.assertEquals(queryId, stmt.getQueryId());
        Assertions.assertEquals(planNodeIds, stmt.getPlanNodeIds());
    }

    @Test
    public void testAnalyzeProfileWithLastQueryId() {
        String queryId = "last_query_id()";
        List<Integer> planNodeIds = Collections.emptyList();
        AnalyzeProfileStmt stmt = new AnalyzeProfileStmt(queryId, planNodeIds, NodePosition.ZERO);

        Assertions.assertEquals(queryId, stmt.getQueryId());
        Assertions.assertEquals(planNodeIds, stmt.getPlanNodeIds());
    }

    @Test
    public void testAnalyzeProfileWithLastQueryIdAndPlanNodes() {
        String queryId = "last_query_id()";
        List<Integer> planNodeIds = Arrays.asList(0, 1);
        AnalyzeProfileStmt stmt = new AnalyzeProfileStmt(queryId, planNodeIds, NodePosition.ZERO);

        Assertions.assertEquals(queryId, stmt.getQueryId());
        Assertions.assertEquals(planNodeIds, stmt.getPlanNodeIds());
    }
}
