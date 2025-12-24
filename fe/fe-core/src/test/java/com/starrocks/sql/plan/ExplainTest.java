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

import com.starrocks.common.Config;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExplainTest extends PlanTestBase {

    @Test
    public void testExplainUsesConfiguredExplainLevel() throws Exception {
        String originalLevel = Config.query_explain_level;
        Config.query_explain_level = "LOGICAL";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(
                    "EXPLAIN SELECT * FROM t0", connectContext);
            Assertions.assertTrue(statementBase instanceof QueryStatement);
            QueryStatement queryStatement = (QueryStatement) statementBase;
            Assertions.assertTrue(queryStatement.isExplain());
            Assertions.assertEquals(StatementBase.ExplainLevel.LOGICAL, queryStatement.getExplainLevel());
        } finally {
            Config.query_explain_level = originalLevel;
        }
    }

}
