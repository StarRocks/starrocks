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

import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableSampleClauseTest extends PlanTestBase {

    @Test
    public void testBasic() throws Exception {
        final String sql = "select * from t1 sample('method'='by_block', 'seed'='1')";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        // to sql
        Assert.assertEquals("SELECT `test`.`t1`.`v4`, `test`.`t1`.`v5`, `test`.`t1`.`v6`\n" +
                        "FROM `test`.`t1` SAMPLE('method'='BY_BLOCK','seed'='1','percent'='1')",
                AstToSQLBuilder.toSQL(statementBase));

        // explain
        starRocksAssert.query(sql).explainContains("SAMPLE");
    }

}