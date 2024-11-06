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

package com.starrocks.connector.parser.trino;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.SqlParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoDialectDowngradeTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testTrinoDialectDowngrade() throws Exception {
        String querySql = "select doy(date '2022-03-06')";
        try {
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            connectContext.getSessionVariable().setSqlDialect("trino");
            QueryStatement queryStmt =
                    (QueryStatement) SqlParser.parse(querySql, connectContext.getSessionVariable()).get(0);
            assertPlanContains(queryStmt, "dayofyear('2022-03-06 00:00:00')");
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            analyzeFail(querySql, "No matching function with signature: doy(date)");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }


}
