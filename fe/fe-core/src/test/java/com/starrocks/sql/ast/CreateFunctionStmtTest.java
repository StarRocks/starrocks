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

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class CreateFunctionStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        String createFunctionSql = "CREATE FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                + ");";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
        // com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
    }
}
