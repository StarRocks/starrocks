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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Regression coverage for SHOW FULL FUNCTIONS exposing the "isolation" property so users can tell
// whether a UDF/UDAF was created with isolation = "shared".
public class ShowFunctionsIsolationTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test_iso_db").useDatabase("test_iso_db");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_iso_db.shared_udf(int) RETURNS int PROPERTIES " +
                        "(\"symbol\" = \"Echo\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\", " +
                        "\"isolation\" = \"shared\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_iso_db.isolated_udf(int) RETURNS int PROPERTIES " +
                        "(\"symbol\" = \"Echo\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE AGGREGATE FUNCTION test_iso_db.shared_agg(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyShared\", \"type\" = \"StarrocksJar\", " +
                        "\"file\" = \"xxx\", \"isolation\" = \"shared\");");
        starRocksAssert.withFunction(
                "CREATE AGGREGATE FUNCTION test_iso_db.isolated_agg(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyAgg\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
    }

    private static String propertiesOf(String functionName) throws Exception {
        ShowFunctionsStmt stmt = new ShowFunctionsStmt("test_iso_db", false, false, true, null, null);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        // Verbose layout: [signature, returnType, functionType, intermediateType, properties]
        for (var row : rs.getResultRows()) {
            if (row.get(0).startsWith(functionName + "(")) {
                return row.get(4);
            }
        }
        throw new AssertionError("function not found in SHOW FULL FUNCTIONS: " + functionName);
    }

    @Test
    public void testSharedScalarFunctionShowsIsolation() throws Exception {
        String props = propertiesOf("shared_udf");
        Assertions.assertTrue(props.contains("\"isolation\":\"shared\""), props);
    }

    @Test
    public void testIsolatedScalarFunctionShowsIsolation() throws Exception {
        String props = propertiesOf("isolated_udf");
        Assertions.assertTrue(props.contains("\"isolation\":\"isolated\""), props);
    }

    @Test
    public void testSharedAggregateFunctionShowsIsolation() throws Exception {
        String props = propertiesOf("shared_agg");
        Assertions.assertTrue(props.contains("\"isolation\":\"shared\""), props);
    }

    @Test
    public void testIsolatedAggregateFunctionShowsIsolation() throws Exception {
        String props = propertiesOf("isolated_agg");
        Assertions.assertTrue(props.contains("\"isolation\":\"isolated\""), props);
    }
}
