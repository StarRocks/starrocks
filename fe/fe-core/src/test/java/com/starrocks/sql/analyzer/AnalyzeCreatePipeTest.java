// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnalyzeCreatePipeTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() {
        {
            // pipe's database: not set
            // target table's database: not set
            // result: current database
            CreatePipeStmt stmt = (CreatePipeStmt) AnalyzeTestUtil.analyzeSuccess(
                    "create pipe pipe5 properties(\"auto_ingest\"=\"true\") as " +
                            "insert into t0 select col_int, col_string, 1 from files(\"path\"=\"fake://somewhere/1.parquet\"," +
                            " \"format\"=\"parquet\")");
            Assert.assertEquals("test", stmt.getInsertStmt().getTableName().getDb());
            Assert.assertEquals(stmt.getPipeName().getDbName(), stmt.getInsertStmt().getTableName().getDb());
        }
        {
            // pipe's database: not set
            // target table's database: 'test'
            // result: target table's database
            CreatePipeStmt stmt = (CreatePipeStmt) AnalyzeTestUtil.analyzeSuccess(
                    "create pipe pipe5 properties(\"auto_ingest\"=\"true\") as " +
                            "insert into test.t0 select col_int, col_string, 1 from files(\"path\"=\"fake://somewhere/1.parquet\"," +
                            " \"format\"=\"parquet\")");
            Assert.assertEquals(stmt.getPipeName().getDbName(), stmt.getInsertStmt().getTableName().getDb());
        }
        {
            // pipe's database: 'test1'
            // target table's database: 'test'
            // result: error
            AnalyzeTestUtil.analyzeFail(
                    "create pipe test1.pipe5 properties(\"auto_ingest\"=\"true\") as " +
                            "insert into test.t0 select col_int, col_string, 1 from files(\"path\"=\"fake://somewhere/1.parquet\"," +
                            " \"format\"=\"parquet\")");
        }
    }
}
