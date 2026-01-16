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

import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.MockedLocalMetaStore;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DropStmtAnalyzerTest {

    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(
                globalStateMgr,
                globalStateMgr.getRecycleBin(),
                null);
        globalStateMgr.setLocalMetastore(localMetastore);
        
        // Create test database
        localMetastore.createDb("test");
        
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase("test");
    }

    @Test
    public void testDropFunctionWithExplicitDatabase() throws Exception {
        // Test DROP FUNCTION with explicit database name
        // The key test is that the database name is properly parsed
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS test.my_test_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        // Before analysis, verify parsing is correct
        Assertions.assertTrue(stmt instanceof DropFunctionStmt);
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        Assertions.assertEquals("test", dropStmt.getFunctionRef().getDbName());
        Assertions.assertEquals("my_test_func", dropStmt.getFunctionRef().getFunctionName());
        Assertions.assertTrue(dropStmt.dropIfExists());
    }

    @Test
    public void testDropFunctionWithoutDatabase() throws Exception {
        // Test DROP FUNCTION without explicit database
        // This is the bug fix scenario - parser creates FunctionName without db
        // Analyzer should set the database to current database
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS my_test_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        Assertions.assertTrue(stmt instanceof DropFunctionStmt);
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        // Before analysis, function should not have database set
        Assertions.assertNull(dropStmt.getFunctionRef().getDbName());
        Assertions.assertEquals("my_test_func", dropStmt.getFunctionRef().getFunctionName());
    }

    @Test
    public void testDropGlobalFunction() throws Exception {
        // Test DROP GLOBAL FUNCTION
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP GLOBAL FUNCTION IF EXISTS my_global_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        Assertions.assertTrue(stmt instanceof DropFunctionStmt);
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        Assertions.assertTrue(dropStmt.getFunctionRef().isGlobalFunction());
        Assertions.assertEquals("my_global_func", dropStmt.getFunctionRef().getFunctionName());
    }

    @Test
    public void testDropFunctionAnalyzerSetsDatabase() throws Exception {
        // This test verifies the bug fix: when dropping a non-global function without
        // explicit database, the analyzer should set the database in the FunctionName
        
        // Test case 1: Non-global function without explicit database
        // Parse the statement - at this point FunctionRef doesn't have database set
        StatementBase stmt1 = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS my_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt1 = (DropFunctionStmt) stmt1;
        // Before analysis, the function ref doesn't have database
        Assertions.assertNull(dropStmt1.getFunctionRef().getDbName());
        
        // Now test the FunctionRefAnalyzer.resolveFunctionName logic
        // This is what DropStmtAnalyzer calls internally
        FunctionName resolvedName1 = FunctionRefAnalyzer.resolveFunctionName(
                dropStmt1.getFunctionRef(), 
                connectContext.getDatabase());
        
        // After resolution, the database should be set to current database
        Assertions.assertEquals("test", resolvedName1.getDb());
        Assertions.assertEquals("my_func", resolvedName1.getFunction());
        Assertions.assertFalse(resolvedName1.isGlobalFunction());
        
        // Test case 2: Non-global function with explicit database
        StatementBase stmt2 = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS mydb.my_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt2 = (DropFunctionStmt) stmt2;
        Assertions.assertEquals("mydb", dropStmt2.getFunctionRef().getDbName());
        
        FunctionName resolvedName2 = FunctionRefAnalyzer.resolveFunctionName(
                dropStmt2.getFunctionRef(), 
                connectContext.getDatabase());
        
        // Database should remain as explicitly specified
        Assertions.assertEquals("mydb", resolvedName2.getDb());
        Assertions.assertEquals("my_func", resolvedName2.getFunction());
        
        // Test case 3: Global function
        StatementBase stmt3 = com.starrocks.sql.parser.SqlParser.parse(
                "DROP GLOBAL FUNCTION IF EXISTS my_global_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt3 = (DropFunctionStmt) stmt3;
        Assertions.assertTrue(dropStmt3.getFunctionRef().isGlobalFunction());
        
        FunctionName resolvedName3 = FunctionRefAnalyzer.resolveFunctionName(
                dropStmt3.getFunctionRef(), 
                connectContext.getDatabase());
        
        // Global function should have special database
        Assertions.assertEquals(FunctionRefAnalyzer.GLOBAL_UDF_DB, resolvedName3.getDb());
        Assertions.assertTrue(resolvedName3.isGlobalFunction());
    }

    @Test
    public void testDropFunctionIfExists() throws Exception {
        // Test IF EXISTS clause
        StatementBase stmt1 = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS test.my_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        Assertions.assertTrue(((DropFunctionStmt) stmt1).dropIfExists());
        
        StatementBase stmt2 = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION test.my_func(INT)", 
                connectContext.getSessionVariable()).get(0);
        Assertions.assertFalse(((DropFunctionStmt) stmt2).dropIfExists());
    }

    @Test
    public void testDropFunctionCaseInsensitive() throws Exception {
        // Test that function names are case-insensitive (normalized to lowercase)
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS test.MY_TEST_FUNC(INT)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        // Function names should be normalized to lowercase
        Assertions.assertEquals("my_test_func", dropStmt.getFunctionRef().getFunctionName());
    }

    @Test
    public void testDropFunctionWithVariadicArgs() throws Exception {
        // Test variadic arguments (...)
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS test.my_func(INT, ...)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        Assertions.assertTrue(dropStmt.getArgsDef().isVariadic());
    }

    @Test
    public void testDropFunctionArgumentParsing() throws Exception {
        // Test multiple argument types
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                "DROP FUNCTION IF EXISTS test.my_func(INT, VARCHAR, DOUBLE)", 
                connectContext.getSessionVariable()).get(0);
        
        DropFunctionStmt dropStmt = (DropFunctionStmt) stmt;
        Assertions.assertEquals(3, dropStmt.getArgsDef().getArgTypeDefs().size());
    }
}
