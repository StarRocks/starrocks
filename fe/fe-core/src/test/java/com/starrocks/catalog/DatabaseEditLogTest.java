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

package com.starrocks.catalog;

import com.starrocks.common.StarRocksException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class DatabaseEditLogTest {
    private Database masterDatabase;
    private static final String TEST_DB_NAME = "test_db";
    private static final long TEST_DB_ID = 100L;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create test database
        masterDatabase = new Database(TEST_DB_ID, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(masterDatabase);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Add Function Tests ====================

    @Test
    public void testAddFunctionNormalCase() throws Exception {
        // 1. Prepare test data
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // 2. Verify initial state
        Assertions.assertEquals(0, masterDatabase.getFunctions().size());

        // 3. Execute addFunction operation
        masterDatabase.addFunction(function, false, false);

        // 4. Verify master state
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());
        Function addedFunction = masterDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(addedFunction);
        Assertions.assertEquals("test_func", addedFunction.getFunctionName().getFunction());

        // 5. Test follower replay functionality
        Function replayFunction = (Function) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FUNCTION_V2);

        Database.replayCreateFunctionLog(replayFunction);

        // 6. Verify follower state is consistent with master
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        Assertions.assertNotNull(db);
        Assertions.assertEquals(1, db.getFunctions().size());
        Function followerFunction = db.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(followerFunction);
        Assertions.assertEquals("test_func", followerFunction.getFunctionName().getFunction());
    }

    @Test
    public void testAddFunctionEditLogException() throws Exception {
        // 1. Prepare test data
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);
        function.setBinaryType(TFunctionBinaryType.SRJAR);

        // 2. Create a separate Database for exception testing
        Database exceptionDatabase = new Database(TEST_DB_ID, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(exceptionDatabase);
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logAddFunction to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddFunction(any(Function.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionDatabase.getFunctions().size());

        // 4. Execute addFunction operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionDatabase.addFunction(function, false, false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionDatabase.getFunctions().size());
        Function retrievedFunction = exceptionDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(retrievedFunction);
    }

    // ==================== Drop Function Tests ====================

    @Test
    public void testDropFunctionNormalCase() throws Exception {
        // 1. Prepare test data and add function first
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        masterDatabase.addFunction(function, false, false);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());

        // 2. Execute dropFunction operation
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        masterDatabase.dropFunction(functionSearchDesc, false);

        // 3. Verify master state
        Assertions.assertEquals(0, masterDatabase.getFunctions().size());
        Function retrievedFunction = masterDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(retrievedFunction);

        // 4. Test follower replay functionality
        // First, replay the add function operation to set up the follower state
        Function replayAddFunction = (Function) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FUNCTION_V2);
        Database.replayCreateFunctionLog(replayAddFunction);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        Assertions.assertNotNull(db);
        Assertions.assertEquals(1, db.getFunctions().size());

        // Then, replay the drop function operation
        FunctionSearchDesc replayFunctionSearchDesc = (FunctionSearchDesc) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_FUNCTION_V2);
        Database.replayDropFunctionLog(replayFunctionSearchDesc);

        // 5. Verify follower state is consistent with master
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        Assertions.assertNotNull(db);
        Assertions.assertEquals(0, db.getFunctions().size());
        Function followerFunction = db.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(followerFunction);
    }

    @Test
    public void testDropFunctionEditLogException() throws Exception {
        // 1. Prepare test data and add function first
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        Database exceptionDatabase = new Database(TEST_DB_ID, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(exceptionDatabase);
        exceptionDatabase.addFunction(function, false, false);
        Assertions.assertEquals(1, exceptionDatabase.getFunctions().size());

        // 2. Mock EditLog.logDropFunction to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropFunction(any(FunctionSearchDesc.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropFunction operation and expect exception
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionDatabase.dropFunction(functionSearchDesc, false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionDatabase.getFunctions().size());
        Function retrievedFunction = exceptionDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
    }

    // ==================== Add Function with Different Parameters Tests ====================

    @Test
    public void testAddFunctionWithAllowExistsTrue() throws Exception {
        // Test allowExists=true, createIfNotExists=false
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_allow_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // First add
        masterDatabase.addFunction(function, true, false);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());
        long functionId = function.getFunctionId();

        // Add again with allowExists=true should succeed (replace)
        masterDatabase.addFunction(function, true, false);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());
        Function retrievedFunction = masterDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
        Assertions.assertTrue(functionId != retrievedFunction.getFunctionId());
    }

    @Test
    public void testAddFunctionWithCreateIfNotExistsTrue() throws Exception {
        // Test allowExists=false, createIfNotExists=true
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_if_not_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // First add
        masterDatabase.addFunction(function, false, true);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());
        long functionId = function.getFunctionId();

        // Add again with createIfNotExists=true should not add (no-op)
        masterDatabase.addFunction(function, false, true);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());
        Function retrievedFunction = masterDatabase.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
        Assertions.assertTrue(functionId == retrievedFunction.getFunctionId());
    }

    @Test
    public void testAddFunctionWithBothAllowExistsAndCreateIfNotExists() throws Exception {
        // Test allowExists=true, createIfNotExists=true should throw exception
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_both");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterDatabase.addFunction(function, true, true);
        });
        Assertions.assertTrue(exception.getMessage().contains("IF NOT EXISTS\" and \"OR REPLACE\" cannot be used together"));
    }

    @Test
    public void testAddFunctionWithAllowExistsFalseAndCreateIfNotExistsFalse() throws Exception {
        // Test allowExists=false, createIfNotExists=false - should throw exception if exists
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_no_allow");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // First add should succeed
        masterDatabase.addFunction(function, false, false);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());

        // Add again should throw exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterDatabase.addFunction(function, false, false);
        });
        Assertions.assertTrue(exception.getMessage().contains("function already exists"));
    }

    // ==================== Drop Function with Different Parameters Tests ====================

    @Test
    public void testDropFunctionWithDropIfExistsTrue() throws Exception {
        // Test dropIfExists=true when function exists
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_drop_if_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        masterDatabase.addFunction(function, false, false);
        Assertions.assertEquals(1, masterDatabase.getFunctions().size());

        // Drop with dropIfExists=true should succeed
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        masterDatabase.dropFunction(functionSearchDesc, true);
        Assertions.assertEquals(0, masterDatabase.getFunctions().size());
    }

    @Test
    public void testDropFunctionWithDropIfExistsTrueWhenNotExists() throws Exception {
        // Test dropIfExists=true when function does not exist
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_not_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);

        // Drop with dropIfExists=true should not throw exception
        masterDatabase.dropFunction(functionSearchDesc, true);
        Assertions.assertEquals(0, masterDatabase.getFunctions().size());
    }

    @Test
    public void testDropFunctionWithDropIfExistsFalseWhenNotExists() throws Exception {
        // Test dropIfExists=false when function does not exist should throw exception
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func_drop_fail");
        Type[] argTypes = new Type[] {IntegerType.INT};
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);

        // Drop with dropIfExists=false should throw exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterDatabase.dropFunction(functionSearchDesc, false);
        });
        Assertions.assertTrue(exception.getMessage().contains("Unknown function"));
    }
}
    