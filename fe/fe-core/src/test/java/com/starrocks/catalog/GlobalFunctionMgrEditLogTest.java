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

public class GlobalFunctionMgrEditLogTest {
    private GlobalFunctionMgr masterGlobalFunctionMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create GlobalFunctionMgr instance
        masterGlobalFunctionMgr = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== User Add Function Tests ====================

    @Test
    public void testUserAddFunctionNormalCase() throws Exception {
        // 1. Prepare test data
        FunctionName functionName = new FunctionName("", "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // 2. Verify initial state
        Assertions.assertEquals(0, masterGlobalFunctionMgr.getFunctions().size());

        // 3. Execute userAddFunction operation
        masterGlobalFunctionMgr.userAddFunction(function, false, false);

        // 4. Verify master state
        Assertions.assertEquals(1, masterGlobalFunctionMgr.getFunctions().size());
        Function addedFunction = masterGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(addedFunction);
        Assertions.assertEquals("test_func", addedFunction.getFunctionName().getFunction());

        // 5. Test follower replay functionality
        GlobalFunctionMgr followerGlobalFunctionMgr = new GlobalFunctionMgr();
        Assertions.assertEquals(0, followerGlobalFunctionMgr.getFunctions().size());

        Function replayFunction = (Function) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FUNCTION_V2);

        followerGlobalFunctionMgr.replayAddFunction(replayFunction);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerGlobalFunctionMgr.getFunctions().size());
        Function followerFunction = followerGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(followerFunction);
        Assertions.assertEquals("test_func", followerFunction.getFunctionName().getFunction());
    }

    @Test
    public void testUserAddFunctionEditLogException() throws Exception {
        // 1. Prepare test data
        FunctionName functionName = new FunctionName("", "test_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // 2. Create a separate GlobalFunctionMgr for exception testing
        GlobalFunctionMgr exceptionGlobalFunctionMgr = new GlobalFunctionMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logAddFunction to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddFunction(any(Function.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionGlobalFunctionMgr.getFunctions().size());

        // 4. Execute userAddFunction operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionGlobalFunctionMgr.userAddFunction(function, false, false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionGlobalFunctionMgr.getFunctions().size());
        Function retrievedFunction = exceptionGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(retrievedFunction);
    }

    // ==================== User Drop Function Tests ====================

    @Test
    public void testUserDropFunctionNormalCase() throws Exception {
        // 1. Prepare test data and add function first
        FunctionName functionName = new FunctionName("", "test_drop_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        // Check initial function count
        int initialCount = masterGlobalFunctionMgr.getFunctions().size();
        masterGlobalFunctionMgr.userAddFunction(function, false, false);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());

        // 2. Execute userDropFunction operation
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        masterGlobalFunctionMgr.userDropFunction(functionSearchDesc, false);

        // 3. Verify master state
        Assertions.assertEquals(initialCount, masterGlobalFunctionMgr.getFunctions().size());
        Function retrievedFunction = masterGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(retrievedFunction);

        // 4. Test follower replay functionality
        GlobalFunctionMgr followerGlobalFunctionMgr = new GlobalFunctionMgr();
        Assertions.assertEquals(0, followerGlobalFunctionMgr.getFunctions().size());

        // First, replay the add function operation to set up the follower state
        Function replayAddFunction = (Function) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FUNCTION_V2);
        followerGlobalFunctionMgr.replayAddFunction(replayAddFunction);
        Assertions.assertEquals(1, followerGlobalFunctionMgr.getFunctions().size());

        // Then, replay the drop function operation
        FunctionSearchDesc replayFunctionSearchDesc = (FunctionSearchDesc) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_FUNCTION_V2);
        followerGlobalFunctionMgr.replayDropFunction(replayFunctionSearchDesc);

        // 5. Verify follower state is consistent with master
        Assertions.assertEquals(0, followerGlobalFunctionMgr.getFunctions().size());
        Function followerFunction = followerGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNull(followerFunction);
    }

    @Test
    public void testUserDropFunctionEditLogException() throws Exception {
        // 1. Prepare test data and add function first
        FunctionName functionName = new FunctionName("", "test_drop_exception_func");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        GlobalFunctionMgr exceptionGlobalFunctionMgr = new GlobalFunctionMgr();
        exceptionGlobalFunctionMgr.userAddFunction(function, false, false);
        Assertions.assertEquals(1, exceptionGlobalFunctionMgr.getFunctions().size());

        // 2. Mock EditLog.logDropFunction to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropFunction(any(FunctionSearchDesc.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute userDropFunction operation and expect exception
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionGlobalFunctionMgr.userDropFunction(functionSearchDesc, false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionGlobalFunctionMgr.getFunctions().size());
        Function retrievedFunction = exceptionGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
    }

    // ==================== User Add Function with Different Parameters Tests ====================

    @Test
    public void testUserAddFunctionWithAllowExistsTrue() throws Exception {
        // Test allowExists=true, createIfNotExists=false
        FunctionName functionName = new FunctionName("", "test_func_allow_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        int initialCount = masterGlobalFunctionMgr.getFunctions().size();

        // First add
        masterGlobalFunctionMgr.userAddFunction(function, true, false);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());
        long functionId = function.getFunctionId();

        // Add again with allowExists=true should succeed (replace)
        masterGlobalFunctionMgr.userAddFunction(function, true, false);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());
        Function retrievedFunction = masterGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
        Assertions.assertTrue(functionId != retrievedFunction.getFunctionId());
    }

    @Test
    public void testUserAddFunctionWithCreateIfNotExistsTrue() throws Exception {
        // Test allowExists=false, createIfNotExists=true
        FunctionName functionName = new FunctionName("", "test_func_if_not_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        int initialCount = masterGlobalFunctionMgr.getFunctions().size();

        // First add
        masterGlobalFunctionMgr.userAddFunction(function, false, true);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());
        long functionId = function.getFunctionId();
        // Add again with createIfNotExists=true should not add (no-op)
        masterGlobalFunctionMgr.userAddFunction(function, false, true);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());
        Function retrievedFunction = masterGlobalFunctionMgr.getFunction(function, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(retrievedFunction);
        Assertions.assertTrue(functionId == retrievedFunction.getFunctionId());
    }

    @Test
    public void testUserAddFunctionWithBothAllowExistsAndCreateIfNotExists() throws Exception {
        // Test allowExists=true, createIfNotExists=true should throw exception
        FunctionName functionName = new FunctionName("", "test_func_both");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterGlobalFunctionMgr.userAddFunction(function, true, true);
        });
        Assertions.assertTrue(exception.getMessage().contains("IF NOT EXISTS\" and \"OR REPLACE\" cannot be used together"));
    }

    @Test
    public void testUserAddFunctionWithAllowExistsFalseAndCreateIfNotExistsFalse() throws Exception {
        // Test allowExists=false, createIfNotExists=false - should throw exception if exists
        FunctionName functionName = new FunctionName("", "test_func_no_allow");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        int initialCount = masterGlobalFunctionMgr.getFunctions().size();

        // First add should succeed
        masterGlobalFunctionMgr.userAddFunction(function, false, false);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());

        // Add again should throw exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterGlobalFunctionMgr.userAddFunction(function, false, false);
        });
        Assertions.assertTrue(exception.getMessage().contains("function already exists"));
    }

    // ==================== User Drop Function with Different Parameters Tests ====================

    @Test
    public void testUserDropFunctionWithDropIfExistsTrue() throws Exception {
        // Test dropIfExists=true when function exists
        FunctionName functionName = new FunctionName("", "test_func_drop_if_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        Function function = new ScalarFunction(functionName, argTypes, IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        function.setUserVisible(true);

        int initialCount = masterGlobalFunctionMgr.getFunctions().size();
        masterGlobalFunctionMgr.userAddFunction(function, false, false);
        Assertions.assertEquals(initialCount + 1, masterGlobalFunctionMgr.getFunctions().size());

        // Drop with dropIfExists=true should succeed
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);
        masterGlobalFunctionMgr.userDropFunction(functionSearchDesc, true);
        Assertions.assertEquals(initialCount, masterGlobalFunctionMgr.getFunctions().size());
    }

    @Test
    public void testUserDropFunctionWithDropIfExistsTrueWhenNotExists() throws Exception {
        // Test dropIfExists=true when function does not exist
        FunctionName functionName = new FunctionName("", "test_func_not_exists");
        Type[] argTypes = new Type[] {IntegerType.INT};
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);

        int initialCount = masterGlobalFunctionMgr.getFunctions().size();

        // Drop with dropIfExists=true should not throw exception
        masterGlobalFunctionMgr.userDropFunction(functionSearchDesc, true);
        Assertions.assertEquals(initialCount, masterGlobalFunctionMgr.getFunctions().size());
    }

    @Test
    public void testUserDropFunctionWithDropIfExistsFalseWhenNotExists() throws Exception {
        // Test dropIfExists=false when function does not exist should throw exception
        FunctionName functionName = new FunctionName("", "test_func_drop_fail");
        Type[] argTypes = new Type[] {IntegerType.INT};
        FunctionSearchDesc functionSearchDesc = new FunctionSearchDesc(functionName, argTypes, false);

        // Drop with dropIfExists=false should throw exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterGlobalFunctionMgr.userDropFunction(functionSearchDesc, false);
        });
        Assertions.assertTrue(exception.getMessage().contains("Unknown function"));
    }
}

