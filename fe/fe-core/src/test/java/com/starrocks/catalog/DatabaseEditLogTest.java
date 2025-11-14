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
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.catalog.ScalarFunction;
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
        Type[] argTypes = new Type[]{IntegerType.INT};
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
        Type[] argTypes = new Type[]{IntegerType.INT};
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
        Type[] argTypes = new Type[]{IntegerType.INT};
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
    }

    @Test
    public void testDropFunctionEditLogException() throws Exception {
        // 1. Prepare test data and add function first
        FunctionName functionName = new FunctionName(TEST_DB_NAME, "test_func");
        Type[] argTypes = new Type[]{IntegerType.INT};
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
}
    