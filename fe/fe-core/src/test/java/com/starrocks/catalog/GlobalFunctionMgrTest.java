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

import com.starrocks.analysis.FunctionName;
<<<<<<< HEAD
import com.starrocks.common.UserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
=======
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
>>>>>>> 741b28c376 ([BugFix] Fix incorrect matching when multiple global UDFs with the same name exist (#60550))

import java.util.List;

public class GlobalFunctionMgrTest {
    private GlobalFunctionMgr globalFunctionMgr;

    @Before
    public void setUp() {
        globalFunctionMgr = new GlobalFunctionMgr();
    }

    @Test
    public void testAddAndDropFunction() throws UserException {
        Type[] argTypes = new Type[2];
        argTypes[0] = Type.INT;
        argTypes[1] = Type.INT;
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setAsGlobalFunction();
        Function f = new Function(name, argTypes, Type.INT, false);

        // add global udf function.
        {
            globalFunctionMgr.replayAddFunction(f);
            List<Function> functions = globalFunctionMgr.getFunctions();
            Assert.assertEquals(functions.size(), 1);
            Assert.assertTrue(functions.get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
        }
        // drop global udf function ok.
        {
            FunctionSearchDesc desc = new FunctionSearchDesc(name, argTypes, false);
            globalFunctionMgr.replayDropFunction(desc);
            List<Function> functions = globalFunctionMgr.getFunctions();
            Assert.assertEquals(functions.size(), 0);
        }
    }
<<<<<<< HEAD
}
=======

    @Test
    public void testUserAddFunction() throws StarRocksException {
        // User adds addIntInt UDF
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setAsGlobalFunction();
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);
        globalFunctionMgr.userAddFunction(f, false, false);
        // User adds addDoubleDouble UDF
        FunctionName name2 = new FunctionName(null, "addDoubleDouble");
        name2.setAsGlobalFunction();
        final Type[] argTypes2 = {Type.DOUBLE, Type.DOUBLE};
        Function f2 = new Function(name2, argTypes2, Type.DOUBLE, false);
        globalFunctionMgr.userAddFunction(f2, false, false);
    }

    @Test
    public void testUserAddFunctionGivenFunctionAlreadyExists() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setAsGlobalFunction();
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);

        // Add the UDF for the first time
        globalFunctionMgr.userAddFunction(f, false, false);

        // Attempt to add the same UDF again, expecting an exception
        Assertions.assertThrows(StarRocksException.class, () -> globalFunctionMgr.userAddFunction(f, false, false));
    }

    @Test
    public void testUserAddFunctionGivenUdfAlreadyExistsAndAllowExisting() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setAsGlobalFunction();
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);

        // Add the UDF for the first time
        globalFunctionMgr.userAddFunction(f, true, false);
        // Attempt to add the same UDF again
        globalFunctionMgr.userAddFunction(f, true, false);

        List<Function> functions = globalFunctionMgr.getFunctions();
        Assertions.assertEquals(functions.size(), 1);
        Assertions.assertTrue(functions.get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
    }

    @Test
    public void testFunctionOrderingWithNumericPriority() throws StarRocksException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalFunctionMgr = globalStateMgr.getGlobalFunctionMgr();
        FunctionName name = new FunctionName(null, "process");
        name.setAsGlobalFunction();

        final Type[] varcharArgs = {Type.VARCHAR};
        Function varcharFunc = new Function(name, varcharArgs, Type.VARCHAR, false);
        globalFunctionMgr.userAddFunction(varcharFunc, false, false);

        final Type[] intArgs = {Type.INT};
        Function intFunc = new Function(name, intArgs, Type.INT, false);
        globalFunctionMgr.userAddFunction(intFunc, false, false);

        final Type[] doubleArgs = {Type.DOUBLE};
        Function doubleFunc = new Function(name, doubleArgs, Type.DOUBLE, false);
        globalFunctionMgr.userAddFunction(doubleFunc, false, false);

        List<Function> functions = globalFunctionMgr.getFunctions();
        Assertions.assertEquals(3, functions.size());

        for (int i = 0; i < functions.size() - 1; i++) {
            Function current = functions.get(i);
            Function next = functions.get(i + 1);

            Assertions.assertEquals(current.getFunctionName().getFunction(),
                    next.getFunctionName().getFunction());
            Assertions.assertFalse(current.compare(next, Function.CompareMode.IS_IDENTICAL));
        }
        Assertions.assertEquals(intFunc, functions.get(0));
        Assertions.assertEquals(doubleFunc, functions.get(1));
        Assertions.assertEquals(varcharFunc, functions.get(2));

        new MockUp<Authorizer>() {
            @Mock
            public static void checkGlobalFunctionAction(ConnectContext context, Function function,
                                                         PrivilegeType privilegeType) {
            }
        };
        Config.enable_udf = true;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setGlobalStateMgr(globalStateMgr);
        Function selectedFunc = AnalyzerUtils.getUdfFunction(connectContext, name, varcharArgs);
        Assertions.assertEquals(varcharFunc, selectedFunc);

        selectedFunc = AnalyzerUtils.getUdfFunction(connectContext, name, intArgs);
        Assertions.assertEquals(intFunc, selectedFunc);
        Config.enable_udf = false;
    }
}
>>>>>>> 741b28c376 ([BugFix] Fix incorrect matching when multiple global UDFs with the same name exist (#60550))
