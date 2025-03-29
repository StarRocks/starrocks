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
import com.starrocks.common.StarRocksException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

public class GlobalFunctionMgrTest {
    private GlobalFunctionMgr globalFunctionMgr;

    @Before
    public void setUp() {
        globalFunctionMgr = new GlobalFunctionMgr();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return mock();
            }
        };
    }

    @Test
    public void testReplayAddAndDropFunction() {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setAsGlobalFunction();
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);

        // add global udf function.
        globalFunctionMgr.replayAddFunction(f);
        Assert.assertEquals(globalFunctionMgr.getFunctions().size(), 1);
        Assert.assertTrue(globalFunctionMgr.getFunctions().get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
        // drop global udf function ok.
        FunctionSearchDesc desc = new FunctionSearchDesc(name, argTypes, false);
        globalFunctionMgr.replayDropFunction(desc);
        Assert.assertEquals(globalFunctionMgr.getFunctions().size(), 0);
    }

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
        Assert.assertThrows(StarRocksException.class, () -> globalFunctionMgr.userAddFunction(f, false, false));
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
        Assert.assertEquals(functions.size(), 1);
        Assert.assertTrue(functions.get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
    }
}
