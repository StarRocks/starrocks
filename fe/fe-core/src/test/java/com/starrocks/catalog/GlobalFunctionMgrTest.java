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
import com.starrocks.common.UserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
}