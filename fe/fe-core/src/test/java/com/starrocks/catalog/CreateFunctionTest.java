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
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateFunctionTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void tableFunctionSerializeTest() throws IOException {
        String db = "db";
        String fn = "table_function";
        final FunctionName functionName = new FunctionName(db, fn);
        List<String> colNames = new ArrayList<>();
        colNames.add("table_function");

        List<Type> argTypes = new ArrayList<>();
        argTypes.add(Type.VARCHAR);

        List<Type> retTypes = new ArrayList<>();
        retTypes.add(Type.VARCHAR);

        final TableFunction tableFunction = new TableFunction(functionName, colNames, argTypes, retTypes);
        tableFunction.setFunctionId(-1024);

        final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer(4096);
        final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);
        tableFunction.write(byteBufOutputStream);

        final ByteBufInputStream byteBufInputStream = new ByteBufInputStream(byteBuf);
        final TableFunction newFunction = new TableFunction();
        Function.FunctionType.read(byteBufInputStream);
        newFunction.readFields(byteBufInputStream);

        Assertions.assertEquals(newFunction.getFunctionName().getFunction(), fn);
        Assertions.assertEquals(newFunction.getFunctionId(), tableFunction.getFunctionId());
        Assertions.assertEquals(newFunction.getDefaultColumnNames(), colNames);
        Assertions.assertEquals(Arrays.asList(newFunction.getArgs()), argTypes);
        Assertions.assertEquals(newFunction.getTableFnReturnTypes(), retTypes);
        Assertions.assertEquals(newFunction.getTableFnReturnTypes(), retTypes);
    }
}
