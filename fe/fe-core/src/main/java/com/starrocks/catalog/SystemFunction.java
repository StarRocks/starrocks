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
import com.starrocks.catalog.system.function.CboStatsShowExclusion;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SystemFunction extends Function {
    public static final String SYSTEM_FUNCTION_PREFIX = "system$";

    public SystemFunction(FunctionName name, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        super(name, argTypes, retType, hasVarArgs);
        setIsSystemFunction(true);
    }

    public static SystemFunction createSystemFunctionBuiltin(String name, List<Type> argTypes,
                                                             boolean hasVarArgs, Type retType) {
        if (!name.startsWith(SYSTEM_FUNCTION_PREFIX)) {
            throw new Error("System function illegal functionName:" + name);
        }
        SystemFunction fn = new SystemFunction(new FunctionName(name.toUpperCase()), argTypes, retType, hasVarArgs);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.setUserVisible(true);
        fn.setIsSystemFunction(true);
        return fn;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }


        List<Type> argTypes = new ArrayList<>(Arrays.asList(getArgs()));
        List<PrimitiveType> primitiveTypes =
                argTypes.stream().map(Type::getPrimitiveType).collect(Collectors.toList());

        SystemFunction objSysFun = (SystemFunction) obj;
        List<Type> objArgTypes = new ArrayList<>(Arrays.asList(objSysFun.getArgs()));
        List<PrimitiveType> sigPrimitiveTypes =
                objArgTypes.stream().map(Type::getPrimitiveType).collect(Collectors.toList());
        return Objects.equals(functionName().toUpperCase(), objSysFun.functionName().toUpperCase()) &&
                primitiveTypes.equals(sigPrimitiveTypes) &&
                getReturnType().matchesType(objSysFun.getReturnType());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public TFunction toThrift() {
        throw new UnsupportedOperationException("System function cannot be serialized.");
    }

    @Override
    public void write(DataOutput output) throws IOException {
        throw new UnsupportedOperationException("System function cannot be serialized.");
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        throw new UnsupportedOperationException("System function cannot be serialized.");
    }

    public static void initBuiltins(FunctionSet functionSet) {
        functionSet.addSystemFunctionBuiltin(CboStatsShowExclusion.class,
                CboStatsShowExclusion.FN_NAME, false, Type.VARCHAR);
    }
}