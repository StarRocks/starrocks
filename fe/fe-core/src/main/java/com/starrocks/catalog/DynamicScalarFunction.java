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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ScalarFunction.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.starrocks.catalog;

import com.starrocks.analysis.FunctionName;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TScalarFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DynamicScalarFunction extends ScalarFunction {

    public DynamicScalarFunction(
            FunctionName fnName, Type[] argTypes, Type retType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
    }

    public DynamicScalarFunction(DynamicScalarFunction other) {
        super(other);
    }

    public DynamicScalarFunction() {
        super();
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(dbName() + "." + signatureString() + "\n")
                .append(" LOCATION '" + getLocation() + "'\n")
                .append(" SYMBOL='" + getSymbolName() + "'\n");
        return sb.toString();
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.setSymbol(getSymbolName());
        if (getPrepareFnSymbol() != null) {
            scalarFunction.setPrepare_fn_symbol(getPrepareFnSymbol());
        }
        if (getCloseFnSymbol() != null) {
            scalarFunction.setClose_fn_symbol(getCloseFnSymbol());
        }
        scalarFunction.setHas_dynamic_return_type(true);
        fn.setScalar_fn(scalarFunction);
        return fn;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.DYNAMIC_SCALAR.write(output);
        // 2. parent
        super.writeFields(output);
    }

    @Override
    public Function copy() {
        return new DynamicScalarFunction(this);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
    }
}
