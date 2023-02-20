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


package com.starrocks.sql.ast;

import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.util.List;

public class FunctionArgsDef {
    private final List<TypeDef> argTypeDefs;
    private final boolean isVariadic;

    // set after analyze
    private Type[] argTypes;

    public FunctionArgsDef(List<TypeDef> argTypeDefs, boolean isVariadic) {
        this.argTypeDefs = argTypeDefs;
        this.isVariadic = isVariadic;
    }

    public Type[] getArgTypes() {
        return argTypes;
    }

    public boolean isVariadic() {
        return isVariadic;
    }

    public void analyze() throws AnalysisException {
        argTypes = new Type[argTypeDefs.size()];
        int i = 0;
        for (TypeDef typeDef : argTypeDefs) {
            typeDef.analyze();
            argTypes[i++] = typeDef.getType();
        }
    }

    public void setArgTypes(Type[] argTypes) {
        this.argTypes = argTypes;
    }
}
