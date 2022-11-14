// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
