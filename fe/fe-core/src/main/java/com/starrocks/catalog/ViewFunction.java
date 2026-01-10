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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TFunction;
import com.starrocks.type.Type;

public class ViewFunction extends Function {
    @SerializedName(value = "view")
    private final String view;

    private Expr analyzeExpr;

    public ViewFunction(FunctionName name, Type[] argTypes, Type retType, String[] argNames, String view) {
        super(name, argTypes, argNames, retType, false);
        this.view = view;
    }

    public void setAnalyzeExpr(Expr analyzeExpr) {
        this.analyzeExpr = analyzeExpr;
    }

    public Expr getAnalyzeExpr() {
        return analyzeExpr;
    }

    public String getView() {
        return view;
    }

    @Override
    public String getProperties() {
        return view;
    }

    @Override
    public Function copy() {
        return new ViewFunction(this.getFunctionName(), this.getArgs(), this.getReturnType(),
                this.getArgNames(), this.view);
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(dbName()).append(".").append(signatureString()).append(" \n")
                .append(view);
        return sb.toString();
    }

    @Override
    public TFunction toThrift() {
        Preconditions.checkState(false, "view function does not support toThrift");
        return null;
    }
}

