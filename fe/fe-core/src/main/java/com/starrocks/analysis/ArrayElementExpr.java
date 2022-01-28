// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ArrayElementExpr.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class ArrayElementExpr extends Expr {
    public ArrayElementExpr(Expr expr, Expr subscript) {
        this.children.add(expr);
        this.children.add(subscript);
    }

    public ArrayElementExpr(Type type, Expr expr, Expr subscript) {
        this.type = type;
        this.children.add(expr);
        this.children.add(subscript);
    }

    public ArrayElementExpr(ArrayElementExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Expr expr = this.children.get(0);
        Expr subscript = this.children.get(1);
        if (!expr.getType().isArrayType()) {
            throw new AnalysisException("cannot subscript type " + expr.getType()
                    + " because it is not an array");
        }
        if (!subscript.getType().isNumericType()) {
            throw new AnalysisException("array subscript must have type integer");
        }
        if (subscript.getType().getPrimitiveType() != PrimitiveType.INT) {
            castChild(Type.INT, 1);
        }
        this.type = ((ArrayType) expr.getType()).getItemType();
    }

    @Override
    protected String toSqlImpl() {
        Expr expr = this.children.get(0);
        Expr subscript = this.children.get(1);
        return expr.toSqlImpl() + "[" + subscript.toSqlImpl() + "]";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.ARRAY_ELEMENT_EXPR);
    }

    @Override
    public Expr clone() {
        return new ArrayElementExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrayElementExpr(this, context);
    }
}
