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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ArraySliceExpr.java

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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class ArraySliceExpr extends Expr {
    public ArraySliceExpr(Expr expr, Expr lowerBound, Expr upperBound) {
        this.children.add(expr);
        this.children.add(lowerBound);
        this.children.add(upperBound);
    }

    public ArraySliceExpr(ArraySliceExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Expr expr = this.children.get(0);
        Expr lower_bound = this.children.get(1);
        Expr upper_bound = this.children.get(2);
        if (!this.children.get(0).getType().isArrayType()) {
            throw new AnalysisException("cannot subscript type "
                    + this.children.get(0).getType() + " because it is not an array");
        }
        if (lower_bound != null && !lower_bound.getType().isNumericType()) {
            throw new AnalysisException("array subscript must have type integer");
        }
        if (upper_bound != null && upper_bound.getType().getPrimitiveType() != PrimitiveType.INT) {
            castChild(Type.INT, 1);
        }
        this.type = expr.getType();
    }

    @Override
    protected String toSqlImpl() {
        return this.children.get(0).toSqlImpl() + "[" + this.children.get(1).toSqlImpl()
                + ":" + this.children.get(2).toSqlImpl() + "]";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.ARRAY_SLICE_EXPR);
    }

    @Override
    public Expr clone() {
        return new ArraySliceExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArraySliceExpr(this, context);
    }
}
