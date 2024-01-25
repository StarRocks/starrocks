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

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class CollectionElementExpr extends Expr {

    // For trino and presto, access out of bound in map/array, it will throw error msg
    private boolean checkIsOutOfBounds = false;

    public CollectionElementExpr(Expr expr, Expr subscript, boolean checkIsOutOfBounds) {
        super(NodePosition.ZERO);
        this.children.add(expr);
        this.children.add(subscript);
        this.checkIsOutOfBounds = checkIsOutOfBounds;
    }

    public CollectionElementExpr(Type type, Expr expr, Expr subscript, boolean checkIsOutOfBounds) {
        this(type, expr, subscript, checkIsOutOfBounds, NodePosition.ZERO);
    }


    public CollectionElementExpr(Type type, Expr expr, Expr subscript, boolean checkIsOutOfBounds, NodePosition pos) {
        super(pos);
        this.type = type;
        this.children.add(expr);
        this.children.add(subscript);
        this.checkIsOutOfBounds = checkIsOutOfBounds;
    }

    public CollectionElementExpr(CollectionElementExpr other) {
        super(other);
        this.checkIsOutOfBounds = other.checkIsOutOfBounds;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    protected String toSqlImpl() {
        Expr expr = this.children.get(0);
        Expr subscript = this.children.get(1);
        return expr.toSqlImpl() + "[" + subscript.toSqlImpl() + "]";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        if (getChild(0).getType().isArrayType()){
            msg.setNode_type(TExprNodeType.ARRAY_ELEMENT_EXPR);
        } else {
            msg.setNode_type(TExprNodeType.MAP_ELEMENT_EXPR);
        }
        msg.setCheck_is_out_of_bounds(checkIsOutOfBounds);
    }

    @Override
    public Expr clone() {
        return new CollectionElementExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCollectionElementExpr(this, context);
    }

    @Override
    public boolean isSelfMonotonic() {
        boolean ret = true;
        for (Expr child : children) {
            ret &= child.isSelfMonotonic();
        }
        return ret;
    }

    public boolean isCheckIsOutOfBounds() {
        return checkIsOutOfBounds;
    }
}
