// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ArrayExpr.java

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
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArrayExpr extends Expr {
    private boolean explicitType = false;

    public ArrayExpr(Type type, List<Expr> items) {
        super();
        this.type = type;
        this.children = Expr.cloneList(items);
        this.explicitType = this.type != null;
    }

    public ArrayExpr(ArrayExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Type targetItemType = null;
        if (this.type != null) {
            targetItemType = ((ArrayType) this.type).getItemType();
        } else if (!this.children.isEmpty()) {
            targetItemType = findCommonType(this.children);
        } else {
            // Type.NULL can be implicitly cast to any other types.
            this.type = new ArrayType(Type.NULL);
        }

        // Array<DECIMALV3> type is not supported in current version, turn it into DECIMALV2 type
        if (targetItemType != null && targetItemType.isDecimalV3()) {
            targetItemType = ScalarType.DECIMALV2;
        }

        for (int i = 0; i < this.children.size(); i++) {
            if (!this.children.get(i).getType().matchesType(targetItemType)) {
                castChild(targetItemType, i);
            }
        }
        this.type = this.type == null ? new ArrayType(targetItemType) : this.type;
    }

    public boolean isExplicitType() {
        return explicitType;
    }

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        if (this.explicitType) {
            sb.append(this.type.toSql());
        }
        sb.append('[');
        sb.append(children.stream().map(Expr::toSql).collect(Collectors.joining(",")));
        sb.append(']');
        return sb.toString();
    }

    @Override
    protected String toDigestImpl() {
        StringBuilder sb = new StringBuilder();
        if (this.explicitType) {
            sb.append(this.type.toSql());
        }
        sb.append('[');
        sb.append(children.stream().map(Expr::toDigest).collect(Collectors.joining(",")));
        sb.append(']');
        return sb.toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.ARRAY_EXPR);
    }

    @Override
    public Expr clone() {
        return new ArrayExpr(this);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        ArrayList<Expr> newItems = new ArrayList<>();
        ArrayType arrayType = (ArrayType) targetType;
        Type itemType = arrayType.getItemType();
        for (int i = 0; i < getChildren().size(); i++) {
            Expr child = getChild(i);
            if (child.getType().matchesType(itemType)) {
                newItems.add(child);
            } else {
                newItems.add(child.castTo(itemType));
            }
        }
        ArrayExpr e = new ArrayExpr(targetType, newItems);
        e.analysisDone();
        return e;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrayExpr(this, context);
    }
}
