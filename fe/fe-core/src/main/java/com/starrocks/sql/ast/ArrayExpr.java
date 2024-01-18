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

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArrayExpr extends Expr {

    public ArrayExpr(Type type, List<Expr> items) {
        this(type, items, NodePosition.ZERO);
    }

    public ArrayExpr(Type type, List<Expr> items, NodePosition pos) {
        super(pos);
        this.type = type;
        this.children = Expr.cloneList(items);
    }

    public ArrayExpr(ArrayExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    protected String toSqlImpl() {
        return '[' +
                children.stream().map(Expr::toSql).collect(Collectors.joining(",")) +
                ']';
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
