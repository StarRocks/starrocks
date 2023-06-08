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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.ArrayList;
import java.util.List;

public class MapExpr extends Expr {
    private boolean explicitType = false;

    public MapExpr(Type type, List<Expr> items) {
        super();
        this.type = type;
        this.children = Expr.cloneList(items);
        this.explicitType = this.type != null;
    }

    // key expr, value expr, key expr, value expr ...
    public MapExpr(Type type, List<Expr> items, NodePosition pos) {
        super(pos);
        this.type = type;
        this.children = Expr.cloneList(items);
        this.explicitType = this.type != null;
    }

    public MapExpr(MapExpr other) {
        super(other);
    }

    public Type getKeyCommonType() {
        Preconditions.checkState(children.size() > 1 && children.size() % 2 == 0);
        ArrayList<Type> keyExprsType = Lists.newArrayList();
        for (int i = 0; i < children.size(); i += 2) {
            keyExprsType.add(children.get(i).getType());
        }
        return TypeManager.getCommonSuperType(keyExprsType);
    }

    public Type getValueCommonType() {
        Preconditions.checkState(children.size() > 1 && children.size() % 2 == 0);
        ArrayList<Type> valueExprsType = Lists.newArrayList();
        for (int i = 1; i < children.size(); i += 2) {
            valueExprsType.add(children.get(i).getType());
        }
        return TypeManager.getCommonSuperType(valueExprsType);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    public boolean isExplicitType() {
        return explicitType;
    }

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append("map{");
        for (int i = 0; i < children.size(); i += 2) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(children.get(i).toSql() + ":" + children.get(i + 1).toSql());
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        ArrayList<Expr> newItems = new ArrayList<>();
        MapType mapType = (MapType) targetType;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        for (int i = 0; i < getChildren().size(); i++) {
            Expr child = getChild(i);
            if (child.getType().matchesType(i % 2 == 0 ? keyType : valueType)) {
                newItems.add(child);
            } else {
                newItems.add(child.castTo(i % 2 == 0 ? keyType : valueType));
            }
        }
        MapExpr e = new MapExpr(targetType, newItems);
        e.analysisDone();
        return e;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.MAP_EXPR);
    }

    @Override
    public Expr clone() {
        return new MapExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMapExpr(this, context);
    }
}
