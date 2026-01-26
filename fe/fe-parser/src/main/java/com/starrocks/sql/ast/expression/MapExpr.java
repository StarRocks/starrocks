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

package com.starrocks.sql.ast.expression;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MapExpr extends Expr {
    public MapExpr(Type type, List<Expr> items) {
        super();
        this.type = type;
        this.children = items.stream().map(Expr::clone).collect(Collectors.toCollection(ArrayList::new));
    }

    // key expr, value expr, key expr, value expr ...
    public MapExpr(Type type, List<Expr> items, NodePosition pos) {
        super(pos);
        this.type = type;
        this.children = items.stream().map(Expr::clone).collect(Collectors.toCollection(ArrayList::new));
    }

    public MapExpr(MapExpr other) {
        super(other);
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
