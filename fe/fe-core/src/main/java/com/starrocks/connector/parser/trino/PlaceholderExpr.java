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

package com.starrocks.connector.parser.trino;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.ast.AstVisitor;

public class PlaceholderExpr extends Expr {
    private final int index;
    private final Class<? extends Expr> clazz;
    public PlaceholderExpr(int index, Class<? extends Expr> clazz) {
        this.index = index;
        this.clazz = clazz;
    }

    public int getIndex() {
        return index;
    }

    public Class<? extends Expr> getClazz() {
        return clazz;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPlaceholderExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new PlaceholderExpr(index, clazz);
    }
}