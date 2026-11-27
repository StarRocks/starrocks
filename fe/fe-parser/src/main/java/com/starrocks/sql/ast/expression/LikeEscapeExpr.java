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

import java.util.Objects;

/**
 * The pattern of {@code value LIKE pattern ESCAPE escape}. LIKE itself only knows the backslash as the escape
 * character, so this expression evaluates to the same pattern written with backslashes.
 */
public class LikeEscapeExpr extends Expr {

    // A single character, or empty for a pattern without an escape character.
    private final String escape;

    public LikeEscapeExpr(Expr pattern, String escape, NodePosition pos) {
        super(pos);
        this.escape = escape;
        addChild(pattern);
    }

    protected LikeEscapeExpr(LikeEscapeExpr other) {
        super(other);
        escape = other.escape;
    }

    public String getEscape() {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLikeEscapeExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new LikeEscapeExpr(this);
    }

    @Override
    public boolean equalsWithoutChild(Object o) {
        if (!super.equalsWithoutChild(o)) {
            return false;
        }
        return Objects.equals(escape, ((LikeEscapeExpr) o).escape);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), escape);
    }
}
