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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for execution-plan expressions. Unlike {@link com.starrocks.sql.ast.expression.Expr},
 * this hierarchy is purpose-built for the physical plan and Thrift serialization,
 * with no dependency on AST or analysis infrastructure.
 */
public abstract class ExecExpr implements Cloneable {
    protected Type type;
    protected Type originType;
    protected List<ExecExpr> children;
    protected boolean isIndexOnlyFilter;

    protected ExecExpr(Type type) {
        this.type = type;
        this.originType = null;
        this.children = Collections.emptyList();
    }

    protected ExecExpr(Type type, List<ExecExpr> children) {
        this.type = type;
        this.originType = null;
        this.children = children != null ? children : Collections.emptyList();
    }

    // ---- Type ----

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getOriginType() {
        if (originType != null) {
            return originType;
        }
        return type;
    }

    public void setOriginType(Type originType) {
        this.originType = originType;
    }

    // ---- Tree structure ----

    public ExecExpr getChild(int i) {
        return children.get(i);
    }

    public void setChild(int i, ExecExpr child) {
        children.set(i, child);
    }

    public List<ExecExpr> getChildren() {
        return children;
    }

    public int getNumChildren() {
        return children.size();
    }

    public void addChild(ExecExpr child) {
        if (children.isEmpty()) {
            children = new ArrayList<>();
        }
        children.add(child);
    }

    // ---- Properties ----

    public abstract boolean isNullable();

    public boolean hasNullableChild() {
        for (ExecExpr child : children) {
            if (child.isNullable()) {
                return true;
            }
        }
        return false;
    }

    public boolean isMonotonic() {
        if (!isSelfMonotonic()) {
            return false;
        }
        for (ExecExpr child : children) {
            if (!child.isMonotonic()) {
                return false;
            }
        }
        return true;
    }

    public boolean isSelfMonotonic() {
        return false;
    }

    public boolean isConstant() {
        // Match Expr.isConstantImpl() behavior: vacuously true for leaf nodes.
        // Subclasses like ExecSlotRef/ExecPlaceHolder override to return false.
        for (ExecExpr child : children) {
            if (!child.isConstant()) {
                return false;
            }
        }
        return true;
    }

    public boolean isIndexOnlyFilter() {
        return isIndexOnlyFilter;
    }

    public void setIsIndexOnlyFilter(boolean isIndexOnlyFilter) {
        this.isIndexOnlyFilter = isIndexOnlyFilter;
    }

    // ---- Serialization ----

    public abstract TExprNodeType getNodeType();

    /**
     * Populate type-specific fields on the given {@link TExprNode}.
     * Common fields (type, num_children, nullable, monotonic, etc.) are handled
     * by {@link ExecExprSerializer}.
     */
    public abstract void toThrift(TExprNode node);

    // ---- Visitor ----

    public abstract <R, C> R accept(ExecExprVisitor<R, C> visitor, C context);

    // ---- Clone ----

    @Override
    public abstract ExecExpr clone();

    protected List<ExecExpr> cloneChildren() {
        if (children.isEmpty()) {
            return Collections.emptyList();
        }
        List<ExecExpr> cloned = new ArrayList<>(children.size());
        for (ExecExpr child : children) {
            cloned.add(child.clone());
        }
        return cloned;
    }
}
