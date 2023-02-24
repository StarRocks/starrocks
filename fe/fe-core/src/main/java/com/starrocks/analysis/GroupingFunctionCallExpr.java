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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/GroupingFunctionCallExpr.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

/**
 * mapping the real slot to virtual slots, grouping(_id) function will use a virtual slot of BIGINT to substitute
 * real slots, and then set real slot to realChildren
 */
public class GroupingFunctionCallExpr extends FunctionCallExpr {
    private boolean childrenReseted = false;
    private List<Expr> realChildren;

    public GroupingFunctionCallExpr(String functionName, List<Expr> params) {
        this(functionName, params, NodePosition.ZERO);
    }

    public GroupingFunctionCallExpr(String functionName, List<Expr> params, NodePosition pos) {
        super(functionName, params, pos);
        childrenReseted = false;
    }

    public GroupingFunctionCallExpr(FunctionName functionName, FunctionParams params) {
        super(functionName, params);
        childrenReseted = false;
    }

    public GroupingFunctionCallExpr(GroupingFunctionCallExpr other) {
        super(other);
        this.childrenReseted = other.childrenReseted;
    }

    @Override
    public Expr clone() {
        return new GroupingFunctionCallExpr(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    // set child to virtual slot
    public void resetChild(VirtualSlotRef virtualSlot) {
        ArrayList<Expr> newChildren = new ArrayList<>();
        newChildren.add(virtualSlot);
        realChildren = new ArrayList<>();
        realChildren.addAll(children);
        children = newChildren;
        childrenReseted = true;
    }

    @Override
    public Expr reset() {
        if (childrenReseted) {
            children = new ArrayList<>();
            children.addAll(realChildren);
        }
        childrenReseted = false;
        realChildren = null;
        return super.reset();
    }

    // get the origin children of the expr
    public List<Expr> getRealSlot() {
        if (childrenReseted) {
            return new ArrayList<>(realChildren);
        } else if (isAnalyzed()) {
            return new ArrayList<>(children);
        } else {
            return null;
        }
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingFunctionCall(this, context);
    }
}
