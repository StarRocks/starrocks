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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class JoinRelation extends Relation {
    private final JoinOperator joinOp;
    private Relation left;
    private Relation right;
    private Expr onPredicate;
    private String joinHint = "";
    private boolean lateral;
    private boolean isImplicit;

    /**
     * usingColNames is created by parser
     * and will be converted to onPredicate in Analyzer
     */
    private List<String> usingColNames;

    public JoinRelation(JoinOperator joinOp, Relation left, Relation right, Expr onPredicate, boolean isLateral) {
        this(joinOp, left, right, onPredicate, isLateral, NodePosition.ZERO);
    }

    public JoinRelation(JoinOperator joinOp, Relation left, Relation right, Expr onPredicate,
                        boolean isLateral, NodePosition pos) {
        super(pos);
        if (joinOp == null) {
            this.joinOp = JoinOperator.CROSS_JOIN;
            isImplicit = true;
        } else {
            this.joinOp = joinOp;
        }
        this.left = left;
        this.right = right;
        this.onPredicate = onPredicate;
        this.lateral = isLateral;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public Relation getLeft() {
        return left;
    }

    public Relation getRight() {
        return right;
    }

    public void setLeft(Relation left) {
        this.left = left;
    }

    public void setRight(Relation right) {
        this.right = right;
    }

    public Expr getOnPredicate() {
        return onPredicate;
    }

    public void setOnPredicate(Expr onPredicate) {
        this.onPredicate = onPredicate;
    }

    public void setJoinHint(String joinHint) {
        this.joinHint = StringUtils.upperCase(joinHint);
    }

    public String getJoinHint() {
        return joinHint;
    }

    public boolean isLateral() {
        return lateral;
    }

    public void setLateral(boolean lateral) {
        this.lateral = lateral;
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public List<String> getUsingColNames() {
        return usingColNames;
    }

    public void setUsingColNames(List<String> usingColNames) {
        this.usingColNames = usingColNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }
}