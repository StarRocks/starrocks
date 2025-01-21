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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LimitElement.java

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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

/**
 * Combination of limit and offset expressions.
 */
public class LimitElement implements ParseNode {
    public static LimitElement NO_LIMIT = new LimitElement();

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private final Expr limit;
    private final Expr offset;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    private final NodePosition pos;

    public LimitElement() {
        pos = NodePosition.ZERO;
        limit = new IntLiteral(-1);
        offset = new IntLiteral(0);
    }

    public LimitElement(long offset, long limit) {
        this(offset, limit, NodePosition.ZERO);
    }

    public LimitElement(long offset, long limit, NodePosition pos) {
        this.pos = pos;
        this.offset = new IntLiteral(offset);
        this.limit = new IntLiteral(limit);
    }

    public LimitElement(Expr offset, Expr limit, NodePosition pos) {
        this.pos = pos;
        this.offset = offset;
        this.limit = limit;
    }

    protected LimitElement(LimitElement other) {
        pos = other.pos;
        limit = other.limit;
        offset = other.offset;
    }

    @Override
    public LimitElement clone() {
        return new LimitElement(this);
    }

    /**
     * Returns the integer limit, evaluated from the limit expression. Must call analyze()
     * first. If no limit was set, then -1 is returned.
     */
    public long getLimit() {
        Preconditions.checkState(limit instanceof LiteralExpr);
        return ((LiteralExpr) limit).getLongValue();
    }

    public boolean hasLimit() {
        return getLimit() != -1;
    }

    /**
     * Returns the integer offset, evaluated from the offset expression. Must call
     * analyze() first. If no offsetExpr exists, then 0 (the default offset) is returned.
     */
    public long getOffset() {
        Preconditions.checkState(offset instanceof LiteralExpr);
        return ((LiteralExpr) offset).getLongValue();
    }

    public boolean hasOffset() {
        return getOffset() != 0;
    }

    public String toSql() {
        if (getLimit() == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" LIMIT ");
        if (getOffset() != 0) {
            sb.append(getOffset()).append(", ");
        }
        sb.append("").append(getLimit());
        return sb.toString();
    }

    public Expr getLimitExpr() {
        return limit;
    }

    public Expr getOffsetExpr() {
        return offset;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public String toDigest() {
        if (getLimit() == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" limit ");
        if (getOffset() != 0) {
            sb.append(" ?, ");
        }
        sb.append("").append(" ? ");
        return sb.toString();
    }

    public void reset() {
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLimitElement(this, context);
    }
}
