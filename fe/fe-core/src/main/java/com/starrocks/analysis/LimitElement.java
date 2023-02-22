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

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

/**
 * Combination of limit and offset expressions.
 */
public class LimitElement implements ParseNode {
    public static LimitElement NO_LIMIT = new LimitElement();

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private long limit;
    private long offset;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    private final NodePosition pos;

    public LimitElement() {
        pos = NodePosition.ZERO;
        limit = -1;
        offset = 0;
    }

    public LimitElement(long limit) {
        this(0, limit, NodePosition.ZERO);
    }

    public LimitElement(long offset, long limit) {
        this(offset, limit, NodePosition.ZERO);
    }

    public LimitElement(long offset, long limit, NodePosition pos) {
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
        return limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    /**
     * Returns the integer offset, evaluated from the offset expression. Must call
     * analyze() first. If no offsetExpr exists, then 0 (the default offset) is returned.
     */
    public long getOffset() {
        return offset;
    }

    public boolean hasOffset() {
        return offset != 0;
    }

    public String toSql() {
        if (limit == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" LIMIT ");
        if (offset != 0) {
            sb.append(offset).append(", ");
        }
        sb.append("").append(limit);
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public String toDigest() {
        if (limit == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" limit ");
        if (offset != 0) {
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
