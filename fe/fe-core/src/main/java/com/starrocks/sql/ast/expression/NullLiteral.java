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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/NullLiteral.java

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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;

import java.nio.ByteBuffer;

public class NullLiteral extends LiteralExpr {

    private static final LiteralExpr INT_EXPR;

    static {
        try {
            INT_EXPR = new IntLiteral("0", Type.INT);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    public NullLiteral() {
        this(NodePosition.ZERO);
    }

    public NullLiteral(NodePosition pos) {
        super(pos);
        type = Type.NULL;
    }

    public static NullLiteral create(Type type) {
        NullLiteral l = new NullLiteral();
        l.type = type;
        return l;
    }

    protected NullLiteral(NullLiteral other) {
        super(other);
    }

    @Override
    public Expr clone() {
        return new NullLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        return obj instanceof NullLiteral;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 0;
        }
        return -1;
    }

    @Override
    public String getStringValue() {
        return "NULL";
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public Object getRealObjectValue() {
        return getStringValue();
    }

    @Override
    public double getDoubleValue() {
        return 0.0;
    }

    // for distribution prune
    // https://dev.mysql.com/doc/refman/5.7/en/partitioning-handling-nulls.html
    // DATE DATETIME: MIN_VALUE
    // CHAR VARCHAR: ""
    @Override
    public ByteBuffer getHashValue(Type type) {
        return INT_EXPR.getHashValue(Type.INT);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        Preconditions.checkState(targetType.isValid());
        if (!type.equals(targetType)) {
            NullLiteral nullLiteral = new NullLiteral(this);
            nullLiteral.setType(targetType);
            return nullLiteral;
        }
        return this;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitNullLiteral(this, context);
    }
}
