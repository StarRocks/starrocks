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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExistsPredicate.java

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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

public class ExistsPredicate extends Predicate {
    private boolean notExists = false;

    public boolean isNotExists() {
        return notExists;
    }

    public ExistsPredicate(Subquery subquery, boolean notExists) {
        this(subquery, notExists, NodePosition.ZERO);
    }

    public ExistsPredicate(Subquery subquery, boolean notExists, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(subquery);
        children.add(subquery);
        this.notExists = notExists;
    }

    public ExistsPredicate(ExistsPredicate other) {
        super(other);
        notExists = other.notExists;
    }

    @Override
    public Expr negate() {
        return new ExistsPredicate((Subquery) getChild(0), !notExists);
    }


    @Override
    public Expr clone() {
        return new ExistsPredicate(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), notExists);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ExistsPredicate)) {
            return false;
        }

        ExistsPredicate that = (ExistsPredicate) obj;

        return super.equalsWithoutChild(that) && notExists == that.notExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitExistsPredicate(this, context);
    }
}

