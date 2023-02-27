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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SysVariableDesc.java

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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

// System variable
// Converted to StringLiteral in analyze, if this variable is not exist, throw AnalysisException.
public class VariableExpr extends Expr {
    private final SetType setType;
    private final String name;
    private Object value;
    private boolean isNull;

    @VisibleForTesting
    public VariableExpr(String name) {
        this(name, SetType.SESSION);
    }

    public VariableExpr(String name, SetType setType) {
        this(name, setType, NodePosition.ZERO);
    }

    public VariableExpr(String name, SetType setType, NodePosition pos) {
        super(pos);
        this.name = name;
        this.setType = setType;
    }

    protected VariableExpr(VariableExpr other) {
        super(other);
        setType = other.setType;
        name = other.name;
        value = other.value;
        isNull = other.isNull;
    }

    public SetType getSetType() {
        return setType;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setIsNull() {
        isNull = true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitVariableExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new VariableExpr(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, setType, value, isNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        VariableExpr that = (VariableExpr) o;
        return isNull == that.isNull && setType == that.setType && Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }
}
