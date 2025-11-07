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

package com.starrocks.sql.automv.pn;

import com.starrocks.type.Type;

import java.util.Collections;

// Var means typed symbol, it always represents a GenericColumn. Var use the
// GenericColumn's id to represent the GenericColumn, The var can become tenured
// by capturing its underlying GenericColumn.
public final class Var extends Op {
    private final Symbol sym;

    public Var(Type type, int id) {
        super(type);
        sym = Symbol.intern(id);
    }

    public int getId() {
        return sym.getId();
    }

    @Override
    public Op clone() {
        Var var = new Var(type, getId());
        if (this.getSymbol().isTenured()) {
            var.getSymbol().tenured(this.getSymbol().getTenuredColumn());
        }
        return var;
    }

    @Override
    protected int hashCodeImpl() {
        return 0;
    }

    @Override
    protected String toStringImpl() {
        return String.format("(var[%s] #%s)", type.toSql(), getId());
    }

    @Override
    protected String toIsomorphicStringImpl() {
        return String.format("(var[%s])", type.toSql());
    }

    @Override
    protected SymTab getSymTabImpl() {
        return SymTab.of(Collections.singletonList(sym));
    }

    @Override
    public <R, C> R accept(OpVisitor<R, C> visitor, C context) {
        return visitor.visitVar(this, context);
    }

    public Symbol getSymbol() {
        return sym;
    }

    @Override
    public int nary() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Var var = (Var) o;
        return type.equals(var.type) && this.getId() == var.getId();
    }

    @Override
    protected int getHeightImpl() {
        return 0;
    }
}
