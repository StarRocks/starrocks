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

import com.google.common.base.Preconditions;
import com.starrocks.sql.automv.column.GenericColumn;

import java.util.Objects;
import java.util.Optional;

public final class Symbol extends SymTabEntry {
    final Integer id;
    Optional<GenericColumn> tenuredColumn = Optional.empty();

    private Symbol(int id) {
        this.id = id;
    }

    public static Symbol intern(int id) {
        return new Symbol(id);
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCodeImpl() {
        return id.hashCode();
    }

    public GenericColumn getTenuredColumn() {
        Preconditions.checkArgument(tenuredColumn.isPresent());
        return tenuredColumn.get();
    }

    public void tenured(GenericColumn column) {
        Preconditions.checkArgument(!tenuredColumn.isPresent());
        tenuredColumn = Optional.of(column);
    }

    public boolean isTenured() {
        return tenuredColumn.isPresent();
    }

    @Override
    public String toStringImpl() {
        return String.format("Symbol(id=%d)", id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Symbol symbol = (Symbol) o;
        if (symbol.isTenured() && this.isTenured() && symbol.getTenuredColumn().equals(getTenuredColumn())) {
            return true;
        } else {
            return Objects.equals(id, symbol.id);
        }
    }
}
