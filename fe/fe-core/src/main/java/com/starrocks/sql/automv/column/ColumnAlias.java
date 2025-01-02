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

package com.starrocks.sql.automv.column;

import java.util.Objects;
import javax.annotation.Nullable;

public class ColumnAlias {
    public static final String UNQUALIFIED = "UNQUALIFIED";
    private final String tableAlias;
    private final String name;
    private final String qualifiedName;

    private ColumnAlias(String tableAlias, String name) {
        this.tableAlias = Objects.requireNonNull(tableAlias);
        this.name = Objects.requireNonNull(name);
        this.qualifiedName = tableAlias.equals(UNQUALIFIED) ? name : String.format("%s.%s", tableAlias, name);
    }

    public static ColumnAlias of(String tableAlias, String name) {
        return new ColumnAlias(tableAlias, name);
    }

    public static ColumnAlias of(String name) {
        return new ColumnAlias(UNQUALIFIED, name);
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public String getName() {
        return name;
    }

    public ColumnAlias rename(@Nullable String tableAlias, @Nullable String name) {
        if (tableAlias == null || this.tableAlias.equals(tableAlias)) {
            if (name == null || this.name.equals(name)) {
                return this;
            } else {
                return of(this.tableAlias, name);
            }
        } else {
            if (name == null || this.name.equals(name)) {
                return of(tableAlias, this.name);
            } else {
                return of(tableAlias, name);
            }
        }
    }

    @Override
    public String toString() {
        return qualifiedName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnAlias that = (ColumnAlias) o;
        return Objects.equals(tableAlias, that.tableAlias) && Objects.equals(name, that.name) &&
                Objects.equals(qualifiedName, that.qualifiedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableAlias, name, qualifiedName);
    }
}
