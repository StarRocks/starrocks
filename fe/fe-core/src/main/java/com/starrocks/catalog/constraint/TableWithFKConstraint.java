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

package com.starrocks.catalog.constraint;

import com.starrocks.catalog.Table;

import java.util.Objects;

public class TableWithFKConstraint {
    private final Table childTable;
    private final ForeignKeyConstraint refConstraint;

    public TableWithFKConstraint(Table childTable, ForeignKeyConstraint refConstraint) {
        this.childTable = childTable;
        this.refConstraint = refConstraint;
    }

    public static TableWithFKConstraint of(Table parentTable, ForeignKeyConstraint refConstraint) {
        return new TableWithFKConstraint(parentTable, refConstraint);
    }

    public Table getChildTable() {
        return childTable;
    }

    public ForeignKeyConstraint getRefConstraint() {
        return refConstraint;
    }

    @Override
    public int hashCode() {
        return Objects.hash(childTable, refConstraint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof TableWithFKConstraint)) {
            return false;
        }
        TableWithFKConstraint that = (TableWithFKConstraint) obj;
        return Objects.equals(childTable, that.childTable) && Objects.equals(refConstraint, that.refConstraint);
    }
}