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

/**
 * Base class for constraints.
 */
public abstract class Constraint {
    // NOTE: A constraint may contain a name, but currently starrocks only supports internal constraints.
    public static final String TABLE_PROPERTY_CONSTRAINT = "_TABLE_PROPERTIES_";

    /**
     * Type of constraint, unique/foreign key are supported now.
     */
    public enum ConstraintType {
        UNIQUE,
        FOREIGN_KEY
    }
    private final String name;
    private final ConstraintType type;

    public Constraint(ConstraintType type, String name) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public ConstraintType getType() {
        return type;
    }

    /**
     * Called when a table is renamed.
     * @param newTable
     * @return
     */
    public abstract void onTableRename(Table newTable, String oldTableName);
}
