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

package com.starrocks.sql.analyzer;

/**
 * ResolvedField is represent resolved field,
 * contains scope which fields belong to and
 * the field index of relation
 */
public class ResolvedField {
    private final Scope scope;
    private final Field field;
    /**
     * relationFieldIndex is an index with a hierarchical relationship.
     * When the resolved scope contains a parent, an offset of the parent scope will be added
     */
    private final int relationFieldIndex;

    public ResolvedField(Scope scope, Field field, int relationFieldIndex) {
        this.scope = scope;
        this.field = field;
        this.relationFieldIndex = relationFieldIndex;
    }

    public Scope getScope() {
        return scope;
    }

    public Field getField() {
        return field;
    }

    public int getRelationFieldIndex() {
        return relationFieldIndex;
    }
}
