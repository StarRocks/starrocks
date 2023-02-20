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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RelationFields {
    private final List<Field> allFields;

    public RelationFields(Field... fields) {
        this(ImmutableList.copyOf(fields));
    }

    public RelationFields(List<Field> fields) {
        requireNonNull(fields, "fields is null");
        this.allFields = ImmutableList.copyOf(fields);
    }

    /**
     * Gets the index of the specified field.
     */
    public int indexOf(Field field) {
        return allFields.indexOf(field);
    }

    /**
     * Gets the field at the specified index.
     */
    public Field getFieldByIndex(int fieldIndex) {
        checkElementIndex(fieldIndex, allFields.size(), "fieldIndex");
        return allFields.get(fieldIndex);
    }

    public List<Field> getAllFields() {
        return allFields;
    }

    public List<Field> getAllVisibleFields() {
        return allFields.stream().filter(Field::isVisible).collect(Collectors.toList());
    }

    /**
     * Gets the index of all columns matching the specified name
     */
    public List<Field> resolveFields(SlotRef name) {
        return allFields.stream()
                .filter(input -> input.canResolve(name))
                .collect(toImmutableList());
    }

    public RelationFields joinWith(RelationFields other) {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.allFields)
                .addAll(other.allFields)
                .build();

        return new RelationFields(fields);
    }

    public List<Field> resolveFieldsWithPrefix(TableName prefix) {
        return allFields.stream()
                .filter(input -> input.matchesPrefix(prefix))
                .collect(toImmutableList());
    }

    public int size() {
        return allFields.size();
    }

    @Override
    public String toString() {
        return allFields.toString();
    }
}
