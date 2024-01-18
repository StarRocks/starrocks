// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RelationFields {
    private final List<Field> allFields;

    // NOTE: sort fields by name to speedup resolve performance
    private final Multimap<String, Field> names;
    private final boolean resolveStruct;

    public RelationFields(Field... fields) {
        this(ImmutableList.copyOf(fields));
    }

    public RelationFields(List<Field> fields) {
        requireNonNull(fields, "fields is null");
        this.allFields = ImmutableList.copyOf(fields);
        this.resolveStruct = fields.stream().anyMatch(x -> x.getType().isStructType());
        if (!resolveStruct) {
            this.names = this.allFields.stream().collect(ImmutableListMultimap.toImmutableListMultimap(
                    x -> x.getName().toLowerCase(), x -> x));
        } else {
            this.names = null;
        }
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

    /**
     * Gets the index of all columns matching the specified name
     */
    public List<Field> resolveFields(SlotRef name) {
        if (resolveStruct) {
            return allFields.stream().filter(x -> x.canResolve(name)).collect(Collectors.toList());
        }
        // Resolve the slot based on column name first, then table name
        // For the case a table with thousands of columns, resolve by table name could not reduce the cardinality,
        // but resolve by column name first could reduce it a lot
        List<Field> resolved =
                names.get(name.getColumnName().toLowerCase()).stream().collect(ImmutableList.toImmutableList());
        if (name.getTblNameWithoutAnalyzed() == null) {
            return resolved;
        } else {
            return resolved.stream().filter(input -> input.canResolve(name)).collect(toImmutableList());
        }
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
