// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.requireNonNull;

/**
 * QualifiedName is used to represent a string connected by "."
 * Often used to represent an unresolved Table Name such as db.table
 */
public class QualifiedName {
    private final List<String> parts;

    public static QualifiedName of(Iterable<String> originalParts) {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    private QualifiedName(List<String> originalParts) {
        this.parts = originalParts;
    }

    public List<String> getParts() {
        return parts;
    }

    public String toSqlImpl() {
        List<String> backtick = parts.stream().map(s -> "`" + s + "`").collect(Collectors.toList());
        return Joiner.on('.').join(backtick);
    }

    public String getProbablyColumnName() {
        // If the column type is not struct type, the last part of QualifiedName must be column name.
        // Buf if column type is struct type, the last part may be a column name.
        return parts.get(parts.size() - 1);
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode() {
        return parts.hashCode();
    }
}
