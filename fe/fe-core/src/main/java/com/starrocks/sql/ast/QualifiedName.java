// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

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
