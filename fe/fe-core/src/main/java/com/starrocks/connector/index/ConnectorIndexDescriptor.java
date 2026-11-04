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

package com.starrocks.connector.index;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/** Snapshot-bound capability descriptor for one connector index. */
public final class ConnectorIndexDescriptor {
    /** Connector-neutral index capabilities understood by the optimizer. */
    public enum IndexType {
        BITMAP,
        RANGE
    }

    /** SQL operations that a connector index descriptor can explicitly advertise. */
    public enum Operation {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        IN,
        NOT_IN,
        IS_NULL,
        IS_NOT_NULL,
        STARTS_WITH
    }

    /** Coverage of a connector index at the snapshot recorded in {@link ConnectorIndexMetadata}. */
    public enum Coverage {
        UNKNOWN,
        PARTIAL,
        FULL
    }

    private final IndexType type;
    private final int fieldId;
    private final String columnName;
    private final Set<Operation> supportedOperations;
    private final Coverage coverage;

    public ConnectorIndexDescriptor(IndexType type, int fieldId, String columnName,
                                    Set<Operation> supportedOperations,
                                    Coverage coverage) {
        this.type = Objects.requireNonNull(type, "type is null");
        this.fieldId = fieldId;
        this.columnName = Objects.requireNonNull(columnName, "columnName is null");
        this.supportedOperations = ImmutableSet.copyOf(
                Objects.requireNonNull(supportedOperations, "supportedOperations is null"));
        this.coverage = Objects.requireNonNull(coverage, "coverage is null");
    }

    public IndexType getType() {
        return type;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getColumnName() {
        return columnName;
    }

    public Set<Operation> getSupportedOperations() {
        return supportedOperations;
    }

    public Coverage getCoverage() {
        return coverage;
    }

    /** Rebinds the display name while preserving the connector field id as the stable identity. */
    public ConnectorIndexDescriptor withColumnName(String currentColumnName) {
        Objects.requireNonNull(currentColumnName, "currentColumnName is null");
        if (columnName.equals(currentColumnName)) {
            return this;
        }
        return new ConnectorIndexDescriptor(type, fieldId, currentColumnName, supportedOperations, coverage);
    }

    public boolean supports(Operation operation) {
        return supportedOperations.contains(operation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorIndexDescriptor)) {
            return false;
        }
        ConnectorIndexDescriptor that = (ConnectorIndexDescriptor) o;
        return fieldId == that.fieldId
                && type == that.type && Objects.equals(columnName, that.columnName)
                && Objects.equals(supportedOperations, that.supportedOperations) && coverage == that.coverage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, fieldId, columnName, supportedOperations, coverage);
    }

    @Override
    public String toString() {
        return "type=" + type + ", fieldId=" + fieldId
                + ", column=" + columnName + ", coverage=" + coverage;
    }
}
