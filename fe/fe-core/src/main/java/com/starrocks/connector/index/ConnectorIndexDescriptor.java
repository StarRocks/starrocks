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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/** Snapshot-bound capability descriptor for one connector index. */
public final class ConnectorIndexDescriptor {
    public static final String OPTION_METRIC = "metric";
    public static final String OPTION_DIMENSION = "dimension";

    private final ConnectorIndexType type;
    private final String provider;
    private final int fieldId;
    private final String columnName;
    private final boolean columnNullable;
    private final boolean elementNullable;
    private final Map<String, String> options;
    private final Set<ConnectorIndexOperation> supportedOperations;
    private final ConnectorIndexCoverage coverage;

    public ConnectorIndexDescriptor(ConnectorIndexType type, String provider, int fieldId, String columnName,
                                    Map<String, String> options,
                                    Set<ConnectorIndexOperation> supportedOperations,
                                    ConnectorIndexCoverage coverage) {
        this(type, provider, fieldId, columnName, true, true, options, supportedOperations, coverage);
    }

    public ConnectorIndexDescriptor(ConnectorIndexType type, String provider, int fieldId, String columnName,
                                    boolean columnNullable, boolean elementNullable, Map<String, String> options,
                                    Set<ConnectorIndexOperation> supportedOperations,
                                    ConnectorIndexCoverage coverage) {
        this.type = Objects.requireNonNull(type, "type is null");
        this.provider = Objects.requireNonNull(provider, "provider is null");
        this.fieldId = fieldId;
        this.columnName = Objects.requireNonNull(columnName, "columnName is null");
        this.columnNullable = columnNullable;
        this.elementNullable = elementNullable;
        this.options = ImmutableMap.copyOf(Objects.requireNonNull(options, "options is null"));
        this.supportedOperations = ImmutableSet.copyOf(
                Objects.requireNonNull(supportedOperations, "supportedOperations is null"));
        this.coverage = Objects.requireNonNull(coverage, "coverage is null");
    }

    public ConnectorIndexType getType() {
        return type;
    }

    public String getProvider() {
        return provider;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isColumnNullable() {
        return columnNullable;
    }

    public boolean isElementNullable() {
        return elementNullable;
    }

    /** Whether either the indexed value or, for arrays/vectors, its element can be null. */
    public boolean isNullable() {
        return columnNullable || elementNullable;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Set<ConnectorIndexOperation> getSupportedOperations() {
        return supportedOperations;
    }

    public ConnectorIndexCoverage getCoverage() {
        return coverage;
    }

    /** Rebinds the display name while preserving the connector field id as the stable identity. */
    public ConnectorIndexDescriptor withColumnName(String currentColumnName) {
        Objects.requireNonNull(currentColumnName, "currentColumnName is null");
        if (columnName.equals(currentColumnName)) {
            return this;
        }
        return new ConnectorIndexDescriptor(type, provider, fieldId, currentColumnName,
                columnNullable, elementNullable, options, supportedOperations, coverage);
    }

    public boolean supports(ConnectorIndexOperation operation) {
        return supportedOperations.contains(operation);
    }

    public Optional<VectorIndexMetric> getVectorMetric() {
        return VectorIndexMetric.fromOption(options.get(OPTION_METRIC));
    }

    public OptionalInt getVectorDimension() {
        String value = options.get(OPTION_DIMENSION);
        if (value == null) {
            return OptionalInt.empty();
        }
        try {
            int dimension = Integer.parseInt(value);
            return dimension > 0 ? OptionalInt.of(dimension) : OptionalInt.empty();
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
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
        return fieldId == that.fieldId && columnNullable == that.columnNullable
                && elementNullable == that.elementNullable
                && type == that.type && Objects.equals(provider, that.provider)
                && Objects.equals(columnName, that.columnName) && Objects.equals(options, that.options)
                && Objects.equals(supportedOperations, that.supportedOperations) && coverage == that.coverage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, provider, fieldId, columnName, columnNullable, elementNullable,
                options, supportedOperations, coverage);
    }

    @Override
    public String toString() {
        return "type=" + type + ", provider=" + provider + ", fieldId=" + fieldId
                + ", column=" + columnName + ", columnNullable=" + columnNullable
                + ", elementNullable=" + elementNullable + ", coverage=" + coverage;
    }
}
