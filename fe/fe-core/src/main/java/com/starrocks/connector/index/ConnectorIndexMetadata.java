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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/** Snapshot-bound index capabilities exposed by a connector for one table. */
public final class ConnectorIndexMetadata {
    /** Table execution model relevant to connector-index result compatibility. */
    public enum TableType {
        UNKNOWN,
        APPEND_ONLY,
        DATA_EVOLUTION,
        PRIMARY_KEY
    }

    public static final long UNKNOWN_SNAPSHOT_ID = -1L;

    private static final ConnectorIndexMetadata EMPTY = new ConnectorIndexMetadata(
            UNKNOWN_SNAPSHOT_ID, TableType.UNKNOWN, List.of(), Map.of(), Set.of());

    private final long snapshotId;
    private final TableType tableType;
    private final List<ConnectorIndexDescriptor> descriptors;
    private final Map<String, Integer> currentFieldIds;
    private final Set<Integer> partitionFieldIds;

    private ConnectorIndexMetadata(long snapshotId, TableType tableType,
                                   List<ConnectorIndexDescriptor> descriptors,
                                   Map<String, Integer> currentFieldIds,
                                   Set<Integer> partitionFieldIds) {
        this.snapshotId = snapshotId;
        this.tableType = Objects.requireNonNull(tableType, "tableType is null");
        this.descriptors = ImmutableList.copyOf(Objects.requireNonNull(descriptors, "descriptors is null"));
        this.currentFieldIds = ImmutableMap.copyOf(
                Objects.requireNonNull(currentFieldIds, "currentFieldIds is null"));
        this.partitionFieldIds = ImmutableSet.copyOf(
                Objects.requireNonNull(partitionFieldIds, "partitionFieldIds is null"));
    }

    public static ConnectorIndexMetadata empty() {
        return EMPTY;
    }

    public static ConnectorIndexMetadata of(long snapshotId, TableType tableType,
                                            List<ConnectorIndexDescriptor> descriptors) {
        Map<String, Integer> currentFieldIds = descriptors.stream().collect(Collectors.toMap(
                ConnectorIndexDescriptor::getColumnName, ConnectorIndexDescriptor::getFieldId,
                (left, right) -> left));
        return of(snapshotId, tableType, descriptors, currentFieldIds, Set.of());
    }

    public static ConnectorIndexMetadata of(long snapshotId, TableType tableType,
                                            List<ConnectorIndexDescriptor> descriptors,
                                            Map<String, Integer> currentFieldIds,
                                            Set<Integer> partitionFieldIds) {
        if (descriptors.isEmpty() && snapshotId == UNKNOWN_SNAPSHOT_ID
                && tableType == TableType.UNKNOWN) {
            return EMPTY;
        }
        return new ConnectorIndexMetadata(snapshotId, tableType, descriptors, currentFieldIds, partitionFieldIds);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public TableType getTableType() {
        return tableType;
    }

    public List<ConnectorIndexDescriptor> getDescriptors() {
        return descriptors;
    }

    public List<ConnectorIndexDescriptor> getDescriptors(String columnName) {
        OptionalInt fieldId = getCurrentFieldId(columnName);
        return fieldId.isEmpty() ? List.of() : getDescriptors(fieldId.getAsInt());
    }

    public List<ConnectorIndexDescriptor> getDescriptors(int fieldId) {
        return descriptors.stream().filter(descriptor -> descriptor.getFieldId() == fieldId)
                .collect(Collectors.toUnmodifiableList());
    }

    public Optional<ConnectorIndexDescriptor> findDescriptor(String columnName, IndexType type) {
        OptionalInt fieldId = getCurrentFieldId(columnName);
        return fieldId.isEmpty() ? Optional.empty() : findDescriptor(fieldId.getAsInt(), type);
    }

    public Optional<ConnectorIndexDescriptor> findDescriptor(int fieldId, IndexType type) {
        return descriptors.stream()
                .filter(descriptor -> descriptor.getFieldId() == fieldId && descriptor.getType() == type)
                .findFirst();
    }

    public OptionalInt getCurrentFieldId(String columnName) {
        Integer fieldId = currentFieldIds.get(columnName);
        if (fieldId != null) {
            return OptionalInt.of(fieldId);
        }
        return currentFieldIds.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(columnName))
                .map(Map.Entry::getValue)
                .mapToInt(Integer::intValue)
                .findFirst();
    }

    public boolean isPartitionField(int fieldId) {
        return partitionFieldIds.contains(fieldId);
    }

    /** Rebinds immutable snapshot descriptors to the table's current schema without changing descriptor identity. */
    public ConnectorIndexMetadata withCurrentFields(Map<String, Integer> fieldIds, Set<Integer> partitionIds) {
        return new ConnectorIndexMetadata(snapshotId, tableType, descriptors, fieldIds, partitionIds);
    }

    public boolean isEmpty() {
        return descriptors.isEmpty();
    }

    @Override
    public String toString() {
        return "snapshotId=" + snapshotId + ", tableType=" + tableType + ", indexes=" + descriptors;
    }
}
