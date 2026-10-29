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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The immutable, query-owned contract shared by planning, publication and abort. */
public final class PaimonSinkExtra {
    public static final int WRITER_PROTOCOL_VERSION = 1;

    public enum PublicationState {
        WRITING, PREPARED, PUBLISHING, COMMITTED, ABORTED, UNKNOWN
    }

    private final String location;
    private final String tableIdentity;
    private final long schemaId;
    private final String schemaJson;
    private final List<String> columnNames;
    private final List<String> partitionColumns;
    private final List<String> bucketColumns;
    private final List<String> primaryKeyColumns;
    private final int bucketCount;
    private final Map<String, String> options;
    private final String operationId;
    private final String overwriteMode;
    private final Map<String, String> staticPartition;
    private final String timeZone;
    private final String commitUser;
    private final long commitIdentifier;
    private PublicationState publicationState = PublicationState.WRITING;

    private PaimonSinkExtra(FileStoreTable table, String tableIdentity, String mode,
                           Map<String, String> staticPartition, String operationId, String timeZone) {
        TableSchema schema = table.schema();
        this.location = table.location().toString();
        // The native UUID was resolved with the external target before metadata locks.
        // Sink planning only copies that identity and the already-loaded schema.
        this.tableIdentity = tableIdentity;
        this.schemaId = schema.id();
        this.schemaJson = canonicalSchemaJson(table);
        this.columnNames = List.copyOf(schema.fieldNames());
        this.partitionColumns = List.copyOf(schema.partitionKeys());
        this.bucketColumns = List.copyOf(schema.bucketKeys());
        this.primaryKeyColumns = List.copyOf(schema.primaryKeys());
        this.bucketCount = schema.numBuckets();
        this.options = Collections.unmodifiableMap(canonicalOptions(table));
        this.operationId = operationId;
        this.overwriteMode = mode;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
        this.timeZone = Objects.requireNonNull(timeZone, "timeZone");
        this.commitUser = "starrocks-" + operationId;
        this.commitIdentifier = BatchWriteBuilder.COMMIT_IDENTIFIER;
    }

    public static PaimonSinkExtra from(PaimonTable table, String mode, Map<String, String> staticPartition,
                                       String operationId, String timeZone) {
        if (!(table.getNativeTable() instanceof FileStoreTable nativeTable)) {
            throw new StarRocksConnectorException("Paimon writes require a file-store table");
        }
        PaimonWriteCapabilities.validateWrite(nativeTable);
        if (operationId == null || !operationId.matches("[A-Za-z0-9_-]{1,128}")) {
            throw new StarRocksConnectorException("Invalid Paimon write operation identity");
        }
        if (!List.of("APPEND", "FULL", "STATIC", "DYNAMIC").contains(mode)) {
            throw new StarRocksConnectorException("Unsupported Paimon overwrite mode");
        }
        Map<String, String> partition = staticPartition == null ? Map.of() : staticPartition;
        if (!nativeTable.partitionKeys().containsAll(partition.keySet())) {
            throw new StarRocksConnectorException("Paimon static partition contains an unknown column");
        }
        if (mode.equals("STATIC") && partition.isEmpty()) {
            throw new StarRocksConnectorException("Paimon static overwrite requires a partition target");
        }
        return new PaimonSinkExtra(nativeTable, table.getNativeWriteIdentity(), mode, partition, operationId, timeZone);
    }

    public static String canonicalSchemaJson(FileStoreTable table) {
        return table.schema().copy(canonicalOptions(table)).toString();
    }

    private static Map<String, String> canonicalOptions(FileStoreTable table) {
        Map<String, String> result = new LinkedHashMap<>(table.options());
        // Java's AbstractFileStoreTable injects its location into an in-memory schema copy;
        // native writers read the persisted schema, which need not contain this derived option.
        if (table.location().toString().equals(result.get("path"))) {
            result.remove("path");
        }
        return result;
    }

    public void validateTable(FileStoreTable table) {
        if (!tableIdentity.equals(table.uuid()) || schemaId != table.schema().id()
                || !schemaJson.equals(canonicalSchemaJson(table)) || !location.equals(table.location().toString())) {
            throw new StarRocksConnectorException("Paimon target identity or schema changed during the write");
        }
        PaimonWriteCapabilities.validateWrite(table);
    }

    public synchronized PublicationState getPublicationState() {
        return publicationState;
    }

    public synchronized void transition(PublicationState expected, PublicationState next) {
        if (publicationState != expected) {
            throw new StarRocksConnectorException("Invalid Paimon publication state transition");
        }
        publicationState = next;
    }

    public synchronized boolean canAbort() {
        return publicationState == PublicationState.WRITING || publicationState == PublicationState.PREPARED;
    }

    public boolean isOverwrite() {
        return !overwriteMode.equals("APPEND");
    }

    public String getLocation() { return location; }
    public String getTableIdentity() { return tableIdentity; }
    public long getSchemaId() { return schemaId; }
    public String getSchemaJson() { return schemaJson; }
    public List<String> getColumnNames() { return columnNames; }
    public List<String> getPartitionColumns() { return partitionColumns; }
    public List<String> getBucketColumns() { return bucketColumns; }
    public List<String> getPrimaryKeyColumns() { return primaryKeyColumns; }
    public int getBucketCount() { return bucketCount; }
    public Map<String, String> getOptions() { return options; }
    public String getOperationId() { return operationId; }
    public String getOverwriteMode() { return overwriteMode; }
    public Map<String, String> getStaticPartition() { return staticPartition; }
    public String getTimeZone() { return timeZone; }
    public String getCommitUser() { return commitUser; }
    public long getCommitIdentifier() { return commitIdentifier; }
}
