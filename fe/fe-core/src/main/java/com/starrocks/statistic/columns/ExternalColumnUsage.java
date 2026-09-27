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

package com.starrocks.statistic.columns;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.statistic.StatisticUtils;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Usage record for a column of an external (non-native) table.
 * <p>
 * Unlike {@link ColumnUsage}, the identity here is not a metastore-resolvable numeric id: external
 * tables have no stable db/table/column id, so the row carries its own catalog/db/table/column names
 * directly. table_uuid and column_name are both persisted as fixed-length hashes
 * ({@link #getTableUuidHash()}, {@link #getColumnNameHash()}) so the PK never depends on the byte
 * length of externally-supplied identifiers (multibyte column names would otherwise make a
 * character-count guard unsafe, since BE's primary_key_limit_size is measured in encoded bytes).
 * table_uuid hashing also matches the PK scheme of {@code external_column_statistics} so both tables
 * can be joined by table_uuid. The raw column_name is kept as a plain (non-key) value column so it can
 * be read back.
 */
public class ExternalColumnUsage {

    @SerializedName("tableUuidHash")
    private final String tableUuidHash;

    @SerializedName("catalogName")
    private final String catalogName;

    @SerializedName("dbName")
    private final String dbName;

    @SerializedName("tableName")
    private final String tableName;

    @SerializedName("columnName")
    private final String columnName;

    @SerializedName("useCase")
    private final EnumSet<ColumnUsage.UseCase> useCase;

    @SerializedName("lastUsed")
    private LocalDateTime lastUsed;

    @SerializedName("created")
    private LocalDateTime created;

    public ExternalColumnUsage(String tableUuidHash, String catalogName, String dbName, String tableName,
                               String columnName, ColumnUsage.UseCase useCase) {
        this(tableUuidHash, catalogName, dbName, tableName, columnName, EnumSet.of(useCase));
    }

    public ExternalColumnUsage(String tableUuidHash, String catalogName, String dbName, String tableName,
                               String columnName, EnumSet<ColumnUsage.UseCase> useCase) {
        this.tableUuidHash = tableUuidHash;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.useCase = useCase;
        this.lastUsed = TimeUtils.getSystemNow();
        this.created = TimeUtils.getSystemNow();
    }

    public static ExternalColumnUsage build(Column column, Table table, ColumnUsage.UseCase useCase) {
        String columnName = column.getName();
        String tableUuidHash = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        return new ExternalColumnUsage(tableUuidHash, table.getCatalogName(), table.getCatalogDBName(),
                table.getCatalogTableName(), columnName, useCase);
    }

    public String getTableUuidHash() {
        return tableUuidHash;
    }

    // Reuses the generic murmur3-based hasher (despite its table_uuid-oriented name/javadoc, the
    // implementation is just a fixed-length hash of an arbitrary string) so column_name never affects
    // the PK's byte size, regardless of length or encoding (see the class doc for why this matters).
    public String getColumnNameHash() {
        return StatisticUtils.hashTableUuidForPkStorage(columnName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public EnumSet<ColumnUsage.UseCase> getUseCases() {
        return useCase;
    }

    public String getUseCaseString() {
        return useCase.stream().map(ColumnUsage.UseCase::toString).collect(Collectors.joining(","));
    }

    public void setLastUsed(LocalDateTime lastUsed) {
        this.lastUsed = lastUsed;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getLastUsed() {
        return lastUsed;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    // NOTE: mutable
    public void useNow(ColumnUsage.UseCase useCase) {
        this.lastUsed = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
        this.useCase.add(useCase);
    }

    public boolean needPersist(LocalDateTime lastPersist) {
        return this.lastUsed.isAfter(lastPersist);
    }

    public ExternalColumnUsage merge(ExternalColumnUsage other) {
        Preconditions.checkArgument(other.equals(this));
        ExternalColumnUsage merged = new ExternalColumnUsage(tableUuidHash, catalogName, dbName, tableName,
                columnName, EnumSet.copyOf(this.useCase));
        merged.useCase.addAll(other.useCase);
        merged.lastUsed = this.lastUsed.isBefore(other.getLastUsed()) ? other.getLastUsed() : this.lastUsed;
        return merged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalColumnUsage that = (ExternalColumnUsage) o;
        return Objects.equals(tableUuidHash, that.tableUuidHash) && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUuidHash, columnName);
    }

    @Override
    public String toString() {
        return String.format("ExternalColumnUsage{catalog=%s, db=%s, table=%s, column=%s, useCase=%s}",
                catalogName, dbName, tableName, columnName, getUseCaseString());
    }
}
