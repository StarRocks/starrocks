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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Usage record for a column of an external (non-native) table.
 * <p>
 * Unlike {@link ColumnUsage}, the identity here is not a metastore-resolvable numeric id: external
 * tables have no stable db/table/column id, so the row carries its own catalog/db/table/column names
 * directly. table_uuid is always the fixed-length hash of {@link Table#getUUID()}
 * ({@link StatisticUtils#hashTableUuidForPkStorage}), matching the PK scheme of
 * {@code external_column_statistics} so both tables can be joined by table_uuid.
 */
public class ExternalColumnUsage {

    private static final Logger LOG = LogManager.getLogger(ExternalColumnUsage.class);

    // BE's primary_key_limit_size defaults to 128 bytes (be/src/common/config_primary_key_fwd.h).
    // PrimaryKeyEncoder::encode_exceed_limit adds a 2-byte separator per non-last VARCHAR PK field;
    // our PK is (fe_id, table_uuid, column_name) with column_name last. Budget: fe_id up to 10 digits
    // (+2) + table_uuid fixed 32 hex chars (+2) + column_name (no +2, last field) <= 128, i.e.
    // column_name <= 82. Capped at 80 to leave a small margin.
    private static final int MAX_COLUMN_NAME_LENGTH = 80;

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

    public static Optional<ExternalColumnUsage> build(Column column, Table table, ColumnUsage.UseCase useCase) {
        String columnName = column.getName();
        if (columnName.length() > MAX_COLUMN_NAME_LENGTH) {
            LOG.warn("skip recording external predicate column usage, column name too long: {}.{}",
                    table.getName(), columnName);
            return Optional.empty();
        }
        String tableUuidHash = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        return Optional.of(new ExternalColumnUsage(tableUuidHash, table.getCatalogName(), table.getCatalogDBName(),
                table.getCatalogTableName(), columnName, useCase));
    }

    public String getTableUuidHash() {
        return tableUuidHash;
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
