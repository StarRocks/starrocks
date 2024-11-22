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

package com.starrocks.statistic.predicate_columns;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnUsage {

    @SerializedName("columnId")
    private ColumnId columnId;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;

    // only exists in memory
    private TableName tableName;

    @SerializedName("lastUsed")
    private LocalDateTime lastUsed;

    @SerializedName("useCase")
    private EnumSet<UseCase> useCase;

    @SerializedName("created")
    private LocalDateTime created;

    public ColumnUsage(ColumnId columnId, long dbId, long tableId, TableName tableName, UseCase useCase) {
        this(columnId, dbId, tableId, tableName, EnumSet.of(useCase));
    }

    public ColumnUsage(ColumnId columnId, long dbId, long tableId, TableName tableName, EnumSet<UseCase> useCase) {
        this.columnId = columnId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.useCase = useCase;
        this.lastUsed = TimeUtils.getSystemNow();
        this.created = TimeUtils.getSystemNow();
    }

    public static Optional<ColumnUsage> build(Column column, Table table, UseCase useCase) {
        LocalMetastore meta = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Optional<String> dbName = table.mayGetDatabaseName();
        Optional<Database> db = dbName.flatMap(meta::mayGetDb);
        if (db.isPresent()) {
            TableName tableName = new TableName(dbName.get(), table.getName());
            return Optional.of(
                    new ColumnUsage(column.getColumnId(), db.get().getId(), table.getId(), tableName, useCase));
        }
        return Optional.empty();
    }

    public ColumnId getColumnId() {
        return columnId;
    }

    public TableName getTableName() {
        return tableName;
    }

    public EnumSet<UseCase> getUseCases() {
        return useCase;
    }

    public String getUseCaseString() {
        if (useCase.size() > 1 && useCase.contains(UseCase.NORMAL)) {
            return useCase.stream().filter(x -> x != UseCase.NORMAL).map(UseCase::toString)
                    .collect(Collectors.joining(","));
        }
        return useCase.stream().map(UseCase::toString).collect(Collectors.joining(","));
    }

    public static EnumSet<UseCase> fromUseCaseString(String str) {
        return EnumSet.copyOf(Splitter.on(",").splitToList(str)
                .stream()
                .map(UseCase::valueOf)
                .collect(Collectors.toList()));
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
    public void useNow(UseCase useCase) {
        this.lastUsed = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
        this.useCase.add(useCase);
    }

    public boolean needPersist(LocalDateTime lastPersist) {
        return this.lastUsed.isAfter(lastPersist);
    }

    public ColumnUsage merge(ColumnUsage other) {
        Preconditions.checkArgument(other.equals(this));
        ColumnUsage merged = new ColumnUsage(this.columnId, this.dbId, this.tableId, this.tableName,
                EnumSet.copyOf(this.useCase));
        merged.useCase.addAll(other.useCase);
        merged.lastUsed = this.lastUsed.isBefore(other.getLastUsed()) ? other.getLastUsed() : lastUsed;
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
        ColumnUsage that = (ColumnUsage) o;
        return Objects.equals(columnId, that.columnId) && Objects.equals(tableId, that.tableId) &&
                Objects.equals(dbId, that.dbId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnId, dbId, tableId);
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        return (GsonUtils.GSON.toJson(this));
    }

    public enum UseCase {
        NORMAL,
        PREDICATE,
        JOIN,
        GROUP_BY,
        DISTINCT;

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }

        public static EnumSet<UseCase> all() {
            return EnumSet.allOf(UseCase.class);
        }

        public static EnumSet<UseCase> getPredicateColumnUseCase() {
            return EnumSet.of(PREDICATE, JOIN, GROUP_BY, DISTINCT);
        }
    }
}
