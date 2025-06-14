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
import com.google.common.base.Splitter;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import org.apache.commons.lang3.EnumUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnUsage implements GsonPostProcessable {

    @SerializedName("columnId")
    private ColumnFullId columnId;

    // only exists in memory
    private TableName tableName;

    @SerializedName("lastUsed")
    private LocalDateTime lastUsed;

    @SerializedName("useCase")
    private EnumSet<UseCase> useCase;

    @SerializedName("created")
    private LocalDateTime created;

    public ColumnUsage(ColumnFullId columnId, TableName tableName, UseCase useCase) {
        this(columnId, tableName, EnumSet.of(useCase));
    }

    public ColumnUsage(ColumnFullId columnId, TableName tableName, EnumSet<UseCase> useCase) {
        this.columnId = columnId;
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
            ColumnFullId columnFullId = ColumnFullId.create(db.get(), table, column);
            return Optional.of(new ColumnUsage(columnFullId, tableName, useCase));
        }
        return Optional.empty();
    }

    public ColumnFullId getColumnFullId() {
        return columnId;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getOlapColumnName(OlapTable olap) {
        return Preconditions.checkNotNull(olap.getColumnByUniqueId(columnId.getColumnUniqueId()),
                this + " not exists").getName();
    }

    public EnumSet<UseCase> getUseCases() {
        return useCase;
    }

    public String getUseCaseString() {
        return useCase.stream().map(UseCase::toString).collect(Collectors.joining(","));
    }

    public static EnumSet<UseCase> fromUseCaseString(String str) {
        return EnumSet.copyOf(Splitter.on(",").splitToList(str)
                .stream()
                .map(x -> EnumUtils.getEnumIgnoreCase(UseCase.class, x))
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
        ColumnUsage merged = new ColumnUsage(this.columnId, this.tableName, EnumSet.copyOf(this.useCase));
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
        return Objects.equals(columnId, that.columnId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnId);
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        return (GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        Optional<Pair<TableName, ColumnId>> names = getColumnFullId().toNames();
        if (names.isPresent()) {
            setTableName(names.get().first);
        }
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

        public static EnumSet<UseCase> empty() {
            return EnumSet.noneOf(UseCase.class);
        }

        public static EnumSet<UseCase> getPredicateColumnUseCase() {
            return EnumSet.of(PREDICATE, JOIN, GROUP_BY, DISTINCT);
        }
    }
}
