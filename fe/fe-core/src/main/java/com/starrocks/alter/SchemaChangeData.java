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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

class SchemaChangeData {
    private final Database database;
    private final OlapTable table;
    private final long timeoutInSeconds;
    private final Map<Long, List<Column>> newIndexSchema;
    private final List<Index> indexes;
    private final boolean bloomFilterColumnsChanged;
    private final Set<String> bloomFilterColumns;
    private final double bloomFilterFpp;
    private final boolean hasIndexChanged;
    private final Map<Long, Short> newIndexShortKeyCount;
    private final List<Integer> sortKeyIdxes;
    private final List<Integer> sortKeyUniqueIds;

    static Builder newBuilder() {
        return new Builder();
    }

    @NotNull
    Database getDatabase() {
        return database;
    }

    @NotNull
    OlapTable getTable() {
        return table;
    }

    long getTimeoutInSeconds() {
        return timeoutInSeconds;
    }

    @NotNull
    Map<Long, List<Column>> getNewIndexSchema() {
        return Collections.unmodifiableMap(newIndexSchema);
    }

    @Nullable
    List<Index> getIndexes() {
        return indexes;
    }

    boolean isBloomFilterColumnsChanged() {
        return bloomFilterColumnsChanged;
    }

    @Nullable
    Set<String> getBloomFilterColumns() {
        return bloomFilterColumns;
    }

    double getBloomFilterFpp() {
        return bloomFilterFpp;
    }

    boolean isHasIndexChanged() {
        return hasIndexChanged;
    }


    @NotNull
    Map<Long, Short> getNewIndexShortKeyCount() {
        return Collections.unmodifiableMap(newIndexShortKeyCount);
    }

    @Nullable
    List<Integer> getSortKeyIdxes() {
        return sortKeyIdxes;
    }

    @Nullable
    List<Integer> getSortKeyUniqueIds() {
        return sortKeyUniqueIds;
    }

    private SchemaChangeData(Builder builder) {
        this.database = Objects.requireNonNull(builder.database, "database is null");
        this.table = Objects.requireNonNull(builder.table, "table is null");
        this.timeoutInSeconds = builder.timeoutInSeconds;
        this.newIndexSchema = Objects.requireNonNull(builder.newIndexSchema, "newIndexSchema is null");
        this.indexes = builder.indexes;
        this.bloomFilterColumnsChanged = builder.bloomFilterColumnsChanged;
        this.bloomFilterColumns = builder.bloomFilterColumns;
        this.bloomFilterFpp = builder.bloomFilterFpp;
        this.hasIndexChanged = builder.hasIndexChanged;
        this.newIndexShortKeyCount = Objects.requireNonNull(builder.newIndexShortKeyCount, "newIndexShortKeyCount is null");
        this.sortKeyIdxes = builder.sortKeyIdxes;
        this.sortKeyUniqueIds = builder.sortKeyUniqueIds;
    }

    static class Builder {
        private Database database;
        private OlapTable table;
        private long timeoutInSeconds = Config.alter_table_timeout_second;
        private Map<Long, List<Column>> newIndexSchema = new HashMap<>();
        private List<Index> indexes;
        private boolean bloomFilterColumnsChanged = false;
        private Set<String> bloomFilterColumns;
        private double bloomFilterFpp;
        private boolean hasIndexChanged = false;
        private Map<Long, Short> newIndexShortKeyCount = new HashMap<>();
        private List<Integer> sortKeyIdxes;
        private List<Integer> sortKeyUniqueIds;

        private Builder() {
        }

        Builder withDatabase(@NotNull Database database) {
            this.database = Objects.requireNonNull(database, "database is null");
            return this;
        }

        Builder withTable(@NotNull OlapTable table) {
            this.table = Objects.requireNonNull(table, "table is null");
            return this;
        }

        Builder withTimeoutInSeconds(long timeoutInSeconds) {
            this.timeoutInSeconds = timeoutInSeconds;
            return this;
        }

        Builder withBloomFilterColumnsChanged(boolean changed) {
            this.bloomFilterColumnsChanged = changed;
            return this;
        }

        Builder withBloomFilterColumns(@Nullable Set<String> bfColumns, double bfFpp) {
            this.bloomFilterColumns = bfColumns;
            this.bloomFilterFpp = bfFpp;
            return this;
        }

        Builder withAlterIndexInfo(boolean hasIndexChanged, @NotNull List<Index> indexes) {
            this.hasIndexChanged = hasIndexChanged;
            this.indexes = indexes;
            return this;
        }

        Builder withNewIndexShortKeyCount(long indexId, short shortKeyCount) {
            this.newIndexShortKeyCount.put(indexId, shortKeyCount);
            return this;
        }

        Builder withNewIndexSchema(long indexId, @NotNull List<Column> indexSchema) {
            newIndexSchema.put(indexId, indexSchema);
            return this;
        }

        Builder withSortKeyIdxes(@Nullable List<Integer> sortKeyIdxes) {
            this.sortKeyIdxes = sortKeyIdxes;
            return this;
        }

        Builder withSortKeyUniqueIds(@Nullable List<Integer> sortKeyUniqueIds) {
            this.sortKeyUniqueIds = sortKeyUniqueIds;
            return this;
        }

        @NotNull
        SchemaChangeData build() {
            return new SchemaChangeData(this);
        }
    }
}
