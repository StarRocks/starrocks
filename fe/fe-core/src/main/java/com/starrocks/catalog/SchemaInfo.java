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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SchemaInfo {
    @SerializedName("id")
    private final long id;
    @SerializedName("shortKeyColumnCount")
    private final short shortKeyColumnCount;
    @SerializedName("keysType")
    private final KeysType keysType;
    @SerializedName("storageType")
    private final TStorageType storageType;
    @SerializedName("version")
    private final int version;
    @SerializedName("schemaHahs")
    private final int schemaHash;
    @SerializedName("columns")
    private final List<Column> columns;
    @SerializedName("sortKeyIndexes")
    private final List<Integer> sortKeyIndexes;
    @SerializedName("sortKeyUniqueIds")
    private final List<Integer> sortKeyUniqueIds;
    @SerializedName("indexes")
    private final List<Index> indexes;
    @SerializedName("bfColumns")
    private final Set<ColumnId> bloomFilterColumnNames;
    @SerializedName("bfColumnFpp")
    private final double bloomFilterFpp; // false positive probability

    private SchemaInfo(Builder builder) {
        this.id = builder.id;
        this.shortKeyColumnCount = builder.shortKeyColumnCount;
        this.keysType = builder.keysType;
        this.storageType = builder.storageType;
        this.version = builder.version;
        this.columns = builder.columns;
        this.sortKeyIndexes = builder.sortKeyIndexes;
        this.sortKeyUniqueIds = builder.sortKeyUniqueIds;
        this.indexes = builder.indexes;
        this.bloomFilterColumnNames = builder.bloomFilterColumnNames;
        this.bloomFilterFpp = builder.bloomFilterFpp;
        this.schemaHash = builder.schemaHash;
    }

    public long getId() {
        return id;
    }

    public short getShortKeyColumnCount() {
        return shortKeyColumnCount;
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public TStorageType getStorageType() {
        return storageType;
    }

    public int getVersion() {
        return version;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Integer> getSortKeyIndexes() {
        return sortKeyIndexes;
    }

    public List<Integer> getSortKeyUniqueIds() {
        return sortKeyUniqueIds;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public Set<ColumnId> getBloomFilterColumnNames() {
        return bloomFilterColumnNames;
    }

    public double getBloomFilterFpp() {
        return bloomFilterFpp;
    }

    public TTabletSchema toTabletSchema() {
        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShort_key_column_count(shortKeyColumnCount);
        tSchema.setKeys_type(keysType.toThrift());
        tSchema.setStorage_type(storageType);
        tSchema.setId(id);
        tSchema.setSchema_version(version);
        tSchema.setSchema_hash(schemaHash);
        tSchema.setIs_in_memory(false/*unused now*/);

        List<TColumn> tColumns = new ArrayList<TColumn>();
        for (Column column : columns) {
            TColumn tColumn = column.toThrift();
            // is bloom filter column
            if (bloomFilterColumnNames != null && bloomFilterColumnNames.contains(column.getColumnId())) {
                tColumn.setIs_bloom_filter_column(true);
            }
            tColumns.add(tColumn);
        }
        tSchema.setColumns(tColumns);
        tSchema.setSort_key_idxes(sortKeyIndexes);
        tSchema.setSort_key_unique_ids(sortKeyUniqueIds);

        if (CollectionUtils.isNotEmpty(indexes)) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : indexes) {
                tIndexes.add(index.toThrift());
            }
            tSchema.setIndexes(tIndexes);
        }

        if (bloomFilterColumnNames != null) {
            tSchema.setBloom_filter_fpp(bloomFilterFpp);
        }
        return tSchema;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private int version;
        private int schemaHash;
        private KeysType keysType;
        private short shortKeyColumnCount;
        private TStorageType storageType;
        private List<Column> columns;
        private List<Integer> sortKeyIndexes;
        private List<Integer> sortKeyUniqueIds;
        private List<Index> indexes;
        private Set<ColumnId> bloomFilterColumnNames;
        private double bloomFilterFpp; // false positive probability

        private Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setVersion(int version) {
            this.version = version;
            return this;
        }

        public Builder setKeysType(KeysType keysType) {
            this.keysType = keysType;
            return this;
        }

        public Builder setShortKeyColumnCount(short count) {
            this.shortKeyColumnCount = count;
            return this;
        }

        public Builder setStorageType(TStorageType storageType) {
            this.storageType = storageType;
            return this;
        }

        public Builder addColumn(Column column) {
            Objects.requireNonNull(column, "column is null");
            if (columns == null) {
                this.columns = new ArrayList<>();
            }
            columns.add(column);
            return this;
        }

        public Builder addColumns(List<Column> columns) {
            for (Column col : columns) {
                addColumn(col);
            }
            return this;
        }

        public Builder setSortKeyIndexes(List<Integer> sortKeyIndexes) {
            Preconditions.checkState(this.sortKeyIndexes == null);
            this.sortKeyIndexes = sortKeyIndexes;
            return this;
        }

        public Builder setSortKeyUniqueIds(List<Integer> sortKeyUniqueIds) {
            Preconditions.checkState(this.sortKeyUniqueIds == null);
            this.sortKeyUniqueIds = sortKeyUniqueIds;
            return this;
        }

        public Builder setIndexes(List<Index> indexes) {
            Preconditions.checkState(this.indexes == null);
            this.indexes = indexes;
            return this;
        }

        public Builder setBloomFilterColumnNames(Collection<ColumnId> bloomFilterColumnNames) {
            Preconditions.checkState(this.bloomFilterColumnNames == null);
            if (bloomFilterColumnNames != null) {
                this.bloomFilterColumnNames = new HashSet<>(bloomFilterColumnNames);
            }
            return this;
        }

        public Builder setBloomFilterFpp(double fpp) {
            this.bloomFilterFpp = fpp;
            return this;
        }

        public Builder setSchemaHash(int schemaHash) {
            this.schemaHash = schemaHash;
            return this;
        }

        public SchemaInfo build() {
            Preconditions.checkState(id > 0);
            Preconditions.checkState(keysType != null);
            Preconditions.checkState(shortKeyColumnCount > 0);
            Preconditions.checkState(columns != null);
            Preconditions.checkState(storageType != null);
            return new SchemaInfo(this);
        }
    }
}
