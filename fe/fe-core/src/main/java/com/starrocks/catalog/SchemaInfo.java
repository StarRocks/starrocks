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
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.proto.KeysTypePB;
import com.starrocks.persist.proto.PrimaryKeyEncodingTypePB;
import com.starrocks.persist.proto.SchemaInfoPB;
import com.starrocks.persist.proto.StorageTypePB;
import com.starrocks.persist.proto.ZstdCompressionPageSizesPB;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.thrift.TPrimaryKeyEncodingType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
    @SerializedName("zstdCompressionColumns")
    private final Set<ColumnId> zstdCompressionColumnNames;
    @SerializedName("zstdCompressionPageSizes")
    private final Map<ColumnId, Integer> zstdCompressionPageSizes; // columns using a compression dictionary (a ZSTD dictionary)
    @SerializedName("compressionType")
    private final TCompressionType compressionType;
    @SerializedName("compressionLevel")
    private final int compressionLevel;
    @SerializedName("primaryKeyEncodingType")
    private final TPrimaryKeyEncodingType primaryKeyEncodingType;

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
        this.zstdCompressionColumnNames = builder.zstdCompressionColumnNames;
        this.zstdCompressionPageSizes = builder.zstdCompressionPageSizes;
        this.schemaHash = builder.schemaHash;
        this.compressionType = builder.compressionType;
        this.compressionLevel = builder.compressionLevel;
        this.primaryKeyEncodingType = builder.primaryKeyEncodingType;
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

    public Set<ColumnId> getZstdCompressionColumnNames() {
        return zstdCompressionColumnNames;
    }

    public Map<ColumnId, Integer> getZstdCompressionPageSizes() {
        return zstdCompressionPageSizes;
    }

    public TCompressionType getCompressionType() {
        return compressionType;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public TTabletSchema toTabletSchema() {
        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShort_key_column_count(shortKeyColumnCount);
        tSchema.setKeys_type(ExprToThrift.keysTypeToThrift(keysType));
        tSchema.setStorage_type(storageType);
        tSchema.setId(id);
        tSchema.setSchema_version(version);
        tSchema.setSchema_hash(schemaHash);

        List<TColumn> tColumns = new ArrayList<TColumn>();
        for (Column column : columns) {
            TColumn tColumn = column.toThrift();
            // is bloom filter column
            if (bloomFilterColumnNames != null && bloomFilterColumnNames.contains(column.getColumnId())) {
                tColumn.setIs_bloom_filter_column(true);
            }
            // use column-level compression dictionary (a ZSTD dictionary)
            if (zstdCompressionColumnNames != null && zstdCompressionColumnNames.contains(column.getColumnId())) {
                tColumn.setUse_zstd_compression(true);
                Integer pageSize = zstdCompressionPageSizes == null
                        ? null : zstdCompressionPageSizes.get(column.getColumnId());
                if (pageSize != null && pageSize > 0) {
                    tColumn.setZstd_compression_page_size(pageSize);
                }
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
        if (compressionType != null) {
            tSchema.setCompression_type(compressionType);
            tSchema.setCompression_level(compressionLevel);
        }
        if (primaryKeyEncodingType != null) {
            tSchema.setPrimary_key_encoding_type(primaryKeyEncodingType);
        }
        return tSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaInfo that = (SchemaInfo) o;
        return id == that.id && shortKeyColumnCount == that.shortKeyColumnCount && version == that.version &&
                schemaHash == that.schemaHash && Double.compare(bloomFilterFpp, that.bloomFilterFpp) == 0 &&
                compressionLevel == that.compressionLevel && keysType == that.keysType &&
                storageType == that.storageType &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(sortKeyIndexes, that.sortKeyIndexes) &&
                Objects.equals(sortKeyUniqueIds, that.sortKeyUniqueIds) &&
                Objects.equals(indexes, that.indexes) &&
                Objects.equals(bloomFilterColumnNames, that.bloomFilterColumnNames) &&
                Objects.equals(zstdCompressionColumnNames, that.zstdCompressionColumnNames) &&
                compressionType == that.compressionType &&
                primaryKeyEncodingType == that.primaryKeyEncodingType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, shortKeyColumnCount, keysType, storageType, version, schemaHash, columns, sortKeyIndexes,
                sortKeyUniqueIds, indexes, bloomFilterColumnNames, bloomFilterFpp, zstdCompressionColumnNames, compressionType,
                compressionLevel, primaryKeyEncodingType);
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
        private Set<ColumnId> zstdCompressionColumnNames; // compression dict
        private TCompressionType compressionType;
        private int compressionLevel = -1;
        private TPrimaryKeyEncodingType primaryKeyEncodingType;

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
            Objects.requireNonNull(columns, "column list is null");
            if (this.columns == null) {
                this.columns = new ArrayList<>();
            }
            this.columns.addAll(columns);
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

        private Map<ColumnId, Integer> zstdCompressionPageSizes;

        // The column set and the per-column page sizes are set together on purpose.
        // They are two halves of one property, and a caller that supplies only the
        // names produces a schema the BE reads as "use the default page size" while
        // SHOW CREATE TABLE still reports the size the user asked for.
        public Builder setZstdCompressionColumns(Collection<ColumnId> zstdCompressionColumnNames,
                                                 Map<ColumnId, Integer> zstdCompressionPageSizes) {
            // Copied, like the name set below: the caller hands over OlapTable's live map, and
            // rebuildFullSchema() prunes that map IN PLACE when a column is dropped. A SchemaInfo
            // that is kept -- the historical schema a fast schema evolution snapshots for the
            // transactions still writing the old schema -- would otherwise lose the page size it
            // was taken to remember, and those writes would fall back to the default.
            this.zstdCompressionPageSizes =
                    zstdCompressionPageSizes == null ? null : Maps.newHashMap(zstdCompressionPageSizes);
            return setZstdCompressionColumnNames(zstdCompressionColumnNames);
        }

        private Builder setZstdCompressionColumnNames(Collection<ColumnId> zstdCompressionColumnNames) {
            Preconditions.checkState(this.zstdCompressionColumnNames == null);
            if (zstdCompressionColumnNames != null) {
                this.zstdCompressionColumnNames = new HashSet<>(zstdCompressionColumnNames);
            }
            return this;
        }

        public Builder setSchemaHash(int schemaHash) {
            this.schemaHash = schemaHash;
            return this;
        }

        public Builder setCompressionLevel(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            return this;
        }

        public Builder setCompressionType(TCompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder setPrimaryKeyEncodingType(TPrimaryKeyEncodingType primaryKeyEncodingType) {
            this.primaryKeyEncodingType = primaryKeyEncodingType;
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

    public static SchemaInfo fromMaterializedIndex(OlapTable table, long indexMetaId, MaterializedIndexMeta indexMeta) {
        List<Index> indexes = table.getBaseIndexMetaId() == indexMetaId ? table.getCopiedIndexes() :
                OlapTable.getIndexesBySchema(table.getCopiedIndexes(), indexMeta.getSchema());
        return SchemaInfo.newBuilder()
                .setId(indexMeta.getSchemaId())
                .setVersion(indexMeta.getSchemaVersion())
                .setSchemaHash(indexMeta.getSchemaHash())
                .setKeysType(indexMeta.getKeysType())
                .setShortKeyColumnCount(indexMeta.getShortKeyColumnCount())
                .setStorageType(table.getStorageType())
                .addColumns(indexMeta.getSchema())
                .setSortKeyIndexes(indexMeta.getSortKeyIdxes())
                .setSortKeyUniqueIds(indexMeta.getSortKeyUniqueIds())
                .setIndexes(indexes)
                .setBloomFilterColumnNames(table.getBfColumnIds())
                .setBloomFilterFpp(table.getBfFpp())
                .setZstdCompressionColumns(table.getZstdCompressionColumnIds(),
                        table.getZstdCompressionPageSizes())
                .setCompressionType(table.getCompressionType())
                .setCompressionLevel(table.getCompressionLevel())
                .setPrimaryKeyEncodingType(table.getPrimaryKeyEncodingType())
                .build();
    }

    /** Image v3 form. */
    public SchemaInfoPB toProto() {
        SchemaInfoPB.Builder builder = SchemaInfoPB.newBuilder()
                .setId(id)
                .setShortKeyColumnCount(shortKeyColumnCount)
                .setVersion(version)
                .setSchemaHash(schemaHash)
                .setBloomFilterFpp(bloomFilterFpp)
                .setCompressionLevel(compressionLevel);
        if (keysType != null) {
            builder.setKeysType(ProtoEnums.toProto(keysType, KeysTypePB.class, "KEYS_TYPE_"));
        }
        if (storageType != null) {
            builder.setStorageType(ProtoEnums.toProto(storageType, StorageTypePB.class, "STORAGE_TYPE_"));
        }
        if (columns != null) {
            builder.setColumns(Column.toListProto(columns));
        }
        if (sortKeyIndexes != null) {
            builder.setSortKeyIndexes(ProtoLists.int32List(sortKeyIndexes));
        }
        if (sortKeyUniqueIds != null) {
            builder.setSortKeyUniqueIds(ProtoLists.int32List(sortKeyUniqueIds));
        }
        if (indexes != null) {
            builder.setIndexes(Index.toListProto(indexes));
        }
        if (bloomFilterColumnNames != null) {
            builder.setBloomFilterColumnNames(ProtoLists.stringList(
                    bloomFilterColumnNames.stream().map(ColumnId::getId).collect(Collectors.toList())));
        }
        if (compressionType != null) {
            builder.setCompressionType(ProtoEnums.toProto(compressionType, SchemaInfoPB.CompressionType.class));
        }
        if (primaryKeyEncodingType != null) {
            builder.setPrimaryKeyEncodingType(ProtoEnums.toProto(primaryKeyEncodingType, PrimaryKeyEncodingTypePB.class));
        }
        if (zstdCompressionColumnNames != null) {
            builder.setZstdCompressionColumns(ProtoLists.stringList(
                    zstdCompressionColumnNames.stream().map(ColumnId::getId).collect(Collectors.toList())));
        }
        if (zstdCompressionPageSizes != null) {
            ZstdCompressionPageSizesPB.Builder sizes = ZstdCompressionPageSizesPB.newBuilder();
            zstdCompressionPageSizes.forEach((columnId, size) -> sizes.putValues(columnId.getId(), size));
            builder.setZstdCompressionPageSizes(sizes);
        }
        return builder.build();
    }

    public static SchemaInfo fromProto(SchemaInfoPB pb) {
        return new SchemaInfo(pb);
    }

    /** The fields are final and GSON sets them without the builder's checks, so the image load does the same. */
    private SchemaInfo(SchemaInfoPB pb) {
        this.id = pb.getId();
        this.shortKeyColumnCount = (short) pb.getShortKeyColumnCount();
        this.keysType = pb.hasKeysType() ? ProtoEnums.fromProto(pb.getKeysType(), KeysType.class, "KEYS_TYPE_") : null;
        this.storageType = pb.hasStorageType()
                ? ProtoEnums.fromProto(pb.getStorageType(), TStorageType.class, "STORAGE_TYPE_") : null;
        this.version = pb.getVersion();
        this.schemaHash = pb.getSchemaHash();
        this.columns = pb.hasColumns() ? Column.fromListProto(pb.getColumns()) : null;
        this.sortKeyIndexes = pb.hasSortKeyIndexes() ? ProtoLists.toIntegers(pb.getSortKeyIndexes()) : null;
        this.sortKeyUniqueIds = pb.hasSortKeyUniqueIds() ? ProtoLists.toIntegers(pb.getSortKeyUniqueIds()) : null;
        this.indexes = pb.hasIndexes() ? Index.fromListProto(pb.getIndexes()) : null;
        Set<ColumnId> bfColumns = null;
        if (pb.hasBloomFilterColumnNames()) {
            // An insertion-ordered set, as GSON delivers it.
            bfColumns = new LinkedHashSet<>();
            for (String name : pb.getBloomFilterColumnNames().getValuesList()) {
                bfColumns.add(ColumnId.create(name));
            }
        }
        this.bloomFilterColumnNames = bfColumns;
        this.zstdCompressionColumnNames = pb.hasZstdCompressionColumns()
                ? pb.getZstdCompressionColumns().getValuesList().stream().map(ColumnId::create)
                        .collect(Collectors.toCollection(LinkedHashSet::new)) : null;
        this.zstdCompressionPageSizes = pb.hasZstdCompressionPageSizes()
                ? pb.getZstdCompressionPageSizes().getValuesMap().entrySet().stream()
                        .collect(Collectors.toMap(e -> ColumnId.create(e.getKey()), Map.Entry::getValue)) : null;
        this.bloomFilterFpp = pb.getBloomFilterFpp();
        this.compressionType = pb.hasCompressionType()
                ? ProtoEnums.fromProto(pb.getCompressionType(), TCompressionType.class) : null;
        this.compressionLevel = pb.getCompressionLevel();
        this.primaryKeyEncodingType = pb.hasPrimaryKeyEncodingType()
                ? ProtoEnums.fromProto(pb.getPrimaryKeyEncodingType(), TPrimaryKeyEncodingType.class) : null;
    }
}
