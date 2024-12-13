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

package com.starrocks.connector.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.thrift.THdfsTable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataTable extends Table {
    private final MetadataTableType metadataTableType;

    private final String catalogName;
    private final String originDb;
    private final String originTable;

    protected static final List<Column> PLACEHOLDER_COLUMNS = ImmutableList.<Column>builder()
            .add(new Column("predicate", ScalarType.STRING, true))
            .build();

    public MetadataTable(String catalogName, long id, String name, TableType type, List<Column> baseSchema,
                         String originDb, String originTable, MetadataTableType metadataTableType) {
        super(id, name, type, baseSchema);
        this.catalogName = catalogName;
        this.originDb = originDb;
        this.originTable = originTable;
        this.metadataTableType = metadataTableType;
    }

    public MetadataTableType getMetadataTableType() {
        return metadataTableType;
    }

    public String getOriginDb() {
        return originDb;
    }

    public String getOriginTable() {
        return originTable;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Do not allow to write SchemaTable to image.");
    }

    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Do not allow read SchemaTable from image.");
    }

    public List<Column> getPlaceHolderColumns() {
        return PLACEHOLDER_COLUMNS;
    }

    public boolean supportBuildPlan() {
        return false;
    }

    protected THdfsTable buildThriftTable(List<Column> columns) {
        THdfsTable hdfsTable = new THdfsTable();
        hdfsTable.setColumns(columns.stream().map(Column::toThrift).collect(Collectors.toList()));
        hdfsTable.setPartition_columnsIsSet(false);

        String columnNames = Joiner.on(',').join(columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
        hdfsTable.setHive_column_names(columnNames);

        String columnTypes = Joiner.on('#').join(columns.stream()
                .map(x -> ColumnTypeConverter.toHiveType(x.getType()))
                .collect(Collectors.toList()));
        hdfsTable.setHive_column_types(columnTypes);
        hdfsTable.setTime_zone(TimeUtils.getSessionTimeZone());
        return hdfsTable;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public static class Builder {
        List<Column> columns;

        public Builder() {
            this.columns = Lists.newArrayList();
        }

        public Builder column(String name, Type type) {
            return column(name, type, true);
        }

        public Builder column(String name, Type type, boolean nullable) {
            this.columns.add(new Column(name, type, false, null, nullable, null, ""));
            return this;
        }

        public Builder columns(List<Column> columns) {
            columns.forEach(c -> this.columns.add(new Column(c.getName(), c.getType(), true)));
            return this;
        }

        public List<Column> build() {
            return this.columns;
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
