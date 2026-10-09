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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A read-only virtual table used to distribute connector index evaluation to BEs. */
public final class IndexTable extends Table {
    public static final String INDEX_TABLE_SUFFIX = "$global_index";
    public static final String INDEX_RESULT_COLUMN_NAME = "index_result";
    public static final String ARGS_COLUMN_NAME = "args";

    private static final Column INDEX_RESULT_COLUMN =
            new Column(INDEX_RESULT_COLUMN_NAME, VarbinaryType.VARBINARY, true);
    private static final Column ARGS_COLUMN = new Column(ARGS_COLUMN_NAME, VarcharType.VARCHAR, true);
    private static final List<Column> SCHEMA = List.of(INDEX_RESULT_COLUMN, ARGS_COLUMN);

    private final Table innerTable;

    public IndexTable(Table innerTable) {
        super(TableType.INDEX);
        this.innerTable = Objects.requireNonNull(innerTable, "inner table is null");
        setId(innerTable.getId());
    }

    public Table getInnerTable() {
        return innerTable;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public List<Column> getBaseSchema() {
        return SCHEMA;
    }

    @Override
    public List<Column> getFullSchema() {
        return SCHEMA;
    }

    @Override
    public Map<ColumnId, Column> getIdToColumn() {
        return Map.of(INDEX_RESULT_COLUMN.getColumnId(), INDEX_RESULT_COLUMN,
                ARGS_COLUMN.getColumnId(), ARGS_COLUMN);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        // Keep the connector-specific descriptor payload (Paimon table path, schema and time zone)
        // while exposing the virtual index table's tuple shape. The scan-range discriminator tells
        // BE to instantiate the global-index scanner, so a new wire-level table type is unnecessary.
        TTableDescriptor descriptor = innerTable.toThrift(partitions);
        descriptor.setId(getId());
        descriptor.setNumCols(SCHEMA.size());
        descriptor.setTableName(getName());
        return descriptor;
    }

    @Override
    public String getName() {
        return innerTable.getName() + INDEX_TABLE_SUFFIX;
    }

    @Override
    public String getCatalogName() {
        return innerTable.getCatalogName();
    }
}
