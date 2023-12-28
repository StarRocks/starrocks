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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.DdlException;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableFunctionTable;
import com.starrocks.thrift.TTableType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListFilesTableFunctionTable extends Table {

    public static final String PROPERTY_PATH = "path";

    private String path;
    private final Map<String, String> properties;

    public ListFilesTableFunctionTable(Map<String, String> properties) throws DdlException {
        super(TableType.TABLE_FUNCTION);
        super.setId(-1);
        super.setName("list_files_table_function_table");
        this.properties = properties;

        parseProperties();
        initColumns();
    }

    private void initColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("name", ScalarType.createVarcharType(512), true));
        columns.add(new Column("size", ScalarType.createType(PrimitiveType.BIGINT), true));
        columns.add(new Column("fingerprint", ScalarType.createVarcharType(512), true));
        columns.add(new Column("last_modified", ScalarType.createVarcharType(128), true));
        setNewFullSchema(columns);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableFunctionTable tTbl = new TTableFunctionTable();
        tTbl.setPath(path);

        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getBaseSchema()) {
            tColumns.add(column.toThrift());
        }
        tTbl.setColumns(tColumns);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.TABLE_FUNCTION_TABLE, fullSchema.size(),
                0, "_table_function_table", "_table_function_db");
        tTableDescriptor.setTableFunctionTable(tTbl);
        return tTableDescriptor;
    }

    public String getPath() {
        return path;
    }

    private void parseProperties() throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of table function");
        }

        path = properties.get(PROPERTY_PATH);
        if (Strings.isNullOrEmpty(path)) {
            throw new DdlException("path is null. Please add properties(path='xxx') when create table");
        }
    }

    @Override
    public String toString() {
        return String.format("LIST_FILES('path'='%s')", path);
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
