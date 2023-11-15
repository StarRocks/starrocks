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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.thrift.TPaimonTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;


public class PaimonTable extends Table {
    private static final Logger LOG = LogManager.getLogger(PaimonTable.class);
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final Options paimonOptions;
    private final AbstractFileStoreTable paimonNativeTable;
    private final List<String> partColumnNames;
    private final List<String> paimonFieldNames;

    public PaimonTable(String catalogName, String dbName, String tblName, List<Column> schema,
<<<<<<< HEAD
                       String catalogType, String metastoreUris, String warehousePath,
                       org.apache.paimon.table.Table paimonNativeTable) {
=======
                       Options paimonOptions, org.apache.paimon.table.Table paimonNativeTable, long createTime) {
>>>>>>> e95749dc9d ([BugFix] fix the issue of reading paimon table on OSS (#34996))
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.PAIMON, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.paimonOptions = paimonOptions;
        this.paimonNativeTable = (AbstractFileStoreTable) paimonNativeTable;
        this.partColumnNames = paimonNativeTable.partitionKeys();
        this.paimonFieldNames = paimonNativeTable.rowType().getFields().stream()
                .map(DataField::name)
                .collect(Collectors.toList());
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public AbstractFileStoreTable getNativeTable() {
        return paimonNativeTable;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, databaseName, tableName);
    }

    @Override
    public String getTableLocation() {
        return paimonNativeTable.location().toString();
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColumnNames.isEmpty()) {
            partitionColumns = partColumnNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    public List<String> getFieldNames() {
        return paimonFieldNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TPaimonTable tPaimonTable = new TPaimonTable();
        StringBuilder sb = new StringBuilder();
        for (String key : this.paimonOptions.keySet()) {
            sb.append(key).append("=").append(this.paimonOptions.get(key)).append(",");
        }
        String option = sb.substring(0, sb.length() - 1);

        tPaimonTable.setPaimon_options(option);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.PAIMON_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setPaimonTable(tPaimonTable);
        return tTableDescriptor;
    }
}
