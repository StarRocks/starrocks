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

import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;


public class KuduTable extends Table {
    private static final Logger LOG = LogManager.getLogger(KuduTable.class);
    public static final Set<String> KUDU_INPUT_FORMATS = Sets.newHashSet(
            "org.apache.hadoop.hive.kudu.KuduInputFormat", "org.apache.kudu.mapreduce.KuduTableInputFormat");
    public static final String PARTITION_NULL_VALUE = "null";
    private final String masterAddresses;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final List<String> partColNames;

    public KuduTable(String masterAddresses, String catalogName, String dbName, String tblName, List<Column> schema,
                     List<String> partColNames) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.KUDU, schema);
        this.masterAddresses = masterAddresses;
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.partColNames = partColNames;
    }

    public static KuduTable fromMetastoreTable(org.apache.hadoop.hive.metastore.api.Table table, String catalogName,
                                               List<Column> fullSchema, List<String> partColNames) {
        return new KuduTable(StringUtils.EMPTY, catalogName, table.getDbName(), table.getTableName(),
                fullSchema, partColNames);
    }

    public String getMasterAddresses() {
        return masterAddresses;
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

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColNames.isEmpty()) {
            partitionColumns = partColNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColNames;
    }

    @Override
    public boolean isPartitioned() {
        return !partColNames.isEmpty();
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public static boolean isKuduInputFormat(String inputFormat) {
        return KUDU_INPUT_FORMATS.contains(inputFormat);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(id, TTableType.KUDU_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
    }
}
