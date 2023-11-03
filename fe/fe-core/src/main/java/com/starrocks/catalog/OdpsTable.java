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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.io.Text;
import com.starrocks.connector.odps.EntityConvertUtils;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class OdpsTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(OdpsTable.class);

    private static final String TABLE = "table";
    private static final String PROJECT = "project";
    public static final String PARTITION_NULL_VALUE = "null";

    @SerializedName(value = "tn")
    private String tableName;

    private Map<String, String> properties;
    private String catalogName;
    @SerializedName(value = "dn")
    private String dbName;
    private List<Column> dataColumns;
    private List<Column> partitionColumns;
    private com.aliyun.odps.Table odpsTable;

    public OdpsTable() {
        super(TableType.ODPS);
    }

    public OdpsTable(String catalogName, com.aliyun.odps.Table odpsTable) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), odpsTable.getName(), TableType.ODPS,
                odpsTable.getSchema().getColumns().stream().map(EntityConvertUtils::convertColumn).collect(
                        Collectors.toList()));
        this.odpsTable = odpsTable;
        this.catalogName = catalogName;
        this.dbName = odpsTable.getProject();
        this.tableName = odpsTable.getName();
        this.partitionColumns =
                odpsTable.getSchema().getPartitionColumns().stream().map(EntityConvertUtils::convertColumn).collect(
                        Collectors.toList());
        this.dataColumns = fullSchema;
        this.fullSchema = new ArrayList<>(dataColumns);
        this.fullSchema.addAll(partitionColumns);
    }

    public String getProjectName() {
        return dbName;
    }

    @Override
    public String getResourceName() {
        return tableName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<String> getDataColumnNames() {
        return dataColumns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
    }

    @Override
    public boolean isUnPartitioned() {
        return partitionColumns.isEmpty();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    public String getUUID() {
        if (!Strings.isNullOrEmpty(catalogName)) {
            return String.join(".", catalogName, dbName, name);
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ODPS_TABLE,
                fullSchema.size(), 0, getName(), getProjectName());
        THdfsTable hdfsTable = new THdfsTable();
        hdfsTable.setColumns(getColumns().stream().map(Column::toThrift).collect(Collectors.toList()));
        hdfsTable.setPartition_columns(
                getPartitionColumns().stream().map(Column::toThrift).collect(Collectors.toList()));
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject obj = new JsonObject();
        obj.addProperty(PROJECT, dbName);
        obj.addProperty(TABLE, tableName);
        Text.writeString(out, obj.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String jsonStr = Text.readString(in);
        JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
        tableName = obj.getAsJsonPrimitive(TABLE).getAsString();
        dbName = obj.getAsJsonPrimitive(PROJECT).getAsString();
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
