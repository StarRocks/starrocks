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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class BigQueryTable extends Table {
    private static final Logger LOG = LogManager.getLogger(BigQueryTable.class);

    @SerializedName(value = "tn")
    private String tableName;
    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "cn")
    private String catalogName;
    /**
     * True when this table represents a BigQuery VIEW (not a base table or materialized view).
     * Views require a query-job materialisation step before the Storage Read API can be used.
     */
    @SerializedName(value = "view")
    private boolean isView;

    public BigQueryTable() {
        super(TableType.BIGQUERY);
    }

    public BigQueryTable(String catalogName, String dbName, String tableName,
                         List<Column> fullSchema, long createTime, boolean isView) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asLong(), tableName, TableType.BIGQUERY, fullSchema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.createTime = createTime;
        this.isView = isView;
    }

    @Override
    public String getResourceName() {
        return tableName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    @Override
    public List<String> getDataColumnNames() {
        return fullSchema.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Override
    public List<Column> getPartitionColumns() {
        // BigQuery does not expose partition columns via the Storage Read API metadata.
        return Collections.emptyList();
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return Collections.emptyList();
    }

    @Override
    public boolean isUnPartitioned() {
        return true;
    }

    public boolean isView() {
        return isView;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, dbName, name, Long.toString(createTime));
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.BIGQUERY_TABLE,
                fullSchema.size(), 0, getName(), getCatalogDBName());
        THdfsTable hdfsTable = new THdfsTable();
        hdfsTable.setColumns(getColumns().stream().map(Column::toThrift).collect(Collectors.toList()));
        hdfsTable.setPartition_columnsIsSet(false);
        hdfsTable.setTime_zone(TimeUtils.getSessionTimeZone());
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
