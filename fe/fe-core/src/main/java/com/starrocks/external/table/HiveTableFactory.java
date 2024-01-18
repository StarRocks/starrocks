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

package com.starrocks.external.table;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.exception.DdlException;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateTableStmt;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.catalog.Resource.ResourceType.HIVE;

public class HiveTableFactory extends ExternalTableFactory {
    public static final HiveTableFactory INSTANCE = new HiveTableFactory();

    private HiveTableFactory() {

    }

    public static void copyFromCatalogTable(HiveTable.Builder tableBuilder, HiveTable catalogTable,
                                            Map<String, String> properties) {
        tableBuilder
                .setCatalogName(catalogTable.getCatalogName())
                .setResourceName(properties.get(RESOURCE))
                .setHiveDbName(catalogTable.getDbName())
                .setHiveTableName(catalogTable.getTableName())
                .setPartitionColumnNames(catalogTable.getPartitionColumnNames())
                .setDataColumnNames(catalogTable.getDataColumnNames())
                .setTableLocation(catalogTable.getTableLocation())
                .setStorageFormat(catalogTable.getStorageFormat())
                .setCreateTime(catalogTable.getCreateTime())
                .setProperties(catalogTable.getProperties());
    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database database, CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = gsm.getNextId();
        Table table = getTableFromResourceMappingCatalog(properties, Table.TableType.HIVE, HIVE);
        if (table == null) {
            throw new DdlException("Can not find hive table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }
        HiveTable oHiveTable = (HiveTable) table;

        validateHiveColumnType(columns, oHiveTable);

        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setFullSchema(columns);
        copyFromCatalogTable(tableBuilder, oHiveTable, properties);

        HiveTable hiveTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && hiveTable.getPartitionColumnNames().size() > 0) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", hiveTable.getPartitionColumnNames()) + ")";
            hiveTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            hiveTable.setComment(stmt.getComment());
        }
        return hiveTable;
    }

    private static void validateHiveColumnType(List<Column> columns, Table oTable) throws DdlException {
        for (Column column : columns) {
            Column oColumn = oTable.getColumn(column.getName());
            if (oColumn == null) {
                throw new DdlException("column [" + column.getName() + "] not exists in hive");
            }

            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType()) &&
                    !FeConstants.runningUnitTest) {
                throw new DdlException("can not convert hive external table column type [" + column.getType() + "] " +
                        "to correct type [" + oColumn.getType() + "]");
            }

            for (String partName : oTable.getPartitionColumnNames()) {
                if (!columns.stream().map(Column::getName).collect(Collectors.toList()).contains(partName)) {
                    throw new DdlException("partition column [" + partName + "] must exist in column list");
                }
            }
        }
    }
}
