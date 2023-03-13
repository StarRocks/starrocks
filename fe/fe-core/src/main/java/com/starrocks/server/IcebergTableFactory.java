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

package com.starrocks.server;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.sql.ast.CreateTableStmt;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.catalog.Resource.ResourceType.ICEBERG;

public class IcebergTableFactory extends ExternalTableFactory {
    public static final IcebergTableFactory INSTANCE = new IcebergTableFactory();

    private IcebergTableFactory() {

    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database database, CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = gsm.getNextId();

        IcebergTable oIcebergTable = (IcebergTable) getTableFromResourceMappingCatalog(
                properties, Table.TableType.ICEBERG, ICEBERG);

        if (oIcebergTable == null) {
            throw new DdlException("Can not find iceberg table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }

        validateIcebergColumnType(columns, oIcebergTable);

        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(tableId)
                .setSrTableName(tableName)
                .setCatalogName(oIcebergTable.getCatalogName())
                .setResourceName(oIcebergTable.getResourceName())
                .setRemoteDbName(oIcebergTable.getRemoteDbName())
                .setRemoteTableName(oIcebergTable.getRemoteTableName())
                .setFullSchema(columns)
                .setIcebergProperties(properties)
                .setNativeTable(oIcebergTable.getNativeTable());

        IcebergTable icebergTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && !icebergTable.isUnPartitioned()) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", icebergTable.getPartitionColumnNames()) + ")";
            icebergTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            icebergTable.setComment(stmt.getComment());
        }

        return icebergTable;
    }

    private static void validateIcebergColumnType(List<Column> columns, IcebergTable oTable) throws DdlException {
        for (Column column : columns) {
            Map<String, Types.NestedField> icebergColumns = oTable.getNativeTable().schema().columns().stream()
                    .collect(Collectors.toMap(Types.NestedField::name, field -> field));
            if (!icebergColumns.containsKey(column.getName())) {
                throw new DdlException("column [" + column.getName() + "] not exists in iceberg");
            }

            Column oColumn = oTable.getColumn(column.getName());
            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType()) &&
                    !FeConstants.runningUnitTest) {
                throw new DdlException("can not convert iceberg external table column type [" + column.getType() + "] " +
                        "to correct type [" + oColumn.getType() + "]");
            }

            if (!column.isAllowNull()) {
                throw new DdlException("iceberg extern table not support no-nullable column: [" + column.getName() + "]");
            }
        }
    }
}
