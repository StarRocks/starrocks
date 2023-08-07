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
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.sql.ast.CreateTableStmt;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.catalog.Resource.ResourceType.HUDI;

public class HudiTableFactory extends ExternalTableFactory {
    public static final HudiTableFactory INSTANCE = new HudiTableFactory();

    private HudiTableFactory() {

    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database database, CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = gsm.getNextId();
        Map<String, String> properties = stmt.getProperties();

        Set<String> metaFields = new HashSet<>(Arrays.asList(
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                HoodieRecord.FILENAME_METADATA_FIELD));
        Set<String> includedMetaFields = columns.stream().map(Column::getName)
                .filter(metaFields::contains).collect(Collectors.toSet());
        metaFields.removeAll(includedMetaFields);
        metaFields.forEach(f -> columns.add(new Column(f, Type.STRING, true)));

        Table table = getTableFromResourceMappingCatalog(properties, Table.TableType.HUDI, HUDI);
        if (table == null) {
            throw new DdlException("Can not find hudi table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }
        HudiTable oHudiTable = (HudiTable) table;
        validateHudiColumnType(columns, oHudiTable);

        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setCatalogName(oHudiTable.getCatalogName())
                .setResourceName(oHudiTable.getResourceName())
                .setHiveDbName(oHudiTable.getDbName())
                .setHiveTableName(oHudiTable.getTableName())
                .setFullSchema(columns)
                .setPartitionColNames(oHudiTable.getPartitionColumnNames())
                .setDataColNames(oHudiTable.getDataColumnNames())
                .setHudiProperties(oHudiTable.getProperties())
                .setCreateTime(oHudiTable.getCreateTime());

        HudiTable hudiTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && hudiTable.getPartitionColumnNames().size() > 0) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", hudiTable.getPartitionColumnNames()) + ")";
            hudiTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            hudiTable.setComment(stmt.getComment());
        }

        return hudiTable;
    }

    private static void validateHudiColumnType(List<Column> columns, HudiTable oTable) throws DdlException {
        String hudiBasePath = oTable.getTableLocation();
        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema hudiTableSchema;
        try {
            hudiTableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new DdlException("Cannot get hudi table schema.");
        }

        for (Column column : columns) {
            if (!column.isAllowNull()) {
                throw new DdlException(
                        "Hudi extern table does not support no-nullable column: [" + column.getName() + "]");
            }
            Schema.Field hudiColumn = hudiTableSchema.getField(column.getName());
            if (hudiColumn == null) {
                throw new DdlException("Column [" + column.getName() + "] not exists in hudi.");
            }
            Column oColumn = oTable.getColumn(column.getName());

            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType())) {
                throw new DdlException("can not convert hudi external table column type [" + column.getPrimitiveType() + "] " +
                        "to correct type [" + oColumn.getPrimitiveType() + "]");
            }
        }

        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        for (String partitionName : oTable.getPartitionColumnNames()) {
            if (!columnNames.contains(partitionName)) {
                throw new DdlException("Partition column [" + partitionName + "] must exist in column list");
            }
        }
    }
}
