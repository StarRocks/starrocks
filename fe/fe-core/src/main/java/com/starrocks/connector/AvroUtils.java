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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.starrocks.connector.hive.HiveStorageFormat.AVRO;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_URL;

public class AvroUtils {
    public static List<FieldSchema> getAvroFields(String catalogName,
                                                  String dbName,
                                                  String tableName,
                                                  Map<String, String> parameters) {
        Properties properties = new Properties();
        properties.putAll(parameters);
        try {
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
            Preconditions.checkState(connector != null,
                    String.format("connector of catalog %s should not be null", catalogName));
            CloudConfiguration cloudConfiguration = connector.getMetadata().getCloudConfiguration();
            HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);

            Schema schema = AvroSerdeUtils.determineSchemaOrThrowException(hdfsEnvironment.getConfiguration(), properties);
            AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
            List<String> columnNames = StringInternUtils.internStringsInList(aoig.getColumnNames());
            List<TypeInfo> columnTypes = aoig.getColumnTypes();
            List<FieldSchema> fieldSchemas = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                fieldSchemas.add(new FieldSchema(columnNames.get(i), columnTypes.get(i).getTypeName(), null));
            }
            return fieldSchemas;
        } catch (IOException | SerDeException e) {
            throw new StarRocksConnectorException("Failed to get Avro fields on [%s.%s.%s], msg: %s",
                    catalogName, dbName, tableName, e.getMessage());
        }
    }

    public static boolean isHiveTableWithAvroSchemas(org.apache.hadoop.hive.metastore.api.Table table) {
        if (!HiveStorageFormat.AVRO.getInputFormat().equals(table.getSd().getInputFormat())) {
            return false;
        }
        if (table.getParameters() == null) {
            return false;
        }
        StorageDescriptor sd = table.getSd();
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        if (serdeInfo == null) {
            throw new StarRocksConnectorException("Table storage descriptor is missing SerDe info");
        }
        return serdeInfo.getSerializationLib() != null &&
                (table.getParameters().get(SCHEMA_URL) != null ||
                        (serdeInfo.getParameters() != null && serdeInfo.getParameters().get(SCHEMA_URL) != null)) &&
                serdeInfo.getSerializationLib().equals(AVRO.getSerde());
    }
}
