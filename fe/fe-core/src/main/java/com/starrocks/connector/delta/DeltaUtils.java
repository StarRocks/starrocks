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


package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
    private static final Logger LOG = LogManager.getLogger(DeltaUtils.class);

    public static DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                       Configuration configuration) {
        DeltaLog deltaLog = DeltaLog.forTable(configuration, path);

        if (!deltaLog.tableExists()) {
            throw new IllegalArgumentException(String.format("Delta log not exist for %s.%s.%s",
                    catalog, dbName, tblName));
        }

        Metadata metadata = deltaLog.snapshot().getMetadata();
        StructType tableSchema = metadata.getSchema();
        List<Column> fullSchema = Lists.newArrayList();

        if (tableSchema == null) {
            throw new IllegalArgumentException(String.format("Unable to find Schema information in Delta log for " +
                    "%s.%s.%s", catalog, dbName, tblName));
        }

        for (StructField field : metadata.getSchema().getFields()) {
            DataType dataType = field.getDataType();
            Type type;
            try {
                type = ColumnTypeConverter.fromDeltaLakeType(dataType);
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert delta type {} on {}.{}.{}", dataType.getTypeName(), catalog, dbName, tblName, e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(field.getName(), type, true);
            fullSchema.add(column);
        }

        return new DeltaLakeTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalog, dbName, tblName,
                fullSchema, metadata.getPartitionColumns(), deltaLog);
    }

    public static RemoteFileInputFormat getRemoteFileFormat(String format) {
        if (format.equalsIgnoreCase("ORC")) {
            return RemoteFileInputFormat.ORC;
        } else if (format.equalsIgnoreCase("PARQUET")) {
            return RemoteFileInputFormat.PARQUET;
        } else {
            throw new StarRocksConnectorException("Unexpected file format: " + format);
        }
    }
}
