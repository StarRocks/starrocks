// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.delta;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Type;
import com.starrocks.external.hive.RemoteFileInputFormat;
import com.starrocks.external.iceberg.StarRocksIcebergException;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

import static com.starrocks.external.ColumnTypeConverter.fromDeltaLakeType;
import static com.starrocks.external.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
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
            Type srType = fromDeltaLakeType(dataType);
            Column column = new Column(field.getName(), srType, true);
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
            throw new StarRocksIcebergException("Unexpected file format: " + format);
        }
    }
}
