// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.delta;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.external.hive.RemoteFileInputFormat;
import com.starrocks.external.iceberg.StarRocksIcebergException;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.List;

import static com.starrocks.external.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
    public static DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                       String resourceName) {
        Configuration configuration = new Configuration();
        configuration.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), resourceName);
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
            Type srType = convertColumnType(dataType);
            Column column = new Column(field.getName(), srType, true);
            fullSchema.add(column);
        }

        return new DeltaLakeTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalog, dbName, tblName,
                fullSchema, metadata.getPartitionColumns(), deltaLog);
    }

    public static Type convertColumnType(DataType dataType) {
        if (dataType == null) {
            return Type.NULL;
        }
        PrimitiveType primitiveType;
        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(dataType.getClass());
        switch (deltaDataType) {
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case BYTE:
            case TINYINT:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case LONG:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case STRING:
                return ScalarType.createDefaultString();
            case DECIMAL:
                int precision = ((io.delta.standalone.types.DecimalType) dataType).getPrecision();
                int scale = ((io.delta.standalone.types.DecimalType) dataType).getScale();
                return ScalarType.createUnifiedDecimalType(precision, scale);
            case ARRAY:
                Type type = convertToArrayType((io.delta.standalone.types.ArrayType) dataType);
                if (type.isArrayType()) {
                    return type;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case NULL:
                primitiveType = PrimitiveType.NULL_TYPE;
                break;
            case BINARY:
            case MAP:
            case STRUCT:
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }
        return ScalarType.createType(primitiveType);
    }

    private static ArrayType convertToArrayType(io.delta.standalone.types.ArrayType arrayType) {
        return new ArrayType(convertColumnType(arrayType.getElementType()));
    }

    public static RemoteFileInputFormat getHdfsFileFormat(String format) {
        if (format.equalsIgnoreCase("ORC")) {
            return RemoteFileInputFormat.ORC;
        } else if (format.equalsIgnoreCase("PARQUET")) {
            return RemoteFileInputFormat.PARQUET;
        } else {
            throw new StarRocksIcebergException("Unexpected file format: " + format);
        }
    }
}
