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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static org.apache.iceberg.hive.IcebergHiveCatalog.LOCATION_PROPERTY;

public class IcebergApiConverter {
    private static final Logger LOG = LogManager.getLogger(IcebergApiConverter.class);
    public static final String PARTITION_NULL_VALUE = "null";

    public static IcebergTable toIcebergTable(Table nativeTbl, String catalogName, String remoteDbName,
                                              String remoteTableName, String nativeCatalogType,
                                              Optional<IcebergMetricsReporter> metricsReporter) {
        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setSrTableName(remoteTableName)
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "iceberg"))
                .setRemoteDbName(remoteDbName)
                .setRemoteTableName(remoteTableName)
                .setNativeTable(nativeTbl)
                .setFullSchema(toFullSchemas(nativeTbl))
                .setIcebergProperties(toIcebergProps(nativeCatalogType))
                .setMetricsReporter(metricsReporter);

        return tableBuilder.build();
    }

    public static Schema toIcebergApiSchema(List<Column> columns) {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (Column column : columns) {
            int index = icebergColumns.size();
            org.apache.iceberg.types.Type type = toIcebergColumnType(column.getType());
            Types.NestedField field = Types.NestedField.of(
                    index, column.isAllowNull(), column.getName(), type, column.getComment());
            icebergColumns.add(field);
        }

        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    // TODO(stephen): support iceberg transform partition like `partition by day(dt)`
    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String field : fields) {
            builder.identity(field);
        }
        return builder.build();
    }

    public static Optional<String> getTableLocation(Map<String, String> tableProperties) {
        return Optional.ofNullable(tableProperties.get(LOCATION_PROPERTY));
    }

    public static org.apache.iceberg.types.Type toIcebergColumnType(Type type) {
        PrimitiveType primitiveType = type.getPrimitiveType();

        switch (primitiveType) {
            case BOOLEAN:
                return Types.BooleanType.get();
            case INT:
                return Types.IntegerType.get();
            case BIGINT:
                return Types.LongType.get();
            case FLOAT:
                return Types.FloatType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case DATE:
                return Types.DateType.get();
            case DATETIME:
                return Types.TimestampType.withoutZone();
            case VARCHAR:
            case CHAR:
                return Types.StringType.get();
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return Types.DecimalType.of(type.getPrecision(), (((ScalarType) type).getScalarScale()));
            default:
                throw new StarRocksConnectorException("Unsupported column type %s", primitiveType);
        }
    }

    public static List<Column> toFullSchemas(Table nativeTbl) {
        List<Column> fullSchema = Lists.newArrayList();
        List<Types.NestedField> columns;
        try {
            columns = nativeTbl.schema().columns();
        } catch (NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }

        for (Types.NestedField field : columns) {
            Type srType;
            try {
                srType = fromIcebergType(field.type());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert iceberg type {}", field.type().toString(), e);
                srType = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(field.name(), srType, true);
            fullSchema.add(column);
        }
        return fullSchema;
    }

    public static Map<String, String> toIcebergProps(String nativeCatalogType) {
        Map<String, String> options = new HashMap<>();
        options.put(ICEBERG_CATALOG_TYPE, nativeCatalogType);
        return options;
    }

    public static RemoteFileInputFormat getHdfsFileFormat(FileFormat format) {
        switch (format) {
            case ORC:
                return RemoteFileInputFormat.ORC;
            case PARQUET:
                return RemoteFileInputFormat.PARQUET;
            default:
                throw new StarRocksConnectorException("Unexpected file format: " + format);
        }
    }

    public static TIcebergSchema getTIcebergSchema(Schema schema) {
        Types.StructType rootType = schema.asStruct();
        TIcebergSchema tIcebergSchema = new TIcebergSchema();
        List<TIcebergSchemaField> fields = new ArrayList<>(rootType.fields().size());
        for (Types.NestedField nestedField : rootType.fields()) {
            fields.add(getTIcebergSchemaField(nestedField));
        }
        tIcebergSchema.setFields(fields);
        return tIcebergSchema;
    }

    private static TIcebergSchemaField getTIcebergSchemaField(Types.NestedField nestedField) {
        TIcebergSchemaField tIcebergSchemaField = new TIcebergSchemaField();
        tIcebergSchemaField.setField_id(nestedField.fieldId());
        tIcebergSchemaField.setName(nestedField.name());
        if (nestedField.type().isNestedType()) {
            List<TIcebergSchemaField> children = new ArrayList<>(nestedField.type().asNestedType().fields().size());
            for (Types.NestedField child : nestedField.type().asNestedType().fields()) {
                children.add(getTIcebergSchemaField(child));
            }
            tIcebergSchemaField.setChildren(children);
        }
        return tIcebergSchemaField;
    }

    public static Metrics buildDataFileMetrics(TIcebergDataFile dataFile) {
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        if (dataFile.isSetColumn_stats()) {
            TIcebergColumnStats stats = dataFile.column_stats;
            if (stats.isSetColumn_sizes()) {
                columnSizes = stats.column_sizes;
            }
            if (stats.isSetValue_counts()) {
                valueCounts = stats.value_counts;
            }
            if (stats.isSetNull_value_counts()) {
                nullValueCounts = stats.null_value_counts;
            }
            if (stats.isSetLower_bounds()) {
                lowerBounds = stats.lower_bounds;
            }
            if (stats.isSetUpper_bounds()) {
                upperBounds = stats.upper_bounds;
            }
        }

        return new Metrics(dataFile.record_count, columnSizes, valueCounts,
                nullValueCounts, null, lowerBounds, upperBounds);
    }

    public static Map<String, String> rebuildCreateTableProperties(Map<String, String> createProperties) {
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
        createProperties.entrySet().forEach(tableProperties::put);

        String fileFormat = createProperties.getOrDefault("file_format", TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        if ("parquet".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        } else if ("avro".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
        } else if ("orc".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
        } else if (fileFormat != null) {
            throw new IllegalArgumentException("Unsupported format in USING: " + fileFormat);
        }

        return tableProperties.build();
    }
}