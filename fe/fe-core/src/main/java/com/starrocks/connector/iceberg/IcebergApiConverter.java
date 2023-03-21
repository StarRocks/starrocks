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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static org.apache.iceberg.hive.IcebergHiveCatalog.LOCATION_PROPERTY;

public class IcebergApiConverter {
    private static final Logger LOG = LogManager.getLogger(IcebergApiConverter.class);
    public static final String PARTITION_NULL_VALUE = "null";

    public static IcebergTable toIcebergTable(Table nativeTbl, String catalogName, String remoteDbName,
                                              String remoteTableName, String nativeCatalogType) {
        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setSrTableName(remoteTableName)
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "iceberg"))
                .setRemoteDbName(remoteDbName)
                .setRemoteTableName(remoteTableName)
                .setNativeTable(nativeTbl)
                .setFullSchema(toFullSchemas(nativeTbl))
                .setIcebergProperties(toIcebergProps(nativeCatalogType));

        return tableBuilder.build();
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

    public static org.apache.iceberg.types.Type toIcebergColumnType(Type type) {
        PrimitiveType primitiveType = type.getPrimitiveType();

        switch (primitiveType) {
            case BOOLEAN:
                return Types.BooleanType.get();
            case INT:
            case BIGINT:
                return Types.IntegerType.get();
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
                return Types.DecimalType.of(type.getPrecision(), 10);
            default:
                throw new StarRocksConnectorException("Unsupported column type %s", primitiveType);
        }
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String field : fields) {
            builder.identity(field);
        }
        return builder.build();
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
        TIcebergColumnStats stats = dataFile.column_stats;
        return new Metrics(dataFile.record_count, stats.columnSizes, stats.valueCounts,
                stats.nullValueCounts, null, stats.lowerBounds, stats.upperBounds);
    }

    public static PartitionData partitionDataFromPath(String relativePartitionPath, PartitionSpec spec) {
        PartitionData data = new PartitionData(spec.fields().size());
        String[] partitions = relativePartitionPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();

        for (int i = 0; i < partitions.length; i++) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(parts.length == 2 && parts[0] != null &&
                    field.name().equals(parts[0]), "Invalid partition: %s", partitions[i]);

            org.apache.iceberg.types.Type sourceType = spec.partitionType().fields().get(i).type();
            data.set(i, Conversions.fromPartitionString(sourceType, parts[1]));
        }
        return data;
    }

    public static String getIcebergRelativePartitionPath(String tableLocation, String partitionLocation) {
        tableLocation = tableLocation.endsWith("/") ? tableLocation.substring(0, tableLocation.length() - 1) : tableLocation;
        String tableLocationWithData = tableLocation + "/data/";
        String path = PartitionUtil.getSuffixName(tableLocationWithData, partitionLocation);
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    public static Optional<String> getTableLocation(Map<String, String> tableProperties) {
        return Optional.ofNullable(tableProperties.get(LOCATION_PROPERTY));
    }

    public static class PartitionData implements StructLike {
        private final Object[] values;

        private PartitionData(int size) {
            this.values = new Object[size];
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            if (value instanceof ByteBuffer) {
                // ByteBuffer is not Serializable
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                values[pos] = bytes;
            } else {
                values[pos] = value;
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            PartitionData that = (PartitionData) other;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}