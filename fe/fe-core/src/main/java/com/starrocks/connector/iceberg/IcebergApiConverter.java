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
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.TableRenameClause;
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
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;

public class IcebergApiConverter {
    private static final Logger LOG = LogManager.getLogger(IcebergApiConverter.class);
    public static final String PARTITION_NULL_VALUE = "null";
    private static final int FAKE_FIELD_ID = -1;

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

    public static org.apache.iceberg.types.Type toIcebergColumnType(Type type) {
        if (type.isScalarType()) {
            PrimitiveType primitiveType = type.getPrimitiveType();

            switch (primitiveType) {
                case BOOLEAN:
                    return Types.BooleanType.get();
                case TINYINT:
                case SMALLINT:
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
                case VARBINARY:
                    return Types.BinaryType.get();
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    ScalarType scalarType = (ScalarType) type;
                    return Types.DecimalType.of(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                case TIME:
                    return Types.TimeType.get();
                default:
                    throw new StarRocksConnectorException("Unsupported primitive column type %s", primitiveType);
            }
        }

        // handle complex type
        // it's ok to use FAKE_FIELD_ID here because TypeUtil.assignFreshIds will assign ground-truth ids later.
        if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            return Types.ListType.ofOptional(FAKE_FIELD_ID, toIcebergColumnType(arrayType.getItemType()));
        }

        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            return Types.MapType.ofOptional(FAKE_FIELD_ID, FAKE_FIELD_ID, toIcebergColumnType(mapType.getKeyType()),
                    toIcebergColumnType(mapType.getValueType()));
        }

        if (type.isStructType()) {
            StructType structType = (StructType) type;
            List<Types.NestedField> fields = new ArrayList<>();
            for (StructField structField : structType.getFields()) {
                org.apache.iceberg.types.Type subtype = toIcebergColumnType(structField.getType());
                Types.NestedField field = Types.NestedField.of(
                        FAKE_FIELD_ID, true, structField.getName(), subtype, structField.getComment());
                fields.add(field);
            }
            return Types.StructType.of(fields);
        }

        throw new StarRocksConnectorException("Unsupported complex column type %s", type);
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
            column.setComment(field.doc());
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
        String fileFormat = createProperties.getOrDefault(FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        String compressionCodec = null;

        if ("parquet".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.PARQUET_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.PARQUET_COMPRESSION, compressionCodec);
        } else if ("avro".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.AVRO_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.AVRO_COMPRESSION, compressionCodec);
        } else if ("orc".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.ORC_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.ORC_COMPRESSION, compressionCodec);
        } else if (fileFormat != null) {
            throw new IllegalArgumentException("Unsupported format in USING: " + fileFormat);
        }

        if (!PARQUET_COMPRESSION_TYPE_MAP.containsKey(compressionCodec.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Unsupported compression codec in USING: " + compressionCodec);
        }
        tableProperties.put(TableProperties.FORMAT_VERSION, "1");

        return tableProperties.build();
    }

    public static void applySchemaChanges(UpdateSchema updateSchema, List<AlterClause> alterClauses) {
        for (AlterClause clause : alterClauses) {
            if (clause instanceof AddColumnClause) {
                AddColumnClause addColumnClause = (AddColumnClause) clause;
                ColumnPosition pos = addColumnClause.getColPos();
                Column column = addColumnClause.getColumnDef().toColumn();

                // All non-partition columns must use NULL as the default value.
                if (!column.isAllowNull()) {
                    throw new StarRocksConnectorException("column in iceberg table must be nullable.");
                }
                updateSchema.addColumn(
                        column.getName(),
                        toIcebergColumnType(column.getType()),
                        column.getComment());

                // AFTER column / FIRST
                if (pos != null) {
                    if (pos.isFirst()) {
                        updateSchema.moveFirst(column.getName());
                    } else if (pos.getLastCol() != null) {
                        updateSchema.moveAfter(column.getName(), pos.getLastCol());
                    } else {
                        throw new StarRocksConnectorException("Unsupported position: " + pos);
                    }
                }
            } else if (clause instanceof AddColumnsClause) {
                AddColumnsClause addColumnsClause = (AddColumnsClause) clause;
                List<Column> columns = addColumnsClause
                        .getColumnDefs()
                        .stream()
                        .map(ColumnDef::toColumn)
                        .collect(Collectors.toList());

                for (Column column : columns) {
                    if (!column.isAllowNull()) {
                        throw new StarRocksConnectorException("column in iceberg table must be nullable.");
                    }
                    updateSchema.addColumn(
                            column.getName(),
                            toIcebergColumnType(column.getType()),
                            column.getComment());
                }
            } else if (clause instanceof DropColumnClause) {
                DropColumnClause dropColumnClause = (DropColumnClause) clause;
                String columnName = dropColumnClause.getColName();
                updateSchema.deleteColumn(columnName);
            } else if (clause instanceof ColumnRenameClause) {
                ColumnRenameClause columnRenameClause = (ColumnRenameClause) clause;
                updateSchema.renameColumn(columnRenameClause.getColName(), columnRenameClause.getNewColName());
            } else if (clause instanceof ModifyColumnClause) {
                ModifyColumnClause modifyColumnClause = (ModifyColumnClause) clause;
                ColumnPosition colPos = modifyColumnClause.getColPos();
                Column column = modifyColumnClause.getColumnDef().toColumn();
                org.apache.iceberg.types.Type colType = toIcebergColumnType(column.getType());

                // UPDATE column type
                if (!colType.isPrimitiveType()) {
                    throw new StarRocksConnectorException(
                            "Cannot modify " + column.getName() + ", not a primitive type");
                }
                updateSchema.updateColumn(column.getName(), colType.asPrimitiveType());

                // UPDATE comment
                if (column.getComment() != null) {
                    updateSchema.updateColumnDoc(column.getName(), column.getComment());
                }

                // NOT NULL / NULL
                if (column.isAllowNull()) {
                    updateSchema.makeColumnOptional(column.getName());
                } else {
                    throw new StarRocksConnectorException(
                            "column in iceberg table must be nullable.");
                }

                // AFTER column / FIRST
                if (colPos != null) {
                    if (colPos.isFirst()) {
                        updateSchema.moveFirst(column.getName());
                    } else if (colPos.getLastCol() != null) {
                        updateSchema.moveAfter(column.getName(), colPos.getLastCol());
                    } else {
                        throw new StarRocksConnectorException("Unsupported position: " + colPos);
                    }
                }
            } else {
                throw new StarRocksConnectorException(
                        "Unsupported alter operation for iceberg connector");
            }
        }

        updateSchema.commit();
    }

    // modify table comment/properties/name
    public static void applyTableChanges(Transaction transaction,
                                         IcebergCatalog icebergCatalog,
                                         String dbName,
                                         String tableName,
                                         List<AlterClause> tableChanges) {
        Preconditions.checkArgument(transaction != null && transaction.table() != null,
                "Transaction or table cannot be null");

        for (AlterClause clause : tableChanges) {
            if (clause instanceof TableRenameClause) {
                TableRenameClause tableRenameClause = (TableRenameClause) clause;
                Preconditions.checkNotNull(icebergCatalog, "iceberg catalog cannot be null");

                icebergCatalog.renameTable(dbName, tableName, tableRenameClause.getNewTableName());
            } else if (clause instanceof ModifyTablePropertiesClause) {
                ModifyTablePropertiesClause propertiesClause = (ModifyTablePropertiesClause) clause;
                Map<String, String> modifiedProperties = propertiesClause.getProperties();
                Preconditions.checkArgument(modifiedProperties.size() > 0, "Modified property is empty");

                modifyProperties(transaction, modifiedProperties);
            } else if (clause instanceof AlterTableCommentClause) {
                AlterTableCommentClause alterTableCommentClause = (AlterTableCommentClause) clause;
                transaction.updateProperties().set(COMMENT, alterTableCommentClause.getNewComment()).commit();
            } else {
                throw new StarRocksConnectorException(
                        "Unsupported alter operation for iceberg connector");
            }
        }
    }

    private static void modifyProperties(Transaction transaction, Map<String, String> pendingUpdate) {
        UpdateProperties updateProperties = transaction.updateProperties();
        for (Map.Entry<String, String> entry : pendingUpdate.entrySet()) {
            Preconditions.checkNotNull(entry.getValue(), new StarRocksConnectorException("property value cannot be null"));
            switch (entry.getKey().toLowerCase()) {
                case FILE_FORMAT:
                    updateProperties.defaultFormat(FileFormat.fromString(entry.getValue()));
                    break;
                case LOCATION_PROPERTY:
                    updateProperties.commit();
                    transaction.updateLocation().setLocation(entry.getValue()).commit();
                    break;
                case COMPRESSION_CODEC:
                    Preconditions.checkArgument(
                            PARQUET_COMPRESSION_TYPE_MAP.containsKey(entry.getValue().toLowerCase(Locale.ROOT)),
                            "Unsupported compression codec for iceberg connector: " + entry.getValue());

                    String fileFormat = pendingUpdate.get(FILE_FORMAT);
                    // only modify compression_codec or modify both file_format and compression_codec.
                    String currentFileFormat = fileFormat != null ? fileFormat : transaction.table().properties()
                            .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT,
                                    TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);

                    updateCodeCompr(updateProperties, FileFormat.fromString(currentFileFormat), entry.getValue());
                    break;
                default:
                    updateProperties.set(entry.getKey(), entry.getValue());
            }
        }

        updateProperties.commit();
    }

    private static void updateCodeCompr(UpdateProperties updateProperties, FileFormat fileFormat, String codeCompression) {
        switch (fileFormat) {
            case PARQUET:
                updateProperties.set(TableProperties.PARQUET_COMPRESSION, codeCompression);
                break;
            case ORC:
                updateProperties.set(TableProperties.ORC_COMPRESSION, codeCompression);
                break;
            case AVRO:
                updateProperties.set(TableProperties.AVRO_COMPRESSION, codeCompression);
                break;
            default:
                throw new StarRocksConnectorException(
                        "Unsupported file format for iceberg connector");
        }
    }
}