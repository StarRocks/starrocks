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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;

public class IcebergUtil {
    private static final Logger LOG = LogManager.getLogger(IcebergUtil.class);

    /**
     * Get Iceberg table identifier by table property
     */
    public static TableIdentifier getIcebergTableIdentifier(IcebergTable table) {
        return TableIdentifier.of(table.getDb(), table.getTable());
    }

    /**
     * Get Iceberg table identifier by table property
     */
    public static TableIdentifier getIcebergTableIdentifier(String db, String table) {
        return TableIdentifier.of(db, table);
    }

    /**
     * Get IcebergCatalog from IcebergTable
     * Returns the corresponding catalog implementation.
     */
    public static IcebergCatalog getIcebergCatalog(IcebergTable table)
            throws StarRocksIcebergException {
        IcebergCatalogType catalogType = table.getCatalogType();

        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(table.getIcebergProperties(), null);

        switch (catalogType) {
            case HIVE_CATALOG:
                return getIcebergHiveCatalog(table.getIcebergHiveMetastoreUris(), table.getIcebergProperties(),
                        hdfsEnvironment);
            case REST_CATALOG:
                return getIcebergRESTCatalog(table.getIcebergProperties(), hdfsEnvironment);
            case CUSTOM_CATALOG:
                return getIcebergCustomCatalog(table.getCatalogImpl(), table.getIcebergProperties(), hdfsEnvironment);
            default:
                throw new StarRocksIcebergException(
                        "Unexpected catalog type: " + catalogType.toString());
        }
    }

    /**
     * Returns the corresponding hive catalog implementation.
     */
    public static IcebergCatalog getIcebergHiveCatalog(String metastoreUris, Map<String, String> icebergProperties,
                                                       HdfsEnvironment hdfsEnvironment)
            throws StarRocksIcebergException {
        return IcebergHiveCatalog.getInstance(metastoreUris, icebergProperties, hdfsEnvironment);
    }

    /**
     * Returns the corresponding custom catalog implementation.
     */
    public static IcebergCatalog getIcebergCustomCatalog(String catalogImpl, Map<String, String> icebergProperties,
                                                         HdfsEnvironment hdfsEnvironment)
            throws StarRocksIcebergException {
        return (IcebergCatalog) CatalogLoader.custom(String.format("Custom-%s", catalogImpl),
                hdfsEnvironment.getConfiguration(), icebergProperties, catalogImpl).loadCatalog();
    }

    /**
     * Returns the corresponding glue catalog implementation.
     */
    public static IcebergCatalog getIcebergGlueCatalog(String catalogName, Map<String, String> icebergProperties,
                                                       HdfsEnvironment hdfsEnvironment) {
        return IcebergGlueCatalog.getInstance(catalogName, icebergProperties, hdfsEnvironment);
    }

    /**
     * Returns the corresponding rest catalog implementation.
     */
    public static IcebergCatalog getIcebergRESTCatalog(Map<String, String> icebergProperties, HdfsEnvironment hdfsEnvironment) {
        return IcebergRESTCatalog.getInstance(icebergProperties, hdfsEnvironment);
    }

    /**
     * Get hdfs file format in StarRocks use iceberg file format.
     *
     * @param format
     * @return RemoteFileInputFormat
     */
    public static RemoteFileInputFormat getHdfsFileFormat(FileFormat format) {
        switch (format) {
            case ORC:
                return RemoteFileInputFormat.ORC;
            case PARQUET:
                return RemoteFileInputFormat.PARQUET;
            default:
                throw new StarRocksIcebergException(
                        "Unexpected file format: " + format.toString());
        }
    }

    /**
     * Get current snapshot of iceberg table, return null if snapshot do not exist.
     * Refresh table is needed.
     *
     * @param table
     * @return Optional<Snapshot>
     */
    public static Optional<Snapshot> getCurrentTableSnapshot(Table table) {
        return Optional.ofNullable(table.currentSnapshot());
    }

    /**
     * Get table scan for given table and snapshot, filter with given iceberg predicates.
     * Refresh table if needed.
     *
     * @param table
     * @param snapshot
     * @param icebergPredicate
     * @return
     */
    public static TableScan getTableScan(Table table,
                                         Snapshot snapshot,
                                         Expression icebergPredicate) {
        // TODO: use planWith(executorService) after
        // https://github.com/apache/iceberg/commit/74db81f4dd81360bf3c0ad438d4be937c7a812d9 release
        TableScan tableScan = table.newScan().useSnapshot(snapshot.snapshotId()).includeColumnStats();
        if (icebergPredicate != null) {
            tableScan = tableScan.filter(icebergPredicate);
        }

        return tableScan;
    }

    public static void refreshTable(Table table) {
        try {
            if (table instanceof BaseTable) {
                BaseTable baseTable = (BaseTable) table;
                if (baseTable.operations().refresh() == null) {
                    // If table is loaded successfully, current table metadata will never be null.
                    // So when we get a null metadata after refresh, it indicates the table has been dropped.
                    // See: https://github.com/StarRocks/starrocks/issues/3076
                    throw new NoSuchTableException("No such table: %s", table.name());
                }
            } else {
                // table loaded by GlobalStateMgr should be a base table
                throw new StarRocksIcebergException(String.format("Invalid table type of %s, it should be a BaseTable!",
                        table.name()));
            }
        } catch (NoSuchTableException e) {
            throw new StarRocksIcebergException(String.format("No such table  %s", table.name()));
        } catch (IllegalStateException ei) {
            throw new StarRocksIcebergException(String.format("Refresh table %s with failure, the table under hood" +
                            " may have been dropped. You should re-create the external table. cause %s",
                    table.name(), ei.getMessage()));
        }
    }

    /**
     * @param partitionSpec
     * @return
     */
    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec) {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static IcebergTable convertCustomCatalogToSRTable(org.apache.iceberg.Table icebergTable, String catalogImpl,
                                                             String catalogName, String dbName, String tblName,
                                                             Map<String, String> customProperties) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergTable.ICEBERG_CATALOG, catalogName);
        properties.put(IcebergTable.ICEBERG_DB, dbName);
        properties.put(IcebergTable.ICEBERG_TABLE, tblName);
        properties.put(IcebergTable.ICEBERG_CATALOG_TYPE, "CUSTOM_CATALOG");
        properties.put(IcebergTable.ICEBERG_IMPL, catalogImpl);
        properties.putAll(customProperties);
        return convertToSRTable(icebergTable, properties);
    }

    public static List<String> getIdentityPartitionNames(org.apache.iceberg.Table icebergTable) {
        List<String> partitionNames = Lists.newArrayList();
        TableScan tableScan = icebergTable.newScan();
        List<FileScanTask> tasks = Lists.newArrayList(tableScan.planFiles());
        if (icebergTable.spec().isUnpartitioned()) {
            return partitionNames;
        }

        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            return partitionNames;
        }

        for (FileScanTask fileScanTask : tasks) {
            StructLike partition = fileScanTask.file().partition();
            partitionNames.add(convertIcebergPartitionToPartitionName(icebergTable.spec(), partition));
        }
        return partitionNames;
    }

    static String convertIcebergPartitionToPartitionName(PartitionSpec partitionSpec, StructLike partition) {
        int filePartitionFields = partition.size();
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < filePartitionFields; ++index) {
            PartitionField partitionField = partitionSpec.fields().get(index);
            sb.append(partitionField.name());
            sb.append("=");
            String value = partitionField.transform().toHumanString(getPartitionValue(partition, index,
                    partitionSpec.javaClasses()[index]));
            sb.append(value);
            sb.append("/");
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static <T> T getPartitionValue(StructLike partition, int position, Class<?> javaClass) {
        return partition.get(position, (Class<T>) javaClass);
    }

    public static IcebergTable convertHiveCatalogToSRTable(org.apache.iceberg.Table icebergTable, String metastoreURI,
                                                           String catalogName, String dbName,
                                                           String tblName) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergTable.ICEBERG_CATALOG, catalogName);
        properties.put(IcebergTable.ICEBERG_DB, dbName);
        properties.put(IcebergTable.ICEBERG_TABLE, tblName);
        properties.put(IcebergTable.ICEBERG_CATALOG_TYPE, "HIVE_CATALOG");
        properties.put(IcebergTable.ICEBERG_METASTORE_URIS, metastoreURI);
        return convertToSRTable(icebergTable, properties);
    }

    public static IcebergTable convertGlueCatalogToSRTable(org.apache.iceberg.Table icebergTable,
                                                           String catalogName, String dbName,
                                                           String tblName) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergTable.ICEBERG_CATALOG, catalogName);
        properties.put(IcebergTable.ICEBERG_DB, dbName);
        properties.put(IcebergTable.ICEBERG_TABLE, tblName);
        properties.put(IcebergTable.ICEBERG_CATALOG_TYPE, "GLUE_CATALOG");
        return convertToSRTable(icebergTable, properties);
    }

    public static IcebergTable convertRESTCatalogToSRTable(org.apache.iceberg.Table icebergTable,
                                                           String catalogName, String dbName,
                                                           String tblName) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergTable.ICEBERG_CATALOG, catalogName);
        properties.put(IcebergTable.ICEBERG_DB, dbName);
        properties.put(IcebergTable.ICEBERG_TABLE, tblName);
        properties.put(IcebergTable.ICEBERG_CATALOG_TYPE, "REST_CATALOG");
        return convertToSRTable(icebergTable, properties);
    }

    private static IcebergTable convertToSRTable(org.apache.iceberg.Table icebergTable, Map<String, String> properties)
            throws DdlException {
        try {
            List<Column> fullSchema = Lists.newArrayList();
            icebergTable.schema().columns().forEach(
                    field -> {
                        Type srType;
                        try {
                            srType = convertColumnType(field.type());
                        } catch (InternalError | Exception e) {
                            LOG.error("Failed to convert iceberg type {}", field.type().toString(), e);
                            srType = Type.UNKNOWN_TYPE;
                        }
                        Column column = new Column(field.name(), srType, true);
                        fullSchema.add(column);
                    }
            );
            return new IcebergTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), icebergTable,
                    true, icebergTable.name(), fullSchema, properties);
        } catch (Exception e) {
            throw new DdlException("iceberg table is not found.", e);
        }
    }

    public static Type convertColumnType(org.apache.iceberg.types.Type icebergType) {
        if (icebergType == null) {
            return Type.NULL;
        }

        PrimitiveType primitiveType;

        switch (icebergType.typeId()) {
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
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
            case UUID:
                return ScalarType.createDefaultExternalTableString();
            case DECIMAL:
                int precision = ((Types.DecimalType) icebergType).precision();
                int scale = ((Types.DecimalType) icebergType).scale();
                return ScalarType.createUnifiedDecimalType(precision, scale);
            case LIST:
                Type type = convertToArrayType(icebergType);
                if (type.isArrayType()) {
                    return type;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case MAP:
                Type mapType = convertToMapType(icebergType);
                if (mapType.isMapType()) {
                    return mapType;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case STRUCT:
                List<Types.NestedField> fields = icebergType.asStructType().fields();
                Preconditions.checkArgument(fields.size() > 0);
                ArrayList<StructField> structFields = new ArrayList<>(fields.size());
                for (Types.NestedField field : fields) {
                    String fieldName = field.name();
                    Type fieldType = convertColumnType(field.type());
                    if (fieldType.isUnknown()) {
                        return Type.UNKNOWN_TYPE;
                    }
                    structFields.add(new StructField(fieldName, fieldType));
                }
                return new StructType(structFields);
            case TIME:
            case FIXED:
            case BINARY:
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }
        return ScalarType.createType(primitiveType);
    }

    public static Database convertToSRDatabase(String dbName) {
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
    }

    private static ArrayType convertToArrayType(org.apache.iceberg.types.Type icebergType) {
        return new ArrayType(convertColumnType(icebergType.asNestedType().asListType().elementType()));
    }

    private static Type convertToMapType(org.apache.iceberg.types.Type icebergType) {
        Type keyType = convertColumnType(icebergType.asMapType().keyType());
        // iceberg support complex type as key type, but sr is not supported now
        if (keyType.isComplexType() || keyType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        Type valueType = convertColumnType(icebergType.asMapType().valueType());
        if (valueType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        return new MapType(keyType, valueType);
    }
}