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

package com.starrocks.connector.iceberg.procedure;

import com.google.common.base.Strings;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Pair;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergPartitionData;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


public class AddFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddFilesProcedure.class);

    private static final String PROCEDURE_NAME = "add_files";

    public static final String SOURCE_TABLE = "source_table";
    public static final String LOCATION = "location";
    public static final String FILE_FORMAT = "file_format";
    public static final String RECURSIVE = "recursive";

    private static final AddFilesProcedure INSTANCE = new AddFilesProcedure();

    public static AddFilesProcedure getInstance() {
        return INSTANCE;
    }

    private AddFilesProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(SOURCE_TABLE, VarcharType.VARCHAR, false),
                        new NamedArgument(LOCATION, VarcharType.VARCHAR, false),
                        new NamedArgument(FILE_FORMAT, VarcharType.VARCHAR, false),
                        new NamedArgument(RECURSIVE, BooleanType.BOOLEAN, false)
                ),
                IcebergTableOperation.ADD_FILES
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        // Validate arguments - either source_table or location must be provided, but not both
        ConstantOperator sourceTableArg = args.get(SOURCE_TABLE);
        ConstantOperator tableLocationArg = args.get(LOCATION);
        ConstantOperator fileFormatArg = args.get(FILE_FORMAT);

        if (sourceTableArg == null && tableLocationArg == null) {
            throw new StarRocksConnectorException(
                    "Either 'source_table' or 'location' must be provided for add_files operation");
        }

        if (sourceTableArg != null && tableLocationArg != null) {
            throw new StarRocksConnectorException(
                    "Cannot specify both 'source_table' and 'location' for add_files operation");
        }

        if (tableLocationArg != null && fileFormatArg == null) {
            throw new StarRocksConnectorException(
                    "'file_format' must be provided when 'location' is specified");
        }

        String fileFormat = null;
        if (fileFormatArg != null) {
            fileFormat = fileFormatArg.getVarchar().toLowerCase();
            if (!fileFormat.equals("parquet") && !fileFormat.equals("orc")) {
                throw new StarRocksConnectorException(
                        "Unsupported file format: %s. Supported formats are: parquet, orc", fileFormat);
            }
        }

        boolean recursive = true;
        ConstantOperator recursiveArg = args.get(RECURSIVE);
        if (recursiveArg != null) {
            recursive = recursiveArg.getBoolean();
        }

        Table table = context.table();
        PartitionSpec spec = table.spec();
        if (spec.isPartitioned() && spec.fields().stream().anyMatch(f -> !f.transform().isIdentity())) {
            throw new StarRocksConnectorException(
                    "Adding files to partitioned tables with non-identity partitioning is not supported, " +
                            "which will cause data inconsistency");
        }

        Transaction transaction = context.transaction();
        try {
            if (tableLocationArg != null) {
                // Add files from a specific location
                String tableLocation = tableLocationArg.getVarchar();
                addFilesFromLocation(context, table, transaction, tableLocation, recursive, fileFormat);
            } else {
                // Add files from source table
                String sourceTable = sourceTableArg.getVarchar();
                addFilesFromSourceTable(context, table, transaction, sourceTable, recursive);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to execute add_files procedure", e);
            throw new StarRocksConnectorException("Failed to add files: %s", e.getMessage(), e);
        }
    }

    private void addFilesFromLocation(IcebergTableProcedureContext context, Table table, Transaction transaction,
                                      String location, boolean recursive, String fileFormat) throws IOException {
        LOGGER.info("Adding files from location: {}", location);

        // Get the file system for the location
        URI locationUri = new Path(location).toUri();
        FileSystem fileSystem = FileSystem.get(locationUri,
                context.hdfsEnvironment().getConfiguration());

        // Discover data files in the location
        List<DataFile> dataFiles = discoverDataFiles(context, table, fileSystem, location, recursive, fileFormat);

        if (dataFiles.isEmpty()) {
            LOGGER.warn("No data files found at location: {}", location);
            return;
        }

        // Add the files to the table using a transaction
        AppendFiles appendFiles = transaction.newAppend();
        for (DataFile dataFile : dataFiles) {
            appendFiles.appendFile(dataFile);
        }

        // Commit the transaction
        appendFiles.commit();
        LOGGER.info("Successfully added {} files to table", dataFiles.size());
    }

    private List<DataFile> discoverDataFiles(IcebergTableProcedureContext context, Table table, FileSystem fileSystem,
                                             String location, boolean recursive, String fileFormat) throws IOException {
        List<DataFile> dataFiles = new ArrayList<>();
        Path locationPath = new Path(location);

        if (!fileSystem.exists(locationPath)) {
            throw new StarRocksConnectorException("Location does not exist: %s", location);
        }

        FileStatus fileStatus = fileSystem.getFileStatus(locationPath);
        if (fileStatus.isFile()) {
            // Single file
            if (isDataFile(fileStatus)) {
                DataFile dataFile = createDataFileFromLocation(context, table, fileStatus, fileFormat);
                if (dataFile != null) {
                    dataFiles.add(dataFile);
                }
            } else {
                LOGGER.warn("The specified location is a file but not a recognized data file: {}",
                        fileStatus.getPath());
                throw new StarRocksConnectorException("No valid data files found at location: %s", location);
            }
            return dataFiles;
        } else if (fileStatus.isDirectory()) {
            // List all files recursively
            FileStatus[] files = fileSystem.listStatus(locationPath);
            for (FileStatus file : files) {
                if (file.isFile() && isDataFile(file)) {
                    try {
                        DataFile dataFile = createDataFileFromLocation(context, table, file, fileFormat);
                        if (dataFile != null) {
                            dataFiles.add(dataFile);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to process file: {}. Error: {}",
                                file.getPath(), e.getMessage());
                        throw new StarRocksConnectorException("Failed to process file: %s, error: %s",
                                file.getPath(), e.getMessage(), e);
                    }
                } else if (file.isDirectory() && recursive) {
                    // Recursively process subdirectories
                    dataFiles.addAll(discoverDataFiles(context, table, fileSystem, file.getPath().toString(),
                            true, fileFormat));
                }
            }
        }

        return dataFiles;
    }

    private boolean isDataFile(FileStatus fileStatus) {
        // Support common data file formats as per Iceberg specification
        // Skip hidden files and directories (starting with . or _)
        String fileName = fileStatus.getPath().getName();
        if (fileName.startsWith(".") || fileName.startsWith("_")) {
            return false;
        }

        if (!fileStatus.isFile()) {
            return false;
        }

        return fileStatus.getLen() != 0;
    }

    private DataFile createDataFileFromLocation(IcebergTableProcedureContext context, Table table, FileStatus fileStatus,
                                                String fileFormat) {
        String filePath = fileStatus.getPath().toString();

        // Get the table's partition spec
        PartitionSpec spec = table.spec();
        String partitionPath = "";
        if (spec.isPartitioned()) {
            List<String> validPartitionPath = new ArrayList<>();
            String[] partitions = filePath.split("/", -1);
            for (String part : partitions) {
                if (part.contains("=")) {
                    validPartitionPath.add(part);
                }
            }
            partitionPath = String.join("/", validPartitionPath);
        }

        return buildDataFile(context, table, partitionPath, fileStatus, fileFormat);
    }

    private DataFile buildDataFile(IcebergTableProcedureContext context, Table table, String partitionPath,
                                   FileStatus fileStatus, String fileFormat) {
        Optional<StructLike> partition = Optional.empty();
        String filePath = fileStatus.getPath().toString();
        long fileSize = fileStatus.getLen();

        // Get the table's partition spec
        PartitionSpec spec = table.spec();
        if (!Strings.isNullOrEmpty(partitionPath)) {
            partition = Optional.of(IcebergPartitionData.partitionDataFromPath(partitionPath, spec));
        }
        // Extract file metrics based on format
        Metrics metrics = extractFileMetrics(context, table, fileStatus, fileFormat);

        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(filePath)
                .withFileSizeInBytes(fileSize)
                .withFormat(fileFormat.toUpperCase())
                .withMetrics(metrics);
        partition.ifPresent(builder::withPartition);
        return builder.build();

    }

    private List<DataFile> createDataFilesFromPartition(IcebergTableProcedureContext context, Table table, String partitionName,
                                                        RemoteFileInfo remoteFileInfo) {
        String partitionFullPath = remoteFileInfo.getFullPath();
        List<RemoteFileDesc> remoteFiles = remoteFileInfo.getFiles();
        if (remoteFiles == null || remoteFiles.isEmpty()) {
            LOGGER.warn("No files found in RemoteFileInfo for partition: {}", partitionName);
            return null;
        }

        return remoteFiles.stream().map(remoteFile -> {
            String filePath = remoteFile.getFullPath() != null ? remoteFile.getFullPath() :
                    (partitionFullPath.endsWith("/") ? partitionFullPath + remoteFile.getFileName() :
                            partitionFullPath + "/" + remoteFile.getFileName());
            FileStatus fileStatus = new FileStatus(remoteFile.getLength(), false,
                    1, 0, remoteFile.getModificationTime(), new Path(filePath));
            return buildDataFile(context, table, table.spec().isPartitioned() ? partitionName : "", fileStatus,
                    remoteFileInfo.getFormat().name());
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Metrics extractFileMetrics(IcebergTableProcedureContext context, Table table, FileStatus fileStatus,
                                       String fileFormat) {
        try {
            return switch (fileFormat.toLowerCase()) {
                case "parquet" -> extractParquetMetrics(context, table, fileStatus);
                case "orc" -> extractOrcMetrics(context, table, fileStatus);
                default -> {
                    throw new StarRocksConnectorException("Unsupported file format: %s", fileFormat);
                }
            };
        } catch (Exception e) {
            LOGGER.warn("Failed to extract metrics for file: {}, error: {}",
                    fileStatus.getPath(), e.getMessage());
            throw new StarRocksConnectorException("Failed to extract metrics for file: %s, error: %s",
                    fileStatus.getPath(), e.getMessage(), e);
        }
    }

    private Metrics extractParquetMetrics(IcebergTableProcedureContext context, Table table, FileStatus fileStatus)
            throws IOException {
        try {
            // Create Hadoop input file
            HadoopInputFile inputFile = HadoopInputFile.fromStatus(fileStatus,
                    context.hdfsEnvironment().getConfiguration());

            // Read Parquet footer metadata
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();

                long recordCount = 0;
                Map<Integer, Long> columnSizes = new HashMap<>();
                Map<Integer, Long> valueCounts = new HashMap<>();
                Map<Integer, Long> nullValueCounts = new HashMap<>();
                Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
                Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
                Map<Integer, Object> lowerValues = new HashMap<>();
                Map<Integer, Object> upperValues = new HashMap<>();
                Map<Integer, Type> fieldTypes = new HashMap<>();
                Map<Integer, Comparator<Object>> comparators = new HashMap<>();
                Set<Integer> missingStats = new HashSet<>();

                Schema schema = table.schema();

                // Aggregate statistics from all row groups
                for (org.apache.parquet.hadoop.metadata.BlockMetaData blockMeta : metadata.getBlocks()) {
                    recordCount += blockMeta.getRowCount();

                    for (ColumnChunkMetaData columnMeta : blockMeta.getColumns()) {
                        String columnPath = columnMeta.getPath().toDotString();
                        Types.NestedField field = schema.findField(columnPath);

                        if (field != null) {
                            int fieldId = field.fieldId();
                            // Column sizes
                            columnSizes.merge(fieldId, columnMeta.getTotalSize(), Long::sum);
                            // Value counts
                            long valueCount = columnMeta.getValueCount();
                            valueCounts.merge(fieldId, valueCount, Long::sum);
                            // null counts
                            Statistics<?> columnStats = columnMeta.getStatistics();
                            if (columnStats != null && !columnStats.isEmpty()) {
                                if (columnStats.getNumNulls() >= 0) {
                                    nullValueCounts.merge(fieldId, columnStats.getNumNulls(), Long::sum);
                                }

                                // Min/Max values
                                if (columnStats.hasNonNullValue()) {
                                    Comparator<Object> comparator = comparators.get(fieldId);
                                    if (comparator == null) {
                                        comparator = tryGetComparator(field.type());
                                        if (comparator == null) {
                                            missingStats.add(fieldId);
                                            continue;
                                        }
                                        comparators.put(fieldId, comparator);
                                    }

                                    Object minValue = tryConvertStatValue(field.type(), columnStats.genericGetMin());
                                    Object maxValue = tryConvertStatValue(field.type(), columnStats.genericGetMax());
                                    if (minValue == null || maxValue == null) {
                                        missingStats.add(fieldId);
                                        continue;
                                    }

                                    fieldTypes.put(fieldId, field.type());
                                    Object existingMin = lowerValues.get(fieldId);
                                    if (existingMin == null || comparator.compare(minValue, existingMin) < 0) {
                                        lowerValues.put(fieldId, minValue);
                                    }
                                    Object existingMax = upperValues.get(fieldId);
                                    if (existingMax == null || comparator.compare(maxValue, existingMax) > 0) {
                                        upperValues.put(fieldId, maxValue);
                                    }
                                }
                            } else {
                                missingStats.add(fieldId);
                            }
                        }
                    }
                }

                for (Integer fieldId : missingStats) {
                    nullValueCounts.remove(fieldId);
                    lowerBounds.remove(fieldId);
                    upperBounds.remove(fieldId);
                    lowerValues.remove(fieldId);
                    upperValues.remove(fieldId);
                    fieldTypes.remove(fieldId);
                    comparators.remove(fieldId);
                }

                for (Map.Entry<Integer, Object> entry : lowerValues.entrySet()) {
                    int fieldId = entry.getKey();
                    Type fieldType = fieldTypes.get(fieldId);
                    if (fieldType != null) {
                        lowerBounds.put(fieldId, Conversions.toByteBuffer(fieldType, entry.getValue()));
                    }
                }
                for (Map.Entry<Integer, Object> entry : upperValues.entrySet()) {
                    int fieldId = entry.getKey();
                    Type fieldType = fieldTypes.get(fieldId);
                    if (fieldType != null) {
                        upperBounds.put(fieldId, Conversions.toByteBuffer(fieldType, entry.getValue()));
                    }
                }

                return new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts,
                        null, lowerBounds, upperBounds);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to read Parquet metadata for file: {}, error: {}", fileStatus.getPath(), e.getMessage());
            throw new IOException("Failed to extract Parquet metrics", e);
        }
    }

    private Metrics extractOrcMetrics(IcebergTableProcedureContext context, Table table,
                                      FileStatus fileStatus) throws IOException {
        Path hadoopPath = new Path(fileStatus.getPath().toString());
        try {
            // Read ORC file metadata
            try (Reader orcReader = OrcFile.createReader(hadoopPath,
                    OrcFile.readerOptions(context.hdfsEnvironment().getConfiguration()))) {

                long recordCount = orcReader.getNumberOfRows();
                Map<Integer, Long> valueCounts = new HashMap<>();
                Map<Integer, Long> nullValueCounts = new HashMap<>();

                Schema schema = table.schema();
                TypeDescription orcSchema = orcReader.getSchema();
                ColumnStatistics[] columnStats = orcReader.getStatistics();

                // Extract statistics for each column
                for (int colId = 0; colId < columnStats.length; colId++) {
                    ColumnStatistics stats = columnStats[colId];

                    // Map ORC column to Iceberg field
                    String columnName = getColumnNameFromOrcSchema(orcSchema, colId);
                    if (columnName != null) {
                        Types.NestedField field = schema.findField(columnName);
                        if (field != null) {
                            int fieldId = field.fieldId();

                            // Value counts and null counts
                            valueCounts.put(fieldId, stats.getNumberOfValues());
                            if (stats.hasNull()) {
                                long nullCount = recordCount - stats.getNumberOfValues();
                                nullValueCounts.put(fieldId, nullCount);
                            }

                        }
                    }
                }

                return new Metrics(recordCount, null, valueCounts, nullValueCounts, null);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to read ORC metadata for file: {}, error: {}", hadoopPath, e.getMessage());
            throw new IOException("Failed to extract ORC metrics", e);
        }
    }

    private String getColumnNameFromOrcSchema(TypeDescription orcSchema, int columnId) {
        try {
            if (columnId < orcSchema.getChildren().size()) {
                return orcSchema.getFieldNames().get(columnId);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to get column name for ORC column ID: {}", columnId);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static Comparator<Object> tryGetComparator(Type type) {
        if (type == null || !type.isPrimitiveType()) {
            return null;
        }
        try {
            return (Comparator<Object>) Comparators.forType(type.asPrimitiveType());
        } catch (Exception e) {
            return null;
        }
    }

    static Object tryConvertStatValue(Type fieldType, Object statValue) {
        if (fieldType == null || statValue == null) {
            return null;
        }

        switch (fieldType.typeId()) {
            case BOOLEAN:
                return statValue instanceof Boolean ? statValue : null;
            case INTEGER:
            case DATE:
                if (statValue instanceof Integer v) {
                    return v;
                }
                if (statValue instanceof Long v) {
                    if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
                        return v.intValue();
                    }
                }
                return null;
            case LONG:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
                if (statValue instanceof Long v) {
                    return v;
                }
                if (statValue instanceof Integer v) {
                    return v.longValue();
                }
                return null;
            case FLOAT:
                if (statValue instanceof Float v) {
                    return v;
                }
                if (statValue instanceof Double v) {
                    return v.floatValue();
                }
                return null;
            case DOUBLE:
                if (statValue instanceof Double v) {
                    return v;
                }
                if (statValue instanceof Float v) {
                    return v.doubleValue();
                }
                return null;
            case STRING:
                if (statValue instanceof CharSequence value) {
                    return value.toString();
                }
                if (statValue instanceof Binary binary) {
                    return binary.toStringUsingUTF8();
                }
                if (statValue instanceof byte[] bytes) {
                    return new String(bytes, StandardCharsets.UTF_8);
                }
                return null;
            case UUID:
                if (statValue instanceof java.util.UUID uuid) {
                    return uuid;
                }
                if (statValue instanceof Binary binary) {
                    return UUIDUtil.convert(binary.getBytes());
                }
                if (statValue instanceof byte[] bytes) {
                    return UUIDUtil.convert(bytes);
                }
                if (statValue instanceof ByteBuffer buffer) {
                    return UUIDUtil.convert(buffer.duplicate());
                }
                return null;
            case FIXED:
            case BINARY:
            case GEOMETRY:
            case GEOGRAPHY:
                if (statValue instanceof ByteBuffer buffer) {
                    return buffer.duplicate();
                }
                if (statValue instanceof Binary binary) {
                    return ByteBuffer.wrap(binary.getBytes());
                }
                if (statValue instanceof byte[] bytes) {
                    return ByteBuffer.wrap(bytes);
                }
                return null;
            case DECIMAL:
                if (fieldType instanceof Types.DecimalType decimalType) {
                    return tryConvertDecimalStatValue(decimalType, statValue);
                }
                return null;
            default:
                return null;
        }
    }

    static BigDecimal tryConvertDecimalStatValue(Types.DecimalType decimalType, Object statValue) {
        if (statValue == null) {
            return null;
        }

        int scale = decimalType.scale();
        if (statValue instanceof BigDecimal decimal) {
            return decimal;
        }
        if (statValue instanceof Integer v) {
            return BigDecimal.valueOf(v.longValue(), scale);
        }
        if (statValue instanceof Long v) {
            return BigDecimal.valueOf(v, scale);
        }
        if (statValue instanceof Binary binary) {
            return new BigDecimal(new BigInteger(binary.getBytes()), scale);
        }
        if (statValue instanceof byte[] bytes) {
            return new BigDecimal(new BigInteger(bytes), scale);
        }
        if (statValue instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        }

        return null;
    }

    private void addFilesFromSourceTable(IcebergTableProcedureContext context, Table table, Transaction transaction,
                                         String sourceTable, boolean recursive) throws Exception {
        ParseNode where = context.clause().getWhere();
        LOGGER.info("Adding files from source Hive table: {} with partition filter: {}", sourceTable,
                where != null ? AstToSQLBuilder.toSQL(where) : "none");

        // Parse source table name to get database and table
        String[] parts = sourceTable.split("\\.");
        String catalogName;
        String dbName;
        String tableName;

        if (parts.length == 3) {
            // Catalog.database.table format
            catalogName = parts[0];
            dbName = parts[1];
            tableName = parts[2];
        } else {
            throw new StarRocksConnectorException(
                    "Invalid source table format: %s. Expected format: catalog.database.table", sourceTable);
        }

        com.starrocks.catalog.Table sourceHiveTable;
        try {
            sourceHiveTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context.context(), catalogName,
                    dbName, tableName);
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    "Failed to access source table %s: %s", sourceTable, e.getMessage(), e);
        }

        if (sourceHiveTable == null) {
            throw new StarRocksConnectorException(
                    "Source table %s not found", sourceTable);
        }

        // Ensure the source table is a Hive table
        if (!sourceHiveTable.isHiveTable()) {
            throw new StarRocksConnectorException(
                    "Source table %s is not a Hive table. Only Hive tables are supported as source tables.", sourceTable);
        }

        // Use Hive table partition pruning to get filtered partition names
        List<PartitionKey> filteredPartitionKeys = PartitionUtil.getFilteredPartitionKeys(context.context(),
                sourceHiveTable, context.clause().getWhere());
        GetRemoteFilesParams.Builder paramsBuilder;
        List<GetRemoteFilesParams> paramsList = new ArrayList<>();
        if (filteredPartitionKeys == null) {
            // un-partitioned table
            paramsBuilder = GetRemoteFilesParams.newBuilder()
                    .setUseCache(false)
                    .setIsRecursive(recursive);
            paramsList.add(paramsBuilder.build());
        } else if (filteredPartitionKeys.isEmpty()) {
            // all partitions are pruned
            LOGGER.warn("No partitions match the specified filter in source Hive table: {}", sourceTable);
            return;
        } else {
            paramsList.addAll(filteredPartitionKeys.stream().map(p -> GetRemoteFilesParams.newBuilder()
                    .setUseCache(false)
                    .setPartitionKeys(List.of(p))
                    .setIsRecursive(recursive)
                    .build()).toList());
        }


        List<Pair<String, List<RemoteFileInfo>>> partitionRemoteFiles = new ArrayList<>();
        List<String> partitionColumnNames = sourceHiveTable.getPartitionColumnNames();
        try {
            for (GetRemoteFilesParams params : paramsList) {
                List<RemoteFileInfo> remoteFiles = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        getRemoteFiles(sourceHiveTable, params);
                partitionRemoteFiles.add(Pair.create(params.getPartitionKeys() == null ? "" :
                                params.getPartitionKeys().stream().map(partitionKey ->
                                                PartitionUtil.toHivePartitionName(partitionColumnNames, partitionKey)).
                                        collect(Collectors.joining()),
                        remoteFiles));
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    "Failed to get files from source Hive table %s: %s", sourceTable, e.getMessage(), e);
        }

        if (partitionRemoteFiles.isEmpty()) {
            LOGGER.warn("No files found in source Hive table: {}", sourceTable);
            return;
        }

        // Convert remote files to Iceberg DataFiles
        List<DataFile> dataFiles = new ArrayList<>();
        for (Pair<String, List<RemoteFileInfo>> partitionRemoteFileInfo : partitionRemoteFiles) {
            String partitionName = partitionRemoteFileInfo.first;
            List<RemoteFileInfo> remoteFileInfos = partitionRemoteFileInfo.second;

            for (RemoteFileInfo remoteFileInfo : remoteFileInfos) {
                List<DataFile> partitionDataFiles = createDataFilesFromPartition(context, table, partitionName, remoteFileInfo);
                if (partitionDataFiles != null) {
                    dataFiles.addAll(partitionDataFiles);
                }
            }
        }

        if (dataFiles.isEmpty()) {
            LOGGER.warn("No valid data files found after filtering in source Hive table: {}", sourceTable);
            return;
        }

        // Add the files to the target Iceberg table
        AppendFiles appendFiles = transaction.newAppend();
        for (DataFile dataFile : dataFiles) {
            appendFiles.appendFile(dataFile);
        }

        // Commit the transaction
        appendFiles.commit();
        LOGGER.info("Successfully added {} files from source Hive table {} to Iceberg table",
                dataFiles.size(), sourceTable);
    }
}
