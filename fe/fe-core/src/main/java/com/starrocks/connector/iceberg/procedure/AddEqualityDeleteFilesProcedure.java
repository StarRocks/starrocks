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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Registers existing files as Iceberg equality delete files, the delete-side counterpart of add_files.
 * Each file under {@code location} must contain every column named in {@code equality_columns}; a row of
 * the file deletes the table rows whose equality columns equal it. The files are discovered and
 * partitioned the same way as add_files ({@code col=value} directories, identity partitions only), and
 * are committed in a new snapshot, so they apply to the data already in the table.
 */
public class AddEqualityDeleteFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddEqualityDeleteFilesProcedure.class);

    private static final String PROCEDURE_NAME = "add_equality_delete_files";

    public static final String LOCATION = "location";
    public static final String FILE_FORMAT = "file_format";
    public static final String EQUALITY_COLUMNS = "equality_columns";
    public static final String RECURSIVE = "recursive";

    // Types whose file representations the reader converts to the table type without loss, so a delete file
    // either matches exactly or is rejected here. Others (decimal scale, timestamp units, ...) are not checked.
    private static final Set<Type.TypeID> SUPPORTED_EQUALITY_TYPES =
            Set.of(Type.TypeID.INTEGER, Type.TypeID.LONG, Type.TypeID.STRING, Type.TypeID.DATE);

    // Iceberg writes the field id of an ORC column as this type attribute
    private static final String ORC_ICEBERG_ID_ATTRIBUTE = "iceberg.id";
    // Iceberg stores TIME as an ORC LONG with this attribute set to TIME; the reader then converts the value
    private static final String ORC_ICEBERG_LONG_TYPE_ATTRIBUTE = "iceberg.long-type";

    private static final AddEqualityDeleteFilesProcedure INSTANCE = new AddEqualityDeleteFilesProcedure();

    public static AddEqualityDeleteFilesProcedure getInstance() {
        return INSTANCE;
    }

    private AddEqualityDeleteFilesProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(LOCATION, VarcharType.VARCHAR, true),
                        new NamedArgument(FILE_FORMAT, VarcharType.VARCHAR, true),
                        new NamedArgument(EQUALITY_COLUMNS, VarcharType.VARCHAR, true),
                        new NamedArgument(RECURSIVE, BooleanType.BOOLEAN, false)
                ),
                IcebergTableOperation.ADD_EQUALITY_DELETE_FILES
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        ConstantOperator locationArg = args.get(LOCATION);
        ConstantOperator fileFormatArg = args.get(FILE_FORMAT);
        ConstantOperator equalityColumnsArg = args.get(EQUALITY_COLUMNS);
        if (locationArg == null || fileFormatArg == null || equalityColumnsArg == null) {
            throw new StarRocksConnectorException("'%s', '%s' and '%s' must be provided for %s operation",
                    LOCATION, FILE_FORMAT, EQUALITY_COLUMNS, PROCEDURE_NAME);
        }

        String fileFormat = fileFormatArg.getVarchar().toLowerCase();
        if (!fileFormat.equals("parquet") && !fileFormat.equals("orc")) {
            throw new StarRocksConnectorException(
                    "Unsupported file format: %s. Supported formats are: parquet, orc", fileFormat);
        }

        boolean recursive = true;
        ConstantOperator recursiveArg = args.get(RECURSIVE);
        if (recursiveArg != null) {
            recursive = recursiveArg.getBoolean();
        }

        Table table = context.table();
        int formatVersion = TableUtil.formatVersion(table);
        if (formatVersion < 2) {
            throw new StarRocksConnectorException(
                    "Equality delete files require Iceberg format version 2 or higher, but table is v%d",
                    formatVersion);
        }
        PartitionSpec spec = table.spec();
        if (spec.isPartitioned() && spec.fields().stream().anyMatch(f -> !f.transform().isIdentity())) {
            throw new StarRocksConnectorException(
                    "Adding equality delete files to partitioned tables with non-identity partitioning " +
                            "is not supported");
        }
        checkNoLiveDataInOtherSpecs(table);
        // with this session variable the scan reads equality deletes that span several specs
        ConnectContext connectContext = context.context();
        if (connectContext == null ||
                !connectContext.getSessionVariable().enableReadIcebergEqDeleteWithPartitionEvolution()) {
            checkNoEqualityDeletesInOtherSpecs(table);
        }
        int[] equalityFieldIds = resolveEqualityFieldIds(table.schema(), equalityColumnsArg.getVarchar());

        String location = locationArg.getVarchar();
        List<DeleteFile> deleteFiles = new ArrayList<>();
        try {
            List<DataFile> files = AddFilesProcedure.getInstance()
                    .discoverFilesFromLocation(context, table, location, recursive, fileFormat);
            Configuration conf = context.hdfsEnvironment().getConfiguration();
            for (DataFile file : files) {
                checkPartitionPath(spec, file.location());
                checkEqualityColumns(conf, table.schema(), file, fileFormat, equalityFieldIds);
                deleteFiles.add(toEqualityDeleteFile(spec, file, equalityFieldIds));
            }
        } catch (StarRocksConnectorException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Failed to execute {} procedure", PROCEDURE_NAME, e);
            throw new StarRocksConnectorException("Failed to add equality delete files: %s", e.getMessage(), e);
        }

        if (deleteFiles.isEmpty()) {
            LOGGER.warn("No equality delete files found at location: {}", location);
            return null;
        }

        RowDelta rowDelta = context.transaction().newRowDelta();
        deleteFiles.forEach(rowDelta::addDeletes);
        rowDelta.commit();
        LOGGER.info("Successfully added {} equality delete files to table", deleteFiles.size());
        return null;
    }

    // A partitioned delete only applies to data files of the same spec and partition, so deletes registered
    // under the current spec would never reach data still written under an older spec. An unpartitioned
    // current spec is fine: its deletes are global.
    private static void checkNoLiveDataInOtherSpecs(Table table) {
        PartitionSpec spec = table.spec();
        Snapshot snapshot = table.currentSnapshot();
        if (spec.isUnpartitioned() || snapshot == null) {
            return;
        }
        for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
            if (manifest.partitionSpecId() != spec.specId() && (manifest.hasAddedFiles() || manifest.hasExistingFiles())) {
                throw new StarRocksConnectorException(
                        "Table has data files written under partition spec %d, which equality deletes registered " +
                                "under the current spec %d would not apply to", manifest.partitionSpecId(), spec.specId());
            }
        }
    }

    // The scan rejects a table whose equality deletes span more than one partition spec unless
    // enable_read_iceberg_equality_delete_with_partition_evolution is set, so adding current-spec deletes next to
    // live older-spec ones would make a readable table fail every full scan.
    private static void checkNoEqualityDeletesInOtherSpecs(Table table) {
        PartitionSpec spec = table.spec();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return;
        }
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            if (manifest.partitionSpecId() == spec.specId() ||
                    !(manifest.hasAddedFiles() || manifest.hasExistingFiles())) {
                continue;
            }
            try (ManifestReader<DeleteFile> reader =
                         ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile deleteFile : reader) {
                    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                        throw new StarRocksConnectorException(
                                "Table has equality delete files written under partition spec %d; adding ones under " +
                                        "the current spec %d would mix specs, which the scan does not support",
                                manifest.partitionSpecId(), spec.specId());
                    }
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to read delete manifest %s: %s", manifest.path(),
                        e.getMessage(), e);
            }
        }
    }

    // A file whose path lacks a partition field would get a null value for it, so the delete would only apply to
    // the null partition. Partition values come from col=value path components.
    private static void checkPartitionPath(PartitionSpec spec, String location) {
        if (spec.isUnpartitioned()) {
            return;
        }
        Set<String> pathKeys = new HashSet<>();
        for (String part : location.split("/")) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                pathKeys.add(part.substring(0, eq));
            }
        }
        for (PartitionField field : spec.fields()) {
            if (!pathKeys.contains(field.name())) {
                throw new StarRocksConnectorException(
                        "Equality delete file %s has no %s=<value> directory for partition field %s",
                        location, field.name(), field.name());
            }
        }
    }

    static int[] resolveEqualityFieldIds(Schema schema, String equalityColumns) {
        List<Integer> fieldIds = new ArrayList<>();
        for (String name : equalityColumns.split(",")) {
            String column = name.trim();
            if (column.isEmpty()) {
                continue;
            }
            Types.NestedField field = schema.caseInsensitiveFindField(column);
            if (field == null || schema.columns().stream().noneMatch(c -> c.fieldId() == field.fieldId())) {
                throw new StarRocksConnectorException("Equality column %s is not a top-level column of the table",
                        column);
            }
            if (!SUPPORTED_EQUALITY_TYPES.contains(field.type().typeId())) {
                throw new StarRocksConnectorException(
                        "Equality column %s has type %s; only INT, BIGINT, STRING and DATE columns are supported",
                        column, field.type());
            }
            if (!fieldIds.contains(field.fieldId())) {
                fieldIds.add(field.fieldId());
            }
        }
        if (fieldIds.isEmpty()) {
            throw new StarRocksConnectorException("'%s' must name at least one column", EQUALITY_COLUMNS);
        }
        return fieldIds.stream().mapToInt(Integer::intValue).toArray();
    }

    // A file that carries field ids is read by field id: Iceberg readers switch to ids when any column has one,
    // and the BE parquet reader when its first column has one. An equality column without the table's field id
    // would then be read as NULL, so the deletes would silently not apply. A file without ids is read by name.
    // A column the reader cannot read losslessly as the table's type would fail every scan that applies the file,
    // or match the wrong rows.
    private static void checkEqualityColumns(Configuration conf, Schema schema, DataFile file, String fileFormat,
                                             int[] equalityFieldIds) throws IOException {
        Map<String, FileColumn> fileColumns = fileFormat.equals("parquet") ?
                parquetColumns(conf, file.location()) : orcColumns(conf, file.location());
        List<Integer> fileFieldIds = fileColumns.values().stream().map(FileColumn::fieldId)
                .filter(Objects::nonNull).toList();
        boolean fileHasFieldIds = !fileFieldIds.isEmpty();
        // the reader keeps the first column for a field id, which may not be the equality column of that name
        if (fileFieldIds.size() != new HashSet<>(fileFieldIds).size()) {
            throw new StarRocksConnectorException("Equality delete file %s has duplicate field ids", file.location());
        }
        for (int fieldId : equalityFieldIds) {
            String column = schema.findColumnName(fieldId);
            FileColumn fileColumn = fileColumns.get(column);
            if (fileColumn == null) {
                throw new StarRocksConnectorException("Equality delete file %s does not contain equality column %s",
                        file.location(), column);
            }
            if (fileHasFieldIds && (fileColumn.fieldId() == null || fileColumn.fieldId() != fieldId)) {
                throw new StarRocksConnectorException(
                        "Equality delete file %s has field id %s for column %s, but the table's field id is %d",
                        file.location(), fileColumn.fieldId(), column, fieldId);
            }
            Type.TypeID typeId = schema.findType(fieldId).typeId();
            if (!fileColumn.readableAs().contains(typeId)) {
                throw new StarRocksConnectorException(
                        "Equality delete file %s stores column %s as %s, which cannot be read as the table's type %s",
                        file.location(), column, fileColumn.fileType(), schema.findType(fieldId));
            }
        }
    }

    // fieldId is null for a column written without one; readableAs holds the supported equality column types the
    // reader converts the column to without loss.
    private record FileColumn(Integer fieldId, String fileType, Set<Type.TypeID> readableAs) {
    }

    private static Map<String, FileColumn> parquetColumns(Configuration conf, String location) throws IOException {
        Map<String, FileColumn> columns = new HashMap<>();
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(location), conf))) {
            for (org.apache.parquet.schema.Type column : reader.getFileMetaData().getSchema().getFields()) {
                Set<Type.TypeID> readableAs = Set.of();
                String fileType = "GROUP";
                if (column.isPrimitive()) {
                    PrimitiveType primitive = column.asPrimitiveType();
                    readableAs = parquetReadableAs(primitive.getPrimitiveTypeName(),
                            primitive.getLogicalTypeAnnotation());
                    fileType = primitive.getPrimitiveTypeName() + (primitive.getLogicalTypeAnnotation() == null ?
                            "" : " (" + primitive.getLogicalTypeAnnotation() + ")");
                }
                columns.put(column.getName(), new FileColumn(
                        column.getId() == null ? null : column.getId().intValue(), fileType, readableAs));
            }
        }
        return columns;
    }

    private static Map<String, FileColumn> orcColumns(Configuration conf, String location) throws IOException {
        Map<String, FileColumn> columns = new HashMap<>();
        try (Reader reader = OrcFile.createReader(new Path(location), OrcFile.readerOptions(conf))) {
            TypeDescription orcSchema = reader.getSchema();
            for (int i = 0; i < orcSchema.getFieldNames().size(); i++) {
                TypeDescription child = orcSchema.getChildren().get(i);
                String id = child.getAttributeValue(ORC_ICEBERG_ID_ATTRIBUTE);
                boolean icebergTime = "TIME".equals(child.getAttributeValue(ORC_ICEBERG_LONG_TYPE_ATTRIBUTE));
                columns.put(orcSchema.getFieldNames().get(i), new FileColumn(id == null ? null : Integer.parseInt(id),
                        child.getCategory().name() + (icebergTime ? " (TIME)" : ""),
                        icebergTime ? Set.of() : orcReadableAs(child.getCategory())));
            }
        }
        return columns;
    }

    // Plain or signed integer annotations only: an INT32 annotated as a date, decimal or unsigned integer holds
    // a different value than the integer the table column compares with.
    private static Set<Type.TypeID> parquetReadableAs(PrimitiveTypeName type, LogicalTypeAnnotation annotation) {
        boolean signedInteger = annotation == null ||
                (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation &&
                        intAnnotation.isSigned());
        return switch (type) {
            case INT32 -> annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ?
                    Set.of(Type.TypeID.DATE) :
                    signedInteger ? Set.of(Type.TypeID.INTEGER, Type.TypeID.LONG) : Set.of();
            case INT64 -> signedInteger ? Set.of(Type.TypeID.LONG) : Set.of();
            case BINARY -> annotation == null || annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation ?
                    Set.of(Type.TypeID.STRING) : Set.of();
            case FIXED_LEN_BYTE_ARRAY -> annotation == null ? Set.of(Type.TypeID.STRING) : Set.of();
            default -> Set.of();
        };
    }

    // CHAR is left out: its values are padded to the declared length.
    private static Set<Type.TypeID> orcReadableAs(TypeDescription.Category category) {
        return switch (category) {
            case BYTE, SHORT, INT -> Set.of(Type.TypeID.INTEGER, Type.TypeID.LONG);
            case LONG -> Set.of(Type.TypeID.LONG);
            case STRING, VARCHAR -> Set.of(Type.TypeID.STRING);
            case DATE -> Set.of(Type.TypeID.DATE);
            default -> Set.of();
        };
    }

    // Only the record count is kept: the column stats from the add_files helpers are not reliable enough for
    // delete-file pruning (ORC value counts exclude nulls, timestamp bounds ignore the physical unit), and a
    // wrongly pruned delete file would silently not apply.
    private static DeleteFile toEqualityDeleteFile(PartitionSpec spec, DataFile file, int[] equalityFieldIds) {
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec)
                .ofEqualityDeletes(equalityFieldIds)
                .withPath(file.location())
                .withFormat(file.format())
                .withFileSizeInBytes(file.fileSizeInBytes())
                .withRecordCount(file.recordCount());
        if (spec.isPartitioned()) {
            builder.withPartition(file.partition());
        }
        return builder.build();
    }
}
