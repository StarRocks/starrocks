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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.BranchOptions;
import com.starrocks.connector.ConnectorAlterTableExecutor;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.TagOptions;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateOrReplaceBranchClause;
import com.starrocks.sql.ast.CreateOrReplaceTagClause;
import com.starrocks.sql.ast.DropBranchClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropTagClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.connector.iceberg.IcebergUtil.fileName;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;

public class IcebergAlterTableExecutor extends ConnectorAlterTableExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergAlterTableExecutor.class);
    private Table table;
    private IcebergCatalog icebergCatalog;
    private Transaction transaction;
    private HdfsEnvironment hdfsEnvironment;

    private static final int DELETE_BATCH_SIZE = 1000;

    // TODO:Support using session to set default retention_threshold.
    private static final Duration DEFAULT_RETENTION_THRESHOLD = Duration.ofDays(7);

    public IcebergAlterTableExecutor(AlterTableStmt stmt,
            Table table,
            IcebergCatalog icebergCatalog,
            HdfsEnvironment hdfsEnvironment) {
        super(stmt);
        this.table = table;
        this.icebergCatalog = icebergCatalog;
        this.hdfsEnvironment = hdfsEnvironment;
    }

    @Override
    public void applyClauses() throws DdlException {
        transaction = table.newTransaction();
        super.applyClauses();
        transaction.commitTransaction();
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            ColumnPosition pos = clause.getColPos();
            Column column = clause.getColumnDef().toColumn(null);

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

            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            List<Column> columns = clause
                    .getColumnDefs()
                    .stream()
                    .map(columnDef -> columnDef.toColumn(null))
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
            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            String columnName = clause.getColName();
            updateSchema.deleteColumn(columnName).commit();
        });
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        actions.add(() -> {
            ColumnRenameClause columnRenameClause = (ColumnRenameClause) clause;
            UpdateSchema updateSchema = this.transaction.updateSchema();
            updateSchema.renameColumn(columnRenameClause.getColName(), columnRenameClause.getNewColName()).commit();
        });
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            ColumnPosition colPos = clause.getColPos();
            Column column = clause.getColumnDef().toColumn(null);
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

            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitAddFieldClause(AddFieldClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropFieldClause(DropFieldClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateProperties updateProperties = this.transaction.updateProperties();
            Map<String, String> pendingUpdate = clause.getProperties();
            if (pendingUpdate.isEmpty()) {
                throw new StarRocksConnectorException("Modified property is empty");
            }

            for (Map.Entry<String, String> entry : pendingUpdate.entrySet()) {
                Preconditions.checkNotNull(entry.getValue(), "property value cannot be null");
                switch (entry.getKey().toLowerCase()) {
                    case FILE_FORMAT:
                        updateProperties.defaultFormat(FileFormat.fromString(entry.getValue()));
                        break;
                    case LOCATION_PROPERTY:
                        updateProperties.commit();
                        transaction.updateLocation().setLocation(entry.getValue()).commit();
                        break;
                    case COMPRESSION_CODEC:
                        if (!PARQUET_COMPRESSION_TYPE_MAP.containsKey(entry.getValue().toLowerCase(Locale.ROOT))) {
                            throw new StarRocksConnectorException(
                                    "Unsupported compression codec for iceberg connector: " + entry.getValue());
                        }

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
        });
        return null;
    }

    private void updateCodeCompr(UpdateProperties updateProperties, FileFormat fileFormat, String codeCompression) {
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

    @Override
    public Void visitAlterTableCommentClause(AlterTableCommentClause clause, ConnectContext context) {
        AlterTableCommentClause alterTableCommentClause = (AlterTableCommentClause) clause;
        UpdateProperties updateProperties = this.transaction.updateProperties();
        updateProperties.set(COMMENT, alterTableCommentClause.getNewComment()).commit();
        return null;
    }

    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        icebergCatalog.renameTable(tableName.getDb(), tableName.getTbl(), clause.getNewTableName());
        return null;
    }

    @Override
    public Void visitCreateOrReplaceBranchClause(CreateOrReplaceBranchClause clause, ConnectContext context) {
        actions.add(() -> {
            String branchName = clause.getBranchName();
            BranchOptions branchOptions = clause.getBranchOptions();
            boolean create = clause.isCreate();
            boolean replace = clause.isReplace();
            boolean ifNotExists = clause.isIfNotExists();

            Long snapshotId = branchOptions.getSnapshotId().orElse(
                    Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));
            ManageSnapshots manageSnapshots = transaction.manageSnapshots();

            Runnable safeCreateBranch = () -> {
                if (snapshotId == null) {
                    manageSnapshots.createBranch(branchName);
                } else {
                    manageSnapshots.createBranch(branchName, snapshotId);
                }
            };

            boolean refExists = table.refs().get(branchName) != null;
            if (create && replace && !refExists) {
                safeCreateBranch.run();
            } else if (replace) {
                Preconditions.checkArgument(snapshotId != null,
                        "Cannot complete replace branch operation on %s, main has no snapshot", table.name());
                manageSnapshots.replaceBranch(branchName, snapshotId);
            } else {
                if (refExists && ifNotExists) {
                    return;
                }
                safeCreateBranch.run();
            }

            if (branchOptions.getNumSnapshots().isPresent()) {
                manageSnapshots.setMinSnapshotsToKeep(branchName, branchOptions.getNumSnapshots().get());
            }

            if (branchOptions.getSnapshotRetain().isPresent()) {
                manageSnapshots.setMaxSnapshotAgeMs(branchName, branchOptions.getSnapshotRetain().get());
            }

            if (branchOptions.getSnapshotRefRetain().isPresent()) {
                manageSnapshots.setMaxRefAgeMs(branchName, branchOptions.getSnapshotRefRetain().get());
            }

            manageSnapshots.commit();
        });
        return null;
    }

    @Override
    public Void visitCreateOrReplaceTagClause(CreateOrReplaceTagClause clause, ConnectContext context) {
        actions.add(() -> {
            String tagName = clause.getTagName();
            TagOptions tagOptions = clause.getTagOptions();
            boolean create = clause.isCreate();
            boolean replace = clause.isReplace();
            boolean ifNotExists = clause.isIfNotExists();

            Long snapshotId = tagOptions.getSnapshotId().orElse(
                    Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));

            Preconditions.checkArgument(snapshotId != null,
                    "Cannot complete create or replace tag operation on %s, main has no snapshot", table.name());
            ManageSnapshots manageSnapshots = transaction.manageSnapshots();

            boolean refExists = table.refs().get(tagName) != null;

            if (create && replace && !refExists) {
                manageSnapshots.createTag(tagName, snapshotId);
            } else if (replace) {
                manageSnapshots.replaceTag(tagName, snapshotId);
            } else {
                if (refExists && ifNotExists) {
                    return;
                }
                manageSnapshots.createTag(tagName, snapshotId);
            }

            if (tagOptions.getSnapshotRefRetain().isPresent()) {
                manageSnapshots.setMaxRefAgeMs(tagName, tagOptions.getSnapshotRefRetain().get());
            }

            manageSnapshots.commit();
        });
        return null;
    }

    @Override
    public Void visitDropBranchClause(DropBranchClause clause, ConnectContext context) {
        actions.add(() -> {
            String branchName = clause.getBranch();
            boolean ifExists = clause.isIfExists();
            SnapshotRef snapshotRef = table.refs().get(branchName);

            if (snapshotRef != null || !ifExists) {
                transaction.manageSnapshots().removeBranch(branchName).commit();
            }
        });

        return null;
    }

    @Override
    public Void visitDropTagClause(DropTagClause clause, ConnectContext context) {
        actions.add(() -> {
            String tagName = clause.getTag();
            boolean ifExists = clause.isIfExists();
            SnapshotRef snapshotRef = table.refs().get(tagName);

            if (snapshotRef != null || !ifExists) {
                transaction.manageSnapshots().removeTag(tagName).commit();
            }
        });

        return null;
    }

    @Override
    public Void visitAlterTableOperationClause(AlterTableOperationClause clause, ConnectContext context) {
        IcebergTableOperation op = IcebergTableOperation.fromString(clause.getTableOperationName());
        if (op == IcebergTableOperation.UNKNOWN) {
            throw new StarRocksConnectorException("Unknown iceberg table operation : %s", clause.getTableOperationName());
        }
        List<ConstantOperator> args = clause.getArgs();

        switch (op) {
            case FAST_FORWARD:
                fastForward(args);
                break;
            case CHERRYPICK_SNAPSHOT:
                cherryPickSnapshot(args);
                break;
            case EXPIRE_SNAPSHOTS:
                expireSnapshots(args);
                break;
            case REMOVE_ORPHAN_FILES:
                removeOrphanFiles(args);
                break;
            default:
                throw new StarRocksConnectorException("Unsupported table operation %s", op);
        }

        return null;
    }

    private void fastForward(List<ConstantOperator> args) {
        if (args.size() != 2) {
            throw new StarRocksConnectorException("invalid args. fast forward must contain `from branch` and `to branch`");
        }

        String from = args.get(0)
                .castTo(Type.VARCHAR)
                .map(ConstantOperator::getChar)
                .orElseThrow(() -> new StarRocksConnectorException("invalid arg %s", args.get(0)));

        String to = args.get(1)
                .castTo(Type.VARCHAR)
                .map(ConstantOperator::getChar)
                .orElseThrow(() -> new StarRocksConnectorException("invalid arg %s", args.get(1)));

        actions.add(() -> {
            transaction.manageSnapshots().fastForwardBranch(from, to).commit();
        });
    }

    private void cherryPickSnapshot(List<ConstantOperator> args) {
        if (args.size() != 1) {
            throw new StarRocksConnectorException("invalid args. cherrypick snapshot must contain `snapshot id`");
        }

        long snapshotId = args.get(0)
                .castTo(Type.BIGINT)
                .map(ConstantOperator::getBigint)
                .orElseThrow(() -> new StarRocksConnectorException("invalid arg %s", args.get(0)));

        actions.add(() -> {
            transaction.manageSnapshots().cherrypick(snapshotId).commit();
        });
    }

    private void expireSnapshots(List<ConstantOperator> args) {
        if (args.size() > 1) {
            throw new StarRocksConnectorException("invalid args. only support `older_than` in the expire snapshot operation");
        }

        long olderThanMillis;
        if (args.isEmpty()) {
            olderThanMillis = -1L;
        } else {
            LocalDateTime time = Optional.ofNullable(args.get(0))
                    .flatMap(arg -> arg.castTo(Type.DATETIME).map(ConstantOperator::getDatetime))
                    .orElseThrow(() -> new StarRocksConnectorException("invalid arg %s", args.get(0)));
            olderThanMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
        }

        actions.add(() -> {
            ExpireSnapshots expireSnapshots = transaction.expireSnapshots();
            if (olderThanMillis != -1) {
                expireSnapshots = expireSnapshots.expireOlderThan(olderThanMillis);
            }
            expireSnapshots.commit();
        });
    }

    private void removeOrphanFiles(List<ConstantOperator> args) {
        if (args.size() > 1) {
            throw new StarRocksConnectorException("invalid args. only support " +
                    "`older_than` in the remove orphan files operation");
        }

        long olderThanMillis;
        if (args.isEmpty()) {
            LocalDateTime time = LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
            olderThanMillis = time.minus(DEFAULT_RETENTION_THRESHOLD).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
            LocalDateTime time = Optional.ofNullable(args.get(0))
                    .flatMap(arg -> arg.castTo(Type.DATETIME)
                    .map(ConstantOperator::getDatetime))
                    .orElseThrow(() -> new StarRocksConnectorException("invalid arg %s", args.get(0)));
            olderThanMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
        }

        if (table.currentSnapshot() == null) {
            return;
        }

        Set<String> processedManifestFilePaths = new HashSet<>();
        Set<String> validFileNames = new HashSet<>();

        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.manifestListLocation() != null) {
                validFileNames.add(fileName(snapshot.manifestListLocation()));
            }

            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    continue;
                }

                validFileNames.add(fileName(manifest.path()));
                try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(table, manifest)) {
                    for (ContentFile<?> contentFile : manifestReader) {
                        validFileNames.add(fileName(contentFile.location()));
                    }
                } catch (IOException e) {
                    throw new StarRocksConnectorException("Unable to list manifest file content from " + manifest.path(), e);
                }
            }
        }

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        validFileNames.add("version-hint.text");

        actions.add(() -> scanAndDeleteInvalidFiles(table.location(), olderThanMillis, validFileNames));
    }

    private static ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest) {
        return switch (manifest.content()) {
            case DATA -> ManifestFiles.read(manifest, table.io());
            case DELETES -> ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
        };
    }

    private void scanAndDeleteInvalidFiles(String tableLocation, long expiration, Set<String> validFiles) {
        try {
            URI uri = new Path(tableLocation).toUri();
            FileSystem fileSystem = FileSystem.get(uri, hdfsEnvironment.getConfiguration());
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(new Path(tableLocation), true);
            List<Path> filesToDelete = new ArrayList<>();
            while (allFiles.hasNext()) {
                LocatedFileStatus entry = allFiles.next();
                FileStatus status = fileSystem.getFileStatus(entry.getPath());
                if (status.getModificationTime() < expiration && !validFiles.contains(entry.getPath().getName())) {
                    filesToDelete.add(entry.getPath());
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        deleteFiles(fileSystem, filesToDelete);
                        filesToDelete.clear();
                    }
                }
            }
            if (!filesToDelete.isEmpty()) {
                deleteFiles(fileSystem, filesToDelete);
                filesToDelete.clear();
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed accessing data: ", e);
        }
    }

    // TODO:implement deleteFiles in FsUtils
    @VisibleForTesting
    public static void deleteFiles(FileSystem fs, List<Path> files) {
        files.forEach(file -> {
            try {
                fs.delete(file, false);
                LOGGER.debug("Deleted file {}", file);
            } catch (IOException e) {
                LOGGER.error("Failed to delete file {}", file, e);
                throw new StarRocksConnectorException("Failed to delete file " + file, e);
            }
        });
    }
}
