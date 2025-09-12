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

import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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
import java.util.Map;
import java.util.Set;

import static com.starrocks.connector.iceberg.IcebergUtil.fileName;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;

public class RemoveOrphanFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveOrphanFilesProcedure.class);

    private static final Duration DEFAULT_RETENTION_THRESHOLD = Duration.ofDays(7);
    private static final int DELETE_BATCH_SIZE = 1000;

    private static final String PROCEDURE_NAME = "remove_orphan_files";

    public static final String OLDER_THAN = "older_than";

    private static final RemoveOrphanFilesProcedure INSTANCE = new RemoveOrphanFilesProcedure();

    public static RemoveOrphanFilesProcedure getInstance() {
        return INSTANCE;
    }

    private RemoveOrphanFilesProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(OLDER_THAN, Type.DATETIME, false)
                ),
                IcebergTableOperation.REMOVE_ORPHAN_FILES
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() > 1) {
            throw new StarRocksConnectorException("invalid args. only support " +
                    "`older_than` in the remove orphan files operation");
        }

        long olderThanMillis;
        ConstantOperator olderThanArg = args.get(OLDER_THAN);
        if (olderThanArg == null) {
            LocalDateTime time = LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
            olderThanMillis = time.minus(DEFAULT_RETENTION_THRESHOLD).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
            LocalDateTime time = olderThanArg.castTo(Type.DATETIME).
                    map(ConstantOperator::getDatetime).orElseThrow(() ->
                            new StarRocksConnectorException("invalid argument type for %s, expected DATETIME", OLDER_THAN));
            olderThanMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
        }

        Table table = context.table();
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

        scanAndDeleteInvalidFiles(table.location(), olderThanMillis, validFileNames, context.hdfsEnvironment());
    }

    private ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest) {
        return switch (manifest.content()) {
            case DATA -> ManifestFiles.read(manifest, table.io());
            case DELETES -> ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
        };
    }

    private void scanAndDeleteInvalidFiles(String tableLocation, long expiration, Set<String> validFiles,
                                           HdfsEnvironment hdfsEnvironment) {
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

    private void deleteFiles(FileSystem fs, List<Path> files) {
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