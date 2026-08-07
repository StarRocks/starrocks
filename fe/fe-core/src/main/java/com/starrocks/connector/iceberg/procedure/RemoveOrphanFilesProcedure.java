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

import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.iceberg.IcebergUtil.fileName;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;

public class RemoveOrphanFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveOrphanFilesProcedure.class);

    private static final Duration DEFAULT_RETENTION_THRESHOLD = Duration.ofDays(7);
    private static final int DELETE_BATCH_SIZE = 1000;

    private static final String PROCEDURE_NAME = "remove_orphan_files";

    private static final String MIN_RETENTION_CONF = "iceberg_remove_orphan_files_min_retention_seconds";

    // We only need each content file's path to build the set of reachable file names
    private static final List<String> MANIFEST_ENTRY_PROJECTION = List.of("file_path");

    public static final String OLDER_THAN = "older_than";
    public static final String LOCATION = "location";

    private static final RemoveOrphanFilesProcedure INSTANCE = new RemoveOrphanFilesProcedure();

    public static RemoveOrphanFilesProcedure getInstance() {
        return INSTANCE;
    }

    private RemoveOrphanFilesProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(OLDER_THAN, DateType.DATETIME, false),
                        new NamedArgument(LOCATION, VarcharType.VARCHAR, false)
                ),
                IcebergTableOperation.REMOVE_ORPHAN_FILES
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() > 2) {
            throw new StarRocksConnectorException("invalid args. only support " +
                    "`older_than` and `location` in the remove orphan files operation");
        }

        IcebergMaintenanceTaskStats stats = context.stats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);

        long olderThanMillis;
        ConstantOperator olderThanArg = args.get(OLDER_THAN);
        if (olderThanArg == null) {
            LocalDateTime time = LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
            olderThanMillis = time.minus(DEFAULT_RETENTION_THRESHOLD).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
            LocalDateTime time = olderThanArg.castTo(DateType.DATETIME).
                    map(ConstantOperator::getDatetime).orElseThrow(() ->
                            new StarRocksConnectorException("invalid argument type for %s, expected DATETIME", OLDER_THAN));
            olderThanMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
            validateRetentionInterval(olderThanMillis);
        }

        Table table = context.table();
        if (table.currentSnapshot() == null) {
            return null;
        }
        if (table.location() == null || table.location().isEmpty()) {
            throw new StarRocksConnectorException("table location is empty");
        }

        String location;
        ConstantOperator locationArg = args.get(LOCATION);
        if (locationArg != null) {
            location = validateAndResolveScanLocation(locationArg.getVarchar(), table.location());
        } else {
            location = table.location();
        }

        // A null executor (e.g. the manual ALTER path) makes the reads run sequentially in this
        // thread; the auto-maintenance path passes a worker pool to read snapshots in parallel.
        ExecutorService executor = context.executorService();

        // Collect the set of files still referenced by any snapshot.
        Set<String> processedManifestFilePaths = ConcurrentHashMap.newKeySet();
        Set<String> validFileNames = ConcurrentHashMap.newKeySet();
        Set<ManifestFile> manifestsToRead = ConcurrentHashMap.newKeySet();

        // Phase 1 (parallel over snapshots): read each snapshot's manifest list, record manifest-list
        // names and the deduplicated set of manifests.
        parallelizable(table.snapshots(), executor)
                .run(snapshot -> {
                    if (snapshot.manifestListLocation() != null) {
                        validFileNames.add(fileName(snapshot.manifestListLocation()));
                    }
                    try (CloseableIterable<ManifestFile> manifests =
                                 IcebergUtil.readManifests(snapshot, table.io())) {
                        for (ManifestFile manifest : manifests) {
                            if (processedManifestFilePaths.add(manifest.path())) {
                                validFileNames.add(fileName(manifest.path()));
                                manifestsToRead.add(manifest.copy());
                            }
                        }
                    } catch (IOException e) {
                        throw new StarRocksConnectorException(
                                "Unable to read manifests for snapshot " + snapshot.snapshotId(), e);
                    }
                });

        // Phase 2 (parallel over manifests): read each unique manifest's content.
        parallelizable(manifestsToRead, executor)
                .run(manifest -> {
                    try (ManifestReader<? extends ContentFile<?>> manifestReader =
                                 readerForManifest(table, manifest)) {
                        for (ContentFile<?> contentFile : manifestReader) {
                            validFileNames.add(fileName(contentFile.location()));
                        }
                    } catch (IOException e) {
                        throw new StarRocksConnectorException(
                                "Unable to list manifest file content from " + manifest.path(), e);
                    }
                });

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        validFileNames.add("version-hint.text");

        scanAndDeleteInvalidFiles(location, olderThanMillis, validFileNames, context.hdfsEnvironment(), stats);
        stats.setExecuted(true);
        return null;
    }

    /**
     * Rejects an `older_than` that leaves too small a retention window.
     * <p>
     * The valid file names are collected from the table state loaded above and storage is only listed
     * afterwards, so every file that appears in between - including the data files of an INSERT that has not
     * committed yet - looks orphaned. The modification time cutoff is what keeps those files safe, so it has
     * to stay far enough in the past.
     */
    private static void validateRetentionInterval(long olderThanMillis) {
        long minRetentionSeconds = Config.iceberg_remove_orphan_files_min_retention_seconds;
        if (minRetentionSeconds < 0) {
            throw new StarRocksConnectorException("invalid FE configuration `%s`: %d, it must not be negative. A " +
                    "negative minimum retention stretches the window into the future and would admit exactly the %s " +
                    "values this check exists to reject.", MIN_RETENTION_CONF, minRetentionSeconds, OLDER_THAN);
        }
        if (System.currentTimeMillis() - olderThanMillis < Duration.ofSeconds(minRetentionSeconds).toMillis()) {
            throw new StarRocksConnectorException("invalid argument value for %s, it must be at least the minimum " +
                    "retention of %d seconds before now. Removing orphan files with a shorter interval may delete " +
                    "files that concurrent writes have not committed yet and leave the table unreadable. Adjust the " +
                    "FE configuration `%s` if no concurrent write can be affected.",
                    OLDER_THAN, minRetentionSeconds, MIN_RETENTION_CONF);
        }
    }

    /**
     * Validates that the given location is non-empty and is the table root or a subdirectory of it
     * Returns the normalized path for use in scanning.
     */
    private static String validateAndResolveScanLocation(String location, String tableLocation) {
        if (location == null || location.isEmpty()) {
            throw new StarRocksConnectorException("invalid argument value for %s, expected non-empty string",
                    LOCATION);
        }

        if (tableLocation.equals(location)) {
            return location;
        }

        URI tableUri = new Path(tableLocation).toUri();
        URI locationUri = new Path(location).toUri();
        String tablePath = stripTrailingSlash(tableUri.getPath());
        String locationPath = stripTrailingSlash(locationUri.getPath());

        if (!Objects.equals(tableUri.getScheme(), locationUri.getScheme()) ||
                !Objects.equals(tableUri.getAuthority(), locationUri.getAuthority()) ||
                !locationPath.startsWith(tablePath + Path.SEPARATOR)) {
            throw new StarRocksConnectorException("invalid argument value for %s, location must be a subdirectory of " +
                    "table location %s, got %s", LOCATION, tableLocation, location);
        }

        return locationUri.toString();
    }

    private static String stripTrailingSlash(String path) {
        if (path == null || path.isEmpty()) {
            return path;
        }
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    // Build a Tasks runner over the items; use the executor when present, otherwise run
    // sequentially in the calling thread (a null executor means the caller did not opt into
    // parallelism). Reads keep the default throwFailureWhenFinished (fail-fast).
    private static <T> Tasks.Builder<T> parallelizable(Iterable<T> items, ExecutorService executor) {
        Tasks.Builder<T> builder = Tasks.foreach(items);
        return executor != null ? builder.executeWith(executor) : builder;
    }

    private ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest) {
        return switch (manifest.content()) {
            case DATA -> ManifestFiles.read(manifest, table.io()).select(MANIFEST_ENTRY_PROJECTION);
            case DELETES ->
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs()).select(MANIFEST_ENTRY_PROJECTION);
        };
    }

    private void scanAndDeleteInvalidFiles(String tableLocation, long expiration, Set<String> validFiles,
                                           HdfsEnvironment hdfsEnvironment, IcebergMaintenanceTaskStats stats) {
        try {
            URI uri = new Path(tableLocation).toUri();
            FileSystem fileSystem = FileSystem.get(uri, hdfsEnvironment.getConfiguration());
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(new Path(tableLocation), true);
            List<FileStatus> filesToDelete = new ArrayList<>();
            while (allFiles.hasNext()) {
                LocatedFileStatus entry = allFiles.next();
                if (entry.getModificationTime() < expiration && !validFiles.contains(entry.getPath().getName())) {
                    filesToDelete.add(entry);
                    stats.addOrphanDetected(1);
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        deleteFiles(fileSystem, filesToDelete, stats);
                        filesToDelete.clear();
                    }
                }
            }
            if (!filesToDelete.isEmpty()) {
                deleteFiles(fileSystem, filesToDelete, stats);
                filesToDelete.clear();
            }
        } catch (IOException e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            markPartialIfStarted(stats);
            throw new StarRocksConnectorException("Failed accessing data: " + msg, e);
        }
    }

    private void deleteFiles(FileSystem fs, List<FileStatus> files, IcebergMaintenanceTaskStats stats) {
        files.forEach(file -> {
            try {
                if (fs.delete(file.getPath(), false)) {
                    stats.addOrphanRemoved(1, file.getLen());
                    LOGGER.debug("Deleted file {}", file.getPath());
                } else {
                    LOGGER.warn("Delete returned false for orphan file {}, not counting it as removed",
                            file.getPath());
                }
            } catch (IOException e) {
                LOGGER.error("Failed to delete file {}", file.getPath(), e);
                markPartialIfStarted(stats);
                throw new StarRocksConnectorException("Failed to delete file " + file.getPath(), e);
            }
        });
    }

    private static void markPartialIfStarted(IcebergMaintenanceTaskStats stats) {
        if (stats.getOrphanFilesRemoved() > 0) {
            stats.setPartiallyApplied(true);
        }
    }
}