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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT;

/**
 * the difference with org.apache.iceberg.BaseMetastoreTableOperations is support initWithMetadata,
 * so that we don't need to parse tableMetadata every time.
 *
 */

public abstract class BaseHmsTableOps implements TableOperations {
    private static final Logger LOG = LogManager.
            getLogger(BaseHmsTableOps.class);

    public static final String TABLE_TYPE_PROP = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    public static final String METADATA_LOCATION_PROP = "metadata_location";
    public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";

    private static final String METADATA_FOLDER_NAME = "metadata";

    private TableMetadata currentMetadata = null;
    private String currentMetadataLocation = null;
    private boolean shouldRefresh = true;
    private int version = -1;

    protected BaseHmsTableOps() {}

    public void initWithMetadata(TableMetadata tableMetadata) {
        Preconditions.checkState(currentMetadata == null, "already initialized");
        currentMetadata = tableMetadata;
        currentMetadataLocation = tableMetadata.metadataFileLocation();
        shouldRefresh = false;
        version = parseVersion(currentMetadataLocation);
    }

    /**
     * The full name of the table used for logging purposes only. For example for HiveTableOperations
     * it is catalogName + "." + database + "." + table.
     *
     * @return The full name
     */
    protected abstract String tableName();

    @Override
    public TableMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    public String currentMetadataLocation() {
        return currentMetadataLocation;
    }

    public int currentVersion() {
        return version;
    }

    @Override
    public TableMetadata refresh() {
        boolean currentMetadataWasAvailable = currentMetadata != null;
        try {
            doRefresh();
        } catch (NoSuchTableException e) {
            if (currentMetadataWasAvailable) {
                LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
                shouldRefresh = true;
            }

            currentMetadata = null;
            currentMetadataLocation = null;
            version = -1;
            throw e;
        }
        return current();
    }

    protected void doRefresh() {
        throw new UnsupportedOperationException("Not implemented: doRefresh");
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        // if the metadata is already out of date, reject it
        if (base != current()) {
            if (base != null) {
                throw new CommitFailedException("Cannot commit: stale table metadata");
            } else {
                // when current is non-null, the table exists. but when base is null, the commit is trying
                // to create the table
                throw new AlreadyExistsException("Table already exists: %s", tableName());
            }
        }
        // if the metadata is not changed, return early
        if (base == metadata) {
            LOG.info("Nothing to commit.");
            return;
        }

        long start = System.currentTimeMillis();
        doCommit(base, metadata);
        deleteRemovedMetadataFiles(base, metadata);
        requestRefresh();

        LOG.info(
                "Successfully committed to table {} in {} ms",
                tableName(),
                System.currentTimeMillis() - start);
    }

    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Not implemented: doCommit");
    }

    public void requestRefresh() {
        this.shouldRefresh = true;
    }

    public void disableRefresh() {
        this.shouldRefresh = false;
    }

    protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
        String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
        OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

        // write the new metadata
        // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
        // always unique because it includes a UUID.
        TableMetadataParser.overwrite(metadata, newMetadataLocation);

        return newMetadataLocation.location();
    }

    protected void refreshFromMetadataLocation(String newLocation) {
        refreshFromMetadataLocation(newLocation, null, 20);
    }

    protected void refreshFromMetadataLocation(String newLocation, int numRetries) {
        refreshFromMetadataLocation(newLocation, null, numRetries);
    }

    protected void refreshFromMetadataLocation(
            String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
        refreshFromMetadataLocation(
                newLocation,
                shouldRetry,
                numRetries,
                metadataLocation -> TableMetadataParser.read(io(), metadataLocation));
    }

    protected void refreshFromMetadataLocation(
            String newLocation,
            Predicate<Exception> shouldRetry,
            int numRetries,
            Function<String, TableMetadata> metadataLoader) {
        // use null-safe equality check because new tables have a null metadata location
        if (!Objects.equal(currentMetadataLocation, newLocation)) {
            LOG.info("Refreshing table metadata from new version: {}", newLocation);

            AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
            Tasks.foreach(newLocation)
                    .retry(numRetries)
                    .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
                    .throwFailureWhenFinished()
                    .stopRetryOn(NotFoundException.class) // overridden if shouldRetry is non-null
                    .shouldRetryTest(shouldRetry)
                    .run(metadataLocation -> newMetadata.set(metadataLoader.apply(metadataLocation)));

            String newUUID = newMetadata.get().uuid();
            if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
                Preconditions.checkState(
                        newUUID.equals(currentMetadata.uuid()),
                        "Table UUID does not match: current=%s != refreshed=%s",
                        currentMetadata.uuid(),
                        newUUID);
            }

            this.currentMetadata = newMetadata.get();
            this.currentMetadataLocation = newLocation;
            this.version = parseVersion(newLocation);
            updateMetadata(currentMetadata);
        }
        this.shouldRefresh = false;
    }

    public abstract void updateMetadata(TableMetadata tableMetadata);

    private String metadataFileLocation(TableMetadata metadata, String filename) {
        String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

        if (metadataLocation != null) {
            return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
        } else {
            return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
        }
    }

    @Override
    public String metadataFileLocation(String filename) {
        return metadataFileLocation(current(), filename);
    }

    @Override
    public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(current().location(), current().properties());
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
        return new TableOperations() {
            @Override
            public TableMetadata current() {
                return uncommittedMetadata;
            }

            @Override
            public TableMetadata refresh() {
                throw new UnsupportedOperationException(
                        "Cannot call refresh on temporary table operations");
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return BaseHmsTableOps.this.metadataFileLocation(
                        uncommittedMetadata, fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return LocationProviders.locationsFor(
                        uncommittedMetadata.location(), uncommittedMetadata.properties());
            }

            @Override
            public FileIO io() {
                return BaseHmsTableOps.this.io();
            }

            @Override
            public EncryptionManager encryption() {
                return BaseHmsTableOps.this.encryption();
            }

            @Override
            public long newSnapshotId() {
                return BaseHmsTableOps.this.newSnapshotId();
            }
        };
    }

    protected enum CommitStatus {
        FAILURE,
        SUCCESS,
        UNKNOWN
    }

    /**
     * Attempt to load the table and see if any current or past metadata location matches the one we
     * were attempting to set. This is used as a last resort when we are dealing with exceptions that
     * may indicate the commit has failed but are not proof that this is the case. Past locations must
     * also be searched on the chance that a second committer was able to successfully commit on top
     * of our commit.
     *
     * @param newMetadataLocation the path of the new commit file
     * @param config metadata to use for configuration
     * @return Commit Status of Success, Failure or Unknown
     */
    protected BaseHmsTableOps.CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata config) {
        int maxAttempts =
                PropertyUtil.propertyAsInt(
                        config.properties(), COMMIT_NUM_STATUS_CHECKS, COMMIT_NUM_STATUS_CHECKS_DEFAULT);
        long minWaitMs =
                PropertyUtil.propertyAsLong(
                        config.properties(),
                        COMMIT_STATUS_CHECKS_MIN_WAIT_MS,
                        COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT);
        long maxWaitMs =
                PropertyUtil.propertyAsLong(
                        config.properties(),
                        COMMIT_STATUS_CHECKS_MAX_WAIT_MS,
                        COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT);
        long totalRetryMs =
                PropertyUtil.propertyAsLong(
                        config.properties(),
                        COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS,
                        COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT);

        AtomicReference<BaseHmsTableOps.CommitStatus> status = new AtomicReference<>(
                BaseHmsTableOps.CommitStatus.UNKNOWN);

        Tasks.foreach(newMetadataLocation)
                .retry(maxAttempts)
                .suppressFailureWhenFinished()
                .exponentialBackoff(minWaitMs, maxWaitMs, totalRetryMs, 2.0)
                .onFailure(
                        (location, checkException) ->
                                LOG.error("Cannot check if commit to {} exists.", tableName(), checkException))
                .run(
                        location -> {
                            TableMetadata metadata = refresh();
                            String currentMetadataFileLocation = metadata.metadataFileLocation();
                            boolean commitSuccess =
                                    currentMetadataFileLocation.equals(newMetadataLocation)
                                            || metadata.previousFiles().stream()
                                            .anyMatch(log -> log.file().equals(newMetadataLocation));
                            if (commitSuccess) {
                                LOG.info(
                                        "Commit status check: Commit to {} of {} succeeded",
                                        tableName(),
                                        newMetadataLocation);
                                status.set(BaseHmsTableOps.CommitStatus.SUCCESS);
                            } else {
                                LOG.warn(
                                        "Commit status check: Commit to {} of {} unknown, new metadata location is not current "
                                                + "or in history",
                                        tableName(),
                                        newMetadataLocation);
                            }
                        });

        if (status.get() == BaseHmsTableOps.CommitStatus.UNKNOWN) {
            LOG.error(
                    "Cannot determine commit state to {}. Failed during checking {} times. "
                            + "Treating commit state as unknown.",
                    tableName(),
                    maxAttempts);
        }
        return status.get();
    }

    private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
        String codecName =
                meta.property(
                        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
        String fileExtension = TableMetadataParser.getFileExtension(codecName);
        return metadataFileLocation(
                meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
    }

    /**
     * Parse the version from table metadata file name.
     *
     * @param metadataLocation table metadata file location
     * @return version of the table metadata file in success case and -1 if the version is not
     *     parsable (as a sign that the metadata is not part of this catalog)
     */
    private static int parseVersion(String metadataLocation) {
        int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
        int versionEnd = metadataLocation.indexOf('-', versionStart);
        if (versionEnd < 0) {
            // found filesystem table's metadata
            return -1;
        }

        try {
            return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
        } catch (NumberFormatException e) {
            LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
            return -1;
        }
    }

    private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
        if (base == null) {
            return;
        }

        boolean deleteAfterCommit =
                metadata.propertyAsBoolean(
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

        if (deleteAfterCommit) {
            Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles =
                    Sets.newHashSet(base.previousFiles());
            removedPreviousMetadataFiles.removeAll(metadata.previousFiles());
            Tasks.foreach(removedPreviousMetadataFiles)
                    .noRetry()
                    .suppressFailureWhenFinished()
                    .onFailure(
                            (previousMetadataFile, exc) ->
                                    LOG.warn(
                                            "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
                    .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
        }
    }
}
