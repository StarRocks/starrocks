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

package com.starrocks.connector.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Version;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;
import static com.starrocks.connector.hive.HiveMetadata.STARROCKS_QUERY_ID;
import static com.starrocks.connector.hive.HivePartitionStats.ReduceOperator.SUBTRACT;
import static com.starrocks.connector.hive.HivePartitionStats.fromCommonStats;
import static com.starrocks.connector.hive.HiveWriteUtils.fileCreatedByQuery;
import static com.starrocks.connector.hive.HiveWriteUtils.isS3Url;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class HiveCommitter {
    private static final Logger LOG = LogManager.getLogger(HiveCommitter.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 20;
    private static final String BACKGROUND_THREAD_NAME_PREFIX = "background-refresh-others-fe-metadata-";
    private final HiveTable table;
    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOps;
    private final Executor updateStatsExecutor;
    private final Executor refreshOthersFeExecutor;
    private final AtomicBoolean fsTaskCancelled = new AtomicBoolean(false);
    private final List<CompletableFuture<?>> fsTaskFutures = new ArrayList<>();
    private final Queue<DirectoryCleanUpTask> clearTasksForAbort = new ConcurrentLinkedQueue<>();
    private final List<Path> clearPathsForFinish = new ArrayList<>();
    private final Set<Path> remoteFilesCacheToRefresh = new LinkedHashSet<>();
    private final AddPartitionsTask addPartitionsTask = new AddPartitionsTask();
    private final List<RenameDirectoryTask> renameDirTasksForAbort = new ArrayList<>();

    private final List<UpdateStatisticsTask> updateStatisticsTasks = new ArrayList<>();
    private final Path stagingDir;

    public HiveCommitter(HiveMetastoreOperations hmsOps, RemoteFileOperations fileOps, Executor updateStatsExecutor,
                         Executor refreshOthersFeExecutor, HiveTable table, Path stagingDir) {
        this.hmsOps = hmsOps;
        this.fileOps = fileOps;
        this.updateStatsExecutor = updateStatsExecutor;
        this.refreshOthersFeExecutor = refreshOthersFeExecutor;
        this.table = table;
        this.stagingDir = stagingDir;
    }

    public void commit(List<PartitionUpdate> partitionUpdates) {
        try {
            prepare(partitionUpdates);
            doCommit();
            asyncRefreshOthersFeMetadataCache(partitionUpdates);
        } catch (Throwable t) {
            LOG.warn("Rolling back due to commit failure", t);
            try {
                cancelNotStartFsTasks();
                undoUpdateStatsTasks();
                undoAddPartitionsTask();
                waitAsyncFsTaskSuppressThrowable();
                runClearTasksForAbort();
                runRenameDirTasksForAbort();
            } catch (RuntimeException e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }
            throw t;
        } finally {
            runRefreshFilesCacheTasks();
            runClearPathsForFinish();
            clearStagingDir();
        }
    }

    public void prepare(List<PartitionUpdate> partitionUpdates) {
        List<Pair<PartitionUpdate, HivePartitionStats>> insertExistsPartitions = new ArrayList<>();
        for (PartitionUpdate pu : partitionUpdates) {
            PartitionUpdate.UpdateMode mode = pu.getUpdateMode();
            HivePartitionStats updateStats = fromCommonStats(pu.getRowCount(), pu.getTotalSizeInBytes());
            if (table.isUnPartitioned()) {
                if (partitionUpdates.size() != 1) {
                    throw new StarRocksConnectorException("There are multiple updates in the unpartition table: %s.%s",
                            table.getDbName(), table.getTableName());
                }

                if (mode == PartitionUpdate.UpdateMode.APPEND) {
                    prepareAppendTable(pu, updateStats);
                } else if (mode == PartitionUpdate.UpdateMode.OVERWRITE) {
                    prepareOverwriteTable(pu, updateStats);
                }
            } else {
                if (mode == PartitionUpdate.UpdateMode.NEW) {
                    prepareAddPartition(pu, updateStats);
                } else if (mode == PartitionUpdate.UpdateMode.APPEND) {
                    insertExistsPartitions.add(Pair.create(pu, updateStats));
                } else if (mode == PartitionUpdate.UpdateMode.OVERWRITE) {
                    prepareOverwritePartition(pu, updateStats);
                }
            }
        }

        if (!insertExistsPartitions.isEmpty()) {
            prepareAppendPartition(insertExistsPartitions);
        }
    }

    public void doCommit() {
        waitAsyncFsTasks();
        runAddPartitionsTask();
        runUpdateStatsTasks();
    }

    public void asyncRefreshOthersFeMetadataCache(List<PartitionUpdate> partitionUpdates) {
        String catalogName = table.getCatalogName();
        String dbName = table.getDbName();
        String tableName = table.getTableName();
        List<String> partitionNames;
        if (table.isUnPartitioned()) {
            partitionNames = new ArrayList<>();
        } else {
            partitionNames = partitionUpdates.stream()
                    .map(PartitionUpdate::getName)
                    .collect(Collectors.toList());
        }

        refreshOthersFeExecutor.execute(() -> {
            LOG.info("Start to refresh others fe hive metadata cache on {}.{}.{}.{}",
                    catalogName, dbName, tableName, partitionNames);
            try {
                GlobalStateMgr.getCurrentState().refreshOthersFeTable(
                        new TableName(catalogName, dbName, tableName), partitionNames, false);
            } catch (DdlException e) {
                LOG.error("Failed to refresh others fe hive metdata cache", e);
                throw new StarRocksConnectorException(e.getMessage());
            }
            LOG.info("Finish to refresh others fe hive metadata cache on {}.{}.{}.{}",
                    catalogName, dbName, tableName, partitionNames);
        });
    }

    private void prepareAppendTable(PartitionUpdate pu, HivePartitionStats updateStats) {
        Path targetPath = new Path(table.getTableLocation());
        remoteFilesCacheToRefresh.add(targetPath);
        clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));
        if (!pu.isS3Url()) {
            fileOps.asyncRenameFiles(fsTaskFutures, fsTaskCancelled, pu.getWritePath(), pu.getTargetPath(), pu.getFileNames());
        }
        updateStatisticsTasks.add(new UpdateStatisticsTask(
                table.getDbName(),
                table.getTableName(),
                Optional.empty(),
                updateStats,
                true));
    }

    private void prepareOverwriteTable(PartitionUpdate pu, HivePartitionStats updateStats) {
        Path writePath = pu.getWritePath();
        Path targetPath = pu.getTargetPath();
        remoteFilesCacheToRefresh.add(targetPath);

        Path oldTableStagingPath = new Path(targetPath.getParent(), "_temp_" + targetPath.getName() + "_" +
                ConnectContext.get().getQueryId().toString());
        fileOps.renameDirectory(targetPath, oldTableStagingPath,
                () -> renameDirTasksForAbort.add(new RenameDirectoryTask(oldTableStagingPath, targetPath)));
        clearPathsForFinish.add(oldTableStagingPath);

        fileOps.renameDirectory(writePath, targetPath,
                () -> clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));

        UpdateStatisticsTask updateStatsTask = new UpdateStatisticsTask(table.getDbName(), table.getTableName(),
                Optional.empty(), updateStats, false);
        updateStatisticsTasks.add(updateStatsTask);
    }

    private void prepareAddPartition(PartitionUpdate pu, HivePartitionStats updateStats) {
        Path writePath = pu.getWritePath();
        Path targetPath = pu.getTargetPath();
        fsTaskFutures.add(CompletableFuture.runAsync(() -> {
            if (fsTaskCancelled.get()) {
                return;
            }

            if (!pu.isS3Url()) {
                fileOps.renameDirectory(
                        writePath,
                        targetPath,
                        () -> clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));
            } else {
                clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true));
            }
        }, fileOps.getUpdateFsExecutor()));

        HivePartition hivePartition = buildHivePartition(pu);
        HivePartitionWithStats partitionWithStats = new HivePartitionWithStats(pu.getName(), hivePartition, updateStats);
        addPartitionsTask.addPartition(partitionWithStats);
    }

    private void prepareAppendPartition(List<Pair<PartitionUpdate, HivePartitionStats>> partitions) {
        for (List<Pair<PartitionUpdate, HivePartitionStats>> partitionBatch : Iterables.partition(partitions, 100)) {
            List<String> partitionNames = partitionBatch.stream()
                    .map(pair -> pair.first.getName())
                    .collect(Collectors.toList());

            Map<String, Partition> partitionsByNames = hmsOps.getPartitionByNames(table, partitionNames);

            for (int i = 0; i < partitionsByNames.size(); i++) {
                String partitionName = partitionNames.get(i);
                if (partitionsByNames.get(partitionName) == null) {
                    throw new StarRocksConnectorException("Partition [%s] not found", partitionName);
                }

                PartitionUpdate pu = partitionBatch.get(i).first;
                HivePartitionStats updateStats = partitionBatch.get(i).second;

                Path writePath = pu.getWritePath();
                Path targetPath = pu.getTargetPath();
                remoteFilesCacheToRefresh.add(targetPath);
                clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));

                if (!pu.isS3Url()) {
                    fileOps.asyncRenameFiles(fsTaskFutures, fsTaskCancelled, writePath, targetPath, pu.getFileNames());
                }

                UpdateStatisticsTask updateStatsTask = new UpdateStatisticsTask(table.getDbName(), table.getTableName(),
                        Optional.of(pu.getName()), updateStats, true);
                updateStatisticsTasks.add(updateStatsTask);
            }
        }
    }

    private void prepareOverwritePartition(PartitionUpdate pu, HivePartitionStats updateStats) {
        Path writePath = pu.getWritePath();
        Path targetPath = pu.getTargetPath();

        if (pu.isS3Url()) {
            String queryId = ConnectContext.get().getQueryId().toString();
            fileOps.removeNotCurrentQueryFiles(targetPath, queryId);
        } else {
            Path oldPartitionStagingPath = new Path(targetPath.getParent(), "_temp_" + targetPath.getName()
                    + "_" + ConnectContext.get().getQueryId().toString());

            fileOps.renameDirectory(
                    targetPath,
                    oldPartitionStagingPath,
                    () -> renameDirTasksForAbort.add(new RenameDirectoryTask(oldPartitionStagingPath, targetPath)));
            clearPathsForFinish.add(oldPartitionStagingPath);

            fileOps.renameDirectory(
                    writePath,
                    targetPath,
                    () -> clearTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));
        }

        remoteFilesCacheToRefresh.add(targetPath);
        UpdateStatisticsTask updateStatsTask = new UpdateStatisticsTask(table.getDbName(), table.getTableName(),
                Optional.of(pu.getName()), updateStats, false);
        updateStatisticsTasks.add(updateStatsTask);
    }

    private void waitAsyncFsTasks() {
        for (CompletableFuture<?> future : fsTaskFutures) {
            getFutureValue(future, StarRocksConnectorException.class);
        }
    }

    private void runAddPartitionsTask() {
        if (!addPartitionsTask.isEmpty()) {
            addPartitionsTask.run(hmsOps);
        }
    }

    private void runUpdateStatsTasks() {
        ImmutableList.Builder<CompletableFuture<?>> updateStatsFutures = ImmutableList.builder();
        List<String> failedUpdateStatsTaskDescs = new ArrayList<>();
        List<Throwable> suppressedExceptions = new ArrayList<>();
        for (UpdateStatisticsTask task : updateStatisticsTasks) {
            updateStatsFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    task.run(hmsOps);
                } catch (Throwable t) {
                    addSuppressedExceptions(suppressedExceptions, t, failedUpdateStatsTaskDescs, task.getDescription());
                }
            }, updateStatsExecutor));
        }

        for (CompletableFuture<?> executeUpdateFuture : updateStatsFutures.build()) {
            getFutureValue(executeUpdateFuture);
        }

        if (!suppressedExceptions.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append("Failed to update following tasks: ");
            Joiner.on("; ").appendTo(message, failedUpdateStatsTaskDescs);
            StarRocksConnectorException exception = new StarRocksConnectorException(message.toString());
            suppressedExceptions.forEach(exception::addSuppressed);
            // Insert into Hive4 table occur failure caused by compatibility issue between Hive3 and Hive4 thrift HMS client.
            // Check https://github.com/StarRocks/starrocks/issues/38620 and HIVE-27984 for more details.
            if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().enableHiveColumnStats()) {
                throw exception;
            } else {
                LOG.error(exception);
            }
        }
    }

    private void cancelNotStartFsTasks() {
        fsTaskCancelled.set(true);
    }

    private void undoUpdateStatsTasks() {
        ImmutableList.Builder<CompletableFuture<?>> undoUpdateFutures = ImmutableList.builder();
        for (UpdateStatisticsTask task : updateStatisticsTasks) {
            undoUpdateFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    task.undo(hmsOps);
                } catch (Throwable throwable) {
                    LOG.error("Failed to rollback: {}", task.getDescription(), throwable);
                }
            }, updateStatsExecutor));
        }

        for (CompletableFuture<?> undoUpdateFuture : undoUpdateFutures.build()) {
            getFutureValue(undoUpdateFuture);
        }
    }

    private void undoAddPartitionsTask() {
        if (addPartitionsTask.isEmpty()) {
            return;
        }

        HivePartition firstPartition = addPartitionsTask.getPartitions().get(0).getHivePartition();
        String dbName = firstPartition.getDatabaseName();
        String tableName = firstPartition.getTableName();
        List<List<String>> rollbackFailedPartitions = addPartitionsTask.rollback(hmsOps);
        LOG.error("Failed to rollback: add_partition for partition values {}.{}.{}",
                dbName, tableName, rollbackFailedPartitions);
    }

    private void waitAsyncFsTaskSuppressThrowable() {
        for (CompletableFuture<?> future : fsTaskFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    private void runClearTasksForAbort() {
        for (DirectoryCleanUpTask cleanUpTask : clearTasksForAbort) {
            recursiveDeleteItems(cleanUpTask.getPath(), cleanUpTask.isDeleteEmptyDir());
        }
    }

    private void runRenameDirTasksForAbort() {
        for (RenameDirectoryTask directoryRenameTask : renameDirTasksForAbort) {
            try {
                if (fileOps.pathExists(directoryRenameTask.getRenameFrom())) {
                    fileOps.renameDirectory(directoryRenameTask.getRenameFrom(), directoryRenameTask.getRenameTo(), () -> {});
                }
            } catch (Throwable t) {
                LOG.error("Failed to undo rename dir from {} to {}",
                        directoryRenameTask.getRenameFrom(), directoryRenameTask.getRenameTo(), t);
            }
        }
    }

    private void runRefreshFilesCacheTasks() {
        for (Path path : remoteFilesCacheToRefresh) {
            fileOps.refreshPartitionFilesCache(path);
        }
    }

    private void runClearPathsForFinish() {
        for (Path path : clearPathsForFinish) {
            try {
                if (!fileOps.deleteIfExists(path, true)) {
                    LOG.warn("Failed to recursively delete path {}", path);
                }
            } catch (Exception e) {
                LOG.warn("Failed to recursively delete path {}", path, e);
            }
        }
    }

    private void clearStagingDir() {
        if (!isS3Url(stagingDir.toString()) && !fileOps.deleteIfExists(stagingDir, true)) {
            LOG.warn("Failed to clear staging dir {}", stagingDir);
        }
    }

    private HivePartition buildHivePartition(PartitionUpdate partitionUpdate) {
        return HivePartition.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumnNames().stream()
                        .map(table::getColumn)
                        .collect(Collectors.toList()))
                .setValues(toPartitionValues(partitionUpdate.getName()))
                .setParameters(ImmutableMap.<String, String>builder()
                        .put("starrocks_version", Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH)
                        .put(STARROCKS_QUERY_ID, ConnectContext.get().getQueryId().toString())
                        .buildOrThrow())
                .setStorageFormat(table.getStorageFormat())
                .setLocation(partitionUpdate.getTargetPath().toString())
                .build();
    }

    private static class RenameDirectoryTask {
        private final Path renameFrom;
        private final Path renameTo;

        public RenameDirectoryTask(Path renameFrom, Path renameTo) {
            this.renameFrom = requireNonNull(renameFrom, "renameFrom is null");
            this.renameTo = requireNonNull(renameTo, "renameTo is null");
        }

        public Path getRenameFrom() {
            return renameFrom;
        }

        public Path getRenameTo() {
            return renameTo;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RenameDirectoryTask.class.getSimpleName() + "[", "]")
                    .add("renameFrom=" + renameFrom)
                    .add("renameTo=" + renameTo)
                    .toString();
        }
    }

    private static class DirectoryCleanUpTask {
        private final Path path;
        private final boolean deleteEmptyDir;

        public DirectoryCleanUpTask(Path path, boolean deleteEmptyDir) {
            this.path = path;
            this.deleteEmptyDir = deleteEmptyDir;
        }

        public Path getPath() {
            return path;
        }

        public boolean isDeleteEmptyDir() {
            return deleteEmptyDir;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", DirectoryCleanUpTask.class.getSimpleName() + "[", "]")
                    .add("path=" + path)
                    .add("deleteEmptyDir=" + deleteEmptyDir)
                    .toString();
        }
    }

    public static class AddPartitionsTask {
        private final List<HivePartitionWithStats> partitions = new ArrayList<>();
        private List<List<String>> createdPartitionValues = new ArrayList<>();

        public boolean isEmpty() {
            return partitions.isEmpty();
        }

        public List<HivePartitionWithStats> getPartitions() {
            return partitions;
        }

        public void addPartition(HivePartitionWithStats partition) {
            partitions.add(partition);
        }

        public void run(HiveMetastoreOperations hmsOps) {
            HivePartition firstPartition = partitions.get(0).getHivePartition();
            String dbName = firstPartition.getDatabaseName();
            String tableName = firstPartition.getTableName();
            List<List<HivePartitionWithStats>> batchedPartitions = Lists.partition(partitions, PARTITION_COMMIT_BATCH_SIZE);
            for (List<HivePartitionWithStats> batch : batchedPartitions) {
                try {
                    hmsOps.addPartitions(dbName, tableName, batch);
                    for (HivePartitionWithStats partition : batch) {
                        createdPartitionValues.add(partition.getHivePartition().getValues());
                    }
                } catch (Throwable t) {
                    LOG.error("Failed to add partition", t);
                    boolean addedSuccess = true;
                    for (HivePartitionWithStats partition : batch) {
                        try {
                            Partition remotePartition = hmsOps.getPartition(
                                    dbName, tableName, partition.getHivePartition().getValues());

                            if (checkIsSamePartition(remotePartition, partition.getHivePartition())) {
                                createdPartitionValues.add(partition.getHivePartition().getValues());
                            } else {
                                addedSuccess = false;
                            }
                        } catch (Throwable ignored) {
                            addedSuccess = false;
                        }
                    }

                    if (!addedSuccess) {
                        throw t;
                    }
                }
            }
            partitions.clear();
        }

        public List<List<String>> rollback(HiveMetastoreOperations hmsOps) {
            HivePartition firstPartition = partitions.get(0).getHivePartition();
            String dbName = firstPartition.getDatabaseName();
            String tableName = firstPartition.getTableName();
            List<List<String>> rollbackFailedPartitions = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    hmsOps.dropPartition(dbName, tableName, createdPartitionValue, false);
                } catch (Throwable t) {
                    LOG.warn("Failed to drop partition on {}.{}.{} when rollback",
                            dbName, tableName, rollbackFailedPartitions);
                    rollbackFailedPartitions.add(createdPartitionValue);
                }
            }
            createdPartitionValues = rollbackFailedPartitions;
            return rollbackFailedPartitions;
        }
    }

    // 1. alter table or alter partition
    // 2. update table or partition statistics
    private static class UpdateStatisticsTask {
        private final String dbName;
        private final String tableName;
        private final Optional<String> partitionName;
        private final HivePartitionStats updatePartitionStat;
        private final boolean merge;

        private boolean done;

        public UpdateStatisticsTask(String dbName, String tableName, Optional<String> partitionName,
                                         HivePartitionStats statistics, boolean merge) {
            this.dbName = requireNonNull(dbName, "dbName is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.partitionName = requireNonNull(partitionName, "partitionName is null");
            this.updatePartitionStat = requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HiveMetastoreOperations hmsOps) {
            if (partitionName.isPresent()) {
                hmsOps.updatePartitionStatistics(dbName, tableName, partitionName.get(), this::updateStatistics);
            } else {
                hmsOps.updateTableStatistics(dbName, tableName, this::updateStatistics);
            }
            done = true;
        }

        public void undo(HiveMetastoreOperations hmsOps) {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                hmsOps.updatePartitionStatistics(dbName, tableName, partitionName.get(), this::resetStatistics);
            } else {
                hmsOps.updateTableStatistics(dbName, tableName, this::resetStatistics);
            }
        }

        public String getDescription() {
            if (partitionName.isPresent()) {
                return "alter partition parameters " + tableName + " " + partitionName.get();
            } else {
                return "alter table parameters " +  tableName;
            }
        }

        private HivePartitionStats updateStatistics(HivePartitionStats currentStats) {
            return merge ? HivePartitionStats.merge(currentStats, updatePartitionStat) : updatePartitionStat;
        }

        private HivePartitionStats resetStatistics(HivePartitionStats currentStatistics) {
            return HivePartitionStats.reduce(currentStatistics, updatePartitionStat, SUBTRACT);
        }
    }

    public static class DeleteRecursivelyResult {
        private final boolean dirNotExists;
        private final List<String> notDeletedEligibleItems;

        public DeleteRecursivelyResult(boolean dirNotExists, List<String> notDeletedEligibleItems) {
            this.dirNotExists = dirNotExists;
            this.notDeletedEligibleItems = notDeletedEligibleItems;
        }

        public boolean dirNotExists() {
            return dirNotExists;
        }

        public List<String> getNotDeletedEligibleItems() {
            return notDeletedEligibleItems;
        }
    }

    public static Optional<String> getQueryId(Map<String, String> params) {
        params = params != null ? params : new HashMap<>();
        return Optional.ofNullable(params.get(STARROCKS_QUERY_ID));
    }

    public static boolean checkIsSamePartition(Partition remotePartition, HivePartition curPartition) {
        if (remotePartition == null) {
            return false;
        }

        if (!getQueryId(remotePartition.getParameters()).isPresent()) {
            return false;
        }

        return getQueryId(remotePartition.getParameters()).equals(getQueryId(curPartition.getParameters()));
    }

    private void recursiveDeleteItems(Path directory, boolean deleteEmptyDir) {
        DeleteRecursivelyResult deleteResult = recursiveDeleteFiles(directory, deleteEmptyDir);

        if (!deleteResult.getNotDeletedEligibleItems().isEmpty()) {
            LOG.error("Failed to delete directory {}. Some eligible items can't be deleted: {}.",
                    directory.toString(), deleteResult.getNotDeletedEligibleItems());
        } else if (deleteEmptyDir && !deleteResult.dirNotExists()) {
            LOG.error("Failed to delete directory {} due to dir isn't empty", directory.toString());
        }
    }

    public DeleteRecursivelyResult recursiveDeleteFiles(Path directory, boolean deleteEmptyDir) {
        try {
            if (!fileOps.pathExists(directory)) {
                return new DeleteRecursivelyResult(true, ImmutableList.of());
            }
        } catch (StarRocksConnectorException e) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory.toString() + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        return doRecursiveDeleteFiles(directory, ConnectContext.get().getQueryId().toString(), deleteEmptyDir);
    }

    private DeleteRecursivelyResult doRecursiveDeleteFiles(Path directory, String queryId, boolean deleteEmptyDir) {
        FileStatus[] allFiles;
        try {
            allFiles = fileOps.listStatus(directory);
        } catch (StarRocksConnectorException e) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        boolean isEmptyDir = true;
        List<String> notDeletedEligibleItems = new ArrayList<>();
        for (FileStatus fileStatus : allFiles) {
            if (fileStatus.isFile()) {
                Path filePath = fileStatus.getPath();
                String fileName = filePath.getName();
                boolean eligible = fileCreatedByQuery(fileName, queryId);

                if (eligible) {
                    if (!fileOps.deleteIfExists(filePath, false)) {
                        isEmptyDir = false;
                        notDeletedEligibleItems.add(filePath.toString());
                    }
                } else {
                    isEmptyDir = false;
                }
            } else if (fileStatus.isDirectory()) {
                DeleteRecursivelyResult subResult = doRecursiveDeleteFiles(fileStatus.getPath(), queryId, deleteEmptyDir);
                if (!subResult.dirNotExists()) {
                    isEmptyDir = false;
                }
                if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                    notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
                }
            } else {
                isEmptyDir = false;
                notDeletedEligibleItems.add(fileStatus.getPath().toString());
            }
        }

        if (isEmptyDir && deleteEmptyDir) {
            verify(notDeletedEligibleItems.isEmpty());
            if (!fileOps.deleteIfExists(directory, false)) {
                return new DeleteRecursivelyResult(false, ImmutableList.of(directory + "/"));
            }
            // all items of the location have been deleted.
            return new DeleteRecursivelyResult(true, ImmutableList.of());
        }

        return new DeleteRecursivelyResult(false, notDeletedEligibleItems);
    }

    private synchronized void addSuppressedExceptions(
            List<Throwable> suppressedExceptions, Throwable t,
             List<String> descriptions, String description) {
        descriptions.add(description);
        if (suppressedExceptions.size() < 3) {
            suppressedExceptions.add(t);
        }
    }
}
