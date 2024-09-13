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
package com.starrocks.task;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Status;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTablet;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TabletTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(TabletTaskExecutor.class);

    public static void buildPartitionsSequentially(long dbId, OlapTable table, List<PhysicalPartition> partitions,
                                                   int numReplicas,
                                                   int numBackends, long warehouseId) throws DdlException {
        // Try to bundle at least 200 CreateReplicaTask's in a single AgentBatchTask.
        // The number 200 is just an experiment value that seems to work without obvious problems, feel free to
        // change it if you have a better choice.
        long start = System.currentTimeMillis();
        int avgReplicasPerPartition = numReplicas / partitions.size();
        int partitionGroupSize = Math.max(1, numBackends * 200 / Math.max(1, avgReplicasPerPartition));
        boolean enableTabletCreationOptimization = table.isCloudNativeTableOrMaterializedView()
                && Config.lake_enable_tablet_creation_optimization;
        for (int i = 0; i < partitions.size(); i += partitionGroupSize) {
            int endIndex = Math.min(partitions.size(), i + partitionGroupSize);
            List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partitions.subList(i, endIndex),
                    warehouseId, enableTabletCreationOptimization);
            int partitionCount = endIndex - i;
            int indexCountPerPartition = partitions.get(i).getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size();
            int timeout = Config.tablet_create_timeout_second * countMaxTasksPerBackend(tasks);
            // Compatible with older versions, `Config.max_create_table_timeout_second` is the timeout time for a single index.
            // Here we assume that all partitions have the same number of indexes.
            int maxTimeout = partitionCount * indexCountPerPartition * Config.max_create_table_timeout_second;
            try {
                LOG.info("build partitions sequentially, send task one by one, all tasks timeout {}s",
                        Math.min(timeout, maxTimeout));
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
                LOG.info("build partitions sequentially, all tasks finished, took {}ms",
                        System.currentTimeMillis() - start);
                tasks.clear();
            } finally {
                for (CreateReplicaTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                }
            }
        }
    }

    public static void buildPartitionsConcurrently(long dbId, OlapTable table, List<PhysicalPartition> partitions,
                                                   int numReplicas,
                                                   int numBackends, long warehouseId) throws DdlException {
        long start = System.currentTimeMillis();
        int timeout = Math.max(1, numReplicas / numBackends) * Config.tablet_create_timeout_second;
        int numIndexes = partitions.stream().mapToInt(
                partition -> partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size()).sum();
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        boolean enableTabletCreationOptimization = table.isCloudNativeTableOrMaterializedView()
                && Config.lake_enable_tablet_creation_optimization;
        if (enableTabletCreationOptimization) {
            numReplicas = numIndexes;
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(numReplicas);
        Map<Long, List<Long>> taskSignatures = new HashMap<>();
        try {
            int numFinishedTasks;
            int numSendedTasks = 0;
            long startTime = System.currentTimeMillis();
            long maxWaitTimeMs = Math.min(timeout, maxTimeout) * 1000L;
            for (PhysicalPartition partition : partitions) {
                if (!countDownLatch.getStatus().ok()) {
                    break;
                }
                List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partition, warehouseId,
                        enableTabletCreationOptimization);
                for (CreateReplicaTask task : tasks) {
                    List<Long> signatures =
                            taskSignatures.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
                    signatures.add(task.getSignature());
                }
                sendCreateReplicaTasks(tasks, countDownLatch);
                numSendedTasks += tasks.size();
                numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                // Since there is no mechanism to cancel tasks, if we send a lot of tasks at once and some error or timeout
                // occurs in the middle of the process, it will create a lot of useless replicas that will be deleted soon and
                // waste machine resources. Sending a lot of tasks at once may also block other users' tasks for a long time.
                // To avoid these situations, new tasks are sent only when the average number of tasks on each node is less
                // than 200.
                // (numSendedTasks - numFinishedTasks) is number of tasks that have been sent but not yet finished.
                while (numSendedTasks - numFinishedTasks > 200 * numBackends) {
                    long currentTime = System.currentTimeMillis();
                    // Add timeout check
                    if (currentTime > startTime + maxWaitTimeMs) {
                        throw new TimeoutException("Wait in buildPartitionsConcurrently exceeded timeout");
                    }
                    ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                    numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                }
            }
            LOG.info("build partitions concurrently for {}, waiting for all tasks finish with timeout {}s",
                    table.getName(), Math.min(timeout, maxTimeout));
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
            LOG.info("build partitions concurrently for {}, all tasks finished, took {}ms",
                    table.getName(), System.currentTimeMillis() - start);

        } catch (Exception e) {
            LOG.warn("Failed to execute buildPartitionsConcurrently", e);
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw new DdlException(e.getMessage());
        } finally {
            if (!countDownLatch.getStatus().ok()) {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.CREATE, signature);
                    }
                }
            }
        }
    }

    private static List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, List<PhysicalPartition> partitions,
                                                                  long warehouseId, boolean enableTabletCreationOptimization)
            throws DdlException {
        List<CreateReplicaTask> tasks = new ArrayList<>();
        for (PhysicalPartition partition : partitions) {
            tasks.addAll(
                    buildCreateReplicaTasks(dbId, table, partition, warehouseId, enableTabletCreationOptimization));
        }
        return tasks;
    }

    private static List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, PhysicalPartition partition,
                                                                  long warehouseId, boolean enableTabletCreationOptimization)
            throws DdlException {
        ArrayList<CreateReplicaTask> tasks = new ArrayList<>((int) partition.storageReplicaCount());
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition, index, warehouseId, enableTabletCreationOptimization));
        }
        return tasks;
    }

    private static List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, PhysicalPartition partition,
                                                                  MaterializedIndex index, long warehouseId,
                                                                  boolean enableTabletCreationOptimization) throws DdlException {
        LOG.info("build create replica tasks for index {} db {} table {} partition {}",
                index, dbId, table.getId(), partition);
        boolean isCloudNativeTable = table.isCloudNativeTableOrMaterializedView();
        boolean createSchemaFile = true;
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(index.getId());
        TTabletType tabletType = isCloudNativeTable ? TTabletType.TABLET_TYPE_LAKE : TTabletType.TABLET_TYPE_DISK;
        TStorageMedium storageMedium = table.getPartitionInfo().getDataProperty(partition.getParentId()).getStorageMedium();
        TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                .setId(indexMeta.getSchemaId())
                .setVersion(indexMeta.getSchemaVersion())
                .setKeysType(indexMeta.getKeysType())
                .setShortKeyColumnCount(indexMeta.getShortKeyColumnCount())
                .setSchemaHash(indexMeta.getSchemaHash())
                .setStorageType(indexMeta.getStorageType())
                .setIndexes(table.getIndexes())
                .setSortKeyIndexes(indexMeta.getSortKeyIdxes())
                .setSortKeyUniqueIds(indexMeta.getSortKeyUniqueIds())
                .setBloomFilterColumnNames(table.getBfColumnIds())
                .setBloomFilterFpp(table.getBfFpp())
                .addColumns(indexMeta.getSchema())
                .build().toTabletSchema();

        for (Tablet tablet : index.getTablets()) {
            List<Long> nodeIdsOfReplicas = new ArrayList<>();
            if (isCloudNativeTable) {
                long nodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) tablet).getId();

                nodeIdsOfReplicas.add(nodeId);
            } else {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    nodeIdsOfReplicas.add(replica.getBackendId());
                }
            }
            Preconditions.checkState(!isCloudNativeTable || nodeIdsOfReplicas.size() == 1);

            for (Long nodeId : nodeIdsOfReplicas) {
                CreateReplicaTask task = CreateReplicaTask.newBuilder()
                        .setNodeId(nodeId)
                        .setDbId(dbId)
                        .setTableId(table.getId())
                        .setPartitionId(partition.getId())
                        .setIndexId(index.getId())
                        .setTabletId(tablet.getId())
                        .setVersion(partition.getVisibleVersion())
                        .setStorageMedium(storageMedium)
                        .setEnablePersistentIndex(table.enablePersistentIndex())
                        .setPersistentIndexType(table.getPersistentIndexType())
                        .setPrimaryIndexCacheExpireSec(table.primaryIndexCacheExpireSec())
                        .setBinlogConfig(table.getCurBinlogConfig())
                        .setTabletType(tabletType)
                        .setCompressionType(table.getCompressionType())
                        .setCompressionLevel(table.getCompressionLevel())
                        .setTabletSchema(tabletSchema)
                        .setCreateSchemaFile(createSchemaFile)
                        .setEnableTabletCreationOptimization(enableTabletCreationOptimization)
                        .build();
                tasks.add(task);
                createSchemaFile = false;
            }

            if (enableTabletCreationOptimization) {
                break;
            }
        }
        return tasks;
    }

    // NOTE: Unfinished tasks will NOT be removed from the AgentTaskQueue.
    private static void sendCreateReplicaTasksAndWaitForFinished(List<CreateReplicaTask> tasks, long timeout)
            throws DdlException {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(tasks.size());
        sendCreateReplicaTasks(tasks, countDownLatch);
        waitForFinished(countDownLatch, timeout);
    }

    private static void sendCreateReplicaTasks(List<CreateReplicaTask> tasks,
                                              MarkedCountDownLatch<Long, Long> countDownLatch) {
        HashMap<Long, List<AgentTask>> backendToBatchTask = new HashMap<>();

        for (CreateReplicaTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTabletId());

            List<AgentTask> batchTask = backendToBatchTask.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
            batchTask.add(task);
        }

        try {
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (Map.Entry<Long, List<AgentTask>> entry : backendToBatchTask.entrySet()) {
                AgentTaskQueue.addTaskList(entry.getValue());
                futures.add(sendTask(entry.getKey(), entry.getValue()));
            }

            for (CompletableFuture<Boolean> future : futures) {
                if (!future.get()) {
                    countDownLatch.countDownToZero(Status.internalError("Send Create Replica Task fail"));
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            countDownLatch.countDownToZero(Status.internalError(e.getMessage()));
            throw new RuntimeException(e);
        }
    }

    private static CompletableFuture<Boolean> sendTask(Long backendId, List<AgentTask> agentBatchTask) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr()
                        .getClusterInfo().getBackendOrComputeNode(backendId);
                if (computeNode == null || !computeNode.isAlive()) {
                    throw new RuntimeException("Can't get backend " + backendId);
                }

                List<TAgentTaskRequest> agentTaskRequests =
                        agentBatchTask.stream().map(AgentBatchTask::toAgentTaskRequest).collect(Collectors.toList());

                ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.backendPool,
                        new TNetworkAddress(computeNode.getHost(), computeNode.getBePort()),
                        client -> client.submit_tasks(agentTaskRequests));
                return true;
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }, AgentTaskExecutor.EXECUTOR);
    }

    // REQUIRE: must set countDownLatch to error stat before throw an exception.
    private static void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) throws DdlException {
        try {
            if (countDownLatch.await(timeout, TimeUnit.SECONDS)) {
                if (!countDownLatch.getStatus().ok()) {
                    String errMsg = "fail to create tablet: " + countDownLatch.getStatus().getErrorMsg();
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } else { // timed out
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> firstThree =
                        unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                StringBuilder sb = new StringBuilder("Table creation timed out. unfinished replicas");
                sb.append("(").append(firstThree.size()).append("/").append(unfinishedMarks.size()).append("): ");
                // Show details of the first 3 unfinished tablets.
                for (Map.Entry<Long, Long> mark : firstThree) {
                    sb.append(mark.getValue()); // TabletId
                    sb.append('(');
                    Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(mark.getKey());
                    sb.append(backend != null ? backend.getHost() : "N/A");
                    sb.append(") ");
                }
                sb.append(" timeout=").append(timeout).append('s');
                String errMsg = sb.toString();
                LOG.warn(errMsg);

                String userErrorMsg = String.format(
                        errMsg + "\n You can increase the timeout by increasing the " +
                                "config \"tablet_create_timeout_second\" and try again.\n" +
                                "To increase the config \"tablet_create_timeout_second\" (currently %d), " +
                                "run the following command:\n" +
                                "```\nadmin set frontend config(\"tablet_create_timeout_second\"=\"%d\")\n```\n" +
                                "or add the following configuration to the fe.conf file and restart the process:\n" +
                                "```\ntablet_create_timeout_second=%d\n```",
                        Config.tablet_create_timeout_second,
                        Config.tablet_create_timeout_second * 2,
                        Config.tablet_create_timeout_second * 2
                );
                countDownLatch.countDownToZero(new Status(TStatusCode.TIMEOUT, "timed out"));
                throw new DdlException(userErrorMsg);
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute waitForFinished", e);
            countDownLatch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        }
    }

    public static int countMaxTasksPerBackend(List<CreateReplicaTask> tasks) {
        Map<Long, Integer> tasksPerBackend = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            tasksPerBackend.compute(task.getBackendId(), (k, v) -> (v == null) ? 1 : v + 1);
        }
        return Collections.max(tasksPerBackend.values());
    }

    public static void deleteAllReplicas(OlapTable olapTable) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        // drop all replicas
        for (Partition partition : olapTable.getAllPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                List<MaterializedIndex> allIndices = physicalPartition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    long indexId = materializedIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    List<Tablet> tabletList = GlobalStateMgr.getCurrentState().getTabletMetastore()
                            .getAllTablets(materializedIndex);
                    for (Tablet tablet : tabletList) {
                        long tabletId = tablet.getId();
                        List<Replica> replicaList = GlobalStateMgr.getCurrentState().getTabletMetastore().getAllReplicas(tablet);
                        for (Replica replica : replicaList) {
                            long backendId = replica.getBackendId();
                            DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                            AgentBatchTask batchTask = batchTaskMap.get(backendId);
                            if (batchTask == null) {
                                batchTask = new AgentBatchTask();
                                batchTaskMap.put(backendId, batchTask);
                            }
                            batchTask.addTask(dropTask);
                            LOG.info("delete tablet[{}] from backend[{}] because table {}-{} is dropped",
                                    tabletId, backendId, olapTable.getId(), olapTable.getName());
                        } // end for replicas
                    }
                }
            } // end for indices
        } // end for partitions

        int numDropTaskPerBe = Config.max_agent_tasks_send_per_be;
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentBatchTask originTasks = entry.getValue();
            if (originTasks.getTaskNum() > numDropTaskPerBe) {
                AgentBatchTask partTask = new AgentBatchTask();
                List<AgentTask> allTasks = originTasks.getAllTasks();
                int curTask = 1;
                for (AgentTask task : allTasks) {
                    partTask.addTask(task);
                    if (curTask++ > numDropTaskPerBe) {
                        AgentTaskExecutor.submit(partTask);
                        curTask = 1;
                        partTask = new AgentBatchTask();
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
                    }
                }
                if (!partTask.getAllTasks().isEmpty()) {
                    AgentTaskExecutor.submit(partTask);
                }
            } else {
                AgentTaskExecutor.submit(originTasks);
            }
        }
    }
}
