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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/master/ReportHandler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.leader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendStatus;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ClearTransactionTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.CreateReplicaTask.RecoverySource;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.task.LeaderTask;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.task.StorageMediaMigrationTask;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.task.UpdateSchemaTask;
import com.starrocks.thrift.TBackend;
<<<<<<< HEAD
=======
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TDataCacheMetrics;
>>>>>>> 113d301de2 ([Enhancement] Update tablet schema after fast schema evolution (#39869))
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TOlapTableColumnParam;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTxnType;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupOp;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class ReportHandler extends Daemon {
    public enum ReportType {
        UNKNOWN_REPORT,
        TABLET_REPORT,
        DISK_REPORT,
        TASK_REPORT,
        RESOURCE_GROUP_REPORT,
        RESOURCE_USAGE_REPORT
    }

    /**
     * If the time of report handling exceeds this limit, we will log it.
     */
    private static final long MAX_REPORT_HANDLING_TIME_LOGGING_THRESHOLD_MS = 3000;

    private static final Logger LOG = LogManager.getLogger(ReportHandler.class);

    private BlockingQueue<Pair<Long, ReportType>> reportQueue = Queues.newLinkedBlockingQueue();

    private Map<ReportType, Map<Long, ReportTask>> pendingTaskMap = Maps.newHashMap();

    /**
     * Record the mapping of <tablet id, backend id> to the to be dropped time of tablet.
     * We will delay the drop of tablet based on configuration `tablet_report_drop_tablet_delay_sec`
     * if we don't find the meta of the tablet in FE.
     * <p>
     * There's no concurrency here since it will only be used by a single thread of ReportHandler, so
     * we don't need lock protection.
     * <p>
     * And because the tablet drop only relies on some runtime state, if the map is lost after restart,
     * the drop can retry. So we don't need to persist this map either.
     */
    private static final Table<Long, Long, Long> TABLET_TO_DROP_TIME = HashBasedTable.create();

    public ReportHandler() {
        super("ReportHandler");
        GaugeMetric<Long> gaugeQueueSize = new GaugeMetric<Long>(
                "report_queue_size", MetricUnit.NOUNIT, "report queue size") {
            @Override
            public Long getValue() {
                return (long) reportQueue.size();
            }
        };
        MetricRepo.addMetric(gaugeQueueSize);
        pendingTaskMap.put(ReportType.TABLET_REPORT, Maps.newHashMap());
        pendingTaskMap.put(ReportType.DISK_REPORT, Maps.newHashMap());
        pendingTaskMap.put(ReportType.TASK_REPORT, Maps.newHashMap());
        pendingTaskMap.put(ReportType.RESOURCE_GROUP_REPORT, Maps.newHashMap());
        pendingTaskMap.put(ReportType.RESOURCE_USAGE_REPORT, Maps.newHashMap());
    }

    public TMasterResult handleReport(TReportRequest request) throws TException {
        TMasterResult result = new TMasterResult();
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBe_port();
        long beId;
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend != null) {
            beId = backend.getId();
        } else {
            ComputeNode computeNode = null;
            // Compute node only reports resource usage.
            if (request.isSetResource_usage()) {
                computeNode = GlobalStateMgr.getCurrentSystemInfo().getComputeNodeWithBePort(host, bePort);
            }

            if (computeNode != null) {
                beId = computeNode.getId();
            } else {
                tStatus.setStatus_code(TStatusCode.INTERNAL_ERROR);
                List<String> errorMsgs = Lists.newArrayList();
                errorMsgs.add("backend or compute node [" + host + ":" + bePort + "] does not exist.");
                tStatus.setError_msgs(errorMsgs);
                return result;
            }
        }

        Map<TTaskType, Set<Long>> tasks = null;
        Map<String, TDisk> disks = null;
        Map<Long, TTablet> tablets = null;
        List<TWorkGroup> activeWorkGroups = null;
        TResourceUsage resourceUsage = null;
        long reportVersion = -1;

        ReportType reportType = ReportType.UNKNOWN_REPORT;
        if (request.isSetTasks()) {
            tasks = request.getTasks();
            reportType = ReportType.TASK_REPORT;
        }

        if (request.isSetDisks()) {
            if (reportType != ReportType.UNKNOWN_REPORT) {
                buildErrorResult(tStatus,
                        "invalid report request, multi fields " + reportType + " " + ReportType.TASK_REPORT);
                return result;
            }
            disks = request.getDisks();
            reportType = ReportType.DISK_REPORT;
        }

        if (request.isSetTablets()) {
            if (reportType != ReportType.UNKNOWN_REPORT) {
                buildErrorResult(tStatus,
                        "invalid report request, multi fields " + reportType + " " + ReportType.TABLET_REPORT);
                return result;
            }
            tablets = request.getTablets();
            reportVersion = request.getReport_version();
            reportType = ReportType.TABLET_REPORT;
        } else if (request.isSetTablet_list()) {
            if (reportType != ReportType.UNKNOWN_REPORT) {
                buildErrorResult(tStatus,
                        "invalid report request, multi fields " + reportType + " " + ReportType.TABLET_REPORT);
                return result;
            }
            // the 'tablets' member will be deprecated in the future.
            tablets = buildTabletMap(request.getTablet_list());
            reportVersion = request.getReport_version();
            reportType = ReportType.TABLET_REPORT;
        }

        if (backend != null && request.isSetTablet_max_compaction_score()) {
            backend.setTabletMaxCompactionScore(request.getTablet_max_compaction_score());
        }

        if (request.isSetActive_workgroups()) {
            if (reportType != ReportType.UNKNOWN_REPORT) {
                buildErrorResult(tStatus,
                        "invalid report request, multi fields " + reportType + " " + ReportType.RESOURCE_GROUP_REPORT);
                return result;
            }
            activeWorkGroups = request.active_workgroups;
            reportType = ReportType.RESOURCE_GROUP_REPORT;
        }

        if (request.isSetResource_usage()) {
            if (reportType != ReportType.UNKNOWN_REPORT) {
                buildErrorResult(tStatus,
                        "invalid report request, multi fields " + reportType + " " + ReportType.RESOURCE_USAGE_REPORT);
                return result;
            }

            resourceUsage = request.getResource_usage();
            reportType = ReportType.RESOURCE_USAGE_REPORT;
        }

        List<TWorkGroupOp> workGroupOps =
                GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroupsNeedToDeliver(beId);
        result.setWorkgroup_ops(workGroupOps);

        ReportTask reportTask =
                new ReportTask(beId, reportType, tasks, disks, tablets, reportVersion, activeWorkGroups, resourceUsage);
        try {
            putToQueue(reportTask);
        } catch (Exception e) {
            tStatus.setStatus_code(TStatusCode.INTERNAL_ERROR);
            List<String> errorMsgs = Lists.newArrayList();
            errorMsgs.add("failed to put report task to queue. queue size: " + reportQueue.size());
            errorMsgs.add("err: " + e.getMessage());
            tStatus.setError_msgs(errorMsgs);

            LOG.warn(errorMsgs);
            return result;
        }

        LOG.debug("report received from be/computeNode {}. type: {}, current queue size: {}",
                beId, reportType, reportQueue.size());
        return result;
    }

    private void buildErrorResult(TStatus tStatus, String msg) {
        tStatus.setStatus_code(TStatusCode.INTERNAL_ERROR);
        List<String> errorMsgs = Lists.newArrayList();
        errorMsgs.add(msg);
        tStatus.setError_msgs(errorMsgs);
        LOG.warn(errorMsgs);
    }

    private void putToQueue(ReportTask reportTask) throws Exception {
        synchronized (pendingTaskMap) {
            if (!pendingTaskMap.containsKey(reportTask.type)) {
                throw new Exception("Unknown report task type" + reportTask.toString());
            }
            ReportTask oldTask = pendingTaskMap.get(reportTask.type).get(reportTask.beId);
            if (oldTask == null) {
                reportQueue.put(Pair.create(reportTask.beId, reportTask.type));
            } else {
                LOG.info("update be {} report task, type: {}", oldTask.beId, oldTask.type);
            }
            pendingTaskMap.get(reportTask.type).put(reportTask.beId, reportTask);
        }
    }

    private Map<Long, TTablet> buildTabletMap(List<TTablet> tabletList) {
        Map<Long, TTablet> tabletMap = Maps.newHashMap();
        for (TTablet tTablet : tabletList) {
            if (tTablet.getTablet_infos().isEmpty()) {
                continue;
            }

            tabletMap.put(tTablet.getTablet_infos().get(0).getTablet_id(), tTablet);
        }
        return tabletMap;
    }

    private class ReportTask extends LeaderTask {

        public long beId;
        public ReportType type;
        private Map<TTaskType, Set<Long>> tasks;
        private Map<String, TDisk> disks;
        private Map<Long, TTablet> tablets;
        private long reportVersion;
        private List<TWorkGroup> activeWorkGroups;
        private TResourceUsage resourceUsage;

        public ReportTask(long beId, ReportType type, Map<TTaskType, Set<Long>> tasks,
                          Map<String, TDisk> disks,
                          Map<Long, TTablet> tablets, long reportVersion,
                          List<TWorkGroup> activeWorkGroups,
                          TResourceUsage resourceUsage) {
            this.beId = beId;
            this.type = type;
            this.tasks = tasks;
            this.disks = disks;
            this.tablets = tablets;
            this.reportVersion = reportVersion;
            this.activeWorkGroups = activeWorkGroups;
            this.resourceUsage = resourceUsage;
        }

        @Override
        protected void exec() {
            if (tasks != null) {
                ReportHandler.taskReport(beId, tasks);
            }
            if (disks != null) {
                ReportHandler.diskReport(beId, disks);
            }
            if (tablets != null) {
                ReportHandler.tabletReport(beId, tablets, reportVersion);
            }
            if (activeWorkGroups != null) {
                ReportHandler.workgroupReport(beId, activeWorkGroups);
            }
            if (resourceUsage != null) {
                ReportHandler.resourceUsageReport(beId, resourceUsage);
            }
        }
    }

    private static void tabletReport(long backendId, Map<Long, TTablet> backendTablets, long backendReportVersion) {
        if (RunMode.isSharedDataMode()) {
            return;
        }
        long start = System.currentTimeMillis();
        LOG.info("backend[{}] reports {} tablet(s). report version: {}",
                backendId, backendTablets.size(), backendReportVersion);

        // storage medium map
        HashMap<Long, TStorageMedium> storageMediumMap =
                GlobalStateMgr.getCurrentState().getPartitionIdToStorageMediumMap();

        // db id -> tablet id
        ListMultimap<Long, Long> tabletSyncMap = ArrayListMultimap.create();
        // db id -> tablet id
        ListMultimap<Long, Long> tabletDeleteFromMeta = ArrayListMultimap.create();
        // tablet ids which schema hash is valid
        Set<Long> foundTabletsWithValidSchema = new HashSet<Long>();
        // tablet ids which schema hash is invalid
        Map<Long, TTabletInfo> foundTabletsWithInvalidSchema = new HashMap<Long, TTabletInfo>();
        // storage medium -> tablet id
        ListMultimap<TStorageMedium, Long> tabletMigrationMap = ArrayListMultimap.create();

        // dbid -> txn id -> partition id -> [partition info]
        Map<Long, Map<Long, Map<Long, TPartitionVersionInfo>>> transactionsToPublish = Maps.newHashMap();

        Map<Long, Long> transactionsToCommitTime = Maps.newHashMap();
        ListMultimap<Long, Long> transactionsToClear = ArrayListMultimap.create();

        // db id -> tablet id
        ListMultimap<Long, Long> tabletRecoveryMap = ArrayListMultimap.create();

        Set<Long> tabletWithoutPartitionId = Sets.newHashSet();

        // 1. do the diff. find out (intersection) / (be - meta) / (meta - be)
        GlobalStateMgr.getCurrentInvertedIndex().tabletReport(backendId, backendTablets, storageMediumMap,
                tabletSyncMap,
                tabletDeleteFromMeta,
                foundTabletsWithValidSchema,
                foundTabletsWithInvalidSchema,
                tabletMigrationMap,
                transactionsToPublish,
                transactionsToCommitTime,
                transactionsToClear,
                tabletRecoveryMap,
                tabletWithoutPartitionId);

        // 2. sync
        sync(backendTablets, tabletSyncMap, backendId, backendReportVersion);

        // 3. delete (meta - be)
        // BE will automatically drop defective tablets. these tablets should also be dropped in globalStateMgr
        deleteFromMeta(tabletDeleteFromMeta, backendId, backendReportVersion);

        // 4. handle (be - meta)
        deleteFromBackend(backendTablets, foundTabletsWithValidSchema, foundTabletsWithInvalidSchema, backendId);

        // 5. migration (ssd <-> hdd)
        handleMigration(tabletMigrationMap, backendId);

        // 6. send clear transactions to be
        handleClearTransactions(transactionsToClear, backendId);

        // 7. send publish version request to be
        handleRepublishVersionInfo(transactionsToPublish, transactionsToCommitTime, backendId);

        // 8. send recover request to be
        handleRecoverTablet(tabletRecoveryMap, backendTablets, backendId);

        // 9. send set tablet partition info to be
        handleSetTabletPartitionId(backendId, tabletWithoutPartitionId);

        // 10. send set tablet in memory to be
        handleSetTabletInMemory(backendId, backendTablets);

        // 11. send set tablet enable persistent index to be
        handleSetTabletEnablePersistentIndex(backendId, backendTablets);

        // 12. send set table binlog config to be
        handleSetTabletBinlogConfig(backendId, backendTablets);

        // 13. send primary index cache expire sec to be
        handleSetPrimaryIndexCacheExpireSec(backendId, backendTablets);

<<<<<<< HEAD
        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentSystemInfo();
=======
        // 14. send update tablet schema to be
        handleUpdateTableSchema(backendId, backendTablets);

        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
>>>>>>> 113d301de2 ([Enhancement] Update tablet schema after fast schema evolution (#39869))
        Backend reportBackend = currentSystemInfo.getBackend(backendId);
        if (reportBackend != null) {
            BackendStatus backendStatus = reportBackend.getBackendStatus();
            backendStatus.lastSuccessReportTabletsTime = TimeUtils.longToTimeString(start);
        }

        long cost = System.currentTimeMillis() - start;
        if (cost > MAX_REPORT_HANDLING_TIME_LOGGING_THRESHOLD_MS) {
            LOG.info("tablet report from backend[{}] cost: {} ms", backendId, cost);
        }
    }

    private static void taskReport(long backendId, Map<TTaskType, Set<Long>> runningTasks) {
        LOG.debug("begin to handle task report from backend {}", backendId);
        long start = System.currentTimeMillis();

        if (LOG.isDebugEnabled()) {
            for (TTaskType type : runningTasks.keySet()) {
                Set<Long> taskSet = runningTasks.get(type);
                if (!taskSet.isEmpty()) {
                    String signatures = StringUtils.join(taskSet, ", ");
                    LOG.debug("backend task[{}]: {}", type.name(), signatures);
                }
            }
        }

        List<AgentTask> diffTasks = AgentTaskQueue.getDiffTasks(backendId, runningTasks);

        AgentBatchTask batchTask = new AgentBatchTask();
        long taskReportTime = System.currentTimeMillis();
        for (AgentTask task : diffTasks) {
            // these tasks no need to do diff
            // 1. CREATE
            // 2. SYNC DELETE
            // 3. CHECK_CONSISTENCY
            if (task.getTaskType() == TTaskType.CREATE || task.getTaskType() == TTaskType.CHECK_CONSISTENCY) {
                continue;
            }

            // to escape sending duplicate agent task to be
            if (task.shouldResend(taskReportTime)) {
                batchTask.addTask(task);
            }
        }

        LOG.debug("get {} diff task(s) to resend", batchTask.getTaskNum());
        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }

        long cost = System.currentTimeMillis() - start;
        if (batchTask.getTaskNum() != 0 || cost > MAX_REPORT_HANDLING_TIME_LOGGING_THRESHOLD_MS) {
            LOG.info("finished to handle task report from backend {}, diff task num: {}. cost: {} ms",
                    backendId, batchTask.getTaskNum(), cost);
        }
    }

    private static void diskReport(long backendId, Map<String, TDisk> backendDisks) {
        LOG.debug("begin to handle disk report from backend {}", backendId);
        long start = System.currentTimeMillis();
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend doesn't exist. id: " + backendId);
            return;
        }

        backend.updateDisks(backendDisks);
        long cost = System.currentTimeMillis() - start;
        if (cost > MAX_REPORT_HANDLING_TIME_LOGGING_THRESHOLD_MS) {
            LOG.info("finished to handle disk report from backend {}, cost: {} ms",
                    backendId, cost);
        }
    }

    private static void workgroupReport(long backendId, List<TWorkGroup> workGroups) {
        LOG.debug("begin to handle workgroup report from backend{}", backendId);
        long start = System.currentTimeMillis();
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend does't exist. id: " + backendId);
        }
        GlobalStateMgr.getCurrentState().getResourceGroupMgr().saveActiveResourceGroupsForBe(backendId, workGroups);
        LOG.debug("finished to handle workgroup report from backend{}, cost: {} ms, num: {}",
                backendId, System.currentTimeMillis() - start, workGroups.size());
    }

    // For test.
    public static void testHandleResourceUsageReport(long backendId, TResourceUsage usage) {
        resourceUsageReport(backendId, usage);
    }

    private static void resourceUsageReport(long backendId, TResourceUsage usage) {
        LOG.debug("begin to handle resource usage report from backend {}", backendId);
        long start = System.currentTimeMillis();
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(
                backendId, usage.getNum_running_queries(), usage.getMem_limit_bytes(), usage.getMem_used_bytes(),
                usage.getCpu_used_permille(), usage.isSetGroup_usages() ? usage.getGroup_usages() : null);
        LOG.debug("finished to handle resource usage report from backend {}, cost: {} ms",
                backendId, (System.currentTimeMillis() - start));
    }

    private static void sync(Map<Long, TTablet> backendTablets, ListMultimap<Long, Long> tabletSyncMap,
                             long backendId, long backendReportVersion) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        for (Long dbId : tabletSyncMap.keySet()) {
            Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }
            List<Long> allTabletIds = tabletSyncMap.get(dbId);
            int offset = 0;

            LOG.info("before sync tablets in db[{}]. report num: {}. backend[{}]",
                    dbId, allTabletIds.size(), backendId);
            while (offset < allTabletIds.size()) {
                int syncCounter = 0;
                int logSyncCounter = 0;
                List<Long> tabletIds = allTabletIds.subList(offset, allTabletIds.size());
                db.writeLock();
                try {
                    List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
                    for (int i = 0; i < tabletMetaList.size(); i++) {
                        offset++;
                        TabletMeta tabletMeta = tabletMetaList.get(i);
                        if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                            continue;
                        }
                        long tabletId = tabletIds.get(i);
                        long tableId = tabletMeta.getTableId();
                        OlapTable olapTable = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
                        if (olapTable == null) {
                            continue;
                        }

                        long partitionId = tabletMeta.getPartitionId();
                        Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(olapTable, partitionId);
                        if (partition == null) {
                            continue;
                        }

                        long indexId = tabletMeta.getIndexId();
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index == null) {
                            continue;
                        }
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                        LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                        if (tablet == null) {
                            continue;
                        }

                        Replica replica = tablet.getReplicaByBackendId(backendId);
                        if (replica == null) {
                            continue;
                        }
                        // yiguolei: it is very important here, if the replica is under schema change or
                        // rollup
                        // should ignore the report.
                        // eg.
                        // original replica import successfully, but the dest schema change replica
                        // failed
                        // the fe will sync the replica with the original replica, but ignore the schema
                        // change replica.
                        // if the last failed version is changed, then fe will think schema change
                        // successfully.
                        // this is an fatal error.
                        if (replica.getState() == ReplicaState.NORMAL) {
                            long metaVersion = replica.getVersion();
                            long backendVersion = -1L;
                            long backendMinReadableVersion = 0;
                            long rowCount = -1L;
                            long dataSize = -1L;
                            // schema change maybe successfully in fe, but not inform be, then be will
                            // report two schema hash
                            // just select the dest schema hash
                            for (TTabletInfo tabletInfo : backendTablets.get(tabletId).getTablet_infos()) {
                                if (tabletInfo.getSchema_hash() == schemaHash) {
                                    if (Config.enable_sync_publish) {
                                        backendVersion = tabletInfo.getMax_readable_version();
                                    } else {
                                        backendVersion = tabletInfo.getVersion();
                                    }
                                    backendMinReadableVersion = tabletInfo.getMin_readable_version();
                                    rowCount = tabletInfo.getRow_count();
                                    dataSize = tabletInfo.getData_size();
                                    break;
                                }
                            }
                            if (backendVersion == -1L) {
                                continue;
                            }

                            // 1. replica is not set bad force
                            // 2. metaVersion < backendVersion or (metaVersion == backendVersion &&
                            // replica.isBad())
                            if (!replica.isSetBadForce() &&
                                    ((metaVersion < backendVersion) ||
                                            (metaVersion == backendVersion && replica.isBad()))) {

                                // happens when
                                // 1. PUSH finished in BE but failed or not yet report to FE
                                // 2. repair for VERSION_INCOMPLETE finished in BE, but failed or not yet report
                                // to FE
                                replica.updateRowCount(backendVersion, backendMinReadableVersion, dataSize, rowCount);

                                if (replica.getLastFailedVersion() < 0) {
                                    // last failed version < 0 means this replica becomes health after sync,
                                    // so we write an edit log to sync this operation
                                    replica.setBad(false);
                                    ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tableId,
                                            partitionId, indexId, tabletId, backendId, replica.getId(),
                                            replica.getVersion(), schemaHash,
                                            dataSize, rowCount,
                                            replica.getLastFailedVersion(),
                                            replica.getLastSuccessVersion(),
                                            replica.getMinReadableVersion());
                                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateReplica(info);
                                    ++logSyncCounter;
                                }

                                ++syncCounter;
                                LOG.debug("sync replica {} of tablet {} in backend {} in db {}. report version: {}",
                                        replica.getId(), tabletId, backendId, dbId, backendReportVersion);
                            } else {
                                LOG.debug("replica {} of tablet {} in backend {} version is changed"
                                                + " between check and real sync. meta[{}]. backend[{}]",
                                        replica.getId(), tabletId, backendId, metaVersion,
                                        backendVersion);
                            }
                        }
                        // update replica operation is heavy, couldn't do much in db write lock
                        if (logSyncCounter > 10) {
                            break;
                        }
                    } // end for tabletMetaSyncMap
                } finally {
                    db.writeUnlock();
                }
                LOG.info("sync {} update {} in {} tablets in db[{}]. backend[{}]", syncCounter, logSyncCounter,
                        offset, dbId, backendId);
            }
        } // end for dbs
    }

    private static void deleteFromMeta(ListMultimap<Long, Long> tabletDeleteFromMeta, long backendId,
                                       long backendReportVersion) {
        AgentBatchTask createReplicaBatchTask = new AgentBatchTask();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        final long MAX_DB_WLOCK_HOLDING_TIME_MS = 1000L;
        DB_TRAVERSE:
        for (Long dbId : tabletDeleteFromMeta.keySet()) {
            Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            long lockStartTime = System.currentTimeMillis();
            try {
                int deleteCounter = 0;
                List<Long> tabletIds = tabletDeleteFromMeta.get(dbId);
                List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    // Because we need to write bdb with db write lock hold,
                    // to avoid block other threads too long, we periodically release and
                    // acquire the db write lock (every MAX_DB_WLOCK_HOLDING_TIME_MS milliseconds).
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lockStartTime > MAX_DB_WLOCK_HOLDING_TIME_MS) {
                        db.writeUnlock();
                        db = globalStateMgr.getDbIncludeRecycleBin(dbId);
                        if (db == null) {
                            continue DB_TRAVERSE;
                        }
                        db.writeLock();
                        lockStartTime = currentTime;
                    }

                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                        continue;
                    }
                    long tabletId = tabletIds.get(i);
                    long tableId = tabletMeta.getTableId();
                    OlapTable olapTable = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
                    if (olapTable == null) {
                        continue;
                    }

                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(olapTable, partitionId);
                    if (partition == null) {
                        continue;
                    }

                    short replicationNum =
                            globalStateMgr.getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(),
                                    partition.getId());
                    if (replicationNum == (short) -1) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }
                    if (index.getState() == IndexState.SHADOW) {
                        // This index is under schema change or rollup, tablet may not be created on BE.
                        // ignore it.
                        continue;
                    }

                    LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (replica == null) {
                        continue;
                    }

                    long currentBackendReportVersion =
                            GlobalStateMgr.getCurrentSystemInfo().getBackendReportVersion(backendId);
                    if (backendReportVersion < currentBackendReportVersion) {
                        continue;
                    }

                    ReplicaState state = replica.getState();
                    if (state == ReplicaState.NORMAL || state == ReplicaState.SCHEMA_CHANGE) {
                        // if state is PENDING / ROLLUP / CLONE
                        // it's normal that the replica is not created in BE but exists in meta.
                        // so we do not delete it.
                        List<Replica> replicas = tablet.getImmutableReplicas();
                        if (replicas.size() <= 1) {
                            LOG.debug("backend [{}] invalid situation. tablet[{}] has few replica[{}], "
                                            + "replica num setting is [{}]",
                                    backendId, tabletId, replicas.size(), replicationNum);
                            // there is a replica in FE, but not in BE and there is only one replica in this tablet
                            // in this case, it means data is lost.
                            // should generate a create replica request to BE to create a replica forcibly.
                            if (replicas.size() == 1) {
                                if (Config.recover_with_empty_tablet) {
                                    // only create this task if force recovery is true
                                    LOG.warn("tablet {} has only one replica {} on backend {}"
                                                    + " and it is lost. create an empty replica to recover it",
                                            tabletId, replica.getId(), backendId);
                                    MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                                    Set<String> bfColumns = olapTable.getCopiedBfColumns();
                                    double bfFpp = olapTable.getBfFpp();
                                    CreateReplicaTask createReplicaTask = new CreateReplicaTask(backendId, dbId,
                                            tableId, partitionId, indexId, tabletId, indexMeta.getShortKeyColumnCount(),
                                            indexMeta.getSchemaHash(), indexMeta.getSchemaVersion(),
                                            partition.getVisibleVersion(),
                                            indexMeta.getKeysType(),
                                            olapTable.getStorageType(),
                                            TStorageMedium.HDD, indexMeta.getSchema(), bfColumns, bfFpp, null,
                                            olapTable.getCopiedIndexes(),
                                            olapTable.isInMemory(),
                                            olapTable.enablePersistentIndex(),
                                            olapTable.primaryIndexCacheExpireSec(),
                                            olapTable.getPartitionInfo().getTabletType(partitionId),
                                            olapTable.getCompressionType(), indexMeta.getSortKeyIdxes(),
                                            indexMeta.getSortKeyUniqueIds());
                                    createReplicaTask.setRecoverySource(RecoverySource.REPORT);
                                    createReplicaBatchTask.addTask(createReplicaTask);
                                } else {
                                    // just set this replica as bad
                                    if (replica.setBad(true)) {
                                        LOG.warn("tablet {} has only one replica {} on backend {}"
                                                        + " and it is lost, set it as bad",
                                                tabletId, replica.getId(), backendId);
                                        BackendTabletsInfo tabletsInfo = new BackendTabletsInfo(backendId);
                                        tabletsInfo.setBad(true);
                                        ReplicaPersistInfo replicaPersistInfo = ReplicaPersistInfo.createForReport(
                                                dbId, tableId, partitionId, indexId, tabletId, backendId,
                                                replica.getId());
                                        tabletsInfo.addReplicaInfo(replicaPersistInfo);
                                        GlobalStateMgr.getCurrentState().getEditLog()
                                                .logBackendTabletsInfo(tabletsInfo);
                                    }
                                }
                            }
                            continue;
                        }

                        // Defer the meta delete to next tablet report, see `Replica.deferReplicaDeleteToNextReport`
                        // for details.
                        if (replica.getDeferReplicaDeleteToNextReport()) {
                            replica.setDeferReplicaDeleteToNextReport(false);
                            continue;
                        } else {
                            tablet.deleteReplicaByBackendId(backendId);
                            ++deleteCounter;
                        }

                        // remove replica related tasks
                        AgentTaskQueue.removeReplicaRelatedTasks(backendId, tabletId);

                        // write edit log
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(dbId, tableId, partitionId,
                                indexId, tabletId, backendId);

                        GlobalStateMgr.getCurrentState().getEditLog().logDeleteReplica(info);
                        LOG.warn("delete replica[{}] with state[{}] in tablet[{}] from meta. backend[{}]," +
                                        " report version: {}, current report version: {}",
                                replica.getId(), replica.getState().name(), tabletId, backendId, backendReportVersion,
                                currentBackendReportVersion);

                        // check for clone
                        replicas = tablet.getImmutableReplicas();
                        if (replicas.size() == 0) {
                            LOG.error("invalid situation. tablet[{}] is empty", tabletId);
                        }
                    }
                } // end for tabletMetas
                LOG.info("delete {} replica(s) from globalStateMgr in db[{}]", deleteCounter, dbId);
            } finally {
                db.writeUnlock();
            }
        } // end for dbs

        if (Config.recover_with_empty_tablet && createReplicaBatchTask.getTaskNum() > 0) {
            // must add to queue, so that when task finish report, the task can be found in queue.
            // the task will be eventually removed from queue by task report, so no need to worry
            // about the residuals.
            AgentTaskQueue.addBatchTask(createReplicaBatchTask);
            AgentTaskExecutor.submit(createReplicaBatchTask);
        }
    }

    private static void addDropReplicaTask(AgentBatchTask batchTask, long backendId,
                                           long tabletId, int schemaHash, String reason, boolean force) {
        DropReplicaTask task =
                new DropReplicaTask(backendId, tabletId, schemaHash, force);
        batchTask.addTask(task);
        LOG.info("delete tablet[{}] from backend[{}] because {}",
                tabletId, backendId, reason);
    }

    @VisibleForTesting
    public static boolean checkReadyToBeDropped(long tabletId, long backendId) {
        Long time = TABLET_TO_DROP_TIME.get(tabletId, backendId);
        long currentTimeMs = System.currentTimeMillis();
        if (time == null) {
            TABLET_TO_DROP_TIME.put(tabletId, backendId,
                    currentTimeMs + Config.tablet_report_drop_tablet_delay_sec * 1000);
        } else {
            boolean ready = currentTimeMs > time;
            if (ready) {
                // clean the map
                TABLET_TO_DROP_TIME.remove(tabletId, backendId);
                return true;
            }
        }

        return false;
    }

    private static void deleteFromBackend(Map<Long, TTablet> backendTablets,
                                          Set<Long> foundTabletsWithValidSchema,
                                          Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                                          long backendId) {
        int deleteFromBackendCounter = 0;
        int addToMetaCounter = 0;
        int maxTaskSendPerBe = Config.max_agent_tasks_send_per_be;
        AgentBatchTask batchTask = new AgentBatchTask();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Long tabletId : backendTablets.keySet()) {
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null && maxTaskSendPerBe > 0) {
                if (!checkReadyToBeDropped(tabletId, backendId)) {
                    continue;
                }
                // We need to clean these ghost tablets from current backend, or else it will
                // continue to report them to FE forever and add some processing overhead(the tablet report
                // process is protected with DB S lock).
                addDropReplicaTask(batchTask, backendId, tabletId,
                        -1 /* Unknown schema hash */, "not found in meta", invertedIndex.tabletForceDelete(tabletId, backendId));
                if (!FeConstants.runningUnitTest) {
                    invertedIndex.eraseTabletForceDelete(tabletId, backendId);
                }
                ++deleteFromBackendCounter;
                --maxTaskSendPerBe;
                continue;
            }

            if (tabletMeta != null && tabletMeta.isLakeTablet()) {
                continue;
            }

            TTablet backendTablet = backendTablets.get(tabletId);
            for (TTabletInfo backendTabletInfo : backendTablet.getTablet_infos()) {
                boolean needDelete = false;
                String errorMsgAddingReplica = null;
                if (!foundTabletsWithValidSchema.contains(tabletId)) {
                    if (isBackendReplicaHealthy(backendTabletInfo)) {
                        // if this tablet is not in meta. try adding it.
                        // if add failed. delete this tablet from backend.
                        try {
                            addReplica(tabletId, backendTabletInfo, backendId);
                            // update counter
                            ++addToMetaCounter;
                        } catch (MetaNotFoundException e) {
                            // Enough replica causing replica add failed should be treated as a normal
                            // case to avoid too many useless warning logs. This will happen when a clone task
                            // finished for decommission or balance, and the redundant replica has been deleted
                            // from some BE, but the BE's tablet report doesn't see this deletion and still report
                            // the deleted tablet info to FE.
                            if (e.getErrorCode() != InternalErrorCode.REPLICA_ENOUGH_ERR) {
                                LOG.debug("failed add to meta. tablet[{}], backend[{}]. {}",
                                        tabletId, backendId, e.getMessage());
                            }
                            errorMsgAddingReplica = e.getMessage();
                            needDelete = true;
                        }
                    } else {
                        needDelete = true;
                    }
                }

                if (needDelete && maxTaskSendPerBe > 0) {
                    // drop replica
                    addDropReplicaTask(batchTask, backendId, tabletId, backendTabletInfo.getSchema_hash(),
                            "invalid meta, " +
                                    (errorMsgAddingReplica != null ? errorMsgAddingReplica : "replica unhealthy"),
                            true);
                    ++deleteFromBackendCounter;
                    --maxTaskSendPerBe;
                }
            } // end for tabletInfos

            if (foundTabletsWithInvalidSchema.containsKey(tabletId) && maxTaskSendPerBe > 0) {
                // this tablet is found in meta but with invalid schema hash.
                // delete it.
                int schemaHash = foundTabletsWithInvalidSchema.get(tabletId).getSchema_hash();
                addDropReplicaTask(batchTask, backendId, tabletId, schemaHash,
                        "invalid schema hash: " + schemaHash, true);
                ++deleteFromBackendCounter;
                --maxTaskSendPerBe;
            }
        } // end for backendTabletIds

        AgentTaskExecutor.submit(batchTask);

        if (deleteFromBackendCounter != 0 || addToMetaCounter != 0) {
            LOG.info("delete {} tablet(s), add {} replica(s) to meta, backend[{}]",
                    deleteFromBackendCounter, addToMetaCounter, backendId);
        }
    }

    // replica is used and no version missing
    private static boolean isBackendReplicaHealthy(TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            return false;
        }
        if (backendTabletInfo.isSetVersion_miss() && backendTabletInfo.isVersion_miss()) {
            return false;
        }
        return true;
    }

    public static boolean migratableTablet(Database db, OlapTable tbl, long physicalPartitionId, long indexId, long tabletId) {
        if (tbl.getKeysType() != KeysType.PRIMARY_KEYS) {
            return true;
        }

        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentSystemInfo();
        List<Backend> backends = currentSystemInfo.getBackends();
        long maxLastSuccessReportTabletsTime = -1L;

        for (Backend be : backends) {
            long lastSuccessReportTabletsTime = TimeUtils.timeStringToLong(be.getBackendStatus().lastSuccessReportTabletsTime);
            maxLastSuccessReportTabletsTime = Math.max(maxLastSuccessReportTabletsTime, lastSuccessReportTabletsTime);
        }

        db.readLock();
        try {
            PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null || physicalPartition.getVisibleVersionTime() > maxLastSuccessReportTabletsTime) {
                // partition is null or tablet report has not been updated, unmigratable
                return false;
            }
            MaterializedIndex idx = physicalPartition.getIndex(indexId);
            if (idx == null) {
                // index is null, unmigratable
                return false;
            }

            LocalTablet tablet = (LocalTablet) idx.getTablet(tabletId);
            // get max rowset creation time for all replica of the tablet
            long maxRowsetCreationTime = -1L;
            for (Replica replica : tablet.getImmutableReplicas()) {
                maxRowsetCreationTime = Math.max(maxRowsetCreationTime, replica.getMaxRowsetCreationTime());
                if (replica.getLastReportVersion() <= 1) {
                    // unmigratable if it is a empty tablet
                    return false;
                }
            }

            // get negative max rowset creation time or too close to the max rowset creation time, unmigratable
            if (maxRowsetCreationTime < 0 || System.currentTimeMillis() - maxRowsetCreationTime * 1000 <=
                    Config.primary_key_disk_schedule_time * 1000) {
                return false;
            }

            return true;
        } finally {
            db.readUnlock();
        }
    }

    protected static void handleMigration(ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap,
                                          long backendId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        AgentBatchTask batchTask = new AgentBatchTask();

        OUTER:
        for (TStorageMedium storageMedium : tabletMetaMigrationMap.keySet()) {
            List<Long> tabletIds = tabletMetaMigrationMap.get(storageMedium);
            List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                long tabletId = tabletIds.get(i);
                TabletMeta tabletMeta = tabletMetaList.get(i);

                // 1. If size of tabletMigrationMap exceeds (Config.tablet_sched_max_migration_task_sent_once - running_tasks_on_be),
                // dot not send more tasks. The number of tasks running on BE cannot exceed Config.tablet_sched_max_migration_task_sent_once
                if (batchTask.getTaskNum() >=
                        Config.tablet_sched_max_migration_task_sent_once
                                - AgentTaskQueue.getTaskNum(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, false)) {
                    LOG.debug("size of tabletMigrationMap + size of running tasks on BE is bigger than {}",
                            Config.tablet_sched_max_migration_task_sent_once);
                    break OUTER;
                }

                // 2. If the task already running on BE, do not send again
                if (AgentTaskQueue.getTask(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, tabletId) != null) {
                    LOG.debug("migrate of tablet:{} is already running on BE", tabletId);
                    continue;
                }

                // 3. There are some limitations for primary table, details in migratableTablet()
                Database db = GlobalStateMgr.getCurrentState().getDb(tabletMeta.getDbId());
                if (db == null) {
                    continue;
                }
                OlapTable table;
                db.readLock();
                try {
                    table = (OlapTable) db.getTable(tabletMeta.getTableId());
                    if (table == null) {
                        continue;
                    }
                } finally {
                    db.readUnlock();
                }

                if (!migratableTablet(db, table, tabletMeta.getPhysicalPartitionId(), tabletMeta.getIndexId(), tabletId)) {
                    continue;
                }

                // always get old schema hash(as effective one)
                int effectiveSchemaHash = tabletMeta.getOldSchemaHash();
                StorageMediaMigrationTask task = new StorageMediaMigrationTask(backendId, tabletId,
                        effectiveSchemaHash, storageMedium);
                batchTask.addTask(task);
            }
        }

        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleRepublishVersionInfo(
            Map<Long, Map<Long, Map<Long, TPartitionVersionInfo>>> transactionsToPublish,
            Map<Long, Long> transactionsToCommitTime,
            long backendId) {
        AgentBatchTask batchTask = new AgentBatchTask();
        long createPublishVersionTaskTime = System.currentTimeMillis();
        for (Long dbId : transactionsToPublish.keySet()) {
            Map<Long, Map<Long, TPartitionVersionInfo>> map = transactionsToPublish.get(dbId);
            for (long txnId : map.keySet()) {
                long commitTime = transactionsToCommitTime.get(txnId);
                PublishVersionTask task =
                        new PublishVersionTask(backendId, txnId, dbId, commitTime,
                                map.get(txnId).values().stream().collect(Collectors.toList()), null, null,
                                createPublishVersionTaskTime, null,
                                Config.enable_sync_publish, TTxnType.TXN_NORMAL);
                batchTask.addTask(task);
                // add to AgentTaskQueue for handling finish report.
                AgentTaskQueue.addTask(task);
            }
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleRecoverTablet(ListMultimap<Long, Long> tabletRecoveryMap,
                                            Map<Long, TTablet> backendTablets, long backendId) {
        if (tabletRecoveryMap.isEmpty()) {
            return;
        }

        // print a warning log here to indicate the exceptions on the backend
        LOG.warn("find {} tablets on backend {} which is bad or misses versions that need clone or force recovery",
                tabletRecoveryMap.size(), backendId);

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        BackendTabletsInfo backendTabletsInfo = new BackendTabletsInfo(backendId);
        backendTabletsInfo.setBad(true);
        for (Long dbId : tabletRecoveryMap.keySet()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                List<Long> tabletIds = tabletRecoveryMap.get(dbId);
                List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                        continue;
                    }
                    long tabletId = tabletIds.get(i);
                    long tableId = tabletMeta.getTableId();
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }

                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }

                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                    LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (replica == null) {
                        continue;
                    }

                    if (replica.setBad(true)) {
                        LOG.warn("set bad for replica {} of tablet {} on backend {}",
                                replica.getId(), tabletId, backendId);
                        ReplicaPersistInfo replicaPersistInfo = ReplicaPersistInfo.createForReport(
                                dbId, tableId, partitionId, indexId, tabletId, backendId, replica.getId());
                        backendTabletsInfo.addReplicaInfo(replicaPersistInfo);
                    }
                }
            } finally {
                db.writeUnlock();
            }
        } // end for recovery map

        if (!backendTabletsInfo.isEmpty()) {
            // need to write edit log the sync the bad info to other FEs
            GlobalStateMgr.getCurrentState().getEditLog().logBackendTabletsInfo(backendTabletsInfo);
        }
    }

    private static void handleSetTabletPartitionId(long backendId, Set<Long> tabletWithoutPartitionId) {
        if (!tabletWithoutPartitionId.isEmpty()) {
            LOG.info("find [{}] tablets without partition id, try to set them", tabletWithoutPartitionId.size());
        }
        if (tabletWithoutPartitionId.isEmpty()) {
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory
                .createPartitionIdUpdateTask(backendId, tabletWithoutPartitionId);
        batchTask.addTask(task);
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleSetTabletInMemory(long backendId, Map<Long, TTablet> backendTablets) {
        // <tablet id, tablet in memory>
        List<Pair<Long, Boolean>> tabletToInMemory = Lists.newArrayList();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetIs_in_memory()) {
                    continue;
                }
                long tabletId = tabletInfo.getTablet_id();
                boolean beIsInMemory = tabletInfo.is_in_memory;
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
                long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;
                long partitionId =
                        tabletMeta != null ? tabletMeta.getPartitionId() : TabletInvertedIndex.NOT_EXIST_VALUE;

                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }
                    boolean feIsInMemory = olapTable.getPartitionInfo().getIsInMemory(partitionId);
                    if (beIsInMemory != feIsInMemory) {
                        tabletToInMemory.add(new Pair<>(tabletId, feIsInMemory));
                    }
                } finally {
                    db.readUnlock();
                }
            }
        }

        // When reported, needn't synchronous
        if (!tabletToInMemory.isEmpty()) {
            LOG.info("find [{}] tablet(s) which need to be set with in-memory state", tabletToInMemory.size());
            AgentBatchTask batchTask = new AgentBatchTask();
            TabletMetadataUpdateAgentTask
                    task = TabletMetadataUpdateAgentTaskFactory.createIsInMemoryUpdateTask(backendId, tabletToInMemory);
            batchTask.addTask(task);
            AgentTaskExecutor.submit(batchTask);
        }
    }

    public static void testHandleSetTabletEnablePersistentIndex(long backendId, Map<Long, TTablet> backendTablets) {
        handleSetTabletEnablePersistentIndex(backendId, backendTablets);
    }

    public static void testHandleSetTabletBinlogConfig(long backendId, Map<Long, TTablet> backendTablets) {
        handleSetTabletBinlogConfig(backendId, backendTablets);
    }

    private static void handleSetTabletEnablePersistentIndex(long backendId, Map<Long, TTablet> backendTablets) {
        List<Pair<Long, Boolean>> tabletToEnablePersistentIndex = Lists.newArrayList();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetEnable_persistent_index()) {
                    continue;
                }
                long tabletId = tabletInfo.getTablet_id();
                boolean beEnablePersistentIndex = tabletInfo.enable_persistent_index;
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
                long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;

                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }
                    boolean feEnablePersistentIndex = olapTable.enablePersistentIndex();
                    if (beEnablePersistentIndex != feEnablePersistentIndex) {
                        tabletToEnablePersistentIndex.add(new Pair<>(tabletId, feEnablePersistentIndex));
                    }
                } finally {
                    db.readUnlock();
                }
            }
        }

        if (!tabletToEnablePersistentIndex.isEmpty()) {
            LOG.info("find [{}] tablet(s) which need to be set with persistent index enabled",
                    tabletToEnablePersistentIndex.size());
            AgentBatchTask batchTask = new AgentBatchTask();
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory
                    .createEnablePersistentIndexUpdateTask(backendId, tabletToEnablePersistentIndex);
            batchTask.addTask(task);
            if (FeConstants.runningUnitTest) {
                AgentTaskExecutor.submit(batchTask);
            }
        }
    }


    public static void testHandleSetPrimaryIndexCacheExpireSec(long backendId, Map<Long, TTablet> backendTablets) {
        handleSetPrimaryIndexCacheExpireSec(backendId, backendTablets);
    }

    private static void handleSetPrimaryIndexCacheExpireSec(long backendId, Map<Long, TTablet> backendTablets) {
        List<Pair<Long, Integer>> tabletToPrimaryCacheExpireSec = Lists.newArrayList();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetPrimary_index_cache_expire_sec()) {
                    continue;
                }
                long tabletId = tabletInfo.getTablet_id();
                int bePrimaryIndexCacheExpireSec = tabletInfo.primary_index_cache_expire_sec;
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
                long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;

                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }
                    int fePrimaryIndexCacheExpireSec = olapTable.primaryIndexCacheExpireSec();
                    if (bePrimaryIndexCacheExpireSec != fePrimaryIndexCacheExpireSec) {
                        tabletToPrimaryCacheExpireSec.add(new Pair<>(tabletId, fePrimaryIndexCacheExpireSec));
                    }
                } finally {
                    db.readUnlock();
                }
            }
        }

        if (!tabletToPrimaryCacheExpireSec.isEmpty()) {
            LOG.info("find [{}] tablet(s) which need to be set primary index cache expire sec",
                    tabletToPrimaryCacheExpireSec.size());
            AgentBatchTask batchTask = new AgentBatchTask();
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory
                    .createPrimaryIndexCacheExpireTimeUpdateTask(backendId, tabletToPrimaryCacheExpireSec);
            batchTask.addTask(task);
            if (!FeConstants.runningUnitTest) {
                AgentTaskExecutor.submit(batchTask);
            }
        }
    }

    public static void testHandleUpdateTableSchema(long backendId, Map<Long, TTablet> backendTablets) {
        handleUpdateTableSchema(backendId, backendTablets);
    }

    private static void handleUpdateTableSchema(long backendId, Map<Long, TTablet> backendTablets) {
        Table<Long, Long, List<Long>> tableToIndexTabletMap = HashBasedTable.create();
        Map<Long, Long> tableToDb = Maps.newHashMap();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // split tablets by db, table and index
        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetTablet_schema_version()) {
                    continue;
                }
                long tabletId = tabletInfo.getTablet_id();
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    continue;
                }

                long dbId = tabletMeta.getDbId();
                long tableId = tabletMeta.getTableId();
                long indexId = tabletMeta.getIndexId();

                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    continue;
                }
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }
                    MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                    if (indexMeta == null) {
                        continue;
                    }
                    int schemaVersion = tabletInfo.tablet_schema_version;
                    int latestSchemaVersion = indexMeta.getSchemaVersion();
                    if (schemaVersion < latestSchemaVersion) {
                        List<Long> tabletsList = tableToIndexTabletMap.get(tableId, indexId);
                        if (tabletsList != null) {
                            tabletsList.add(Long.valueOf(tabletId));
                        } else {
                            tabletsList = Lists.newArrayList();
                            tabletsList.add(Long.valueOf(tabletId));
                            tableToIndexTabletMap.put(tableId, indexId, tabletsList);
                        }
                        tableToDb.put(tableId, dbId);
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }

        // create AgentBatch Task
        AgentBatchTask updateSchemaBatchTask = new AgentBatchTask();
        for (Table.Cell<Long, Long, List<Long>> cell : tableToIndexTabletMap.cellSet()) {
            Long tableId = cell.getRowKey();
            Long indexId = cell.getColumnKey();
            List<Long> tablets = cell.getValue();
            Long dbId = tableToDb.get(tableId);

            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable == null) {
                    continue;
                }
                MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                if (indexMeta == null) {
                    continue;
                }
                
                // already has one update scheam task, ignore to prevent send too many task
                if (indexMeta.hasUpdateSchemaTask(backendId)) {
                    continue;
                }

                List<String> columns = Lists.newArrayList();
                List<TColumn> columnsDesc = Lists.newArrayList();
                List<Integer> columnSortKeyUids = Lists.newArrayList();

                for (Column column : indexMeta.getSchema()) {
                    TColumn tColumn = column.toThrift();
                    tColumn.setColumn_name(
                            column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX, tColumn.column_name));
                    column.setIndexFlag(tColumn, olapTable.getIndexes(), olapTable.getBfColumns());
                    columnsDesc.add(tColumn);
                }
                if (indexMeta.getSortKeyUniqueIds() != null) {
                    columnSortKeyUids.addAll(indexMeta.getSortKeyUniqueIds());
                }
                TOlapTableColumnParam columnParam = new TOlapTableColumnParam(columnsDesc, columnSortKeyUids,
                                            indexMeta.getShortKeyColumnCount());

                UpdateSchemaTask task = new UpdateSchemaTask(backendId, db.getId(), olapTable.getId(),
                            indexId, tablets, indexMeta.getSchemaId(), indexMeta.getSchemaVersion(),
                            columnParam);
                updateSchemaBatchTask.addTask(task);
                indexMeta.addUpdateSchemaBackend(backendId);

            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        // send agent batch task
        if (updateSchemaBatchTask.getTaskNum() > 0) {
            for (AgentTask task : updateSchemaBatchTask.getAllTasks()) {
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(updateSchemaBatchTask);
        }

    }

    private static void handleSetTabletBinlogConfig(long backendId, Map<Long, TTablet> backendTablets) {
        List<Pair<Long, BinlogConfig>> tabletToBinlogConfig = Lists.newArrayList();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetBinlog_config_version()) {
                    continue;
                }
                long tabletId = tabletInfo.getTablet_id();
                long beBinlogConfigVersion = tabletInfo.binlog_config_version;
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
                long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;

                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    continue;
                }
                db.readLock();

                boolean needToCheck = false;
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                try {
                    if (olapTable == null) {
                        continue;
                    }

                    BinlogConfig binlogConfig = olapTable.getCurBinlogConfig();
                    // backward compatible
                    if (binlogConfig == null) {
                        continue;
                    }
                    Long feBinlogConfigVersion = binlogConfig.getVersion();
                    if (beBinlogConfigVersion < feBinlogConfigVersion) {
                        tabletToBinlogConfig.add(new Pair<>(tabletId, olapTable.getCurBinlogConfig()));
                    } else if (beBinlogConfigVersion == feBinlogConfigVersion) {
                        if (olapTable.isBinlogEnabled() && olapTable.getBinlogAvailableVersion().isEmpty()) {
                            // not to check here is that the function may need to get the write db lock
                            needToCheck = true;
                        }
                    } else {
                        LOG.warn("table {} binlog config version of tabletId: {}, BeId: {}, is {} " +
                                        "greater than version of FE, which is {}", olapTable.getName(), tabletId, backendId,
                                beBinlogConfigVersion, feBinlogConfigVersion);
                    }
                } finally {
                    db.readUnlock();
                }

                if (needToCheck) {
                    GlobalStateMgr.getCurrentState().getBinlogManager().checkAndSetBinlogAvailableVersion(db,
                            olapTable, tabletId, backendId);
                }
            }
        }

        LOG.debug("find [{}] tablets need set binlog config ", tabletToBinlogConfig.size());
        if (!tabletToBinlogConfig.isEmpty()) {
            AgentBatchTask batchTask = new AgentBatchTask();
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory.createBinlogConfigUpdateTask(
                    backendId, tabletToBinlogConfig);
            batchTask.addTask(task);
            AgentTaskExecutor.submit(batchTask);
        }
    }

    private static void handleClearTransactions(ListMultimap<Long, Long> transactionsToClear, long backendId) {
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long transactionId : transactionsToClear.keySet()) {
            ClearTransactionTask clearTransactionTask = new ClearTransactionTask(backendId,
                    transactionId, transactionsToClear.get(transactionId), TTxnType.TXN_NORMAL);
            batchTask.addTask(clearTransactionTask);
        }

        AgentTaskExecutor.submit(batchTask);
    }

    private static void addReplica(long tabletId, TTabletInfo backendTabletInfo, long backendId)
            throws MetaNotFoundException {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        long partitionId = tabletMeta != null ? tabletMeta.getPartitionId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        long indexId = tabletMeta != null ? tabletMeta.getIndexId() : TabletInvertedIndex.NOT_EXIST_VALUE;

        int schemaHash = backendTabletInfo.getSchema_hash();
        long version = backendTabletInfo.getVersion();
        long minReadableVersion = backendTabletInfo.getMin_readable_version();
        long dataSize = backendTabletInfo.getData_size();
        long rowCount = backendTabletInfo.getRow_count();

        Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db[" + dbId + "] does not exist");
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
            if (olapTable == null) {
                throw new MetaNotFoundException("table[" + tableId + "] does not exist");
            }

            Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(olapTable, partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("partition[" + partitionId + "] does not exist");
            }
            short replicationNum =
                    globalStateMgr.getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(), partition.getId());
            if (replicationNum == (short) -1) {
                throw new MetaNotFoundException("invalid replication number of partition [" + partitionId + "]");
            }

            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new MetaNotFoundException("index[" + indexId + "] does not exist");
            }

            LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                throw new MetaNotFoundException("tablet[" + tabletId + "] does not exist");
            }

            long visibleVersion = partition.getVisibleVersion();

            // check replica version
            if (version < visibleVersion) {
                throw new MetaNotFoundException(
                        String.format("version is invalid. tablet:%d < partitionVisible:%d replicas:%s", version,
                                visibleVersion, tablet.getReplicaInfos()));
            }

            // check schema hash
            if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                throw new MetaNotFoundException("schema hash is diff[" + schemaHash + "-"
                        + olapTable.getSchemaHashByIndexId(indexId) + "]");
            }

            // colocate table will delete Replica in meta when balancing,
            // but we need to rely on MetaNotFoundException to decide whether delete the tablet in backend.
            // delete tablet from backend if colocate tablet is healthy.
            ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
            if (colocateTableIndex.isColocateTable(olapTable.getId())) {
                ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
                Preconditions.checkState(groupId != null);
                int tabletOrderIdx = materializedIndex.getTabletOrderIdx(tabletId);
                Preconditions.checkState(tabletOrderIdx != -1);
                Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, tabletOrderIdx);
                TabletStatus status =
                        tablet.getColocateHealthStatus(visibleVersion, replicationNum, backendsSet);
                if (status == TabletStatus.HEALTHY) {
                    throw new MetaNotFoundException("colocate tablet [" + tabletId + "] is healthy");
                } else {
                    return;
                }
            }

            List<Long> aliveBeIdsInCluster = infoService.getBackendIds(true);
            Pair<TabletStatus, TabletSchedCtx.Priority> status = tablet.getHealthStatusWithPriority(infoService,
                    visibleVersion, replicationNum, aliveBeIdsInCluster);

            if (status.first == TabletStatus.VERSION_INCOMPLETE || status.first == TabletStatus.REPLICA_MISSING) {
                long lastFailedVersion = -1L;

                boolean initPartitionCreateByOldVersionStarRocks =
                        partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION &&
                                version == 2;

                if (initPartitionCreateByOldVersionStarRocks) {
                    // For some partition created by old version's StarRocks
                    // The init partition's version in FE is (1-0), the tablet's version in BE is (2-0)
                    // If the BE report version is (2-0) and partition's version is (1-0),
                    // we should add the tablet to meta.
                } else if (version > partition.getNextVersion() - 1) {
                    // this is a fatal error
                    throw new MetaNotFoundException("version is invalid. tablet[" + version + "-" + "]"
                            + ", partition's max version [" + (partition.getNextVersion() - 1) + "]");
                } else if (version < partition.getCommittedVersion()) {
                    lastFailedVersion = partition.getCommittedVersion();
                }

                long replicaId = GlobalStateMgr.getCurrentState().getNextId();
                Replica replica = new Replica(replicaId, backendId, version, schemaHash,
                        dataSize, rowCount, ReplicaState.NORMAL,
                        lastFailedVersion, version);
                tablet.addReplica(replica);

                // write edit log
                ReplicaPersistInfo info = ReplicaPersistInfo.createForAdd(dbId, tableId, partitionId, indexId,
                        tabletId, backendId, replicaId,
                        version, schemaHash, dataSize, rowCount,
                        lastFailedVersion, version, minReadableVersion);

                GlobalStateMgr.getCurrentState().getEditLog().logAddReplica(info);

                LOG.info("add replica[{}-{}] to globalStateMgr. backend:[{}] replicas: {}", tabletId, replicaId, backendId,
                        tablet.getReplicaInfos());
            } else {
                // replica is enough. check if this tablet is already in meta
                // (status changed between 'tabletReport()' and 'addReplica()')
                for (Replica replica : tablet.getImmutableReplicas()) {
                    if (replica.getBackendId() == backendId) {
                        // tablet is already in meta. return true
                        return;
                    }
                }
                throw new MetaNotFoundException(InternalErrorCode.REPLICA_ENOUGH_ERR,
                        "replica is enough[" + tablet.getImmutableReplicas().size() + "-" + replicationNum + "]");
            }
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    protected void runOneCycle() {
        while (true) {
            try {
                Pair<Long, ReportType> pair = reportQueue.take();
                ReportTask task = null;
                synchronized (pendingTaskMap) {
                    // using the lastest task
                    task = pendingTaskMap.get(pair.second).get(pair.first);
                    if (task == null) {
                        throw new Exception("pendingTaskMap not exists " + pair.first);
                    }
                    pendingTaskMap.get(task.type).remove(task.beId, task);
                }
                task.exec();
            } catch (Exception e) {
                LOG.warn("got interupted exception when executing report", e);
            }
        }
    }
}
