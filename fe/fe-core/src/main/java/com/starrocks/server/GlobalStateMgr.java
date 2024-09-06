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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Catalog.java

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

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.backup.BackupHandler;
import com.starrocks.binlog.BinlogManager;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MetaReplayState;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RefreshDictionaryCacheTaskDaemon;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.constraint.GlobalConstraintManager;
import com.starrocks.clone.ColocateTableBalancer;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.clone.TabletChecker;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.QueryableReentrantLock;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.elasticsearch.EsRepository;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.connector.hive.events.MetastoreEventsProcessor;
import com.starrocks.consistency.ConsistencyChecker;
import com.starrocks.consistency.LockChecker;
import com.starrocks.consistency.MetaRecoveryDaemon;
import com.starrocks.encryption.KeyMgr;
import com.starrocks.encryption.KeyRotationDaemon;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.ha.StateChangeExecution;
import com.starrocks.healthchecker.SafeModeChecker;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalFactory;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.lake.ShardManager;
import com.starrocks.lake.StarMgrMetaSyncer;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.vacuum.AutovacuumDaemon;
import com.starrocks.leader.Checkpoint;
import com.starrocks.leader.TaskRunStateSynchronizer;
import com.starrocks.listener.GlobalLoadJobListenerBus;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.ExportChecker;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.Load;
import com.starrocks.load.loadv2.LoadEtlChecker;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadLoadingChecker;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.loadv2.LoadTimeoutChecker;
import com.starrocks.load.loadv2.LoadsHistorySyncer;
import com.starrocks.load.pipe.PipeListener;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.load.pipe.PipeScheduler;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.routineload.RoutineLoadScheduler;
import com.starrocks.load.routineload.RoutineLoadTaskScheduler;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.memory.ProcProfileCollector;
import com.starrocks.meta.MetaContext;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageHeader;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.Storage;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockLoader;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.DefaultAuthorizationProvider;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.JournalObservable;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.VariableMgr;
import com.starrocks.qe.scheduler.slot.GlobalSlotProvider;
import com.starrocks.qe.scheduler.slot.LocalSlotProvider;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.qe.scheduler.slot.SlotProvider;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.scheduler.mv.MVJobExecutor;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.PortConnectivityChecker;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTaskExecutor;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalStateMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalStateMgr.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int REPLAY_INTERVAL_MS = 1;
    public static final String IMAGE_DIR = "/image";
    // will break the loop and refresh in-memory data after at most 10w logs or at most 1 seconds
    private static final long REPLAYER_MAX_MS_PER_LOOP = 1000L;
    private static final long REPLAYER_MAX_LOGS_PER_LOOP = 100000L;

    /**
     * Meta and Image context
     */
    private String imageDir;
    private final MetaContext metaContext;
    private long epoch = 0;

    // Lock to perform atomic modification on map like 'idToDb' and 'fullNameToDb'.
    // These maps are all thread safe, we only use lock to perform atomic operations.
    // Operations like Get or Put do not need lock.
    // We use fair ReentrantLock to avoid starvation. Do not use this lock in critical code pass
    // because fair lock has poor performance.
    // Using QueryableReentrantLock to print owner thread in debug mode.
    private final QueryableReentrantLock lock;

    /**
     * System Manager
     */
    private final NodeMgr nodeMgr;
    private final HeartbeatMgr heartbeatMgr;

    /**
     * Alter Job Manager
     */
    private final AlterJobMgr alterJobMgr;

    private final PortConnectivityChecker portConnectivityChecker;

    private final Load load;
    private final LoadMgr loadMgr;
    private final RoutineLoadMgr routineLoadMgr;
    private final StreamLoadMgr streamLoadMgr;
    private final ExportMgr exportMgr;
    private final MaterializedViewMgr materializedViewMgr;

    private final ConsistencyChecker consistencyChecker;
    private final BackupHandler backupHandler;
    private final PublishVersionDaemon publishVersionDaemon;
    private final DeleteMgr deleteMgr;
    private final UpdateDbUsedDataQuotaDaemon updateDbUsedDataQuotaDaemon;

    private FrontendDaemon labelCleaner; // To clean old LabelInfo, ExportJobInfos
    private FrontendDaemon txnTimeoutChecker; // To abort timeout txns
    private FrontendDaemon taskCleaner;   // To clean expire Task/TaskRun
    private FrontendDaemon tableKeeper;   // Maintain internal history tables
    private JournalWriter journalWriter; // leader only: write journal log
    private Daemon replayer;
    private Daemon timePrinter;
    private final EsRepository esRepository;  // it is a daemon, so add it here
    private final MetastoreEventsProcessor metastoreEventsProcessor;
    private final ConnectorTableMetadataProcessor connectorTableMetadataProcessor;

    // set to true after finished replay all meta and ready to serve
    // set to false when globalStateMgr is not ready.
    private final AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private final AtomicBoolean canRead = new AtomicBoolean(false);

    // True indicates that the node is transferring to the leader, using this state avoids forwarding stmt to its own node.
    private volatile boolean isInTransferringToLeader = false;

    // false if default_warehouse is not created.
    private boolean isDefaultWarehouseCreated = false;

    private FrontendNodeType feType;

    // The time when this node becomes leader.
    private long dominationStartTimeMs;

    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;

    private final CatalogIdGenerator idGenerator = new CatalogIdGenerator(NEXT_ID_INIT_VALUE);

    private EditLog editLog;
    private Journal journal;
    // For checkpoint and observer memory replayed marker
    private final AtomicLong replayedJournalId;

    private static GlobalStateMgr CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;

    private HAProtocol haProtocol = null;

    private final JournalObservable journalObservable;

    private final TabletInvertedIndex tabletInvertedIndex;
    private ColocateTableIndex colocateTableIndex;

    private final CatalogRecycleBin recycleBin;
    private final FunctionSet functionSet;

    private final MetaReplayState metaReplayState;

    private final ResourceMgr resourceMgr;

    private final GlobalTransactionMgr globalTransactionMgr;

    private final TabletStatMgr tabletStatMgr;

    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    private DomainResolver domainResolver;

    private final TabletSchedulerStat stat;

    private final TabletScheduler tabletScheduler;

    private final TabletChecker tabletChecker;

    // Thread pools for pending and loading task, separately
    private final LeaderTaskExecutor pendingLoadTaskScheduler;
    private final PriorityLeaderTaskExecutor loadingLoadTaskScheduler;

    private final LoadJobScheduler loadJobScheduler;

    private final LoadTimeoutChecker loadTimeoutChecker;
    private final LoadsHistorySyncer loadsHistorySyncer;
    private final LoadEtlChecker loadEtlChecker;
    private final LoadLoadingChecker loadLoadingChecker;
    private final LockChecker lockChecker;

    private final RoutineLoadScheduler routineLoadScheduler;
    private final RoutineLoadTaskScheduler routineLoadTaskScheduler;

    private final MVJobExecutor mvMVJobExecutor;

    private final SmallFileMgr smallFileMgr;

    private final DynamicPartitionScheduler dynamicPartitionScheduler;

    private final PluginMgr pluginMgr;

    private final AuditEventProcessor auditEventProcessor;

    private final StatisticsMetaManager statisticsMetaManager;

    private final StatisticAutoCollector statisticAutoCollector;

    private final SafeModeChecker safeModeChecker;

    private final AnalyzeMgr analyzeMgr;

    private StatisticStorage statisticStorage;

    private long imageJournalId;

    private long feStartTime;

    private boolean isSafeMode = false;

    private final ResourceGroupMgr resourceGroupMgr;

    private StarOSAgent starOSAgent;

    private final StarMgrMetaSyncer starMgrMetaSyncer;

    private MetadataMgr metadataMgr;
    private final CatalogMgr catalogMgr;
    private final ConnectorMgr connectorMgr;
    private final ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr;

    private final TaskManager taskManager;
    private final InsertOverwriteJobMgr insertOverwriteJobMgr;

    private final LocalMetastore localMetastore;
    private final GlobalFunctionMgr globalFunctionMgr;

    @Deprecated
    private final ShardManager shardManager;

    private final StateChangeExecution execution;

    private TaskRunStateSynchronizer taskRunStateSynchronizer;

    private final BinlogManager binlogManager;

    // For LakeTable
    private final CompactionMgr compactionMgr;

    private final WarehouseManager warehouseMgr;

    private final ConfigRefreshDaemon configRefreshDaemon;

    private final StorageVolumeMgr storageVolumeMgr;

    private AutovacuumDaemon autovacuumDaemon;

    private final PipeManager pipeManager;
    private final PipeListener pipeListener;
    private final PipeScheduler pipeScheduler;
    private final MVActiveChecker mvActiveChecker;

    private final ReplicationMgr replicationMgr;

    private final KeyMgr keyMgr;
    private final KeyRotationDaemon keyRotationDaemon;

    private LockManager lockManager;

    private final ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
    private final SlotManager slotManager = new SlotManager(resourceUsageMonitor);
    private final GlobalSlotProvider globalSlotProvider = new GlobalSlotProvider();
    private final SlotProvider localSlotProvider = new LocalSlotProvider();
    private final GlobalLoadJobListenerBus operationListenerBus = new GlobalLoadJobListenerBus();

    private final DictionaryMgr dictionaryMgr = new DictionaryMgr();
    private final RefreshDictionaryCacheTaskDaemon refreshDictionaryCacheTaskDaemon;

    private MemoryUsageTracker memoryUsageTracker;

    private ProcProfileCollector procProfileCollector;

    private final MetaRecoveryDaemon metaRecoveryDaemon = new MetaRecoveryDaemon();

    private TemporaryTableMgr temporaryTableMgr;
    private TemporaryTableCleaner temporaryTableCleaner;

    private final GtidGenerator gtidGenerator;
    private final GlobalConstraintManager globalConstraintManager;

    private final SqlParser sqlParser;
    private final Analyzer analyzer;
    private final Authorizer authorizer;
    private final DDLStmtExecutor ddlStmtExecutor;
    private final ShowExecutor showExecutor;

    public NodeMgr getNodeMgr() {
        return nodeMgr;
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    public TNodesInfo createNodesInfo(long warehouseId, SystemInfoService systemInfoService) {
        TNodesInfo nodesInfo = new TNodesInfo();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeNodeIds = warehouseMgr.getAllComputeNodeIds(warehouseId);
            for (Long cnId : computeNodeIds) {
                ComputeNode cn = systemInfoService.getBackendOrComputeNode(cnId);
                nodesInfo.addToNodes(new TNodeInfo(cnId, 0, cn.getIP(), cn.getBrpcPort()));
            }
        } else {
            for (Long id : systemInfoService.getBackendIds(false)) {
                Backend backend = systemInfoService.getBackend(id);
                nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getIP(), backend.getBrpcPort()));
            }
        }

        return nodesInfo;
    }

    public HeartbeatMgr getHeartbeatMgr() {
        return heartbeatMgr;
    }

    public TabletInvertedIndex getTabletInvertedIndex() {
        return this.tabletInvertedIndex;
    }

    // only for test
    public void setColocateTableIndex(ColocateTableIndex colocateTableIndex) {
        this.colocateTableIndex = colocateTableIndex;
        localMetastore.setColocateTableIndex(colocateTableIndex);
    }

    public ColocateTableIndex getColocateTableIndex() {
        return this.colocateTableIndex;
    }

    public CatalogRecycleBin getRecycleBin() {
        return this.recycleBin;
    }

    public MetaReplayState getMetaReplayState() {
        return metaReplayState;
    }

    public DynamicPartitionScheduler getDynamicPartitionScheduler() {
        return this.dynamicPartitionScheduler;
    }

    public long getFeStartTime() {
        return feStartTime;
    }

    public LocalMetastore getLocalMetastore() {
        return localMetastore;
    }

    public TemporaryTableMgr getTemporaryTableMgr() {
        return temporaryTableMgr;
    }

    public CompactionMgr getCompactionMgr() {
        return compactionMgr;
    }

    public ConfigRefreshDaemon getConfigRefreshDaemon() {
        return configRefreshDaemon;
    }

    public RefreshDictionaryCacheTaskDaemon getRefreshDictionaryCacheTaskDaemon() {
        return refreshDictionaryCacheTaskDaemon;
    }

    private static class SingletonHolder {
        private static final GlobalStateMgr INSTANCE = new GlobalStateMgr();
    }

    @VisibleForTesting
    protected GlobalStateMgr() {
        this(new NodeMgr());
    }

    @VisibleForTesting
    protected GlobalStateMgr(NodeMgr nodeMgr) {
        this(false, nodeMgr);
    }

    private GlobalStateMgr(boolean isCkptGlobalState) {
        this(isCkptGlobalState, new NodeMgr());
    }

    // if isCkptGlobalState is true, it means that we should not collect thread pool metric
    private GlobalStateMgr(boolean isCkptGlobalState, NodeMgr nodeMgr) {
        if (!isCkptGlobalState) {
            RunMode.detectRunMode();
        }

        if (RunMode.isSharedDataMode()) {
            this.starOSAgent = new StarOSAgent();
        }

        // System Manager
        this.nodeMgr = Objects.requireNonNullElseGet(nodeMgr, NodeMgr::new);
        this.heartbeatMgr = new HeartbeatMgr(!isCkptGlobalState);
        this.portConnectivityChecker = new PortConnectivityChecker();

        // Alter Job Manager
        // Alter Job Manager
        this.alterJobMgr = new AlterJobMgr(
                    new SchemaChangeHandler(),
                    new MaterializedViewHandler(),
                    new SystemHandler());

        this.load = new Load();
        this.streamLoadMgr = new StreamLoadMgr();
        this.routineLoadMgr = new RoutineLoadMgr();
        this.exportMgr = new ExportMgr();
        this.materializedViewMgr = new MaterializedViewMgr();

        this.consistencyChecker = new ConsistencyChecker();
        this.lock = new QueryableReentrantLock(true);
        this.backupHandler = new BackupHandler(this);
        this.publishVersionDaemon = new PublishVersionDaemon();
        this.deleteMgr = new DeleteMgr();
        this.updateDbUsedDataQuotaDaemon = new UpdateDbUsedDataQuotaDaemon();
        this.statisticsMetaManager = new StatisticsMetaManager();
        this.statisticAutoCollector = new StatisticAutoCollector();
        this.safeModeChecker = new SafeModeChecker();
        this.statisticStorage = new CachedStatisticStorage();

        this.replayedJournalId = new AtomicLong(0L);
        this.synchronizedTimeMs = 0;
        this.feType = FrontendNodeType.INIT;

        this.journalObservable = new JournalObservable();

        this.tabletInvertedIndex = new TabletInvertedIndex();
        this.colocateTableIndex = new ColocateTableIndex();
        this.recycleBin = new CatalogRecycleBin();
        this.functionSet = new FunctionSet();
        this.functionSet.init();

        this.metaReplayState = new MetaReplayState();

        this.resourceMgr = new ResourceMgr();

        this.globalTransactionMgr = new GlobalTransactionMgr(this);
        this.tabletStatMgr = new TabletStatMgr();
        this.authenticationMgr = new AuthenticationMgr();
        this.domainResolver = new DomainResolver(authenticationMgr);
        this.authorizationMgr = new AuthorizationMgr(this, new DefaultAuthorizationProvider());

        this.resourceGroupMgr = new ResourceGroupMgr();

        this.esRepository = new EsRepository();
        this.metastoreEventsProcessor = new MetastoreEventsProcessor();
        this.connectorTableMetadataProcessor = new ConnectorTableMetadataProcessor();

        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();

        this.globalFunctionMgr = new GlobalFunctionMgr();
        this.tabletScheduler = new TabletScheduler(stat);
        this.tabletChecker = new TabletChecker(tabletScheduler, stat);

        this.pendingLoadTaskScheduler =
                    new LeaderTaskExecutor("pending_load_task_scheduler", Config.max_broker_load_job_concurrency,
                                Config.desired_max_waiting_jobs, !isCkptGlobalState);
        // One load job will be split into multiple loading tasks, the queue size is not
        // determined, so set desired_max_waiting_jobs * 10
        this.loadingLoadTaskScheduler = new PriorityLeaderTaskExecutor("loading_load_task_scheduler",
                    Config.max_broker_load_job_concurrency,
                    Config.desired_max_waiting_jobs * 10, !isCkptGlobalState);

        this.loadJobScheduler = new LoadJobScheduler();
        this.loadMgr = new LoadMgr(loadJobScheduler);
        this.loadTimeoutChecker = new LoadTimeoutChecker(loadMgr);
        this.loadsHistorySyncer = new LoadsHistorySyncer();
        this.loadEtlChecker = new LoadEtlChecker(loadMgr);
        this.loadLoadingChecker = new LoadLoadingChecker(loadMgr);
        this.lockChecker = new LockChecker();
        this.routineLoadScheduler = new RoutineLoadScheduler(routineLoadMgr);
        this.routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadMgr);
        this.mvMVJobExecutor = new MVJobExecutor();

        this.smallFileMgr = new SmallFileMgr();

        this.dynamicPartitionScheduler = new DynamicPartitionScheduler("DynamicPartitionScheduler",
                    Config.dynamic_partition_check_interval_seconds * 1000L);

        setMetaDir();

        this.pluginMgr = new PluginMgr();
        this.auditEventProcessor = new AuditEventProcessor(this.pluginMgr);
        this.analyzeMgr = new AnalyzeMgr();
        this.localMetastore = new LocalMetastore(this, recycleBin, colocateTableIndex);
        this.temporaryTableMgr = new TemporaryTableMgr();
        this.warehouseMgr = new WarehouseManager();
        this.connectorMgr = new ConnectorMgr();
        this.connectorTblMetaInfoMgr = new ConnectorTblMetaInfoMgr();
        this.metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, connectorMgr, connectorTblMetaInfoMgr);
        this.catalogMgr = new CatalogMgr(connectorMgr);

        this.taskManager = new TaskManager();
        this.insertOverwriteJobMgr = new InsertOverwriteJobMgr();
        this.shardManager = new ShardManager();
        this.compactionMgr = new CompactionMgr();
        this.configRefreshDaemon = new ConfigRefreshDaemon();
        this.starMgrMetaSyncer = new StarMgrMetaSyncer();
        this.refreshDictionaryCacheTaskDaemon = new RefreshDictionaryCacheTaskDaemon();

        this.binlogManager = new BinlogManager();
        this.pipeManager = new PipeManager();
        this.pipeListener = new PipeListener(this.pipeManager);
        this.pipeScheduler = new PipeScheduler(this.pipeManager);
        this.mvActiveChecker = new MVActiveChecker();

        if (RunMode.isSharedDataMode()) {
            this.storageVolumeMgr = new SharedDataStorageVolumeMgr();
            this.autovacuumDaemon = new AutovacuumDaemon();
        } else {
            this.storageVolumeMgr = new SharedNothingStorageVolumeMgr();
        }

        this.lockManager = new LockManager();

        this.gtidGenerator = new GtidGenerator();
        this.globalConstraintManager = new GlobalConstraintManager();

        GlobalStateMgr gsm = this;
        this.execution = new StateChangeExecution() {
            @Override
            public void transferToLeader() {
                isInTransferringToLeader = true;
                try {
                    gsm.transferToLeader();
                } finally {
                    isInTransferringToLeader = false;
                }
            }

            @Override
            public void transferToNonLeader(FrontendNodeType newType) {
                gsm.transferToNonLeader(newType);
            }
        };

        getConfigRefreshDaemon().registerListener(() -> {
            try {
                if (Config.max_broker_load_job_concurrency != loadingLoadTaskScheduler.getCorePoolSize()) {
                    loadingLoadTaskScheduler.setPoolSize(Config.max_broker_load_job_concurrency);
                }
                if (Config.max_broker_load_job_concurrency != pendingLoadTaskScheduler.getCorePoolSize()) {
                    pendingLoadTaskScheduler.setPoolSize(Config.max_broker_load_job_concurrency);
                }
            } catch (Exception e) {
                LOG.warn("check config failed", e);
            }
        });

        this.replicationMgr = new ReplicationMgr();

        this.keyMgr = new KeyMgr();
        this.keyRotationDaemon = new KeyRotationDaemon(keyMgr);

        nodeMgr.registerLeaderChangeListener(globalSlotProvider::leaderChangeListener);

        this.memoryUsageTracker = new MemoryUsageTracker();
        this.procProfileCollector = new ProcProfileCollector();

        this.sqlParser = new SqlParser(AstBuilder.getInstance());
        this.analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        AccessControlProvider accessControlProvider;
        if (Config.access_control.equals("ranger")) {
            accessControlProvider = new AccessControlProvider(new AuthorizerStmtVisitor(), new RangerStarRocksAccessController());
        } else {
            accessControlProvider = new AccessControlProvider(new AuthorizerStmtVisitor(), new NativeAccessController());
        }
        this.authorizer = new Authorizer(accessControlProvider);
        this.ddlStmtExecutor = new DDLStmtExecutor(DDLStmtExecutor.StmtExecutorVisitor.getInstance());
        this.showExecutor = new ShowExecutor(ShowExecutor.ShowExecutorVisitor.getInstance());
        this.temporaryTableCleaner = new TemporaryTableCleaner();
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public static GlobalStateMgr getCurrentState() {
        if (isCheckpointThread()) {
            // only checkpoint thread itself will go here.
            // so no need to care about the thread safe.
            if (CHECKPOINT == null) {
                CHECKPOINT = new GlobalStateMgr(true);
            }
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    public boolean isSafeMode() {
        return isSafeMode;
    }

    public void setSafeMode(boolean isSafeMode) {
        this.isSafeMode = isSafeMode;
    }

    // NOTICE: in most case, we should use getCurrentState() to get the right globalStateMgr.
    // but in some cases, we should get the serving globalStateMgr explicitly.
    public static GlobalStateMgr getServingState() {
        return SingletonHolder.INSTANCE;
    }

    public BrokerMgr getBrokerMgr() {
        return nodeMgr.getBrokerMgr();
    }

    public ResourceMgr getResourceMgr() {
        return resourceMgr;
    }

    public GlobalFunctionMgr getGlobalFunctionMgr() {
        return globalFunctionMgr;
    }

    public GlobalTransactionMgr getGlobalTransactionMgr() {
        return globalTransactionMgr;
    }

    public PluginMgr getPluginMgr() {
        return pluginMgr;
    }

    public AnalyzeMgr getAnalyzeMgr() {
        return analyzeMgr;
    }

    public AuthenticationMgr getAuthenticationMgr() {
        return authenticationMgr;
    }

    public void setAuthenticationMgr(AuthenticationMgr authenticationMgr) {
        this.authenticationMgr = authenticationMgr;
    }

    public AuthorizationMgr getAuthorizationMgr() {
        return authorizationMgr;
    }

    public void setAuthorizationMgr(AuthorizationMgr authorizationMgr) {
        this.authorizationMgr = authorizationMgr;
    }

    public ResourceGroupMgr getResourceGroupMgr() {
        return resourceGroupMgr;
    }

    public TabletScheduler getTabletScheduler() {
        return tabletScheduler;
    }

    public TabletChecker getTabletChecker() {
        return tabletChecker;
    }

    public AuditEventProcessor getAuditEventProcessor() {
        return auditEventProcessor;
    }

    public static int getCurrentStateStarRocksMetaVersion() {
        return MetaContext.get().getStarRocksMetaVersion();
    }

    public static boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public StatisticStorage getStatisticStorage() {
        return statisticStorage;
    }

    public TabletStatMgr getTabletStatMgr() {
        return tabletStatMgr;
    }

    // Only used in UT
    public void setStatisticStorage(StatisticStorage statisticStorage) {
        this.statisticStorage = statisticStorage;
    }

    public StarOSAgent getStarOSAgent() {
        return starOSAgent;
    }

    public StarMgrMetaSyncer getStarMgrMetaSyncer() {
        return starMgrMetaSyncer;
    }

    public CatalogMgr getCatalogMgr() {
        return catalogMgr;
    }

    public ConnectorMgr getConnectorMgr() {
        return connectorMgr;
    }

    public MetadataMgr getMetadataMgr() {
        return metadataMgr;
    }

    @VisibleForTesting
    public void setMetadataMgr(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
    }

    @VisibleForTesting
    public void setStarOSAgent(StarOSAgent starOSAgent) {
        this.starOSAgent = starOSAgent;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public BinlogManager getBinlogManager() {
        return binlogManager;
    }

    public InsertOverwriteJobMgr getInsertOverwriteJobMgr() {
        return insertOverwriteJobMgr;
    }

    public WarehouseManager getWarehouseMgr() {
        return warehouseMgr;
    }

    public List<QueryStatisticsInfo> getQueryStatisticsInfoFromOtherFEs() {
        return nodeMgr.getQueryStatisticsInfoFromOtherFEs();
    }

    public StorageVolumeMgr getStorageVolumeMgr() {
        return storageVolumeMgr;
    }

    public PipeManager getPipeManager() {
        return pipeManager;
    }

    public PipeScheduler getPipeScheduler() {
        return pipeScheduler;
    }

    public PipeListener getPipeListener() {
        return pipeListener;
    }

    public MVActiveChecker getMvActiveChecker() {
        return mvActiveChecker;
    }

    public ConnectorTblMetaInfoMgr getConnectorTblMetaInfoMgr() {
        return connectorTblMetaInfoMgr;
    }

    public ConnectorTableMetadataProcessor getConnectorTableMetadataProcessor() {
        return connectorTableMetadataProcessor;
    }

    public ReplicationMgr getReplicationMgr() {
        return replicationMgr;
    }

    public KeyMgr getKeyMgr() {
        return keyMgr;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public void setLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public Authorizer getAuthorizer() {
        return authorizer;
    }

    public DDLStmtExecutor getDdlStmtExecutor() {
        return ddlStmtExecutor;
    }

    public ShowExecutor getShowExecutor() {
        return showExecutor;
    }

    public GtidGenerator getGtidGenerator() {
        return gtidGenerator;
    }

    public GlobalConstraintManager getGlobalConstraintManager() {
        return globalConstraintManager;
    }

    // Use tryLock to avoid potential deadlock
    public boolean tryLock(boolean mustLock) {
        while (true) {
            try {
                if (!lock.tryLock(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    // to see which thread held this lock for long time.
                    Thread owner = lock.getOwner();
                    if (owner != null) {
                        LOG.warn("globalStateMgr lock is held by: {}", LogUtil.dumpThread(owner, 50));
                    }

                    if (mustLock) {
                        continue;
                    } else {
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                LOG.warn("got exception while getting globalStateMgr lock", e);
                if (mustLock) {
                    continue;
                } else {
                    return lock.isHeldByCurrentThread();
                }
            }
        }
    }

    public void unlock() {
        if (lock.isHeldByCurrentThread()) {
            this.lock.unlock();
        }
    }

    public String getImageDir() {
        return imageDir;
    }

    private void setMetaDir() {
        this.imageDir = Config.meta_dir + IMAGE_DIR;
        nodeMgr.setImageDir(imageDir);
    }

    public void initialize(String[] args) throws Exception {
        // set meta dir first.
        // we already set these variables in constructor. but GlobalStateMgr is a singleton class.
        // so they may be set before Config is initialized.
        // set them here again to make sure these variables use values in fe.conf.
        setMetaDir();

        // must judge whether it is first time start here before initializing GlobalStateMgr.
        // Possibly remove clusterId and role to ensure that the system is not left in a half-initialized state.
        boolean isFirstTimeStart = nodeMgr.isVersionAndRoleFilesNotExist();
        try {
            // 0. get local node and helper node info
            nodeMgr.initialize(args);

            // 1. create dirs and files
            if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
                File imageDir = new File(this.imageDir);
                if (!imageDir.exists()) {
                    imageDir.mkdirs();
                }
                File imageV2Dir = new File(this.imageDir + "/v2");
                if (!imageV2Dir.exists()) {
                    imageV2Dir.mkdirs();
                }
            } else {
                LOG.error("Invalid edit log type: {}", Config.edit_log_type);
                System.exit(-1);
            }

            // init plugin manager
            pluginMgr.init();
            auditEventProcessor.start();

            // 2. get cluster id and role (Observer or Follower)
            nodeMgr.getClusterIdAndRoleOnStartup();

            // 3. Load image first and replay edits
            initJournal();
            loadImage(this.imageDir); // load image file

            // 4. create load and export job label cleaner thread
            createLabelCleaner();

            // 5. create txn timeout checker thread
            createTxnTimeoutChecker();

            // 6. start task cleaner thread
            createTaskCleaner();
            createTableKeeper();

            // 7. init starosAgent
            if (RunMode.isSharedDataMode() && !starOSAgent.init(null)) {
                LOG.error("init starOSAgent failed");
                System.exit(-1);
            }
        } catch (Exception e) {
            try {
                if (isFirstTimeStart) {
                    // If it is the first time we start, we remove the cluster ID and role
                    // to prevent leaving the system in an inconsistent state.
                    nodeMgr.removeClusterIdAndRole();
                }
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }

    protected void initJournal() throws JournalException, InterruptedException {
        BlockingQueue<JournalTask> journalQueue =
                    new ArrayBlockingQueue<JournalTask>(Config.metadata_journal_queue_size);
        journal = JournalFactory.create(nodeMgr.getNodeName());
        journalWriter = new JournalWriter(journal, journalQueue);

        editLog = new EditLog(journalQueue);
    }

    // wait until FE is ready.
    public void waitForReady() throws InterruptedException {
        long lastLoggingTimeMs = System.currentTimeMillis();
        while (true) {
            if (isReady()) {
                LOG.info("globalStateMgr is ready. FE type: {}", feType);
                feStartTime = System.currentTimeMillis();
                break;
            }

            Thread.sleep(2000);
            LOG.info("wait globalStateMgr to be ready. FE type: {}. is ready: {}", feType, isReady.get());

            if (System.currentTimeMillis() - lastLoggingTimeMs > 60000L) {
                lastLoggingTimeMs = System.currentTimeMillis();
                LOG.warn("It took too much time for FE to transfer to a stable state(LEADER/FOLLOWER), " +
                            "it maybe caused by one of the following reasons: " +
                            "1. There are too many BDB logs to replay, because of previous failure of checkpoint" +
                            "(you can check the create time of image file under meta/image dir). " +
                            "2. Majority voting members(LEADER or FOLLOWER) of the FE cluster haven't started completely. " +
                            "3. FE node has multiple IPs, you should configure the priority_networks in fe.conf " +
                            "to match the ip record in meta/image/ROLE. And we don't support change the ip of FE node. " +
                            "Ignore this reason if you are using FQDN. " +
                            "4. The time deviation between FE nodes is greater than 5s, " +
                            "please use ntp or other tools to keep clock synchronized. " +
                            "5. The configuration of edit_log_port has changed, please reset to the original value. " +
                            "6. The replayer thread may get stuck, please use jstack to find the details.");
            }
        }
    }

    public boolean isReady() {
        return isReady.get();
    }

    public static String genFeNodeName(String host, int port, boolean isOldStyle) {
        String name = host + "_" + port;
        if (isOldStyle) {
            return name;
        } else {
            return name + "_" + System.currentTimeMillis();
        }
    }

    private void transferToLeader() {
        FrontendNodeType oldType = feType;
        // stop replayer
        if (replayer != null) {
            replayer.setStop();
            try {
                replayer.join();
            } catch (InterruptedException e) {
                LOG.warn("got exception when stopping the replayer thread", e);
            }
            replayer = null;
        }

        // set this after replay thread stopped. to avoid replay thread modify them.
        isReady.set(false);

        // setup for journal
        try {
            journal.open();
            if (!haProtocol.fencing()) {
                throw new Exception("fencing failed. will exit");
            }
            long maxJournalId = journal.getMaxJournalId();
            replayJournal(maxJournalId);
            nodeMgr.checkCurrentNodeExist();
            journalWriter.init(maxJournalId);
        } catch (Exception e) {
            // TODO: gracefully exit
            LOG.error("failed to init journal after transfer to leader! will exit", e);
            System.exit(-1);
        }

        journalWriter.startDaemon();

        // Set the feType to LEADER before writing edit log, because the feType must be Leader when writing edit log.
        // It will be set to the old type if any error happens in the following procedure
        feType = FrontendNodeType.LEADER;
        dominationStartTimeMs = System.currentTimeMillis();

        try {
            // Log meta_version
            int starrocksMetaVersion = MetaContext.get().getStarRocksMetaVersion();
            if (starrocksMetaVersion < FeConstants.STARROCKS_META_VERSION) {
                editLog.logMetaVersion(new MetaVersion(FeConstants.STARROCKS_META_VERSION));
                MetaContext.get().setStarRocksMetaVersion(FeConstants.STARROCKS_META_VERSION);
            }

            // Log the first frontend
            if (nodeMgr.isFirstTimeStartUp()) {
                // if isFirstTimeStartUp is true, frontends must contain this Node.
                Frontend self = nodeMgr.getMySelf();
                Preconditions.checkNotNull(self);
                // OP_ADD_FIRST_FRONTEND is emitted, so it can write to BDBJE even if canWrite is false
                editLog.logAddFirstFrontend(self);
            }

            // MUST set leader ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-leader FE to push image
            nodeMgr.setLeaderInfo();

            // start all daemon threads that only running on MASTER FE
            startLeaderOnlyDaemonThreads();
            // start other daemon threads that should run on all FEs
            startAllNodeTypeDaemonThreads();
            insertOverwriteJobMgr.cancelRunningJobs();

            if (!isDefaultWarehouseCreated) {
                initDefaultWarehouse();
            }

            MetricRepo.init();

            isReady.set(true);

            String msg = "leader finished to replay journal, can write now.";
            Util.stdoutWithTime(msg);
            LOG.info(msg);

            // for leader, there are some new thread pools need to register metric
            ThreadPoolManager.registerAllThreadPoolMetric();

            if (nodeMgr.isFirstTimeStartUp()) {
                // When the cluster is initially deployed, we set ENABLE_ADAPTIVE_SINK_DOP so
                // that the load is automatically configured as the best performance
                // configuration. If it is upgraded from an old version, the original
                // configuration is retained to avoid system stability problems caused by
                // changes in concurrency
                VariableMgr.setSystemVariable(VariableMgr.getDefaultSessionVariable(), new SystemVariable(SetType.GLOBAL,
                                        SessionVariable.ENABLE_ADAPTIVE_SINK_DOP,
                                        LiteralExpr.create("true", Type.BOOLEAN)),
                            false);
            }
        } catch (UserException e) {
            LOG.warn("Failed to set ENABLE_ADAPTIVE_SINK_DOP", e);
        } catch (Throwable t) {
            LOG.warn("transfer to leader failed with error", t);
            feType = oldType;
            throw t;
        }

        createBuiltinStorageVolume();
        resourceGroupMgr.createBuiltinResourceGroupsIfNotExist();
        keyMgr.initDefaultMasterKey();
    }

    public void setFrontendNodeType(FrontendNodeType newType) {
        // just for test, don't call it directly
        feType = newType;
    }

    // start all daemon threads only running on Master
    private void startLeaderOnlyDaemonThreads() {
        if (RunMode.isSharedDataMode()) {
            // register service to starMgr
            if (!getStarOSAgent().registerAndBootstrapService()) {
                System.exit(-1);
            }
        }

        // start checkpoint thread
        checkpointer = new Checkpoint(journal);
        checkpointer.setMetaContext(metaContext);
        // set "checkpointThreadId" before the checkpoint thread start, because the thread
        // need to check the "checkpointThreadId" when running.
        checkpointThreadId = checkpointer.getId();

        checkpointer.start();
        LOG.info("checkpointer thread started. thread id is {}", checkpointThreadId);

        keyRotationDaemon.start();

        // heartbeat mgr
        heartbeatMgr.setLeader(nodeMgr.getClusterId(), nodeMgr.getToken(), epoch);
        heartbeatMgr.start();
        // New load scheduler
        pendingLoadTaskScheduler.start();
        loadingLoadTaskScheduler.start();
        loadMgr.prepareJobs();
        loadJobScheduler.start();
        loadTimeoutChecker.start();
        loadsHistorySyncer.start();
        loadEtlChecker.start();
        loadLoadingChecker.start();
        // Export checker
        ExportChecker.init(Config.export_checker_interval_second * 1000L);
        ExportChecker.startAll();
        // Tablet checker and scheduler
        tabletChecker.start();
        tabletScheduler.start();
        // Colocate tables balancer
        ColocateTableBalancer.getInstance().start();
        // Publish Version Daemon
        publishVersionDaemon.start();
        // Start txn timeout checker
        txnTimeoutChecker.start();
        // Alter
        getAlterJobMgr().start();
        // Consistency checker
        getConsistencyChecker().start();
        // Backup handler
        getBackupHandler().start();
        // globalStateMgr recycle bin
        getRecycleBin().start();
        // time printer
        createTimePrinter();
        timePrinter.start();
        // start routine load scheduler
        routineLoadScheduler.start();
        routineLoadTaskScheduler.start();
        // start dynamic partition task
        dynamicPartitionScheduler.start();
        // start daemon thread to update db used data quota for db txn manager periodically
        updateDbUsedDataQuotaDaemon.start();
        statisticsMetaManager.start();
        statisticAutoCollector.start();
        taskManager.start();
        taskCleaner.start();
        mvMVJobExecutor.start();
        pipeListener.start();
        pipeScheduler.start();
        mvActiveChecker.start();

        // start daemon thread to report the progress of RunningTaskRun to the follower by editlog
        taskRunStateSynchronizer = new TaskRunStateSynchronizer();
        taskRunStateSynchronizer.start();

        if (RunMode.isSharedDataMode()) {
            starMgrMetaSyncer.start();
            autovacuumDaemon.start();
        }

        if (Config.enable_safe_mode) {
            LOG.info("Start safe mode checker!");
            safeModeChecker.start();
        }

        replicationMgr.start();

        if (Config.metadata_enable_recovery_mode) {
            LOG.info("run system in recovery mode");
            metaRecoveryDaemon.start();
        }
        temporaryTableCleaner.start();

    }

    // start threads that should run on all FE
    private void startAllNodeTypeDaemonThreads() {
        portConnectivityChecker.start();
        tabletStatMgr.start();
        // load and export job label cleaner thread
        labelCleaner.start();
        // ES state store
        esRepository.start();

        if (Config.enable_hms_events_incremental_sync) {
            metastoreEventsProcessor.start();
        }

        connectorTableMetadataProcessor.start();

        // domain resolver
        domainResolver.start();
        if (RunMode.isSharedDataMode()) {
            compactionMgr.start();
        }
        configRefreshDaemon.start();

        slotManager.start();

        lockChecker.start();

        refreshDictionaryCacheTaskDaemon.start();

        procProfileCollector.start();

        // The memory tracker should be placed at the end
        memoryUsageTracker.start();
    }

    private void transferToNonLeader(FrontendNodeType newType) {
        isReady.set(false);

        if (feType == FrontendNodeType.OBSERVER || feType == FrontendNodeType.FOLLOWER) {
            Preconditions.checkState(newType == FrontendNodeType.UNKNOWN);
            LOG.warn("{} to UNKNOWN, still offer read service", feType.name());
            // not set canRead here, leave canRead as what it was.
            // if meta out of date, canRead will be set to false in replayer thread.
            metaReplayState.setTransferToUnknown();
            feType = newType;
            return;
        }

        // transfer from INIT/UNKNOWN to OBSERVER/FOLLOWER

        if (replayer == null) {
            createReplayer();
            replayer.start();
        }

        startAllNodeTypeDaemonThreads();

        if (!isDefaultWarehouseCreated) {
            initDefaultWarehouse();
        }

        MetricRepo.init();

        feType = newType;
    }

    // The manager that loads meta from image must be a member of GlobalStateMgr and cannot be SINGLETON,
    // since Checkpoint uses a separate memory.
    public void loadImage(String imageDir) throws IOException {
        ImageLoader imageLoader = new ImageLoader(imageDir);
        File curFile = imageLoader.getImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("image does not exist: {}", curFile.getAbsolutePath());
            return;
        }
        replayedJournalId.set(imageLoader.getImageJournalId());
        LOG.info("start load image from {}. is ckpt: {}", curFile.getAbsolutePath(),
                    GlobalStateMgr.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();

        Map<SRMetaBlockID, SRMetaBlockLoader> loadImages = ImmutableMap.<SRMetaBlockID, SRMetaBlockLoader>builder()
                    .put(SRMetaBlockID.NODE_MGR, nodeMgr::load)
                    .put(SRMetaBlockID.LOCAL_META_STORE, localMetastore::load)
                    .put(SRMetaBlockID.ALTER_MGR, alterJobMgr::load)
                    .put(SRMetaBlockID.CATALOG_RECYCLE_BIN, recycleBin::load)
                    .put(SRMetaBlockID.VARIABLE_MGR, VariableMgr::load)
                    .put(SRMetaBlockID.RESOURCE_MGR, resourceMgr::loadResourcesV2)
                    .put(SRMetaBlockID.EXPORT_MGR, exportMgr::loadExportJobV2)
                    .put(SRMetaBlockID.BACKUP_MGR, backupHandler::loadBackupHandlerV2)
                    .put(SRMetaBlockID.GLOBAL_TRANSACTION_MGR, globalTransactionMgr::loadTransactionStateV2)
                    .put(SRMetaBlockID.COLOCATE_TABLE_INDEX, colocateTableIndex::loadColocateTableIndexV2)
                    .put(SRMetaBlockID.ROUTINE_LOAD_MGR, routineLoadMgr::loadRoutineLoadJobsV2)
                    .put(SRMetaBlockID.LOAD_MGR, loadMgr::loadLoadJobsV2JsonFormat)
                    .put(SRMetaBlockID.SMALL_FILE_MGR, smallFileMgr::loadSmallFilesV2)
                    .put(SRMetaBlockID.PLUGIN_MGR, pluginMgr::load)
                    .put(SRMetaBlockID.DELETE_MGR, deleteMgr::load)
                    .put(SRMetaBlockID.ANALYZE_MGR, analyzeMgr::load)
                    .put(SRMetaBlockID.RESOURCE_GROUP_MGR, resourceGroupMgr::load)
                    .put(SRMetaBlockID.AUTHENTICATION_MGR, authenticationMgr::loadV2)
                    .put(SRMetaBlockID.AUTHORIZATION_MGR, authorizationMgr::loadV2)
                    .put(SRMetaBlockID.TASK_MGR, taskManager::loadTasksV2)
                    .put(SRMetaBlockID.CATALOG_MGR, catalogMgr::load)
                    .put(SRMetaBlockID.INSERT_OVERWRITE_JOB_MGR, insertOverwriteJobMgr::load)
                    .put(SRMetaBlockID.COMPACTION_MGR, compactionMgr::load)
                    .put(SRMetaBlockID.STREAM_LOAD_MGR, streamLoadMgr::load)
                    .put(SRMetaBlockID.MATERIALIZED_VIEW_MGR, materializedViewMgr::load)
                    .put(SRMetaBlockID.GLOBAL_FUNCTION_MGR, globalFunctionMgr::load)
                    .put(SRMetaBlockID.STORAGE_VOLUME_MGR, storageVolumeMgr::load)
                    .put(SRMetaBlockID.DICTIONARY_MGR, dictionaryMgr::load)
                    .put(SRMetaBlockID.REPLICATION_MGR, replicationMgr::load)
                    .put(SRMetaBlockID.KEY_MGR, keyMgr::load)
                    .put(SRMetaBlockID.PIPE_MGR, pipeManager.getRepo()::load)
                    .build();

        Set<SRMetaBlockID> metaMgrMustExists = new HashSet<>(loadImages.keySet());
        InputStream in = Files.newInputStream(curFile.toPath());
        try {
            imageLoader.setInputStream(in);
            loadHeader(new DataInputStream(imageLoader.getCheckedInputStream()));
            while (true) {
                SRMetaBlockReader reader = imageLoader.getBlockReader();
                SRMetaBlockID srMetaBlockID = reader.getHeader().getSrMetaBlockID();

                try {
                    SRMetaBlockLoader metaBlockLoader = loadImages.get(srMetaBlockID);
                    if (metaBlockLoader == null) {
                        /*
                         * The expected read module does not match the module stored in the image,
                         * and the json chunk is skipped directly. This usually occurs in several situations.
                         * 1. When the obsolete image code is deleted.
                         * 2. When the new version rolls back to the old version,
                         *    the old version ignores the functions of the new version
                         */
                        LOG.warn(String.format("Ignore this invalid meta block, sr meta block id mismatch" +
                                    "(expect sr meta block id %s)", srMetaBlockID));
                        continue;
                    }

                    metaBlockLoader.apply(reader);
                    metaMgrMustExists.remove(srMetaBlockID);
                    LOG.info("Success load StarRocks meta block " + srMetaBlockID + " from image");
                } catch (SRMetaBlockEOFException srMetaBlockEOFException) {
                    /*
                     * The number of json expected to be read is more than the number of json actually stored in the image
                     */
                    metaMgrMustExists.remove(srMetaBlockID);
                    LOG.warn("Got EOF exception, ignore, ", srMetaBlockEOFException);
                } catch (Throwable t) {
                    LOG.warn("load meta block {} failed", srMetaBlockID, t);
                    // throw the exception again, because the following steps will depend on this error.
                    throw t;
                } finally {
                    reader.close();
                }
            }
        } catch (EOFException exception) {
            if (!metaMgrMustExists.isEmpty()) {
                LOG.warn("Miss meta block [" + Joiner.on(",").join(new ArrayList<>(metaMgrMustExists)) + "], " +
                            "This may not be a fatal error. It may be because there are new features in the version " +
                            "you upgraded this time, but there is no relevant metadata.");
            } else {
                LOG.info("Load meta-image EOF, successful loading all requires meta module");
            }
        } catch (SRMetaBlockException e) {
            LOG.error("load meta block failed ", e);
            throw new IOException("load meta block failed ", e);
        } finally {
            imageLoader.readTheRemainingBytes();
            in.close();
        }

        imageLoader.checkCheckSum();

        try {
            postLoadImage();
        } catch (Exception t) {
            LOG.warn("there is an exception during processing after load image. exception:", t);
        }

        long loadImageEndTime = System.currentTimeMillis();
        this.imageJournalId = imageLoader.getImageJournalId();
        LOG.info("finished to load image in " + (loadImageEndTime - loadImageStartTime) + " ms");
    }

    private void postLoadImage() {
        processMvRelatedMeta();
    }

    private void processMvRelatedMeta() {
        List<String> dbNames = metadataMgr.listDbNames(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

        long startMillis = System.currentTimeMillis();
        for (String dbName : dbNames) {
            Database db = metadataMgr.getDb(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName);
            for (MaterializedView mv : db.getMaterializedViews()) {
                mv.onReload();
            }
        }

        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("finish processing all tables' related materialized views in {}ms", duration);
    }

    public void loadHeader(DataInputStream dis) throws IOException {
        // for new format, version schema is [starrocksMetaVersion], and the int value must be positive
        // for old format, version schema is [-1, metaVersion, starrocksMetaVersion]
        // so we can check the first int to determine the version schema
        int flag = dis.readInt();
        int starrocksMetaVersion;
        if (flag < 0) {
            dis.readInt();
            starrocksMetaVersion = dis.readInt();
        } else {
            // when flag is positive, this is new version format
            starrocksMetaVersion = flag;
        }

        if (starrocksMetaVersion != FeConstants.STARROCKS_META_VERSION) {
            LOG.error("Not compatible with meta version {}, current version is {}",
                        starrocksMetaVersion, FeConstants.STARROCKS_META_VERSION);
            System.exit(-1);
        }

        MetaContext.get().setStarRocksMetaVersion(starrocksMetaVersion);
        ImageHeader header = GsonUtils.GSON.fromJson(Text.readString(dis), ImageHeader.class);
        idGenerator.setId(header.getBatchEndId());
        LOG.info("finished to replay header from image");
    }

    // Only called by checkpoint thread
    public void saveImage() throws IOException {
        try {
            saveImage(ImageFormatVersion.v1);
        } catch (Throwable t) {
            // image v1 may fail because of byte[] size overflow, ignore
            LOG.warn("save image v1 failed, ignore", t);
        }
        saveImage(ImageFormatVersion.v2);
    }

    public void saveImage(ImageFormatVersion formatVersion) throws IOException {
        String destDir;
        if (formatVersion == ImageFormatVersion.v1) {
            destDir = this.imageDir;
        } else {
            destDir = this.imageDir + "/v2";
        }
        // Write image.ckpt
        Storage storage = new Storage(destDir);
        File curFile = storage.getImageFile(replayedJournalId.get());
        File ckpt = new File(destDir, Storage.IMAGE_NEW);
        saveImage(new ImageWriter(destDir, formatVersion, replayedJournalId.get()), ckpt);

        // Move image.ckpt to image.dataVersion
        LOG.info("Move " + ckpt.getAbsolutePath() + " to " + curFile.getAbsolutePath());
        if (!ckpt.renameTo(curFile)) {
            if (!curFile.delete()) {
                LOG.warn("Failed to delete file, filepath={}", curFile.getAbsolutePath());
            }
            throw new IOException();
        }
    }

    // The manager that saves meta to image must be a member of GlobalStateMgr and cannot be SINGLETON,
    // since Checkpoint uses a separate memory.
    public void saveImage(ImageWriter imageWriter, File curFile) throws IOException {
        if (!curFile.exists()) {
            if (!curFile.createNewFile()) {
                LOG.warn("Failed to create file, filepath={}", curFile.getAbsolutePath());
            }
        }

        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), GlobalStateMgr.isCheckpointThread());

        long saveImageStartTime = System.currentTimeMillis();
        try (OutputStream outputStream = Files.newOutputStream(curFile.toPath())) {
            imageWriter.setOutputStream(outputStream);
            try {
                saveHeader(imageWriter.getDataOutputStream());
                nodeMgr.save(imageWriter);
                localMetastore.save(imageWriter);
                alterJobMgr.save(imageWriter);
                recycleBin.save(imageWriter);
                VariableMgr.save(imageWriter);
                resourceMgr.saveResourcesV2(imageWriter);
                exportMgr.saveExportJobV2(imageWriter);
                backupHandler.saveBackupHandlerV2(imageWriter);
                globalTransactionMgr.saveTransactionStateV2(imageWriter);
                colocateTableIndex.saveColocateTableIndexV2(imageWriter);
                routineLoadMgr.saveRoutineLoadJobsV2(imageWriter);
                loadMgr.saveLoadJobsV2JsonFormat(imageWriter);
                smallFileMgr.saveSmallFilesV2(imageWriter);
                pluginMgr.save(imageWriter);
                deleteMgr.save(imageWriter);
                analyzeMgr.save(imageWriter);
                resourceGroupMgr.save(imageWriter);
                authenticationMgr.saveV2(imageWriter);
                authorizationMgr.saveV2(imageWriter);
                taskManager.saveTasksV2(imageWriter);
                catalogMgr.save(imageWriter);
                insertOverwriteJobMgr.save(imageWriter);
                compactionMgr.save(imageWriter);
                streamLoadMgr.save(imageWriter);
                materializedViewMgr.save(imageWriter);
                globalFunctionMgr.save(imageWriter);
                storageVolumeMgr.save(imageWriter);
                dictionaryMgr.save(imageWriter);
                replicationMgr.save(imageWriter);
                keyMgr.save(imageWriter);
                pipeManager.getRepo().save(imageWriter);
            } catch (SRMetaBlockException e) {
                LOG.error("Save meta block failed ", e);
                throw new IOException("Save meta block failed ", e);
            }

            imageWriter.saveChecksum();

            long saveImageEndTime = System.currentTimeMillis();
            LOG.info("Finished save meta block {} in {} ms.",
                        curFile.getAbsolutePath(), (saveImageEndTime - saveImageStartTime));
        }
    }

    public void saveHeader(DataOutputStream dos) throws IOException {
        dos.writeInt(FeConstants.STARROCKS_META_VERSION);
        ImageHeader header = new ImageHeader();
        long id = idGenerator.getBatchEndId();
        header.setBatchEndId(id);
        Text.writeString(dos, GsonUtils.GSON.toJson(header));
    }

    public void createLabelCleaner() {
        labelCleaner = new FrontendDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                clearExpiredJobs();
            }
        };
    }

    public void createTaskCleaner() {
        taskCleaner = new FrontendDaemon("TaskCleaner", Config.task_check_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                doTaskBackgroundJob();
                setInterval(Config.task_check_interval_second * 1000L);
            }
        };
    }

    public void createTableKeeper() {
        tableKeeper = TableKeeper.startDaemon();
    }

    public void createTxnTimeoutChecker() {
        txnTimeoutChecker = new FrontendDaemon("txnTimeoutChecker",
                    Config.transaction_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                globalTransactionMgr.abortTimeoutTxns();

                try {
                    loadMgr.cancelResidualJob();
                } catch (Throwable t) {
                    LOG.warn("load manager cancel residual job failed", t);
                }
            }
        };
    }

    public void createReplayer() {
        replayer = new Daemon("replayer", REPLAY_INTERVAL_MS) {
            private JournalCursor cursor = null;
            // avoid numerous 'meta out of date' log
            private long lastMetaOutOfDateLogTime = 0;

            @Override
            @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
            protected void runOneCycle() {
                boolean err = false;
                boolean hasLog = false;
                try {
                    if (cursor == null) {
                        // 1. set replay to the end
                        LOG.info("start to replay from {}", replayedJournalId.get());
                        cursor = journal.read(replayedJournalId.get() + 1, JournalCursor.CURSOR_END_KEY);
                    } else {
                        cursor.refresh();
                    }
                    // 2. replay with flow control
                    hasLog = replayJournalInner(cursor, true);
                    metaReplayState.setOk();
                } catch (JournalInconsistentException | InterruptedException e) {
                    LOG.warn("got interrupt exception or inconsistent exception when replay journal {}, will exit, ",
                                replayedJournalId.get() + 1, e);
                    // TODO exit gracefully
                    Util.stdoutWithTime(e.getMessage());
                    System.exit(-1);
                } catch (Throwable e) {
                    LOG.error("replayer thread catch an exception when replay journal {}.",
                                replayedJournalId.get() + 1, e);
                    metaReplayState.setException(e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        LOG.error("sleep got exception. ", e);
                    }
                    err = true;
                }

                setCanRead(hasLog, err);
            }

            private void setCanRead(boolean hasLog, boolean err) {
                if (err) {
                    canRead.set(false);
                    isReady.set(false);
                    return;
                }

                if (Config.ignore_meta_check) {
                    // can still offer read, but is not ready
                    canRead.set(true);
                    isReady.set(false);
                    return;
                }

                long currentTimeMs = System.currentTimeMillis();
                if (currentTimeMs - synchronizedTimeMs > Config.meta_delay_toleration_second * 1000L) {
                    if (currentTimeMs - lastMetaOutOfDateLogTime > 5 * 1000L) {
                        // we still need this log to observe this situation
                        // but service may be continued when there is no log being replayed.
                        LOG.warn("meta out of date. current time: {}, synchronized time: {}, has log: {}, fe type: {}",
                                    currentTimeMs, synchronizedTimeMs, hasLog, feType);
                        lastMetaOutOfDateLogTime = currentTimeMs;
                    }
                    if (hasLog || feType == FrontendNodeType.UNKNOWN) {
                        // 1. if we read log from BDB, which means leader is still alive.
                        // So we need to set meta out of date.
                        // 2. if we didn't read any log from BDB and feType is UNKNOWN,
                        // which means this non-leader node is disconnected with leader.
                        // So we need to set meta out of date either.
                        metaReplayState.setOutOfDate(currentTimeMs, synchronizedTimeMs);
                        canRead.set(false);
                        isReady.set(false);
                    }
                } else {
                    canRead.set(true);
                    isReady.set(true);
                }
            }

            // close current db after replayer finished
            @Override
            public void run() {
                super.run();
                if (cursor != null) {
                    cursor.close();
                    LOG.info("quit replay at {}", replayedJournalId.get());
                }
            }
        };

        replayer.setMetaContext(metaContext);
    }

    /**
     * Replay journal from replayedJournalId + 1 to toJournalId
     * used by checkpointer/replay after state change
     * toJournalId is a definite number and cannot set to -1/JournalCursor.CURSOR_END_KEY
     */
    public void replayJournal(long toJournalId) throws JournalException {
        if (toJournalId <= replayedJournalId.get()) {
            LOG.info("skip replay journal because {} <= {}", toJournalId, replayedJournalId.get());
            return;
        }

        long startJournalId = replayedJournalId.get() + 1;
        long replayStartTime = System.currentTimeMillis();
        LOG.info("start to replay journal from {} to {}", startJournalId, toJournalId);

        JournalCursor cursor = null;
        try {
            cursor = journal.read(startJournalId, toJournalId);
            replayJournalInner(cursor, false);
        } catch (InterruptedException | JournalInconsistentException e) {
            LOG.warn("got interrupt exception or inconsistent exception when replay journal {}, will exit, ",
                        replayedJournalId.get() + 1,
                        e);
            // TODO exit gracefully
            Util.stdoutWithTime(e.getMessage());
            System.exit(-1);

        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // verify if all log is replayed
        if (toJournalId != replayedJournalId.get()) {
            throw new JournalException(String.format(
                        "should replay to %d but actual replayed journal id is %d",
                        toJournalId, replayedJournalId.get()));
        }

        streamLoadMgr.cancelUnDurableTaskAfterRestart();

        long replayInterval = System.currentTimeMillis() - replayStartTime;
        LOG.info("finish replay from {} to {} in {} msec", startJournalId, toJournalId, replayInterval);
    }

    /**
     * replay journal until cursor returns null(suggest EOF)
     * return true if any journal is replayed
     */
    protected boolean replayJournalInner(JournalCursor cursor, boolean flowControl)
                throws JournalException, InterruptedException, JournalInconsistentException {
        long startReplayId = replayedJournalId.get();
        long startTime = System.currentTimeMillis();
        long lineCnt = 0;
        while (true) {
            JournalEntity entity = null;
            boolean readSucc = false;
            try {
                entity = cursor.next();

                // EOF or aggressive retry
                if (entity == null) {
                    break;
                }

                readSucc = true;

                // apply
                editLog.loadJournal(this, entity);
            } catch (Throwable e) {
                if (canSkipBadReplayedJournal(e)) {
                    LOG.error("!!! DANGER: SKIP JOURNAL, id: {}, data: {} !!!",
                                replayedJournalId.incrementAndGet(), journalEntityToReadableString(entity), e);
                    if (!readSucc) {
                        cursor.skipNext();
                    }
                    continue;
                }
                // handled in outer loop
                LOG.warn("catch exception when replaying journal, id: {}, data: {},",
                            replayedJournalId.get() + 1, journalEntityToReadableString(entity), e);
                throw e;
            }

            replayedJournalId.incrementAndGet();
            LOG.debug("journal {} replayed.", replayedJournalId);

            if (feType != FrontendNodeType.LEADER) {
                journalObservable.notifyObservers(replayedJournalId.get());
            }
            if (MetricRepo.hasInit) {
                // Metric repo may not init after this replay thread start
                MetricRepo.COUNTER_EDIT_LOG_READ.increase(1L);
            }

            if (flowControl) {
                // cost too much time
                long cost = System.currentTimeMillis() - startTime;
                if (cost > REPLAYER_MAX_MS_PER_LOOP) {
                    LOG.warn("replay journal cost too much time: {} replayedJournalId: {}", cost, replayedJournalId);
                    break;
                }
                // consume too much lines
                lineCnt += 1;
                if (lineCnt > REPLAYER_MAX_LOGS_PER_LOOP) {
                    LOG.warn("replay too many journals: lineCnt {}, replayedJournalId: {}", lineCnt, replayedJournalId);
                    break;
                }
            }

        }
        if (replayedJournalId.get() - startReplayId > 0) {
            LOG.info("replayed journal from {} - {}", startReplayId, replayedJournalId);
            return true;
        }
        return false;
    }

    private String journalEntityToReadableString(JournalEntity entity) {
        if (entity == null) {
            return "null";
        }
        Writable data = entity.getData();
        try {
            return GsonUtils.GSON.toJson(data);
        } catch (Exception e) {
            // In older version, data may not be json, here we just return the class name.
            return data.getClass().getName();
        }
    }

    protected boolean canSkipBadReplayedJournal(Throwable t) {
        if (Config.metadata_enable_recovery_mode) {
            LOG.warn("skip journal load failure because cluster is in recovery mode");
            return true;
        }

        try {
            for (String idStr : Config.metadata_journal_skip_bad_journal_ids.split(",")) {
                if (!StringUtils.isEmpty(idStr) && Long.parseLong(idStr) == replayedJournalId.get() + 1) {
                    LOG.warn("skip bad replayed journal id {} because configured {}",
                                idStr, Config.metadata_journal_skip_bad_journal_ids);
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to parse metadata_journal_skip_bad_journal_ids: {}",
                        Config.metadata_journal_skip_bad_journal_ids, e);
        }

        short opCode = OperationType.OP_INVALID;
        if (t instanceof JournalException) {
            opCode = ((JournalException) t).getOpCode();
        }
        if (t instanceof JournalInconsistentException) {
            opCode = ((JournalInconsistentException) t).getOpCode();
        }

        if (opCode != OperationType.OP_INVALID
                    && OperationType.IGNORABLE_OPERATIONS.contains(opCode)) {
            if (Config.metadata_journal_ignore_replay_failure) {
                LOG.warn("skip ignorable journal load failure, opCode: {}", opCode);
                return true;
            } else {
                LOG.warn("the failure of opCode: {} is ignorable, " +
                            "you can set metadata_journal_ignore_replay_failure to true to ignore this failure", opCode);
                return false;
            }
        }
        return false;
    }

    public void createTimePrinter() {
        // time printer will write timestamp edit log every 10 seconds
        timePrinter = new FrontendDaemon("timePrinter", 10 * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                Timestamp stamp = new Timestamp();
                editLog.logTimestamp(stamp);
            }
        };
    }

    public EditLog getEditLog() {
        return editLog;
    }

    public Journal getJournal() {
        return journal;
    }

    // Get the next available, lock-free because nextId is atomic.
    public long getNextId() {
        return idGenerator.getNextId();
    }

    public ConsistencyChecker getConsistencyChecker() {
        return consistencyChecker;
    }

    public AlterJobMgr getAlterJobMgr() {
        return alterJobMgr;
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return this.alterJobMgr.getSchemaChangeHandler();
    }

    public MaterializedViewHandler getRollupHandler() {
        return this.alterJobMgr.getMaterializedViewHandler();
    }

    public BackupHandler getBackupHandler() {
        return this.backupHandler;
    }

    public DeleteMgr getDeleteMgr() {
        return deleteMgr;
    }

    public Load getLoadInstance() {
        return load;
    }

    public LoadMgr getLoadMgr() {
        return loadMgr;
    }

    public LeaderTaskExecutor getPendingLoadTaskScheduler() {
        return pendingLoadTaskScheduler;
    }

    public PriorityLeaderTaskExecutor getLoadingLoadTaskScheduler() {
        return loadingLoadTaskScheduler;
    }

    public RoutineLoadMgr getRoutineLoadMgr() {
        return routineLoadMgr;
    }

    public StreamLoadMgr getStreamLoadMgr() {
        return streamLoadMgr;
    }

    public RoutineLoadTaskScheduler getRoutineLoadTaskScheduler() {
        return routineLoadTaskScheduler;
    }

    public ExportMgr getExportMgr() {
        return this.exportMgr;
    }

    public MaterializedViewMgr getMaterializedViewMgr() {
        return this.materializedViewMgr;
    }

    public SmallFileMgr getSmallFileMgr() {
        return this.smallFileMgr;
    }

    public long getReplayedJournalId() {
        return this.replayedJournalId.get();
    }

    public HAProtocol getHaProtocol() {
        return this.haProtocol;
    }

    public Long getMaxJournalId() {
        return this.journal.getMaxJournalId();
    }

    public long getEpoch() {
        return this.epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public FrontendNodeType getFeType() {
        return feType;
    }

    public EsRepository getEsRepository() {
        return esRepository;
    }

    public MetastoreEventsProcessor getMetastoreEventsProcessor() {
        return this.metastoreEventsProcessor;
    }

    public void setLeader(LeaderInfo info) {
        nodeMgr.setLeader(info);
    }

    public boolean canRead() {
        return this.canRead.get();
    }

    public boolean isElectable() {
        return nodeMgr.isElectable();
    }

    public boolean isLeader() {
        return feType == FrontendNodeType.LEADER;
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }

    public void setNextId(long id) {
        idGenerator.setId(id);
    }

    public void setHaProtocol(HAProtocol protocol) {
        this.haProtocol = protocol;
    }

    public static short calcShortKeyColumnCount(List<Column> columns, Map<String, String> properties)
                throws DdlException {
        List<Integer> sortKeyIdxes = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            Column column = columns.get(i);
            if (column.isKey()) {
                sortKeyIdxes.add(i);
            }
        }
        return calcShortKeyColumnCount(columns, properties, sortKeyIdxes);
    }

    public static short calcShortKeyColumnCount(List<Column> indexColumns, Map<String, String> properties,
                                                List<Integer> sortKeyIdxes)
                throws DdlException {
        LOG.debug("sort key size: {}", sortKeyIdxes.size());
        Preconditions.checkArgument(sortKeyIdxes.size() > 0);
        // figure out shortKeyColumnCount
        short shortKeyColumnCount = (short) -1;
        try {
            shortKeyColumnCount = PropertyAnalyzer.analyzeShortKeyColumnCount(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (shortKeyColumnCount != (short) -1) {
            // use user specified short key column count
            if (shortKeyColumnCount <= 0) {
                throw new DdlException("Invalid short key: " + shortKeyColumnCount);
            }
            if (shortKeyColumnCount > sortKeyIdxes.size()) {
                throw new DdlException("Short key is too large. should less than: " + sortKeyIdxes.size());
            }
            for (int pos = 0; pos < shortKeyColumnCount; pos++) {
                if (indexColumns.get(sortKeyIdxes.get(pos)).getPrimitiveType() == PrimitiveType.VARCHAR &&
                            pos != shortKeyColumnCount - 1) {
                    throw new DdlException("Varchar should not in the middle of short keys.");
                }
            }
        } else {
            /*
             * Calc short key column count. NOTE: short key column count is
             * calculated as follow: 1. All index column are taking into
             * account. 2. Max short key column count is Min(Num of
             * sortKeyIdxes, META_MAX_SHORT_KEY_NUM). 3. Short key list can
             * contains at most one VARCHAR column. And if contains, it should
             * be at the last position of the short key list.
             */
            shortKeyColumnCount = 0;
            int shortKeySizeByte = 0;
            int maxShortKeyColumnCount = Math.min(sortKeyIdxes.size(), FeConstants.SHORTKEY_MAX_COLUMN_COUNT);
            for (int i = 0; i < maxShortKeyColumnCount; i++) {
                Column column = indexColumns.get(sortKeyIdxes.get(i));
                shortKeySizeByte += column.getOlapColumnIndexSize();
                if (shortKeySizeByte > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
                    if (column.getPrimitiveType().isCharFamily()) {
                        ++shortKeyColumnCount;
                    }
                    break;
                }
                if (column.getType().isFloatingPointType() || column.getType().isComplexType()) {
                    break;
                }
                if (column.getPrimitiveType() == PrimitiveType.VARCHAR) {
                    ++shortKeyColumnCount;
                    break;
                }
                ++shortKeyColumnCount;
            }
            if (indexColumns.isEmpty()) {
                throw new DdlException("Empty schema");
            }
            if (shortKeyColumnCount == 0) {
                throw new DdlException("Data type of first column cannot be " + indexColumns.get(0).getType());
            }
        } // end calc shortKeyColumnCount

        return shortKeyColumnCount;
    }

    // for test only
    @VisibleForTesting
    public void clear() {
        localMetastore.clear();
        temporaryTableMgr.clear();
    }

    public void triggerNewImage() {
        journalWriter.setForceRollJournal();
    }

    /**
     * Returns the function that best matches 'desc' that is registered with the
     * globalStateMgr using 'mode' to check for matching. If desc matches multiple
     * functions in the globalStateMgr, it will return the function with the strictest
     * matching mode. If multiple functions match at the same matching mode,
     * ties are broken by comparing argument types in lexical order. Argument
     * types are ordered by argument precision (e.g. double is preferred over
     * float) and then by alphabetical order of argument type name, to guarantee
     * deterministic results.
     */
    public Function getFunction(Function desc, Function.CompareMode mode) {
        return functionSet.getFunction(desc, mode);
    }

    public List<Function> getBuiltinFunctions() {
        return functionSet.getBuiltinFunctions();
    }

    public boolean isNotAlwaysNullResultWithNullParamFunction(String funcName) {
        return functionSet.isNotAlwaysNullResultWithNullParamFunctions(funcName);
    }

    public void refreshExternalTable(RefreshTableStmt stmt) throws DdlException {
        TableName tableName = stmt.getTableName();
        List<String> partitionNames = stmt.getPartitions();
        refreshExternalTable(tableName, partitionNames);
        refreshOthersFeTable(tableName, partitionNames, true);
    }

    public void refreshOthersFeTable(TableName tableName, List<String> partitions, boolean isSync) throws DdlException {
        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null);
        Map<String, Future<TStatus>> resultMap = Maps.newHashMapWithExpectedSize(allFrontends.size() - 1);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first)) {
                continue;
            }

            resultMap.put(fe.getHost(), refreshOtherFesTable(
                        new TNetworkAddress(fe.getHost(), fe.getRpcPort()), tableName, partitions));
        }

        String errMsg = "";
        for (Map.Entry<String, Future<TStatus>> entry : resultMap.entrySet()) {
            try {
                TStatus status = entry.getValue().get();
                if (status.getStatus_code() != TStatusCode.OK) {
                    String err = "refresh fe " + entry.getKey() + " failed: ";
                    if (status.getError_msgs() != null && status.getError_msgs().size() > 0) {
                        err += String.join(",", status.getError_msgs());
                    }
                    errMsg += err + ";";
                }
            } catch (Exception e) {
                errMsg += "refresh fe " + entry.getKey() + " failed: " + e.getMessage();
            }
        }

        if (!errMsg.equals("")) {
            if (isSync) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_REFRESH_EXTERNAL_TABLE_FAILED, errMsg);
            } else {
                LOG.error("Background refresh others fe failed, {}", errMsg);
            }
        }
    }

    public Future<TStatus> refreshOtherFesTable(TNetworkAddress thriftAddress, TableName tableName,
                                                List<String> partitions) {
        int timeout;
        if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null) {
            timeout = Config.thrift_rpc_timeout_ms * 10;
        } else {
            timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000 + Config.thrift_rpc_timeout_ms;
        }

        FutureTask<TStatus> task = new FutureTask<TStatus>(() -> {
            TRefreshTableRequest request = new TRefreshTableRequest();
            request.setCatalog_name(tableName.getCatalog());
            request.setDb_name(tableName.getDb());
            request.setTable_name(tableName.getTbl());
            request.setPartitions(partitions);
            try {
                TRefreshTableResponse response = ThriftRPCRequestExecutor.call(
                            ThriftConnectionPool.frontendPool,
                            thriftAddress,
                            timeout,
                            client -> client.refreshTable(request));
                return response.getStatus();
            } catch (Exception e) {
                LOG.warn("call fe {} refreshTable rpc method failed", thriftAddress, e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList(e.getMessage()));
                return status;
            }
        });

        new Thread(task).start();

        return task;
    }

    private boolean supportRefreshTableType(Table table) {
        return table.isHiveTable() || table.isHudiTable() || table.isHiveView() || table.isIcebergTable()
                    || table.isJDBCTable() || table.isDeltalakeTable();
    }

    public void refreshExternalTable(TableName tableName, List<String> partitions) {
        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        Database db = metadataMgr.getDb(catalogName, tableName.getDb());
        if (db == null) {
            throw new StarRocksConnectorException("db: " + tableName.getDb() + " not exists");
        }

        Table table;
        table = metadataMgr.getTable(catalogName, dbName, tblName);
        if (table == null) {
            throw new StarRocksConnectorException("table %s.%s.%s not exists", catalogName, dbName,
                        tblName);
        }
        if (!supportRefreshTableType(table)) {
            throw new StarRocksConnectorException("can not refresh external table %s.%s.%s, " +
                        "do not support refresh external table which type is %s", catalogName, dbName,
                        tblName, table.getType());
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            Preconditions.checkState(table instanceof HiveMetaStoreTable);
            catalogName = ((HiveMetaStoreTable) table).getCatalogName();
        }

        metadataMgr.refreshTable(catalogName, dbName, table, partitions, true);
    }

    public void initDefaultWarehouse() {
        warehouseMgr.initDefaultWarehouse();
        isDefaultWarehouseCreated = true;
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        localMetastore.replayUpdateClusterAndBackends(info);
    }

    public String dumpImage() {
        LOG.info("begin to dump meta data");
        String dumpFilePath;
        Map<Long, Database> lockedDbMap = Maps.newTreeMap();
        tryLock(true);
        Locker locker = new Locker();
        try {
            // sort all dbs
            for (long dbId : localMetastore.getDbIds()) {
                Database db = localMetastore.getDb(dbId);
                Preconditions.checkNotNull(db);
                lockedDbMap.put(dbId, db);
            }

            // lock all dbs
            for (Database db : lockedDbMap.values()) {
                locker.lockDatabase(db.getId(), LockType.READ);
            }
            LOG.info("acquired all the dbs' read lock.");

            long journalId = getMaxJournalId();
            File dumpFile = new File(Config.meta_dir, "image." + journalId);
            dumpFilePath = dumpFile.getAbsolutePath();
            try {
                LOG.info("begin to dump {}", dumpFilePath);
                saveImage(new ImageWriter(Config.meta_dir, ImageFormatVersion.v2, journalId), dumpFile);
            } catch (IOException e) {
                LOG.error("failed to dump image to {}", dumpFilePath, e);
            }
        } finally {
            // unlock all
            for (Database db : lockedDbMap.values()) {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
            unlock();
        }

        LOG.info("finished dumping image to {}", dumpFilePath);
        return dumpFilePath;
    }

    public long getImageJournalId() {
        return imageJournalId;
    }

    public void setImageJournalId(long imageJournalId) {
        this.imageJournalId = imageJournalId;
    }

    public void clearExpiredJobs() {
        try {
            loadMgr.removeOldLoadJob();
        } catch (Throwable t) {
            LOG.warn("load manager remove old load jobs failed", t);
        }

        try {
            exportMgr.removeOldExportJobs();
        } catch (Throwable t) {
            LOG.warn("export manager remove old export jobs failed", t);
        }
        try {
            deleteMgr.removeOldDeleteInfo();
        } catch (Throwable t) {
            LOG.warn("delete handler remove old delete info failed", t);
        }
        try {
            globalTransactionMgr.removeExpiredTxns();
        } catch (Throwable t) {
            LOG.warn("transaction manager remove expired txns failed", t);
        }
        try {
            routineLoadMgr.cleanOldRoutineLoadJobs();
        } catch (Throwable t) {
            LOG.warn("routine load manager clean old routine load jobs failed", t);
        }
        try {
            backupHandler.removeOldJobs();
        } catch (Throwable t) {
            LOG.warn("backup handler clean old jobs failed", t);
        }
        try {
            streamLoadMgr.cleanOldStreamLoadTasks(false);
        } catch (Throwable t) {
            LOG.warn("delete handler remove old delete info failed", t);
        }
        try {
            taskManager.removeExpiredTasks();
        } catch (Throwable t) {
            LOG.warn("task manager clean expire tasks failed", t);
        }
        try {
            taskManager.removeExpiredTaskRuns();
        } catch (Throwable t) {
            LOG.warn("task manager clean expire task runs history failed", t);
        }
    }

    public void doTaskBackgroundJob() {
        try {
            taskManager.removeExpiredTasks();
        } catch (Throwable t) {
            LOG.warn("task manager clean expire tasks failed", t);
        }
        try {
            taskManager.removeExpiredTaskRuns();
        } catch (Throwable t) {
            LOG.warn("task manager clean expire task runs history failed", t);
        }
    }

    public StateChangeExecution getStateChangeExecution() {
        return execution;
    }

    public MetaContext getMetaContext() {
        return metaContext;
    }

    public void createBuiltinStorageVolume() {
        try {
            String builtinStorageVolumeId = storageVolumeMgr.createBuiltinStorageVolume();
            if (!builtinStorageVolumeId.isEmpty()) {
                authorizationMgr.grantStorageVolumeUsageToPublicRole(builtinStorageVolumeId);
            }
        } catch (InvalidConfException e) {
            LOG.fatal(e.getMessage());
            System.exit(-1);
        } catch (DdlException | AlreadyExistsException e) {
            LOG.warn("Failed to create or update builtin storage volume", e);
        } catch (PrivilegeException e) {
            LOG.warn("Failed to grant builtin storage volume usage to public role", e);
        }
    }

    public SlotManager getSlotManager() {
        return slotManager;
    }

    public GlobalSlotProvider getGlobalSlotProvider() {
        return globalSlotProvider;
    }

    public SlotProvider getLocalSlotProvider() {
        return localSlotProvider;
    }

    public GlobalLoadJobListenerBus getOperationListenerBus() {
        return operationListenerBus;
    }

    public ResourceUsageMonitor getResourceUsageMonitor() {
        return resourceUsageMonitor;
    }

    public DictionaryMgr getDictionaryMgr() {
        return dictionaryMgr;
    }

    public boolean isInTransferringToLeader() {
        return isInTransferringToLeader;
    }

    public long getDominationStartTimeMs() {
        return dominationStartTimeMs;
    }

    public MetaRecoveryDaemon getMetaRecoveryDaemon() {
        return metaRecoveryDaemon;
    }
}
