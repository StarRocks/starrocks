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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.alter.Alter;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.backup.BackupHandler;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.binlog.BinlogManager;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.BrokerTable;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MetaReplayState;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.clone.ColocateTableBalancer;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.clone.TabletChecker;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.events.MetastoreEventsProcessor;
import com.starrocks.connector.iceberg.IcebergRepository;
import com.starrocks.consistency.ConsistencyChecker;
import com.starrocks.external.elasticsearch.EsRepository;
import com.starrocks.external.starrocks.StarRocksRepository;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.ha.StateChangeExecution;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalFactory;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.lake.ShardDeleter;
import com.starrocks.lake.ShardManager;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.compaction.CompactionManager;
import com.starrocks.leader.Checkpoint;
import com.starrocks.leader.TaskRunStateSynchronizer;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.ExportChecker;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.InsertOverwriteJobManager;
import com.starrocks.load.Load;
import com.starrocks.load.loadv2.LoadEtlChecker;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadLoadingChecker;
import com.starrocks.load.loadv2.LoadManager;
import com.starrocks.load.loadv2.LoadTimeoutChecker;
import com.starrocks.load.routineload.RoutineLoadManager;
import com.starrocks.load.routineload.RoutineLoadScheduler;
import com.starrocks.load.routineload.RoutineLoadTaskScheduler;
import com.starrocks.load.streamload.StreamLoadManager;
import com.starrocks.meta.MetaContext;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthUpgrader;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.AuthUpgradeInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PrivInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.Storage;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TablePropertyInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.JournalObservable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.VariableMgr;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MVActiveChecker;
import com.starrocks.scheduler.mv.MVJobExecutor;
import com.starrocks.scheduler.mv.MVManager;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt.QuotaType;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.AnalyzeManager;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTaskExecutor;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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

    private String metaDir;
    private String imageDir;

    private MetaContext metaContext;
    private long epoch = 0;

    // Lock to perform atomic modification on map like 'idToDb' and 'fullNameToDb'.
    // These maps are all thread safe, we only use lock to perform atomic operations.
    // Operations like Get or Put do not need lock.
    // We use fair ReentrantLock to avoid starvation. Do not use this lock in critical code pass
    // because fair lock has poor performance.
    // Using QueryableReentrantLock to print owner thread in debug mode.
    private QueryableReentrantLock lock;

    private Load load;
    private LoadManager loadManager;
    private RoutineLoadManager routineLoadManager;
    private StreamLoadManager streamLoadManager;
    private ExportMgr exportMgr;
    private Alter alter;
    private ConsistencyChecker consistencyChecker;
    private BackupHandler backupHandler;
    private PublishVersionDaemon publishVersionDaemon;
    private DeleteHandler deleteHandler;
    private UpdateDbUsedDataQuotaDaemon updateDbUsedDataQuotaDaemon;

    private LeaderDaemon labelCleaner; // To clean old LabelInfo, ExportJobInfos
    private LeaderDaemon txnTimeoutChecker; // To abort timeout txns
    private LeaderDaemon taskCleaner;   // To clean expire Task/TaskRun
    private JournalWriter journalWriter; // leader only: write journal log
    private Daemon replayer;
    private Daemon timePrinter;
    private EsRepository esRepository;  // it is a daemon, so add it here
    private StarRocksRepository starRocksRepository;
    private IcebergRepository icebergRepository;
    private MetastoreEventsProcessor metastoreEventsProcessor;

    // set to true after finished replay all meta and ready to serve
    // set to false when globalStateMgr is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;

    private FrontendNodeType feType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;

    private CatalogIdGenerator idGenerator = new CatalogIdGenerator(NEXT_ID_INIT_VALUE);

    private EditLog editLog;
    private Journal journal;
    // For checkpoint and observer memory replayed marker
    private AtomicLong replayedJournalId;

    private static GlobalStateMgr CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;

    private HAProtocol haProtocol = null;

    private JournalObservable journalObservable;

    private TabletInvertedIndex tabletInvertedIndex;
    private ColocateTableIndex colocateTableIndex;

    private CatalogRecycleBin recycleBin;
    private FunctionSet functionSet;

    private MetaReplayState metaReplayState;

    private ResourceMgr resourceMgr;

    private GlobalTransactionMgr globalTransactionMgr;

    private TabletStatMgr tabletStatMgr;

    private Auth auth;

    // We're developing a new privilege & authentication framework
    // This is used to turned on in hard code.
    public static final boolean USING_NEW_PRIVILEGE = true;

    // change to true in UT
    private AtomicBoolean usingNewPrivilege;

    private AuthenticationManager authenticationManager;
    private PrivilegeManager privilegeManager;

    private DomainResolver domainResolver;

    private TabletSchedulerStat stat;

    private TabletScheduler tabletScheduler;

    private TabletChecker tabletChecker;

    // Thread pools for pending and loading task, separately
    private LeaderTaskExecutor pendingLoadTaskScheduler;
    private PriorityLeaderTaskExecutor loadingLoadTaskScheduler;

    private LoadJobScheduler loadJobScheduler;

    private LoadTimeoutChecker loadTimeoutChecker;
    private LoadEtlChecker loadEtlChecker;
    private LoadLoadingChecker loadLoadingChecker;

    private RoutineLoadScheduler routineLoadScheduler;
    private RoutineLoadTaskScheduler routineLoadTaskScheduler;

    private MVJobExecutor mvMVJobExecutor;
    private MVActiveChecker mvActiveChecker;

    private SmallFileMgr smallFileMgr;

    private DynamicPartitionScheduler dynamicPartitionScheduler;

    private PluginMgr pluginMgr;

    private AuditEventProcessor auditEventProcessor;

    private final StatisticsMetaManager statisticsMetaManager;

    private final StatisticAutoCollector statisticAutoCollector;

    private AnalyzeManager analyzeManager;

    private StatisticStorage statisticStorage;

    private long imageJournalId;

    private long feStartTime;

    private ResourceGroupMgr resourceGroupMgr;

    private StarOSAgent starOSAgent;

    private ShardDeleter shardDeleter;

    private MetadataMgr metadataMgr;
    private CatalogMgr catalogMgr;
    private ConnectorMgr connectorMgr;
    private TaskManager taskManager;
    private InsertOverwriteJobManager insertOverwriteJobManager;

    private LocalMetastore localMetastore;
    private NodeMgr nodeMgr;
    private GlobalFunctionMgr globalFunctionMgr;

    @Deprecated
    private ShardManager shardManager;

    private StateChangeExecution execution;

    private TaskRunStateSynchronizer taskRunStateSynchronizer;

    private BinlogManager binlogManager;

    // For LakeTable
    private CompactionManager compactionManager;

    private WarehouseManager warehouseMgr;

    private ConfigRefreshDaemon configRefreshDaemon;

    public List<Frontend> getFrontends(FrontendNodeType nodeType) {
        return nodeMgr.getFrontends(nodeType);
    }

    public List<String> getRemovedFrontendNames() {
        return nodeMgr.getRemovedFrontendNames();
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    public SystemInfoService getOrCreateSystemInfo(Integer clusterId) {
        return nodeMgr.getOrCreateSystemInfo(clusterId);
    }

    public SystemInfoService getClusterInfo() {
        return nodeMgr.getClusterInfo();
    }

    private HeartbeatMgr getHeartbeatMgr() {
        return nodeMgr.getHeartbeatMgr();
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

    public CompactionManager getCompactionManager() {
        return compactionManager;
    }

    public ConfigRefreshDaemon getConfigRefreshDaemon() {
        return configRefreshDaemon;
    }

    private static class SingletonHolder {
        private static final GlobalStateMgr INSTANCE = new GlobalStateMgr();
    }

    private GlobalStateMgr() {
        this(false);
    }

    // if isCkptGlobalState is true, it means that we should not collect thread pool metric
    private GlobalStateMgr(boolean isCkptGlobalState) {
        this.load = new Load();
        this.streamLoadManager = new StreamLoadManager();
        this.routineLoadManager = new RoutineLoadManager();
        this.exportMgr = new ExportMgr();
        this.alter = new Alter();
        this.consistencyChecker = new ConsistencyChecker();
        this.lock = new QueryableReentrantLock(true);
        this.backupHandler = new BackupHandler(this);
        this.publishVersionDaemon = new PublishVersionDaemon();
        this.deleteHandler = new DeleteHandler();
        this.updateDbUsedDataQuotaDaemon = new UpdateDbUsedDataQuotaDaemon();
        this.statisticsMetaManager = new StatisticsMetaManager();
        this.statisticAutoCollector = new StatisticAutoCollector();
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

        this.isDefaultClusterCreated = false;

        this.resourceMgr = new ResourceMgr();

        this.globalTransactionMgr = new GlobalTransactionMgr(this);
        this.tabletStatMgr = new TabletStatMgr();
        initAuth(USING_NEW_PRIVILEGE);

        this.resourceGroupMgr = new ResourceGroupMgr(this);

        this.esRepository = new EsRepository();
        this.starRocksRepository = new StarRocksRepository();
        this.icebergRepository = new IcebergRepository();
        this.metastoreEventsProcessor = new MetastoreEventsProcessor();

        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();
        this.nodeMgr = new NodeMgr(isCkptGlobalState, this);
        this.globalFunctionMgr = new GlobalFunctionMgr();
        this.tabletScheduler = new TabletScheduler(this, nodeMgr.getClusterInfo(), tabletInvertedIndex, stat);
        this.tabletChecker = new TabletChecker(this, nodeMgr.getClusterInfo(), tabletScheduler, stat);

        this.pendingLoadTaskScheduler =
                new LeaderTaskExecutor("pending_load_task_scheduler", Config.async_load_task_pool_size,
                        Config.desired_max_waiting_jobs, !isCkptGlobalState);
        // One load job will be split into multiple loading tasks, the queue size is not
        // determined, so set desired_max_waiting_jobs * 10
        this.loadingLoadTaskScheduler = new PriorityLeaderTaskExecutor("loading_load_task_scheduler",
                Config.async_load_task_pool_size,
                Config.desired_max_waiting_jobs * 10, !isCkptGlobalState);
        this.loadJobScheduler = new LoadJobScheduler();
        this.loadManager = new LoadManager(loadJobScheduler);
        this.loadTimeoutChecker = new LoadTimeoutChecker(loadManager);
        this.loadEtlChecker = new LoadEtlChecker(loadManager);
        this.loadLoadingChecker = new LoadLoadingChecker(loadManager);
        this.routineLoadScheduler = new RoutineLoadScheduler(routineLoadManager);
        this.routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        this.mvMVJobExecutor = new MVJobExecutor();
        this.mvActiveChecker = new MVActiveChecker();

        this.smallFileMgr = new SmallFileMgr();

        this.dynamicPartitionScheduler = new DynamicPartitionScheduler("DynamicPartitionScheduler",
                Config.dynamic_partition_check_interval_seconds * 1000L);

        setMetaDir();

        this.pluginMgr = new PluginMgr();
        this.auditEventProcessor = new AuditEventProcessor(this.pluginMgr);
        this.analyzeManager = new AnalyzeManager();

        if (Config.use_staros) {
            this.starOSAgent = new StarOSAgent();
        }

        this.localMetastore = new LocalMetastore(this, recycleBin, colocateTableIndex, nodeMgr.getClusterInfo());
        this.warehouseMgr = new WarehouseManager();
        this.connectorMgr = new ConnectorMgr();
        this.metadataMgr = new MetadataMgr(localMetastore, connectorMgr);
        this.catalogMgr = new CatalogMgr(connectorMgr);
        this.taskManager = new TaskManager();
        this.insertOverwriteJobManager = new InsertOverwriteJobManager();
        this.shardManager = new ShardManager();
        this.compactionManager = new CompactionManager();
        this.configRefreshDaemon = new ConfigRefreshDaemon();
        this.shardDeleter = new ShardDeleter();

        this.binlogManager = new BinlogManager();

        GlobalStateMgr gsm = this;
        this.execution = new StateChangeExecution() {
            @Override
            public void transferToLeader() {
                gsm.transferToLeader();
            }

            @Override
            public void transferToNonLeader(FrontendNodeType newType) {
                gsm.transferToNonLeader(newType);
            }
        };
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

    @VisibleForTesting
    public ConcurrentHashMap<Long, Database> getIdToDb() {
        return localMetastore.getIdToDb();
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

    public static GlobalTransactionMgr getCurrentGlobalTransactionMgr() {
        return getCurrentState().globalTransactionMgr;
    }

    public GlobalTransactionMgr getGlobalTransactionMgr() {
        return globalTransactionMgr;
    }

    public PluginMgr getPluginMgr() {
        return pluginMgr;
    }

    public AnalyzeManager getAnalyzeManager() {
        return analyzeManager;
    }

    public Auth getAuth() {
        return auth;
    }

    public AuthenticationManager getAuthenticationManager() {
        return authenticationManager;
    }

    public PrivilegeManager getPrivilegeManager() {
        return privilegeManager;
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

    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return localMetastore.getFullNameToDb();
    }

    public AuditEventProcessor getAuditEventProcessor() {
        return auditEventProcessor;
    }

    // use this to get correct ClusterInfoService instance
    public static SystemInfoService getCurrentSystemInfo() {
        return getCurrentState().getClusterInfo();
    }

    public static StarOSAgent getCurrentStarOSAgent() {
        return getCurrentState().getStarOSAgent();
    }

    public static HeartbeatMgr getCurrentHeartbeatMgr() {
        return getCurrentState().getHeartbeatMgr();
    }

    // use this to get correct TabletInvertedIndex instance
    public static TabletInvertedIndex getCurrentInvertedIndex() {
        return getCurrentState().getTabletInvertedIndex();
    }

    // use this to get correct ColocateTableIndex instance
    public static ColocateTableIndex getCurrentColocateIndex() {
        return getCurrentState().getColocateTableIndex();
    }

    public static CatalogRecycleBin getCurrentRecycleBin() {
        return getCurrentState().getRecycleBin();
    }

    // use this to get correct GlobalStateMgr's journal version
    public static int getCurrentStateJournalVersion() {
        return MetaContext.get().getMetaVersion();
    }

    public static int getCurrentStateStarRocksJournalVersion() {
        return MetaContext.get().getStarRocksMetaVersion();
    }

    public static boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static PluginMgr getCurrentPluginMgr() {
        return getCurrentState().getPluginMgr();
    }

    public static AnalyzeManager getCurrentAnalyzeMgr() {
        return getCurrentState().getAnalyzeManager();
    }

    public static StatisticStorage getCurrentStatisticStorage() {
        return getCurrentState().statisticStorage;
    }

    // Only used in UT
    public void setStatisticStorage(StatisticStorage statisticStorage) {
        this.statisticStorage = statisticStorage;
    }

    public static AuditEventProcessor getCurrentAuditEventProcessor() {
        return getCurrentState().getAuditEventProcessor();
    }

    public StarOSAgent getStarOSAgent() {
        return starOSAgent;
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

    public ConnectorMetadata getMetadata() {
        return localMetastore;
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

    public InsertOverwriteJobManager getInsertOverwriteJobManager() {
        return insertOverwriteJobManager;
    }

    public WarehouseManager getWarehouseMgr() {
        return warehouseMgr;
    }

    // Use tryLock to avoid potential dead lock
    public boolean tryLock(boolean mustLock) {
        while (true) {
            try {
                if (!lock.tryLock(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    // to see which thread held this lock for long time.
                    Thread owner = lock.getOwner();
                    if (owner != null) {
                        LOG.warn("globalStateMgr lock is held by: {}", Util.dumpThread(owner, 50));
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
        this.metaDir = Config.meta_dir;
        this.imageDir = this.metaDir + IMAGE_DIR;
        nodeMgr.setImageDir(imageDir);
    }

    public void initialize(String[] args) throws Exception {
        // set meta dir first.
        // we already set these variables in constructor. but GlobalStateMgr is a singleton class.
        // so they may be set before Config is initialized.
        // set them here again to make sure these variables use values in fe.conf.

        setMetaDir();

        // 0. get local node and helper node info
        nodeMgr.initialize(args);

        // 1. create dirs and files
        if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
            File imageDir = new File(this.imageDir);
            if (!imageDir.exists()) {
                imageDir.mkdirs();
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

        // 7. init starosAgent
        if (Config.use_staros && !starOSAgent.init(null)) {
            LOG.error("init starOSAgent failed");
            System.exit(-1);
        }
    }

    // set usingNewPrivilege = true in UT
    public void initAuth(boolean usingNewPrivilege) {
        this.auth = new Auth();
        this.usingNewPrivilege = new AtomicBoolean(usingNewPrivilege);
        if (usingNewPrivilege) {
            this.authenticationManager = new AuthenticationManager();
            this.domainResolver = new DomainResolver(authenticationManager);
            this.privilegeManager = new PrivilegeManager(this, null);
            LOG.info("using new privilege framework..");
        } else {
            this.domainResolver = new DomainResolver(auth);
            this.authenticationManager = null;
            this.privilegeManager = null;
        }
    }

    @VisibleForTesting
    public void setAuth(Auth auth) {
        this.auth = auth;
    }

    public boolean isUsingNewPrivilege() {
        return usingNewPrivilege.get();
    }

    private boolean needUpgradedToNewPrivilege() {
        return !privilegeManager.isLoaded() || !authenticationManager.isLoaded();
    }

    protected void initJournal() throws JournalException, InterruptedException {
        BlockingQueue<JournalTask> journalQueue =
                new ArrayBlockingQueue<JournalTask>(Config.metadata_journal_queue_size);
        journal = JournalFactory.create(nodeMgr.getNodeName());
        journalWriter = new JournalWriter(journal, journalQueue);

        editLog = new EditLog(journalQueue);
        this.globalTransactionMgr.setEditLog(editLog);
        this.idGenerator.setEditLog(editLog);
        this.localMetastore.setEditLog(editLog);
    }

    // wait until FE is ready.
    public void waitForReady() throws InterruptedException {
        while (true) {
            if (isReady()) {
                LOG.info("globalStateMgr is ready. FE type: {}", feType);
                feStartTime = System.currentTimeMillis();
                break;
            }

            Thread.sleep(2000);
            LOG.info("wait globalStateMgr to be ready. FE type: {}. is ready: {}", feType, isReady.get());
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
        try {
            // Log meta_version
            int communityMetaVersion = MetaContext.get().getMetaVersion();
            int starrocksMetaVersion = MetaContext.get().getStarRocksMetaVersion();
            if (communityMetaVersion < FeConstants.meta_version ||
                    starrocksMetaVersion < FeConstants.starrocks_meta_version) {
                editLog.logMetaVersion(new MetaVersion(FeConstants.meta_version, FeConstants.starrocks_meta_version));
                MetaContext.get().setMetaVersion(FeConstants.meta_version);
                MetaContext.get().setStarRocksMetaVersion(FeConstants.starrocks_meta_version);
            }

            // Log the first frontend
            if (nodeMgr.isFirstTimeStartUp()) {
                // if isFirstTimeStartUp is true, frontends must contains this Node.
                Frontend self = nodeMgr.getMySelf();
                Preconditions.checkNotNull(self);
                // OP_ADD_FIRST_FRONTEND is emitted, so it can write to BDBJE even if canWrite is false
                editLog.logAddFirstFrontend(self);
            }

            if (!isDefaultClusterCreated) {
                initDefaultCluster();
            }

            // MUST set leader ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-leader FE to push image
            nodeMgr.setLeaderInfo();

            if (USING_NEW_PRIVILEGE) {
                if (needUpgradedToNewPrivilege()) {
                    AuthUpgrader upgrader = new AuthUpgrader(auth, authenticationManager, privilegeManager, this);
                    // upgrade metadata in old privilege framework to the new one
                    upgrader.upgradeAsLeader();
                    this.domainResolver.setAuthenticationManager(authenticationManager);
                    usingNewPrivilege.set(true);
                }
                auth = null;  // remove references to useless objects to release memory
            }

            // start all daemon threads that only running on MASTER FE
            startLeaderOnlyDaemonThreads();
            // start other daemon threads that should run on all FEs
            startNonLeaderDaemonThreads();
            insertOverwriteJobManager.cancelRunningJobs();

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
                VariableMgr.setVar(VariableMgr.getDefaultSessionVariable(), new SetVar(SetType.GLOBAL,
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
    }

    // start all daemon threads only running on Master
    private void startLeaderOnlyDaemonThreads() {
        if (Config.integrate_starmgr) {
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

        // heartbeat mgr
        nodeMgr.startHearbeat(epoch);
        // New load scheduler
        pendingLoadTaskScheduler.start();
        loadingLoadTaskScheduler.start();
        loadManager.prepareJobs();
        loadJobScheduler.start();
        loadTimeoutChecker.start();
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
        getAlterInstance().start();
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

        // start daemon thread to report the progress of RunningTaskRun to the follower by editlog
        taskRunStateSynchronizer = new TaskRunStateSynchronizer();
        taskRunStateSynchronizer.start();

        if (Config.use_staros) {
            shardDeleter.start();
        }
    }

    // start threads that should running on all FE
    private void startNonLeaderDaemonThreads() {
        tabletStatMgr.start();
        // load and export job label cleaner thread
        labelCleaner.start();
        // ES state store
        esRepository.start();
        starRocksRepository.start();
        // materialized view active checker
        mvActiveChecker.start();

        if (Config.enable_hms_events_incremental_sync) {
            metastoreEventsProcessor.start();
        }
        // domain resolver
        domainResolver.start();
        if (Config.use_staros) {
            compactionManager.start();
        }
        configRefreshDaemon.start();
    }

    private void transferToNonLeader(FrontendNodeType newType) {
        isReady.set(false);
        if (isUsingNewPrivilege() && !needUpgradedToNewPrivilege()) {
            // already upgraded, set auth = null
            auth = null;
        }

        if (feType == FrontendNodeType.OBSERVER || feType == FrontendNodeType.FOLLOWER) {
            Preconditions.checkState(newType == FrontendNodeType.UNKNOWN);
            LOG.warn("{} to UNKNOWN, still offer read service", feType.name());
            // not set canRead here, leave canRead as what is was.
            // if meta out of date, canRead will be set to false in replayer thread.
            metaReplayState.setTransferToUnknown();
            feType = newType;
            return;
        }

        // transfer from INIT/UNKNOWN to OBSERVER/FOLLOWER

        // add helper sockets
        if (Config.edit_log_type.equalsIgnoreCase("BDB")) {
            for (Frontend fe : nodeMgr.getFrontends().values()) {
                if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                    if (getHaProtocol() instanceof BDBHA) {
                        ((BDBHA) getHaProtocol()).addHelperSocket(fe.getHost(), fe.getEditLogPort());
                    }
                }
            }
        }

        if (replayer == null) {
            createReplayer();
            replayer.start();
        }

        startNonLeaderDaemonThreads();

        MetricRepo.init();

        feType = newType;
    }

    public void loadImage(String imageDir) throws IOException, DdlException {
        Storage storage = new Storage(imageDir);
        nodeMgr.setClusterId(storage.getClusterID());
        File curFile = storage.getCurrentImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("image does not exist: {}", curFile.getAbsolutePath());
            return;
        }
        replayedJournalId.set(storage.getImageJournalId());
        LOG.info("start load image from {}. is ckpt: {}", curFile.getAbsolutePath(),
                GlobalStateMgr.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));

        long checksum = 0;
        long remoteChecksum = -1;  // in case of empty image file checksum match
        try {
            // ** NOTICE **: always add new code at the end
            checksum = loadHeader(dis, checksum);
            checksum = nodeMgr.loadLeaderInfo(dis, checksum);
            checksum = nodeMgr.loadFrontends(dis, checksum);
            checksum = nodeMgr.loadBackends(dis, checksum);
            checksum = localMetastore.loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            localMetastore.recreateTabletInvertIndex();
            // rebuild es state state
            esRepository.loadTableFromCatalog();
            starRocksRepository.loadTableFromCatalog();

            checksum = load.loadLoadJob(dis, checksum);
            checksum = loadAlterJob(dis, checksum);
            checksum = recycleBin.loadRecycleBin(dis, checksum);
            checksum = VariableMgr.loadGlobalVariable(dis, checksum);
            checksum = localMetastore.loadCluster(dis, checksum);
            checksum = nodeMgr.loadBrokers(dis, checksum);
            checksum = loadResources(dis, checksum);
            checksum = exportMgr.loadExportJob(dis, checksum);
            checksum = backupHandler.loadBackupHandler(dis, checksum, this);
            checksum = auth.loadAuth(dis, checksum);
            // global transaction must be replayed before load jobs v2
            checksum = globalTransactionMgr.loadTransactionState(dis, checksum);
            checksum = colocateTableIndex.loadColocateTableIndex(dis, checksum);
            checksum = routineLoadManager.loadRoutineLoadJobs(dis, checksum);
            checksum = loadManager.loadLoadJobsV2(dis, checksum);
            checksum = smallFileMgr.loadSmallFiles(dis, checksum);
            checksum = pluginMgr.loadPlugins(dis, checksum);
            checksum = loadDeleteHandler(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = analyzeManager.loadAnalyze(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = resourceGroupMgr.loadResourceGroups(dis, checksum);
            checksum = auth.readAsGson(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = taskManager.loadTasks(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = catalogMgr.loadCatalogs(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = loadInsertOverwriteJobs(dis, checksum);
            checksum = nodeMgr.loadComputeNodes(dis, checksum);
            remoteChecksum = dis.readLong();
            // ShardManager DEPRECATED, keep it for backward compatible
            checksum = loadShardManager(dis, checksum);
            remoteChecksum = dis.readLong();

            checksum = loadCompactionManager(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = loadStreamLoadManager(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = MVManager.getInstance().reload(dis, checksum);
            remoteChecksum = dis.readLong();
            globalFunctionMgr.loadGlobalFunctions(dis, checksum);
            loadRBACPrivilege(dis);
            checksum = warehouseMgr.loadWarehouses(dis, checksum);
            remoteChecksum = dis.readLong();
            // ** NOTICE **: always add new code at the end
        } catch (EOFException exception) {
            LOG.warn("load image eof.", exception);
        } finally {
            dis.close();
        }

        Preconditions.checkState(remoteChecksum == checksum, remoteChecksum + " vs. " + checksum);

        if (isUsingNewPrivilege() && needUpgradedToNewPrivilege() && !isLeader() && !isCheckpointThread()) {
            LOG.warn(
                    "follower has to wait for leader to upgrade the privileges, set usingNewPrivilege = false for now");
            usingNewPrivilege.set(false);
            domainResolver = new DomainResolver(auth);
        }

        try {
            postLoadImage();
        } catch (Exception t) {
            LOG.warn("there is an exception during processing after load image. exception:", t);
        }

        long loadImageEndTime = System.currentTimeMillis();
        this.imageJournalId = storage.getImageJournalId();
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
                for (MaterializedView.BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
                    Table table = baseTableInfo.getTable();
                    if (table == null) {
                        LOG.warn("tableName :{} do not exist. set materialized view:{} to invalid",
                                baseTableInfo.getTableName(), mv.getId());
                        mv.setActive(false);
                        continue;
                    }
                    if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                        LOG.warn("tableName :{} is invalid. set materialized view:{} to invalid",
                                baseTableInfo.getTableName(), mv.getId());
                        mv.setActive(false);
                        continue;
                    }
                    MvId mvId = new MvId(db.getId(), mv.getId());
                    table.addRelatedMaterializedView(mvId);
                }
            }
        }

        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("finish processing all tables' related materialized views in {}ms", duration);
    }

    public long loadHeader(DataInputStream dis, long checksum) throws IOException {
        // for community, version schema is [int], and the int value must be positive
        // for starrocks, version schema is [-1, int, int]
        // so we can check the first int to determine the version schema
        int flag = dis.readInt();
        long newChecksum = checksum ^ flag;
        if (flag < 0) {
            int communityMetaVersion = dis.readInt();
            if (communityMetaVersion > FeConstants.meta_version) {
                LOG.error("invalid meta data version found, cat not bigger than FeConstants.meta_version."
                                + "please update FeConstants.meta_version bigger or equal to {} and restart.",
                        communityMetaVersion);
                System.exit(-1);
            }
            newChecksum ^= communityMetaVersion;
            MetaContext.get().setMetaVersion(communityMetaVersion);
            int starrocksMetaVersion = dis.readInt();
            if (starrocksMetaVersion > FeConstants.starrocks_meta_version) {
                LOG.error("invalid meta data version found, cat not bigger than FeConstants.starrocks_meta_version."
                                + "please update FeConstants.starrocks_meta_version bigger or equal to {} and restart.",
                        starrocksMetaVersion);
                System.exit(-1);
            }
            newChecksum ^= starrocksMetaVersion;
            MetaContext.get().setStarRocksMetaVersion(starrocksMetaVersion);
        } else {
            // when flag is positive, this is community image structure
            int metaVersion = flag;
            if (metaVersion > FeConstants.meta_version) {
                LOG.error("invalid meta data version found, cat not bigger than FeConstants.meta_version."
                                + "please update FeConstants.meta_version bigger or equal to {} and restart.",
                        metaVersion);
                System.exit(-1);
            }
            MetaContext.get().setMetaVersion(metaVersion);
        }

        long replayedJournalId = dis.readLong();
        newChecksum ^= replayedJournalId;

        long catalogId = dis.readLong();
        newChecksum ^= catalogId;
        idGenerator.setId(catalogId);

        if (MetaContext.get().getMetaVersion() >= FeMetaVersion.VERSION_32) {
            isDefaultClusterCreated = dis.readBoolean();
        }

        LOG.info("finished replay header from image");
        return newChecksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum) throws IOException {
        long newChecksum = checksum;
        for (AlterJobV2.JobType type : AlterJobV2.JobType.values()) {
            if (type == AlterJobV2.JobType.DECOMMISSION_BACKEND) {
                if (GlobalStateMgr.getCurrentStateJournalVersion() >= 5) {
                    newChecksum = loadAlterJob(dis, newChecksum, type);
                }
            } else {
                newChecksum = loadAlterJob(dis, newChecksum, type);
            }
        }
        LOG.info("finished replay alterJob from image");
        return newChecksum;
    }

    public void loadRBACPrivilege(DataInputStream dis) throws IOException, DdlException {
        if (isUsingNewPrivilege()) {
            this.authenticationManager = AuthenticationManager.load(dis);
            this.privilegeManager = PrivilegeManager.load(dis, this, null);
            this.domainResolver = new DomainResolver(authenticationManager);
        }
    }

    public long loadAlterJob(DataInputStream dis, long checksum, AlterJobV2.JobType type) throws IOException {
        // alter jobs
        int size = dis.readInt();
        if (size > 0) {
            // It may be upgraded from an earlier version, which is dangerous
            throw new RuntimeException("Old metadata was found, please upgrade to version 2.4 first " +
                    "and then from version 2.4 to the current version.");
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= 2) {
            // finished or cancelled jobs
            size = dis.readInt();
            if (size > 0) {
                // It may be upgraded from an earlier version, which is dangerous
                throw new RuntimeException("Old metadata was found, please upgrade to version 2.4 first " +
                        "and then from version 2.4 to the current version.");
            }
        }

        long newChecksum = checksum;
        // alter job v2
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_61) {
            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                AlterJobV2 alterJobV2 = AlterJobV2.read(dis);
                if (type == AlterJobV2.JobType.ROLLUP || type == AlterJobV2.JobType.SCHEMA_CHANGE) {
                    if (type == AlterJobV2.JobType.ROLLUP) {
                        this.getRollupHandler().addAlterJobV2(alterJobV2);
                    } else {
                        this.getSchemaChangeHandler().addAlterJobV2(alterJobV2);
                    }
                    // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
                    // to prevent TabletInvertedIndex data loss,
                    // So just use AlterJob.replay() instead of AlterHandler.replay().
                    if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                        alterJobV2.replay(alterJobV2);
                        LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
                    }
                } else {
                    LOG.warn("Unknown job type:" + type.name());
                }
            }
        }

        return newChecksum;
    }

    public long loadDeleteHandler(DataInputStream dis, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_82) {
            this.deleteHandler = DeleteHandler.read(dis);
        }
        LOG.info("finished replay deleteHandler from image");
        return checksum;
    }

    public long loadInsertOverwriteJobs(DataInputStream dis, long checksum) throws IOException {
        try {
            this.insertOverwriteJobManager = InsertOverwriteJobManager.read(dis);
        } catch (EOFException e) {
            LOG.warn("no InsertOverwriteJobManager to replay.", e);
        }
        return checksum;
    }

    public long saveInsertOverwriteJobs(DataOutputStream dos, long checksum) throws IOException {
        getInsertOverwriteJobManager().write(dos);
        return checksum;
    }

    public long loadResources(DataInputStream in, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_87) {
            resourceMgr = ResourceMgr.read(in);
        }
        LOG.info("finished replay resources from image");

        LOG.info("start to replay resource mapping catalog");
        catalogMgr.loadResourceMappingCatalog();
        LOG.info("finished replaying resource mapping catalogs from resources");
        return checksum;
    }

    public long loadShardManager(DataInputStream in, long checksum) throws IOException {
        shardManager = ShardManager.read(in);
        LOG.info("finished replay shardManager from image");
        return checksum;
    }

    public long loadCompactionManager(DataInputStream in, long checksum) throws IOException {
        compactionManager = CompactionManager.loadCompactionManager(in);
        checksum ^= compactionManager.getChecksum();
        return checksum;
    }

    public long loadStreamLoadManager(DataInputStream in, long checksum) throws IOException {
        streamLoadManager = StreamLoadManager.loadStreamLoadManager(in);
        checksum ^= streamLoadManager.getChecksum();
        return checksum;
    }

    // Only called by checkpoint thread
    public void saveImage() throws IOException {
        // Write image.ckpt
        Storage storage = new Storage(this.imageDir);
        File curFile = storage.getImageFile(replayedJournalId.get());
        File ckpt = new File(this.imageDir, Storage.IMAGE_NEW);
        saveImage(ckpt, replayedJournalId.get());

        // Move image.ckpt to image.dataVersion
        LOG.info("Move " + ckpt.getAbsolutePath() + " to " + curFile.getAbsolutePath());
        if (!ckpt.renameTo(curFile)) {
            if (!curFile.delete()) {
                LOG.warn("Failed to delete file, filepath={}", curFile.getAbsolutePath());
            }
            throw new IOException();
        }
    }

    public void saveImage(File curFile, long replayedJournalId) throws IOException {
        if (!curFile.exists()) {
            if (!curFile.createNewFile()) {
                LOG.warn("Failed to create file, filepath={}", curFile.getAbsolutePath());
            }
        }

        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), GlobalStateMgr.isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(curFile))) {
            // ** NOTICE **: always add new code at the end
            checksum = saveHeader(dos, replayedJournalId, checksum);
            checksum = nodeMgr.saveLeaderInfo(dos, checksum);
            checksum = nodeMgr.saveFrontends(dos, checksum);
            checksum = nodeMgr.saveBackends(dos, checksum);
            checksum = localMetastore.saveDb(dos, checksum);
            checksum = load.saveLoadJob(dos, checksum);
            checksum = saveAlterJob(dos, checksum);
            checksum = recycleBin.saveRecycleBin(dos, checksum);
            checksum = VariableMgr.saveGlobalVariable(dos, checksum);
            checksum = localMetastore.saveCluster(dos, checksum);
            checksum = nodeMgr.saveBrokers(dos, checksum);
            checksum = resourceMgr.saveResources(dos, checksum);
            checksum = exportMgr.saveExportJob(dos, checksum);
            checksum = backupHandler.saveBackupHandler(dos, checksum);
            checksum = auth.saveAuth(dos, checksum);
            checksum = globalTransactionMgr.saveTransactionState(dos, checksum);
            checksum = colocateTableIndex.saveColocateTableIndex(dos, checksum);
            checksum = routineLoadManager.saveRoutineLoadJobs(dos, checksum);
            checksum = loadManager.saveLoadJobsV2(dos, checksum);
            checksum = smallFileMgr.saveSmallFiles(dos, checksum);
            checksum = pluginMgr.savePlugins(dos, checksum);
            checksum = deleteHandler.saveDeleteHandler(dos, checksum);
            dos.writeLong(checksum);
            checksum = analyzeManager.saveAnalyze(dos, checksum);
            dos.writeLong(checksum);
            checksum = resourceGroupMgr.saveResourceGroups(dos, checksum);
            checksum = auth.writeAsGson(dos, checksum);
            dos.writeLong(checksum);
            checksum = taskManager.saveTasks(dos, checksum);
            dos.writeLong(checksum);
            checksum = catalogMgr.saveCatalogs(dos, checksum);
            dos.writeLong(checksum);
            checksum = saveInsertOverwriteJobs(dos, checksum);
            checksum = nodeMgr.saveComputeNodes(dos, checksum);
            dos.writeLong(checksum);
            // ShardManager Deprecated, keep it for backward compatible
            checksum = shardManager.saveShardManager(dos, checksum);
            dos.writeLong(checksum);
            checksum = compactionManager.saveCompactionManager(dos, checksum);
            dos.writeLong(checksum);
            checksum = streamLoadManager.saveStreamLoadManager(dos, checksum);
            dos.writeLong(checksum);
            checksum = MVManager.getInstance().store(dos, checksum);
            dos.writeLong(checksum);
            globalFunctionMgr.saveGlobalFunctions(dos, checksum);
            saveRBACPrivilege(dos);
            checksum = warehouseMgr.saveWarehouses(dos, checksum);
            dos.writeLong(checksum);
            // ** NOTICE **: always add new code at the end
        }

        long saveImageEndTime = System.currentTimeMillis();
        LOG.info("finished save image {} in {} ms. checksum is {}",
                curFile.getAbsolutePath(), (saveImageEndTime - saveImageStartTime), checksum);
    }

    public long saveHeader(DataOutputStream dos, long replayedJournalId, long checksum) throws IOException {
        // Write meta version
        // community meta version is a positive integer, so we write -1 to distinguish old image structure
        checksum ^= -1;
        dos.writeInt(-1);
        checksum ^= FeConstants.meta_version;
        dos.writeInt(FeConstants.meta_version);
        checksum ^= FeConstants.starrocks_meta_version;
        dos.writeInt(FeConstants.starrocks_meta_version);

        // Write replayed journal id
        checksum ^= replayedJournalId;
        dos.writeLong(replayedJournalId);

        // Write id
        long id = idGenerator.getBatchEndId();
        checksum ^= id;
        dos.writeLong(id);

        dos.writeBoolean(isDefaultClusterCreated);

        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum) throws IOException {
        for (AlterJobV2.JobType type : AlterJobV2.JobType.values()) {
            checksum = saveAlterJob(dos, checksum, type);
        }
        return checksum;
    }

    public void saveRBACPrivilege(DataOutputStream dos) throws IOException {
        if (isUsingNewPrivilege()) {
            this.authenticationManager.save(dos);
            this.privilegeManager.save(dos);
        }
    }

    public long saveAlterJob(DataOutputStream dos, long checksum, AlterJobV2.JobType type) throws IOException {
        Map<Long, AlterJobV2> alterJobsV2 = Maps.newHashMap();
        if (type == AlterJobV2.JobType.ROLLUP) {
            alterJobsV2 = this.getRollupHandler().getAlterJobsV2();
        } else if (type == AlterJobV2.JobType.SCHEMA_CHANGE) {
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        }

        // alter jobs just for compatibility
        int size = 0;
        checksum ^= size;
        dos.writeInt(size);
        // finished or cancelled jobs just for compatibility
        checksum ^= size;
        dos.writeInt(size);

        // alter job v2
        size = alterJobsV2.size();
        checksum ^= size;
        dos.writeInt(size);
        for (AlterJobV2 alterJobV2 : alterJobsV2.values()) {
            alterJobV2.write(dos);
        }

        return checksum;
    }

    public void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        VariableMgr.replayGlobalVariable(variable);
    }

    public void replayGlobalVariableV2(GlobalVarPersistInfo info) throws IOException, DdlException {
        VariableMgr.replayGlobalVariableV2(info);
    }

    public void createLabelCleaner() {
        labelCleaner = new LeaderDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                clearExpiredJobs();
            }
        };
    }

    public void createTaskCleaner() {
        taskCleaner = new LeaderDaemon("TaskCleaner", Config.task_check_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                doTaskBackgroundJob();
            }
        };
    }

    public void createTxnTimeoutChecker() {
        txnTimeoutChecker = new LeaderDaemon("txnTimeoutChecker", Config.transaction_clean_interval_second) {
            @Override
            protected void runAfterCatalogReady() {
                globalTransactionMgr.abortTimeoutTxns();
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
                        cursor = journal.read(replayedJournalId.get() + 1, JournalCursor.CUROSR_END_KEY);
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

        streamLoadManager.cancelUnDurableTaskAfterRestart();


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
            try {
                entity = cursor.next();

                // EOF or aggressive retry
                if (entity == null) {
                    break;
                }

                // apply
                EditLog.loadJournal(this, entity);
            } catch (Throwable e) {
                if (canSkipBadReplayedJournal()) {
                    LOG.error("!!! DANGER: SKIP JOURNAL {}: {} !!!",
                            replayedJournalId.incrementAndGet(),
                            entity == null ? null : entity.getData(),
                            e);
                    cursor.skipNext();
                    continue;
                }
                // handled in outer loop
                LOG.warn("catch exception when replaying {},", replayedJournalId.get() + 1, e);
                throw e;
            }

            replayedJournalId.incrementAndGet();
            LOG.debug("journal {} replayed.", replayedJournalId);

            if (feType != FrontendNodeType.LEADER) {
                journalObservable.notifyObservers(replayedJournalId.get());
            }
            if (MetricRepo.isInit) {
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

    private boolean canSkipBadReplayedJournal() {
        try {
            for (String idStr : Config.metadata_journal_skip_bad_journal_ids.split(",")) {
                if (!StringUtils.isEmpty(idStr) && Long.valueOf(idStr) == replayedJournalId.get() + 1) {
                    LOG.info("skip bad replayed journal id {} because configured {}",
                            idStr, Config.metadata_journal_skip_bad_journal_ids);
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to parse metadata_journal_skip_bad_journal_ids: {}",
                    Config.metadata_journal_skip_bad_journal_ids, e);
        }
        return false;
    }

    public void createTimePrinter() {
        // time printer will write timestamp edit log every 10 seconds
        timePrinter = new LeaderDaemon("timePrinter", 10 * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                Timestamp stamp = new Timestamp();
                editLog.logTimestamp(stamp);
            }
        };
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        nodeMgr.addFrontend(role, host, editLogPort);
    }

    public void modifyFrontendHost(ModifyFrontendAddressClause modifyFrontendAddressClause) throws DdlException {
        nodeMgr.modifyFrontendHost(modifyFrontendAddressClause);
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        nodeMgr.dropFrontend(role, host, port);
    }

    public Frontend checkFeExist(String host, int port) {
        return nodeMgr.checkFeExist(host, port);
    }

    public Frontend getFeByHost(String host) {
        return nodeMgr.getFeByHost(host);
    }

    public Frontend getFeByName(String name) {
        return nodeMgr.getFeByName(name);
    }

    public int getFollowerCnt() {
        return nodeMgr.getFollowerCnt();
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        localMetastore.recoverDatabase(recoverStmt);
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        localMetastore.recoverTable(recoverStmt);
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        localMetastore.recoverPartition(recoverStmt);
    }

    public void replayEraseDatabase(long dbId) {
        localMetastore.replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        localMetastore.replayRecoverDatabase(info);
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        localMetastore.alterDatabaseQuota(stmt);
    }

    public void replayAlterDatabaseQuota(String dbName, long quota, QuotaType quotaType) {
        localMetastore.replayAlterDatabaseQuota(dbName, quota, quotaType);
    }

    public void renameDatabase(AlterDatabaseRenameStatement stmt) throws DdlException {
        localMetastore.renameDatabase(stmt);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        localMetastore.replayRenameDatabase(dbName, newDbName);
    }

    public void createTable(CreateTableStmt stmt) throws DdlException {
        localMetastore.createTable(stmt);
    }

    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        localMetastore.createTableLike(stmt);
    }

    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        localMetastore.addPartitions(db, tableName, addPartitionClause);
    }

    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        localMetastore.replayAddPartition(info);
    }

    public void replayAddPartition(PartitionPersistInfoV2 info) throws DdlException {
        localMetastore.replayAddPartition(info);
    }

    public void dropPartition(Database db, OlapTable olapTable, DropPartitionClause clause) throws DdlException {
        localMetastore.dropPartition(db, olapTable, clause);
    }

    public void replayDropPartition(DropPartitionInfo info) {
        localMetastore.replayDropPartition(info);
    }

    public void replayErasePartition(long partitionId) throws DdlException {
        localMetastore.replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) {
        localMetastore.replayRecoverPartition(info);
    }

    public static void getDdlStmt(Table table, List<String> createTableStmt, List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition,
                                  boolean hidePassword) {
        getDdlStmt(null, table, createTableStmt, addPartitionStmt, createRollupStmt, separatePartition, hidePassword);
    }

    public static void getDdlStmt(String dbName, Table table, List<String> createTableStmt,
                                  List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition, boolean hidePassword) {
        // 1. create table
        // 1.1 materialized view
        if (table.getType() == TableType.MATERIALIZED_VIEW) {
            MaterializedView mv = (MaterializedView) table;
            createTableStmt.add(mv.getMaterializedViewDdlStmt(true));
            return;
        }

        StringBuilder sb = new StringBuilder();
        // 1.2 view
        if (table.getType() == TableType.VIEW) {
            View view = (View) table;
            sb.append("CREATE VIEW `").append(table.getName()).append("` (");
            List<String> colDef = Lists.newArrayList();
            for (Column column : table.getBaseSchema()) {
                StringBuilder colSb = new StringBuilder();
                colSb.append(column.getName());
                if (!Strings.isNullOrEmpty(column.getComment())) {
                    colSb.append(" COMMENT ").append("\"").append(column.getComment()).append("\"");
                }
                colDef.add(colSb.toString());
            }
            sb.append(Joiner.on(", ").join(colDef));
            sb.append(")");
            if (!Strings.isNullOrEmpty(view.getComment())) {
                sb.append(" COMMENT \"").append(view.getComment()).append("\"");
            }
            sb.append(" AS ").append(view.getInlineViewDef()).append(";");
            createTableStmt.add(sb.toString());
            return;
        }

        // 1.3 other table type
        sb.append("CREATE ");
        if (table.getType() == TableType.MYSQL || table.getType() == TableType.ELASTICSEARCH
                || table.getType() == TableType.BROKER || table.getType() == TableType.HIVE
                || table.getType() == TableType.HUDI || table.getType() == TableType.ICEBERG
                || table.getType() == TableType.OLAP_EXTERNAL || table.getType() == TableType.JDBC
                || table.getType() == TableType.FILE) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("`.");
        }
        sb.append("`").append(table.getName()).append("` (\n");
        int idx = 0;
        for (Column column : table.getBaseSchema()) {
            if (idx++ != 0) {
                sb.append(",\n");
            }
            // There MUST BE 2 space in front of each column description line
            // sqlalchemy requires this to parse SHOW CREATE TABLE stmt.
            if (table.isOlapOrLakeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                    sb.append("  ").append(column.toSqlWithoutAggregateTypeName());
                } else {
                    sb.append("  ").append(column.toSql());
                }
            } else {
                sb.append("  ").append(column.toSql());
            }
        }
        if (table.isOlapOrLakeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }
        sb.append("\n) ENGINE=");
        if (table.isLakeTable()) {
            sb.append(EngineType.STARROCKS.name()).append(" ");
        } else {
            sb.append(table.getType().name()).append(" ");
        }

        if (table.isOlapOrLakeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;

            // keys
            sb.append("\n").append(olapTable.getKeysType().toSql()).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : olapTable.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add("`" + column.getName() + "`");
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));
            }

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // order by
            MaterializedIndexMeta index = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
            if (index.getSortKeyIdxes() != null) {
                sb.append("\nORDER BY(");
                List<String> sortKeysColumnNames = Lists.newArrayList();
                for (Integer i : index.getSortKeyIdxes()) {
                    sortKeysColumnNames.add("`" + table.getBaseSchema().get(i).getName() + "`");
                }
                sb.append(Joiner.on(", ").join(sortKeysColumnNames)).append(")");
            }

            // properties
            sb.append("\nPROPERTIES (\n");

            // replicationNum
            Short replicationNum = olapTable.getDefaultReplicationNum();
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
            sb.append(replicationNum).append("\"");

            // bloom filter
            Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
            if (bfColumnNames != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                        .append("\" = \"");
                sb.append(Joiner.on(", ").join(olapTable.getCopiedBfColumns())).append("\"");
            }

            if (separatePartition) {
                // version info
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_VERSION_INFO)
                        .append("\" = \"");
                Partition partition = null;
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    partition = olapTable.getPartition(olapTable.getName());
                } else {
                    Preconditions.checkState(partitionId.size() == 1);
                    partition = olapTable.getPartition(partitionId.get(0));
                }
                sb.append(partition.getVisibleVersion()).append("\"");
            }

            // colocateTable
            String colocateTable = olapTable.getColocateGroup();
            if (colocateTable != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
                        .append("\" = \"");
                sb.append(colocateTable).append("\"");
            }

            // dynamic partition
            if (olapTable.dynamicPartitionExists()) {
                sb.append(olapTable.getTableProperty().getDynamicPartitionProperty().toString());
            }

            // enable storage cache && cache ttl
            if (table.isLakeTable()) {
                Map<String, String> storageProperties = olapTable.getProperties();

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE)).append("\"");

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL)).append("\"");

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK)).append("\"");
            } else {
                // in memory
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_INMEMORY)
                        .append("\" = \"");
                sb.append(olapTable.isInMemory()).append("\"");

                // storage type
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)
                        .append("\" = \"");
                sb.append(olapTable.getStorageFormat()).append("\"");

                // enable_persistent_index
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)
                        .append("\" = \"");
                sb.append(olapTable.enablePersistentIndex()).append("\"");

                // replicated_storage
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)
                        .append("\" = \"");
                sb.append(olapTable.enableReplicatedStorage()).append("\"");

                // binlog config
                if (olapTable.containsBinlogConfig()) {
                    // binlog_version
                    BinlogConfig binlogConfig = olapTable.getCurBinlogConfig();
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION)
                            .append("\" = \"");
                    sb.append(binlogConfig.getVersion()).append("\"");
                    // binlog_enable
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)
                            .append("\" = \"");
                    sb.append(binlogConfig.getBinlogEnable()).append("\"");
                    // binlog_ttl
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_BINLOG_TTL)
                            .append("\" = \"");
                    sb.append(binlogConfig.getBinlogTtlSecond()).append("\"");
                    // binlog_max_size
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)
                            .append("\" = \"");
                    sb.append(binlogConfig.getBinlogMaxSize()).append("\"");
                }

                // write quorum
                if (olapTable.writeQuorum() != TWriteQuorumType.MAJORITY) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)
                            .append("\" = \"");
                    sb.append(WriteQuorum.writeQuorumToName(olapTable.writeQuorum())).append("\"");
                }

                // storage media
                Map<String, String> properties = olapTable.getTableProperty().getProperties();

                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
                            .append("\" = \"");
                    sb.append(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)).append("\"");
                }
            }

            // compression type
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COMPRESSION)
                    .append("\" = \"");
            if (olapTable.getCompressionType() == TCompressionType.LZ4_FRAME) {
                sb.append("LZ4").append("\"");
            } else if (olapTable.getCompressionType() == TCompressionType.LZ4) {
                sb.append("LZ4").append("\"");
            } else {
                sb.append(olapTable.getCompressionType()).append("\"");
            }

            if (table.getType() == TableType.OLAP_EXTERNAL) {
                ExternalOlapTable externalOlapTable = (ExternalOlapTable) table;
                // properties
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("host\" = \"")
                        .append(externalOlapTable.getSourceTableHost()).append("\"");
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("port\" = \"")
                        .append(externalOlapTable.getSourceTablePort()).append("\"");
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("user\" = \"")
                        .append(externalOlapTable.getSourceTableUser()).append("\"");
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("password\" = \"")
                        .append(hidePassword ? "" : externalOlapTable.getSourceTablePassword())
                        .append("\"");
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("database\" = \"")
                        .append(externalOlapTable.getSourceTableDbName()).append("\"");
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append("table\" = \"")
                        .append(externalOlapTable.getSourceTableName()).append("\"");
            }

            sb.append("\n)");
        } else if (table.getType() == TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"host\" = \"").append(mysqlTable.getHost()).append("\",\n");
            sb.append("\"port\" = \"").append(mysqlTable.getPort()).append("\",\n");
            sb.append("\"user\" = \"").append(mysqlTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : mysqlTable.getPasswd()).append("\",\n");
            sb.append("\"database\" = \"").append(mysqlTable.getMysqlDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(mysqlTable.getMysqlTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.BROKER) {
            BrokerTable brokerTable = (BrokerTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"broker_name\" = \"").append(brokerTable.getBrokerName()).append("\",\n");
            sb.append("\"path\" = \"").append(Joiner.on(",").join(brokerTable.getEncodedPaths())).append("\",\n");
            sb.append("\"column_separator\" = \"").append(brokerTable.getReadableColumnSeparator()).append("\",\n");
            sb.append("\"line_delimiter\" = \"").append(brokerTable.getReadableRowDelimiter()).append("\"\n");
            sb.append(")");
            if (!brokerTable.getBrokerProperties().isEmpty()) {
                sb.append("\nBROKER PROPERTIES (\n");
                sb.append(new PrintableMap<>(brokerTable.getBrokerProperties(), " = ", true, true,
                        hidePassword).toString());
                sb.append("\n)");
            }
        } else if (table.getType() == TableType.ELASTICSEARCH) {
            EsTable esTable = (EsTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // partition
            PartitionInfo partitionInfo = esTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                sb.append("\n");
                sb.append("PARTITION BY RANGE(");
                idx = 0;
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    if (idx != 0) {
                        sb.append(", ");
                    }
                    sb.append("`").append(column.getName()).append("`");
                }
                sb.append(")\n()");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"hosts\" = \"").append(esTable.getHosts()).append("\",\n");
            sb.append("\"user\" = \"").append(esTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : esTable.getPasswd()).append("\",\n");
            sb.append("\"index\" = \"").append(esTable.getIndexName()).append("\",\n");
            if (esTable.getMappingType() != null) {
                sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            }
            sb.append("\"transport\" = \"").append(esTable.getTransport()).append("\",\n");
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isDocValueScanEnable()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.maxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isKeywordSniffEnable()).append("\",\n");
            sb.append("\"es.nodes.wan.only\" = \"").append(esTable.wanOnly()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getDbName()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getTableName()).append("\",\n");
            sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\"");
            if (!hiveTable.getHiveProperties().isEmpty()) {
                sb.append(",\n");
            }
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.FILE) {
            FileTable fileTable = (FileTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }
            sb.append("\nPROPERTIES (\n");
            sb.append(new PrintableMap<>(fileTable.getFileProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.HUDI) {
            HudiTable hudiTable = (HudiTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hudiTable.getDbName()).append("\",\n");
            sb.append("\"table\" = \"").append(hudiTable.getTableName()).append("\",\n");
            sb.append("\"resource\" = \"").append(hudiTable.getResourceName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.ICEBERG) {
            IcebergTable icebergTable = (IcebergTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(icebergTable.getDb()).append("\",\n");
            sb.append("\"table\" = \"").append(icebergTable.getTable()).append("\",\n");
            String maxTotalBytes = icebergTable.getFileIOMaxTotalBytes();
            if (!Strings.isNullOrEmpty(maxTotalBytes)) {
                sb.append("\"fileIO.cache.max-total-bytes\" = \"").append(maxTotalBytes).append("\",\n");
            }
            sb.append("\"resource\" = \"").append(icebergTable.getResourceName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.JDBC) {
            JDBCTable jdbcTable = (JDBCTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"resource\" = \"").append(jdbcTable.getResourceName()).append("\",\n");
            sb.append("\"table\" = \"").append(jdbcTable.getJdbcTable()).append("\"");
            sb.append("\n)");
        }
        sb.append(";");

        createTableStmt.add(sb.toString());

        // 2. add partition
        if (separatePartition && (table instanceof OlapTable)
                && ((OlapTable) table).getPartitionInfo().getType() == PartitionType.RANGE
                && ((OlapTable) table).getPartitions().size() > 1) {
            OlapTable olapTable = (OlapTable) table;
            RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            boolean first = true;
            for (Map.Entry<Long, Range<PartitionKey>> entry : partitionInfo.getSortedRangeMap(false)) {
                if (first) {
                    first = false;
                    continue;
                }
                sb = new StringBuilder();
                Partition partition = olapTable.getPartition(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName());
                sb.append(" ADD PARTITION ").append(partition.getName()).append(" VALUES [");
                sb.append(entry.getValue().lowerEndpoint().toSql());
                sb.append(", ").append(entry.getValue().upperEndpoint().toSql()).append(")");
                sb.append("(\"version_info\" = \"");
                sb.append(partition.getVisibleVersion()).append("\"");
                sb.append(");");
                addPartitionStmt.add(sb.toString());
            }
        }

        // 3. rollup
        if (createRollupStmt != null && (table instanceof OlapTable)) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                if (entry.getKey() == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta materializedIndexMeta = entry.getValue();
                sb = new StringBuilder();
                String indexName = olapTable.getIndexNameById(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName()).append(" ADD ROLLUP ").append(indexName);
                sb.append("(");

                List<Column> indexSchema = materializedIndexMeta.getSchema();
                for (int i = 0; i < indexSchema.size(); i++) {
                    Column column = indexSchema.get(i);
                    sb.append(column.getName());
                    if (i != indexSchema.size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(");");
                createRollupStmt.add(sb.toString());
            }
        }
    }

    public void replayCreateTable(String dbName, Table table) {
        localMetastore.replayCreateTable(dbName, table);
    }

    public void replayCreateMaterializedView(String dbName, MaterializedView materializedView) {
        localMetastore.replayCreateMaterializedView(dbName, materializedView);
    }

    // Drop table
    public void dropTable(DropTableStmt stmt) throws DdlException {
        localMetastore.dropTable(stmt);
    }

    public void sendDropTabletTasks(HashMap<Long, AgentBatchTask> batchTaskMap) {
        localMetastore.sendDropTabletTasks(batchTaskMap);
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        localMetastore.replayDropTable(db, tableId, isForceDrop);
    }

    public void replayEraseTable(long tableId) throws DdlException {
        localMetastore.replayEraseTable(tableId);
    }

    public void replayEraseMultiTables(MultiEraseTableInfo multiEraseTableInfo) throws DdlException {
        localMetastore.replayEraseMultiTables(multiEraseTableInfo);
    }

    public void replayRecoverTable(RecoverInfo info) {
        localMetastore.replayRecoverTable(info);
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        localMetastore.replayAddReplica(info);
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        localMetastore.replayUpdateReplica(info);
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        localMetastore.replayDeleteReplica(info);
    }

    public void replayAddFrontend(Frontend fe) {
        nodeMgr.replayAddFrontend(fe);
    }

    public void replayUpdateFrontend(Frontend frontend) {
        nodeMgr.replayUpdateFrontend(frontend);
    }

    public void replayDropFrontend(Frontend frontend) {
        nodeMgr.replayDropFrontend(frontend);
    }

    public int getClusterId() {
        return nodeMgr.getClusterId();
    }

    public String getToken() {
        return nodeMgr.getToken();
    }

    public Database getDb(String name) {
        return localMetastore.getDb(name);
    }

    public Database getDb(long dbId) {
        return localMetastore.getDb(dbId);
    }

    public Database getDbIncludeRecycleBin(long dbId) {
        return localMetastore.getDbIncludeRecycleBin(dbId);
    }

    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        return localMetastore.getTableIncludeRecycleBin(db, tableId);
    }

    public List<Table> getTablesIncludeRecycleBin(Database db) {
        return localMetastore.getTablesIncludeRecycleBin(db);
    }

    public Partition getPartitionIncludeRecycleBin(OlapTable table, long partitionId) {
        return localMetastore.getPartitionIncludeRecycleBin(table, partitionId);
    }

    public Collection<Partition> getPartitionsIncludeRecycleBin(OlapTable table) {
        return localMetastore.getPartitionsIncludeRecycleBin(table);
    }

    public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable table) {
        return localMetastore.getAllPartitionsIncludeRecycleBin(table);
    }

    // NOTE: result can be null, cause partition erase is not in db lock
    public DataProperty getDataPropertyIncludeRecycleBin(PartitionInfo info, long partitionId) {
        return localMetastore.getDataPropertyIncludeRecycleBin(info, partitionId);
    }

    // NOTE: result can be -1, cause partition erase is not in db lock
    public short getReplicationNumIncludeRecycleBin(PartitionInfo info, long partitionId) {
        return localMetastore.getReplicationNumIncludeRecycleBin(info, partitionId);
    }

    public EditLog getEditLog() {
        return editLog;
    }

    public Journal getJournal() {
        return journal;
    }

    // Get the next available, need't lock because of nextId is atomic.
    public long getNextId() {
        return idGenerator.getNextId();
    }

    public List<String> getDbNames() {
        return localMetastore.listDbNames();
    }

    public List<Long> getDbIds() {
        return localMetastore.getDbIds();
    }

    public List<Long> getDbIdsIncludeRecycleBin() {
        return localMetastore.getDbIdsIncludeRecycleBin();
    }

    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        return localMetastore.getPartitionIdToStorageMediumMap();
    }

    public ConsistencyChecker getConsistencyChecker() {
        return this.consistencyChecker;
    }

    public Alter getAlterInstance() {
        return this.alter;
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return (SchemaChangeHandler) this.alter.getSchemaChangeHandler();
    }

    public MaterializedViewHandler getRollupHandler() {
        return (MaterializedViewHandler) this.alter.getMaterializedViewHandler();
    }

    public SystemHandler getClusterHandler() {
        return (SystemHandler) this.alter.getClusterHandler();
    }

    public BackupHandler getBackupHandler() {
        return this.backupHandler;
    }

    public DeleteHandler getDeleteHandler() {
        return this.deleteHandler;
    }

    public Load getLoadInstance() {
        return this.load;
    }

    public LoadManager getLoadManager() {
        return loadManager;
    }

    public LeaderTaskExecutor getPendingLoadTaskScheduler() {
        return pendingLoadTaskScheduler;
    }

    public PriorityLeaderTaskExecutor getLoadingLoadTaskScheduler() {
        return loadingLoadTaskScheduler;
    }

    public RoutineLoadManager getRoutineLoadManager() {
        return routineLoadManager;
    }

    public StreamLoadManager getStreamLoadManager() {
        return streamLoadManager;
    }

    public RoutineLoadTaskScheduler getRoutineLoadTaskScheduler() {
        return routineLoadTaskScheduler;
    }

    public ExportMgr getExportMgr() {
        return this.exportMgr;
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

    public FrontendNodeType getRole() {
        return nodeMgr.getRole();
    }

    public Pair<String, Integer> getHelperNode() {
        return nodeMgr.getHelperNode();
    }

    public List<Pair<String, Integer>> getHelperNodes() {
        return nodeMgr.getHelperNodes();
    }

    public Pair<String, Integer> getSelfNode() {
        return nodeMgr.getSelfNode();
    }

    public String getNodeName() {
        return nodeMgr.getNodeName();
    }

    public FrontendNodeType getFeType() {
        return this.feType;
    }

    public int getLeaderRpcPort() {
        return nodeMgr.getLeaderRpcPort();
    }

    public int getLeaderHttpPort() {
        return nodeMgr.getLeaderHttpPort();
    }

    public String getLeaderIp() {
        return nodeMgr.getLeaderIp();
    }

    public EsRepository getEsRepository() {
        return this.esRepository;
    }

    public StarRocksRepository getStarRocksRepository() {
        return this.starRocksRepository;
    }

    public IcebergRepository getIcebergRepository() {
        return this.icebergRepository;
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
        localMetastore.setEditLog(editLog);
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
            int maxShortKeyColumnCount = Math.min(sortKeyIdxes.size(), FeConstants.shortkey_max_column_count);
            for (int i = 0; i < maxShortKeyColumnCount; i++) {
                Column column = indexColumns.get(sortKeyIdxes.get(i));
                shortKeySizeByte += column.getOlapColumnIndexSize();
                if (shortKeySizeByte > FeConstants.shortkey_maxsize_bytes) {
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

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    public void alterTable(AlterTableStmt stmt) throws UserException {
        localMetastore.alterTable(stmt);
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    public void alterView(AlterViewStmt stmt) throws UserException {
        localMetastore.alterView(stmt);
    }

    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        localMetastore.createMaterializedView(stmt);
    }

    public void createMaterializedView(CreateMaterializedViewStatement statement)
            throws DdlException {
        localMetastore.createMaterializedView(statement);
    }

    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        localMetastore.dropMaterializedView(stmt);
    }

    public void alterMaterializedView(AlterMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        localMetastore.alterMaterializedView(stmt);
    }

    public void replayRenameMaterializedView(RenameMaterializedViewLog log) {
        this.alter.replayRenameMaterializedView(log);
    }

    public void replayChangeMaterializedViewRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        this.alter.replayChangeMaterializedViewRefreshScheme(log);
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        this.alter.replayAlterMaterializedViewProperties(opCode, log);
    }

    /*
     * used for handling CancelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        localMetastore.cancelAlter(stmt);
    }

    /*
     * used for handling backup opt
     */
    public void backup(BackupStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    public void restore(RestoreStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    public void cancelBackup(CancelBackupStmt stmt) throws DdlException {
        getBackupHandler().cancel(stmt);
    }

    // entry of rename table operation
    public void renameTable(Database db, OlapTable table, TableRenameClause tableRenameClause) throws DdlException {
        localMetastore.renameTable(db, table, tableRenameClause);
    }

    public void replayRenameTable(TableInfo tableInfo) {
        localMetastore.replayRenameTable(tableInfo);
    }

    // the invoker should keep db write lock
    public void modifyTableColocate(Database db, OlapTable table, String colocateGroup, boolean isReplay,
                                    GroupId assignedGroupId)
            throws DdlException {
        colocateTableIndex.modifyTableColocate(db, table, colocateGroup, isReplay, assignedGroupId);
    }

    public void replayModifyTableColocate(TablePropertyInfo info) {
        colocateTableIndex.replayModifyTableColocate(info);
    }

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        localMetastore.renameRollup(db, table, renameClause);
    }

    public void replayRenameRollup(TableInfo tableInfo) {
        localMetastore.replayRenameRollup(tableInfo);
    }

    public void renamePartition(Database db, OlapTable table, PartitionRenameClause renameClause) throws DdlException {
        localMetastore.renamePartition(db, table, renameClause);
    }

    public void replayRenamePartition(TableInfo tableInfo) throws DdlException {
        localMetastore.replayRenamePartition(tableInfo);
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.modifyTableDynamicPartition(db, table, properties);
    }

    public void modifyTableReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.modifyTableReplicationNum(db, table, properties);
    }

    // The caller need to hold the db write lock
    public void modifyTableDefaultReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.modifyTableDefaultReplicationNum(db, table, properties);
    }

    public void modifyTableMeta(Database db, OlapTable table, Map<String, String> properties,
                                TTabletMetaType metaType) {
        localMetastore.modifyTableMeta(db, table, properties, metaType);
    }

    public void modifyBinlogMeta(Database db, OlapTable table, BinlogConfig binlogConfig) {
        localMetastore.modifyBinlogMeta(db, table, binlogConfig);
    }

    public void setHasForbitGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
        localMetastore.setHasForbitGlobalDict(dbName, tableName, isForbit);
    }

    public void replayModifyHiveTableColumn(short opCode, ModifyTableColumnOperationLog info) {
        localMetastore.replayModifyHiveTableColumn(opCode, info);
    }

    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info) {
        localMetastore.replayModifyTableProperty(opCode, info);
    }

    /*
     * used for handling AlterClusterStmt
     * (for client is the ALTER CLUSTER command).
     */
    public ShowResultSet alterCluster(AlterSystemStmt stmt) throws UserException {
        return this.alter.processAlterCluster(stmt);
    }

    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        this.alter.getClusterHandler().cancel(stmt);
    }

    // Change current warehouse of this session.
    public void changeWarehouse(ConnectContext ctx, String newWarehouseName) throws AnalysisException {
        if (!warehouseMgr.warehouseExists(newWarehouseName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_WAREHOUSE_ERROR, newWarehouseName);
        }
        ctx.setCurrentWarehouse(newWarehouseName);
    }

    // Change current catalog of this session.
    // We can support "use 'catalog <catalog_name>'" from mysql client or "use catalog <catalog_name>" from jdbc.
    public void changeCatalog(ConnectContext ctx, String newCatalogName) throws AnalysisException {
        if (!catalogMgr.catalogExists(newCatalogName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
        }
        if (isUsingNewPrivilege() && !CatalogMgr.isInternalCatalog(newCatalogName) &&
                !PrivilegeManager.checkAnyActionOnCatalog(ctx, newCatalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USE CATALOG");
        }
        ctx.setCurrentCatalog(newCatalogName);
    }

    // Change current catalog and database of this session.
    // We can support 'USE CATALOG.DB'
    public void changeCatalogDb(ConnectContext ctx, String identifier) throws DdlException {
        String dbName = ctx.getDatabase();

        String[] parts = identifier.split("\\.");
        if (parts.length != 1 && parts.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
        } else if (parts.length == 1) {
            dbName = identifier;
        } else {
            String newCatalogName = parts[0];
            if (catalogMgr.catalogExists(newCatalogName)) {
                dbName = parts[1];
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
            }
            if (isUsingNewPrivilege() && !CatalogMgr.isInternalCatalog(newCatalogName) &&
                    !PrivilegeManager.checkAnyActionOnCatalog(ctx, newCatalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USE CATALOG");
            }
            ctx.setCurrentCatalog(newCatalogName);
        }

        if (metadataMgr.getDb(ctx.getCurrentCatalog(), dbName) == null) {
            LOG.debug("Unknown catalog '%s' and db '%s'", ctx.getCurrentCatalog(), dbName);
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // Check auth for internal catalog.
        // Here we check the request permission that sent by the mysql client or jdbc.
        // So we didn't check UseDbStmt permission in PrivilegeChecker.
        if (CatalogMgr.isInternalCatalog(ctx.getCurrentCatalog())) {
            if (isUsingNewPrivilege()) {
                if (!PrivilegeManager.checkAnyActionOnOrInDb(ctx, dbName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED,
                            ctx.getQualifiedUser(), dbName);
                }
            } else {
                if (!auth.checkDbPriv(ctx, dbName, PrivPredicate.SHOW)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED,
                            ctx.getQualifiedUser(), dbName);
                }
            }
        }

        ctx.setDatabase(dbName);
    }

    // for test only
    @VisibleForTesting
    public void clear() {
        localMetastore.clear();
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        localMetastore.createView(stmt);
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

    public void replayCreateCluster(Cluster cluster) {
        localMetastore.replayCreateCluster(cluster);
    }

    public void setIsDefaultClusterCreated(boolean isDefaultClusterCreated) {
        this.isDefaultClusterCreated = isDefaultClusterCreated;
    }

    public Cluster getCluster() {
        return localMetastore.getCluster();
    }

    public void refreshExternalTable(RefreshTableStmt stmt) throws DdlException {
        refreshExternalTable(stmt.getTableName(), stmt.getPartitions());

        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getFrontends(null);
        Map<String, Future<TStatus>> resultMap = Maps.newHashMapWithExpectedSize(allFrontends.size() - 1);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getSelfNode().first)) {
                continue;
            }

            resultMap.put(fe.getHost(), refreshOtherFesTable(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                    stmt.getTableName(), stmt.getPartitions()));
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
            ErrorReport.reportDdlException(ErrorCode.ERROR_REFRESH_EXTERNAL_TABLE_FAILED, errMsg);
        }
    }

    public Future<TStatus> refreshOtherFesTable(TNetworkAddress thriftAddress, TableName tableName,
                                                List<String> partitions) {
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000
                + Config.thrift_rpc_timeout_ms;
        FutureTask<TStatus> task = new FutureTask<TStatus>(() -> {
            TRefreshTableRequest request = new TRefreshTableRequest();
            request.setCatalog_name(tableName.getCatalog());
            request.setDb_name(tableName.getDb());
            request.setTable_name(tableName.getTbl());
            request.setPartitions(partitions);
            try {
                TRefreshTableResponse response = FrontendServiceProxy.call(thriftAddress,
                        timeout,
                        Config.thrift_rpc_retry_times,
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

    public void refreshExternalTable(TableName tableName, List<String> partitions) {
        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        Database db = metadataMgr.getDb(catalogName, tableName.getDb());
        if (db == null) {
            throw new StarRocksConnectorException("db: " + tableName.getDb() + " not exists");
        }
        HiveMetaStoreTable hmsTable;
        Table table;
        db.readLock();
        try {
            table = metadataMgr.getTable(catalogName, dbName, tblName);
            if (!(table instanceof HiveMetaStoreTable)) {
                throw new StarRocksConnectorException(
                        "table : " + tableName + " not exists, or is not hive/hudi external table");
            }
            hmsTable = (HiveMetaStoreTable) table;
        } finally {
            db.readUnlock();
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            catalogName = hmsTable.getCatalogName();
        }

        metadataMgr.refreshTable(catalogName, dbName, table, partitions);
    }

    public void initDefaultCluster() {
        localMetastore.initDefaultCluster();
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        localMetastore.replayUpdateClusterAndBackends(info);
    }

    public String dumpImage() {
        LOG.info("begin to dump meta data");
        String dumpFilePath;
        Map<Long, Database> lockedDbMap = Maps.newTreeMap();
        tryLock(true);
        try {
            // sort all dbs
            for (long dbId : getDbIds()) {
                Database db = getDb(dbId);
                Preconditions.checkNotNull(db);
                lockedDbMap.put(dbId, db);
            }

            // lock all dbs
            for (Database db : lockedDbMap.values()) {
                db.readLock();
            }
            LOG.info("acquired all the dbs' read lock.");

            long journalId = getMaxJournalId();
            File dumpFile = new File(Config.meta_dir, "image." + journalId);
            dumpFilePath = dumpFile.getAbsolutePath();
            try {
                LOG.info("begin to dump {}", dumpFilePath);
                saveImage(dumpFile, journalId);
            } catch (IOException e) {
                LOG.error("failed to dump image to {}", dumpFilePath, e);
            }
        } finally {
            // unlock all
            for (Database db : lockedDbMap.values()) {
                db.readUnlock();
            }
            unlock();
        }

        LOG.info("finished dumping image to {}", dumpFilePath);
        return dumpFilePath;
    }

    public List<Partition> createTempPartitionsFromPartitions(Database db, Table table,
                                                              String namePostfix, List<Long> sourcePartitionIds,
                                                              List<Long> tmpPartitionIds) {
        return localMetastore.createTempPartitionsFromPartitions(db, table, namePostfix, sourcePartitionIds,
                tmpPartitionIds);
    }

    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        localMetastore.truncateTable(truncateTableStmt);
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        localMetastore.replayTruncateTable(info);
    }

    public void updateResourceUsage(long backendId, TResourceUsage usage) {
        nodeMgr.updateResourceUsage(backendId, usage);
    }

    public void setConfig(AdminSetConfigStmt stmt) throws DdlException {
        nodeMgr.setConfig(stmt);
    }

    public void setFrontendConfig(Map<String, String> configs) throws DdlException {
        nodeMgr.setFrontendConfig(configs);
    }

    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        localMetastore.replayBackendTabletsInfo(backendTabletsInfo);
    }

    public void convertDistributionType(Database db, OlapTable tbl) throws DdlException {
        localMetastore.convertDistributionType(db, tbl);
    }

    public void replayConvertDistributionType(TableInfo tableInfo) {
        localMetastore.replayConvertDistributionType(tableInfo);
    }

    public void replaceTempPartition(Database db, String tableName, ReplacePartitionClause clause) throws DdlException {
        localMetastore.replaceTempPartition(db, tableName, clause);
    }

    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog) {
        localMetastore.replayReplaceTempPartition(replaceTempPartitionLog);
    }

    public void installPlugin(InstallPluginStmt stmt) throws UserException, IOException {
        pluginMgr.installPlugin(stmt);
    }

    public void replayInstallPlugin(PluginInfo pluginInfo) {
        try {
            pluginMgr.replayLoadDynamicPlugin(pluginInfo);
        } catch (Exception e) {
            LOG.warn("replay install plugin failed.", e);
        }
    }

    public void uninstallPlugin(UninstallPluginStmt stmt) throws IOException, UserException {
        PluginInfo info = pluginMgr.uninstallPlugin(stmt.getPluginName());
        if (null != info) {
            editLog.logUninstallPlugin(info);
        }
        LOG.info("uninstall plugin = " + stmt.getPluginName());
    }

    public void replayUninstallPlugin(PluginInfo pluginInfo) {
        try {
            pluginMgr.uninstallPlugin(pluginInfo.getName());
        } catch (Exception e) {
            LOG.warn("replay uninstall plugin failed.", e);
        }
    }

    /**
     * pretend we're using old auth if we have replayed journal from old auth
     */
    public void replayOldAuthJournal(short code, Writable data) throws DdlException {
        if (USING_NEW_PRIVILEGE) {
            LOG.warn("replay old auth journal right after restart, set usingNewPrivilege = false for now");
            usingNewPrivilege.set(false);
            domainResolver = new DomainResolver(auth);
        }
        switch (code) {
            case OperationType.OP_CREATE_USER: {
                auth.replayCreateUser((PrivInfo) data);
                break;
            }
            case OperationType.OP_NEW_DROP_USER: {
                auth.replayDropUser((UserIdentity) data);
                break;
            }
            case OperationType.OP_GRANT_PRIV: {
                auth.replayGrant((PrivInfo) data);
                break;
            }
            case OperationType.OP_REVOKE_PRIV: {
                auth.replayRevoke((PrivInfo) data);
                break;
            }
            case OperationType.OP_SET_PASSWORD: {
                auth.replaySetPassword((PrivInfo) data);
                break;
            }
            case OperationType.OP_CREATE_ROLE: {
                auth.replayCreateRole((PrivInfo) data);
                break;
            }
            case OperationType.OP_DROP_ROLE: {
                auth.replayDropRole((PrivInfo) data);
                break;
            }
            case OperationType.OP_GRANT_ROLE: {
                auth.replayGrantRole((PrivInfo) data);
                break;
            }
            case OperationType.OP_REVOKE_ROLE: {
                auth.replayRevokeRole((PrivInfo) data);
                break;
            }
            case OperationType.OP_UPDATE_USER_PROPERTY: {
                auth.replayUpdateUserProperty((UserPropertyInfo) data);
                break;
            }
            case OperationType.OP_GRANT_IMPERSONATE: {
                auth.replayGrantImpersonate((ImpersonatePrivInfo) data);
                break;
            }
            case OperationType.OP_REVOKE_IMPERSONATE: {
                auth.replayRevokeImpersonate((ImpersonatePrivInfo) data);
                break;
            }
            default:
                throw new DdlException("unknown code " + code);
        }

    }

    public void replayAuthUpgrade(AuthUpgradeInfo info) throws AuthUpgrader.AuthUpgradeUnrecoverableException {
        AuthUpgrader upgrader = new AuthUpgrader(auth, authenticationManager, privilegeManager, this);
        upgrader.replayUpgrade(info.getRoleNameToId());
        usingNewPrivilege.set(true);
        domainResolver.setAuthenticationManager(authenticationManager);
        if (!isCheckpointThread()) {
            this.auth = null;
        }
    }

    // entry of checking tablets operation
    public void checkTablets(AdminCheckTabletsStmt stmt) {
        localMetastore.checkTablets(stmt);
    }

    // Set specified replica's status. If replica does not exist, just ignore it.
    public void setReplicaStatus(AdminSetReplicaStatusStmt stmt) {
        localMetastore.setReplicaStatus(stmt);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        localMetastore.replaySetReplicaStatus(log);
    }

    public void onEraseDatabase(long dbId) {
        localMetastore.onEraseDatabase(dbId);
    }

    public void onErasePartition(Partition partition) {
        localMetastore.onErasePartition(partition);
    }

    public long getImageJournalId() {
        return imageJournalId;
    }

    public void setImageJournalId(long imageJournalId) {
        this.imageJournalId = imageJournalId;
    }

    public void clearExpiredJobs() {
        try {
            loadManager.removeOldLoadJob();
        } catch (Throwable t) {
            LOG.warn("load manager remove old load jobs failed", t);
        }
        try {
            exportMgr.removeOldExportJobs();
        } catch (Throwable t) {
            LOG.warn("export manager remove old export jobs failed", t);
        }
        try {
            deleteHandler.removeOldDeleteInfo();
        } catch (Throwable t) {
            LOG.warn("delete handler remove old delete info failed", t);
        }
        try {
            globalTransactionMgr.removeExpiredTxns();
        } catch (Throwable t) {
            LOG.warn("transaction manager remove expired txns failed", t);
        }
        try {
            routineLoadManager.cleanOldRoutineLoadJobs();
        } catch (Throwable t) {
            LOG.warn("routine load manager clean old routine load jobs failed", t);
        }
        try {
            backupHandler.removeOldJobs();
        } catch (Throwable t) {
            LOG.warn("backup handler clean old jobs failed", t);
        }
        try {
            streamLoadManager.cleanOldStreamLoadTasks();
        } catch (Throwable t) {
            LOG.warn("delete handler remove old delete info failed", t);
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
}
