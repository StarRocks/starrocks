// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.sleepycat.je.rep.InsufficientLogException;
import com.starrocks.StarRocksFE;
import com.starrocks.alter.Alter;
import com.starrocks.alter.AlterJob;
import com.starrocks.alter.AlterJob.JobType;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseQuotaStmt.QuotaType;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.RollupRenameClause;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.backup.BackupHandler;
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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MetaReplayState;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.StarOSAgent;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.catalog.View;
import com.starrocks.catalog.WorkGroupMgr;
import com.starrocks.clone.ColocateTableBalancer;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.clone.TabletChecker;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.cluster.BaseParam;
import com.starrocks.cluster.Cluster;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.consistency.ConsistencyChecker;
import com.starrocks.external.elasticsearch.EsRepository;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.events.MetastoreEventsProcessor;
import com.starrocks.external.iceberg.IcebergRepository;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.ha.MasterInfo;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.ExportChecker;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.Load;
import com.starrocks.load.loadv2.LoadEtlChecker;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadLoadingChecker;
import com.starrocks.load.loadv2.LoadManager;
import com.starrocks.load.loadv2.LoadTimeoutChecker;
import com.starrocks.load.routineload.RoutineLoadManager;
import com.starrocks.load.routineload.RoutineLoadScheduler;
import com.starrocks.load.routineload.RoutineLoadTaskScheduler;
import com.starrocks.master.Checkpoint;
import com.starrocks.meta.MetaContext;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropLinkDbAndUpdateDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.Storage;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TablePropertyInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.JournalObservable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.AnalyzeManager;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.MasterTaskExecutor;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
import org.apache.commons.collections.CollectionUtils;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalStateMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalStateMgr.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int STATE_CHANGE_CHECK_INTERVAL_MS = 100;
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final String BDB_DIR = "/bdb";
    private static final String IMAGE_DIR = "/image";

    private String metaDir;
    private String bdbDir;
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
    private ExportMgr exportMgr;
    private Alter alter;
    private ConsistencyChecker consistencyChecker;
    private BackupHandler backupHandler;
    private PublishVersionDaemon publishVersionDaemon;
    private DeleteHandler deleteHandler;
    private UpdateDbUsedDataQuotaDaemon updateDbUsedDataQuotaDaemon;

    private MasterDaemon labelCleaner; // To clean old LabelInfo, ExportJobInfos
    private MasterDaemon txnTimeoutChecker; // To abort timeout txns
    private MasterDaemon taskCleaner; // To clean expire Task/TaskRun
    private Daemon replayer;
    private Daemon timePrinter;
    private Daemon listener;
    private EsRepository esRepository;  // it is a daemon, so add it here
    private HiveRepository hiveRepository;
    private IcebergRepository icebergRepository;
    private MetastoreEventsProcessor metastoreEventsProcessor;

    // set to true after finished replay all meta and ready to serve
    // set to false when globalStateMgr is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);
    private BlockingQueue<FrontendNodeType> typeTransferQueue;

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;

    private FrontendNodeType feType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;

    private CatalogIdGenerator idGenerator = new CatalogIdGenerator(NEXT_ID_INIT_VALUE);

    private EditLog editLog;
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

    private DomainResolver domainResolver;

    private TabletSchedulerStat stat;

    private TabletScheduler tabletScheduler;

    private TabletChecker tabletChecker;

    // Thread pools for pending and loading task, separately
    private MasterTaskExecutor pendingLoadTaskScheduler;
    private MasterTaskExecutor loadingLoadTaskScheduler;

    private LoadJobScheduler loadJobScheduler;

    private LoadTimeoutChecker loadTimeoutChecker;
    private LoadEtlChecker loadEtlChecker;
    private LoadLoadingChecker loadLoadingChecker;

    private RoutineLoadScheduler routineLoadScheduler;

    private RoutineLoadTaskScheduler routineLoadTaskScheduler;

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

    private WorkGroupMgr workGroupMgr;

    private StarOSAgent starOSAgent;

    private MetadataMgr metadataMgr;
    private CatalogMgr catalogMgr;
    private ConnectorMgr connectorMgr;

    private TaskManager taskManager;

    private LocalMetastore localMetastore;
    private NodeMgr nodeMgr;

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

    private static class SingletonHolder {
        private static final GlobalStateMgr INSTANCE = new GlobalStateMgr();
    }

    private GlobalStateMgr() {
        this(false);
    }

    // if isCheckpointCatalog is true, it means that we should not collect thread pool metric
    private GlobalStateMgr(boolean isCheckpointCatalog) {
        this.load = new Load();
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
        this.typeTransferQueue = Queues.newLinkedBlockingDeque();

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

        this.auth = new Auth();
        this.domainResolver = new DomainResolver(auth);

        this.workGroupMgr = new WorkGroupMgr(this);

        this.esRepository = new EsRepository();
        this.hiveRepository = new HiveRepository();
        this.icebergRepository = new IcebergRepository();
        this.metastoreEventsProcessor = new MetastoreEventsProcessor(hiveRepository);

        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();
        this.nodeMgr = new NodeMgr(isCheckpointCatalog, this);
        this.tabletScheduler = new TabletScheduler(this, nodeMgr.getClusterInfo(), tabletInvertedIndex, stat);
        this.tabletChecker = new TabletChecker(this, nodeMgr.getClusterInfo(), tabletScheduler, stat);

        this.pendingLoadTaskScheduler =
                new MasterTaskExecutor("pending_load_task_scheduler", Config.async_load_task_pool_size,
                        Config.desired_max_waiting_jobs, !isCheckpointCatalog);
        // One load job will be split into multiple loading tasks, the queue size is not determined, so set Integer.MAX_VALUE.
        this.loadingLoadTaskScheduler =
                new MasterTaskExecutor("loading_load_task_scheduler", Config.async_load_task_pool_size,
                        Integer.MAX_VALUE, !isCheckpointCatalog);
        this.loadJobScheduler = new LoadJobScheduler();
        this.loadManager = new LoadManager(loadJobScheduler);
        this.loadTimeoutChecker = new LoadTimeoutChecker(loadManager);
        this.loadEtlChecker = new LoadEtlChecker(loadManager);
        this.loadLoadingChecker = new LoadLoadingChecker(loadManager);
        this.routineLoadScheduler = new RoutineLoadScheduler(routineLoadManager);
        this.routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);

        this.smallFileMgr = new SmallFileMgr();

        this.dynamicPartitionScheduler = new DynamicPartitionScheduler("DynamicPartitionScheduler",
                Config.dynamic_partition_check_interval_seconds * 1000L);

        setMetaDir();

        this.pluginMgr = new PluginMgr();
        this.auditEventProcessor = new AuditEventProcessor(this.pluginMgr);
        this.analyzeManager = new AnalyzeManager();

        this.starOSAgent = new StarOSAgent();
        this.localMetastore = new LocalMetastore(this, recycleBin, colocateTableIndex, nodeMgr.getClusterInfo());
        this.metadataMgr = new MetadataMgr(localMetastore);
        this.connectorMgr = new ConnectorMgr(metadataMgr);
        this.catalogMgr = new CatalogMgr(connectorMgr);
        this.taskManager = new TaskManager();
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public static GlobalStateMgr getCurrentState() {
        if (isCheckpointThread()) {
            // only checkpoint thread it self will goes here.
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

    public WorkGroupMgr getWorkGroupMgr() {
        return workGroupMgr;
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

    public LocalMetastore getLocalMetastore() {
        return localMetastore;
    }

    @VisibleForTesting
    public void setMetadataMgr(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
    }

    public TaskManager getTaskManager() {
        return taskManager;
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

    public String getBdbDir() {
        return bdbDir;
    }

    public String getImageDir() {
        return imageDir;
    }

    private void setMetaDir() {
        this.metaDir = Config.meta_dir;
        this.bdbDir = this.metaDir + BDB_DIR;
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

        // 1. check and create dirs and files
        //      if metaDir is the default config: StarRocksFE.STARROCKS_HOME_DIR + "/meta",
        //      we should check whether both the new default dir (STARROCKS_HOME_DIR + "/meta")
        //      and the old default dir (DORIS_HOME_DIR + "/doris-meta") are present. If both are present,
        //      we need to let users keep only one to avoid starting from outdated metadata.
        String oldDefaultMetaDir = System.getenv("DORIS_HOME") + "/doris-meta";
        String newDefaultMetaDir = StarRocksFE.STARROCKS_HOME_DIR + "/meta";
        if (metaDir.equals(newDefaultMetaDir)) {
            File oldMeta = new File(oldDefaultMetaDir);
            File newMeta = new File(newDefaultMetaDir);
            if (oldMeta.exists() && newMeta.exists()) {
                LOG.error("New default meta dir: {} and Old default meta dir: {} are both present. " +
                                "Please make sure {} has the latest data, and remove the another one.",
                        newDefaultMetaDir, oldDefaultMetaDir, newDefaultMetaDir);
                System.exit(-1);
            }
        }

        File meta = new File(metaDir);
        if (!meta.exists()) {
            // If metaDir is not the default config, it means the user has specified the other directory
            // We should not use the oldDefaultMetaDir.
            // Just exit in this case
            if (!metaDir.equals(newDefaultMetaDir)) {
                LOG.error("meta dir {} dose not exist, will exit", metaDir);
                System.exit(-1);
            }
            File oldMeta = new File(oldDefaultMetaDir);
            if (oldMeta.exists()) {
                // For backward compatible
                Config.meta_dir = oldDefaultMetaDir;
                setMetaDir();
            } else {
                LOG.error("meta dir {} does not exist, will exit", meta.getAbsolutePath());
                System.exit(-1);
            }
        }

        if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
            File bdbDir = new File(this.bdbDir);
            if (!bdbDir.exists()) {
                bdbDir.mkdirs();
            }

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
        this.editLog = new EditLog(nodeMgr.getNodeName());
        loadImage(this.imageDir); // load image file

        editLog.open(); // open bdb env
        this.globalTransactionMgr.setEditLog(editLog);
        this.idGenerator.setEditLog(editLog);
        this.localMetastore.setEditLog(editLog);

        // 4. create load and export job label cleaner thread
        createLabelCleaner();

        // 5. create txn timeout checker thread
        createTxnTimeoutChecker();

        // 6. start task cleaner thread
        createTaskCleaner();

        // 7. start state listener thread
        createStateListener();
        listener.start();
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

    private void transferToMaster(FrontendNodeType oldType) {
        // stop replayer
        if (replayer != null) {
            replayer.exit();
            try {
                replayer.join();
            } catch (InterruptedException e) {
                LOG.warn("got exception when stopping the replayer thread", e);
            }
            replayer = null;
        }

        // set this after replay thread stopped. to avoid replay thread modify them.
        isReady.set(false);
        canRead.set(false);

        editLog.open();

        if (!haProtocol.fencing()) {
            LOG.error("fencing failed. will exit.");
            System.exit(-1);
        }

        long replayStartTime = System.currentTimeMillis();
        // replay journals. -1 means replay all the journals larger than current journal id.
        replayJournal(-1);
        long replayEndTime = System.currentTimeMillis();
        LOG.info("finish replay in " + (replayEndTime - replayStartTime) + " msec");

        nodeMgr.checkCurrentNodeExist();

        editLog.rollEditLog();

        // Set the feType to MASTER before writing edit log, because the feType must be Master when writing edit log.
        // It will be set to the old type if any error happens in the following procedure
        feType = FrontendNodeType.MASTER;
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

            // MUST set master ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-master FE to push image
            nodeMgr.setMasterInfo();

            // start all daemon threads that only running on MASTER FE
            startMasterOnlyDaemonThreads();
            // start other daemon threads that should running on all FE
            startNonMasterDaemonThreads();

            MetricRepo.init();

            canRead.set(true);
            isReady.set(true);

            String msg = "master finished to replay journal, can write now.";
            Util.stdoutWithTime(msg);
            LOG.info(msg);
            // for master, there are some new thread pools need to register metric
            ThreadPoolManager.registerAllThreadPoolMetric();
        } catch (Throwable t) {
            LOG.warn("transfer to master failed with error", t);
            feType = oldType;
            throw t;
        }
    }

    // start all daemon threads only running on Master
    private void startMasterOnlyDaemonThreads() {
        // start checkpoint thread
        checkpointer = new Checkpoint(editLog);
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
        // start daemon thread to update db used data quota for db txn manager periodly
        updateDbUsedDataQuotaDaemon.start();
        statisticsMetaManager.start();
        statisticAutoCollector.start();
        taskManager.start();
        taskCleaner.start();
        // register service to starMgr
        if (Config.integrate_staros) {
            int clusterId = getCurrentState().getClusterId();
            getStarOSAgent().registerAndBootstrapService(Integer.toString(clusterId));
        }
    }

    // start threads that should running on all FE
    private void startNonMasterDaemonThreads() {
        tabletStatMgr.start();
        // load and export job label cleaner thread
        labelCleaner.start();
        // ES state store
        esRepository.start();

        if (Config.enable_hms_events_incremental_sync) {
            // load hive table to event processor and start to process hms events.
            metastoreEventsProcessor.init();
            metastoreEventsProcessor.start();
        }
        // domain resolver
        domainResolver.start();
    }

    private void transferToNonMaster(FrontendNodeType newType) {
        isReady.set(false);

        if (feType == FrontendNodeType.OBSERVER || feType == FrontendNodeType.FOLLOWER) {
            Preconditions.checkState(newType == FrontendNodeType.UNKNOWN);
            LOG.warn("{} to UNKNOWN, still offer read service", feType.name());
            // not set canRead here, leave canRead as what is was.
            // if meta out of date, canRead will be set to false in replayer thread.
            metaReplayState.setTransferToUnknown();
            // get serviceId from starMgr
            if (Config.integrate_staros) {
                int clusterId = getCurrentState().getClusterId();
                getStarOSAgent().getServiceId(Integer.toString(clusterId));
            }
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

        startNonMasterDaemonThreads();

        MetricRepo.init();
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
            checksum = loadHeader(dis, checksum);
            checksum = nodeMgr.loadMasterInfo(dis, checksum);
            checksum = nodeMgr.loadFrontends(dis, checksum);
            checksum = nodeMgr.loadBackends(dis, checksum);
            checksum = localMetastore.loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            localMetastore.recreateTabletInvertIndex();
            // rebuild es state state
            esRepository.loadTableFromCatalog();

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
            checksum = workGroupMgr.loadWorkGroups(dis, checksum);
            checksum = auth.readAsGson(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = taskManager.loadTasks(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = catalogMgr.loadCatalogs(dis, checksum);
            remoteChecksum = dis.readLong();
        } catch (EOFException exception) {
            LOG.warn("load image eof.", exception);
        } finally {
            dis.close();
        }

        Preconditions.checkState(remoteChecksum == checksum, remoteChecksum + " vs. " + checksum);

        long loadImageEndTime = System.currentTimeMillis();
        this.imageJournalId = storage.getImageJournalId();
        LOG.info("finished to load image in " + (loadImageEndTime - loadImageStartTime) + " ms");
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
        for (JobType type : JobType.values()) {
            if (type == JobType.DECOMMISSION_BACKEND) {
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

    public long loadAlterJob(DataInputStream dis, long checksum, JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = null;
        Map<Long, AlterJobV2> alterJobsV2 = Maps.newHashMap();
        if (type == JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        } else if (type == JobType.DECOMMISSION_BACKEND) {
            alterJobs = this.getClusterHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getClusterHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        }

        // alter jobs
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        for (int i = 0; i < size; i++) {
            long tableId = dis.readLong();
            newChecksum ^= tableId;
            AlterJob job = AlterJob.read(dis);
            alterJobs.put(tableId, job);

            // init job
            Database db = getDb(job.getDbId());
            // should check job state here because the job is finished but not removed from alter jobs list
            if (db != null && (job.getState() == com.starrocks.alter.AlterJob.JobState.PENDING
                    || job.getState() == com.starrocks.alter.AlterJob.JobState.RUNNING)) {
                job.replayInitJob(db);
            }
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= 2) {
            // finished or cancelled jobs
            long currentTimeMs = System.currentTimeMillis();
            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                long tableId = dis.readLong();
                newChecksum ^= tableId;
                AlterJob job = AlterJob.read(dis);
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 <= Config.history_job_keep_max_second) {
                    // delete history jobs
                    finishedOrCancelledAlterJobs.add(job);
                }
            }
        }

        // alter job v2
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_61) {
            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                AlterJobV2 alterJobV2 = AlterJobV2.read(dis);
                if (type == JobType.ROLLUP || type == JobType.SCHEMA_CHANGE) {
                    if (type == JobType.ROLLUP) {
                        this.getRollupHandler().addAlterJobV2(alterJobV2);
                    } else {
                        alterJobsV2.put(alterJobV2.getJobId(), alterJobV2);
                    }
                    // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpointed
                    // to prevent TabletInvertedIndex data loss,
                    // So just use AlterJob.replay() instead of AlterHandler.replay().
                    if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                        alterJobV2.replay(alterJobV2);
                        LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
                    }
                } else {
                    alterJobsV2.put(alterJobV2.getJobId(), alterJobV2);
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

    public long loadResources(DataInputStream in, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_87) {
            resourceMgr = ResourceMgr.read(in);
        }
        LOG.info("finished replay resources from image");
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
            curFile.delete();
            throw new IOException();
        }
    }

    public void saveImage(File curFile, long replayedJournalId) throws IOException {
        if (!curFile.exists()) {
            curFile.createNewFile();
        }

        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), GlobalStateMgr.isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(curFile))) {
            checksum = saveHeader(dos, replayedJournalId, checksum);
            checksum = nodeMgr.saveMasterInfo(dos, checksum);
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
            checksum = workGroupMgr.saveWorkGroups(dos, checksum);
            checksum = auth.writeAsGson(dos, checksum);
            dos.writeLong(checksum);
            checksum = taskManager.saveTasks(dos, checksum);
            dos.writeLong(checksum);
            checksum = catalogMgr.saveCatalogs(dos, checksum);
            dos.writeLong(checksum);
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
        for (JobType type : JobType.values()) {
            checksum = saveAlterJob(dos, checksum, type);
        }
        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum, JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = null;
        Map<Long, AlterJobV2> alterJobsV2 = Maps.newHashMap();
        if (type == JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getRollupHandler().getAlterJobsV2();
        } else if (type == JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        } else if (type == JobType.DECOMMISSION_BACKEND) {
            alterJobs = this.getClusterHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getClusterHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        }

        // alter jobs
        int size = alterJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (Entry<Long, AlterJob> entry : alterJobs.entrySet()) {
            long tableId = entry.getKey();
            checksum ^= tableId;
            dos.writeLong(tableId);
            entry.getValue().write(dos);
        }

        // finished or cancelled jobs
        size = finishedOrCancelledAlterJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
            long tableId = alterJob.getTableId();
            checksum ^= tableId;
            dos.writeLong(tableId);
            alterJob.write(dos);
        }

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
        labelCleaner = new MasterDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                clearExpiredJobs();
            }
        };
    }

    public void createTaskCleaner() {
        taskCleaner = new MasterDaemon("TaskCleaner", Config.task_check_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                doTaskBackgroundJob();
            }
        };
    }

    public void createTxnTimeoutChecker() {
        txnTimeoutChecker = new MasterDaemon("txnTimeoutChecker", Config.transaction_clean_interval_second) {
            @Override
            protected void runAfterCatalogReady() {
                globalTransactionMgr.abortTimeoutTxns();
            }
        };
    }

    public void createReplayer() {
        replayer = new Daemon("replayer", REPLAY_INTERVAL_MS) {
            @Override
            protected void runOneCycle() {
                boolean err = false;
                boolean hasLog = false;
                try {
                    hasLog = replayJournal(-1);
                    metaReplayState.setOk();
                } catch (InsufficientLogException insufficientLogEx) {
                    // for InsufficientLogException we should refresh the log and
                    // then exit the process because we may have read dirty data.
                    LOG.error("catch insufficient log exception. please restart", insufficientLogEx);
                    ((BDBJEJournal) editLog.getJournal()).getBdbEnvironment().refreshLog(insufficientLogEx);
                    System.exit(-1);
                } catch (Throwable e) {
                    LOG.error("replayer thread catch an exception when replay journal.", e);
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
        };
        replayer.setMetaContext(metaContext);
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
            // we still need this log to observe this situation
            // but service may be continued when there is no log being replayed.
            LOG.warn("meta out of date. current time: {}, synchronized time: {}, has log: {}, fe type: {}",
                    currentTimeMs, synchronizedTimeMs, hasLog, feType);
            if (hasLog || feType == FrontendNodeType.UNKNOWN) {
                // 1. if we read log from BDB, which means master is still alive.
                // So we need to set meta out of date.
                // 2. if we didn't read any log from BDB and feType is UNKNOWN,
                // which means this non-master node is disconnected with master.
                // So we need to set meta out of date either.
                metaReplayState.setOutOfDate(currentTimeMs, synchronizedTimeMs);
                canRead.set(false);
                isReady.set(false);
            }

            // sleep 5s to avoid numerous 'meta out of date' log
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                LOG.error("unhandled exception when sleep", e);
            }

        } else {
            canRead.set(true);
            isReady.set(true);
        }
    }

    public void notifyNewFETypeTransfer(FrontendNodeType newType) {
        try {
            String msg = "notify new FE type transfer: " + newType;
            LOG.warn(msg);
            Util.stdoutWithTime(msg);
            this.typeTransferQueue.put(newType);
        } catch (InterruptedException e) {
            LOG.error("failed to put new FE type: {}", newType, e);
        }
    }

    public void createStateListener() {
        listener = new Daemon("stateListener", STATE_CHANGE_CHECK_INTERVAL_MS) {
            @Override
            protected synchronized void runOneCycle() {

                while (true) {
                    FrontendNodeType newType = null;
                    try {
                        newType = typeTransferQueue.take();
                    } catch (InterruptedException e) {
                        LOG.error("got exception when take FE type from queue", e);
                        Util.stdoutWithTime("got exception when take FE type from queue. " + e.getMessage());
                        System.exit(-1);
                    }
                    Preconditions.checkNotNull(newType);
                    LOG.info("begin to transfer FE type from {} to {}", feType, newType);
                    if (feType == newType) {
                        return;
                    }

                    /*
                     * INIT -> MASTER: transferToMaster
                     * INIT -> FOLLOWER/OBSERVER: transferToNonMaster
                     * UNKNOWN -> MASTER: transferToMaster
                     * UNKNOWN -> FOLLOWER/OBSERVER: transferToNonMaster
                     * FOLLOWER -> MASTER: transferToMaster
                     * FOLLOWER/OBSERVER -> INIT/UNKNOWN: set isReady to false
                     */
                    switch (feType) {
                        case INIT: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster(feType);
                                    break;
                                }
                                case FOLLOWER:
                                case OBSERVER: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                case UNKNOWN:
                                    break;
                                default:
                                    break;
                            }
                            break;
                        }
                        case UNKNOWN: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster(feType);
                                    break;
                                }
                                case FOLLOWER:
                                case OBSERVER: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
                            }
                            break;
                        }
                        case FOLLOWER: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster(feType);
                                    break;
                                }
                                case UNKNOWN: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
                            }
                            break;
                        }
                        case OBSERVER: {
                            if (newType == FrontendNodeType.UNKNOWN) {
                                transferToNonMaster(newType);
                            }
                            break;
                        }
                        case MASTER: {
                            // exit if master changed to any other type
                            String msg = "transfer FE type from MASTER to " + newType.name() + ". exit";
                            LOG.error(msg);
                            Util.stdoutWithTime(msg);
                            System.exit(-1);
                        }
                        default:
                            break;
                    } // end switch formerFeType

                    feType = newType;
                    LOG.info("finished to transfer FE type to {}", feType);
                }
            } // end runOneCycle
        };

        listener.setMetaContext(metaContext);
    }

    public synchronized boolean replayJournal(long toJournalId) {
        long newToJournalId = toJournalId;
        if (newToJournalId == -1) {
            newToJournalId = getMaxJournalId();
        }
        if (newToJournalId <= replayedJournalId.get()) {
            return false;
        }

        LOG.info("replayed journal id is {}, replay to journal id is {}", replayedJournalId, newToJournalId);
        JournalCursor cursor = editLog.read(replayedJournalId.get() + 1, newToJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", replayedJournalId.get() + 1, newToJournalId);
            return false;
        }

        long startTime = System.currentTimeMillis();
        boolean hasLog = false;
        while (true) {
            JournalEntity entity = cursor.next();
            if (entity == null) {
                break;
            }
            hasLog = true;
            EditLog.loadJournal(this, entity);
            replayedJournalId.incrementAndGet();
            LOG.debug("journal {} replayed.", replayedJournalId);
            if (feType != FrontendNodeType.MASTER) {
                journalObservable.notifyObservers(replayedJournalId.get());
            }
            if (MetricRepo.isInit) {
                // Metric repo may not init after this replay thread start
                MetricRepo.COUNTER_EDIT_LOG_READ.increase(1L);
            }
        }
        long cost = System.currentTimeMillis() - startTime;
        if (cost >= 1000) {
            LOG.warn("replay journal cost too much time: {} replayedJournalId: {}", cost, replayedJournalId);
        }

        return hasLog;
    }

    public void createTimePrinter() {
        // time printer will write timestamp edit log every 10 seconds
        timePrinter = new MasterDaemon("timePrinter", 10 * 1000L) {
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

    // The interface which DdlExecutor needs.
    public void createDb(CreateDbStmt stmt) throws DdlException {
        localMetastore.createDb(stmt);
    }

    // For replay edit log, needn't lock metadata
    public void unprotectCreateDb(Database db) {
        localMetastore.unprotectCreateDb(db);
    }

    // for test
    public void addCluster(Cluster cluster) {
        localMetastore.addCluster(cluster);
    }

    public void replayCreateDb(Database db) {
        localMetastore.replayCreateDb(db);
    }

    public void dropDb(DropDbStmt stmt) throws DdlException {
        localMetastore.dropDb(stmt);
    }

    public void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        localMetastore.replayDropLinkDb(info);
    }

    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        localMetastore.replayDropDb(dbName, isForceDrop);
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

    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        localMetastore.renameDatabase(stmt);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        localMetastore.replayRenameDatabase(dbName, newDbName);
    }

    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return localMetastore.createTable(stmt);
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
        StringBuilder sb = new StringBuilder();

        // 1. create table
        // 1.1 view
        if (table.getType() == TableType.VIEW) {
            View view = (View) table;
            sb.append("CREATE VIEW `").append(table.getName()).append("` (");
            List<String> colDef = Lists.newArrayList();
            for (Column column : table.getBaseSchema()) {
                StringBuilder colSb = new StringBuilder();
                colSb.append(column.getName());
                if (!Strings.isNullOrEmpty(column.getComment())) {
                    colSb.append(" COMMENT ").append("\"").append(column.getDisplayComment()).append("\"");
                }
                colDef.add(colSb.toString());
            }
            sb.append(Joiner.on(", ").join(colDef));
            sb.append(")");
            addTableComment(sb, view);

            sb.append(" AS ").append(view.getInlineViewDef()).append(";");
            createTableStmt.add(sb.toString());
            return;
        }

        // 1.2 other table type
        sb.append("CREATE ");
        if (table.getType() == TableType.MYSQL || table.getType() == TableType.ELASTICSEARCH
                || table.getType() == TableType.BROKER || table.getType() == TableType.HIVE
                || table.getType() == TableType.HUDI || table.getType() == TableType.ICEBERG
                || table.getType() == TableType.OLAP_EXTERNAL || table.getType() == TableType.JDBC) {
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
            // sqlalchemy requires this to parse SHOW CREATE TAEBL stmt.
            if (table.getType() == TableType.OLAP || table.getType() == TableType.OLAP_EXTERNAL) {
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
        if (table.getType() == TableType.OLAP || table.getType() == TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }
        sb.append("\n) ENGINE=");
        sb.append(table.getType().name()).append(" ");

        if (table.getType() == TableType.OLAP || table.getType() == TableType.OLAP_EXTERNAL) {
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

            addTableComment(sb, table);

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (partitionInfo.getType() == PartitionType.RANGE
                    || partitionInfo.getType() == PartitionType.LIST) {
                sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));
            }

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // properties
            sb.append("\nPROPERTIES (\n");

            // replicationNum
            Short replicationNum = olapTable.getDefaultReplicationNum();
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
            sb.append(replicationNum).append("\"");

            // bloom filter
            Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
            if (bfColumnNames != null) {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS).append("\" = \"");
                sb.append(Joiner.on(", ").join(olapTable.getCopiedBfColumns())).append("\"");
            }

            if (separatePartition) {
                // version info
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VERSION_INFO).append("\" = \"");
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
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH).append("\" = \"");
                sb.append(colocateTable).append("\"");
            }

            // dynamic partition
            if (olapTable.dynamicPartitionExists()) {
                sb.append(olapTable.getTableProperty().getDynamicPartitionProperty().toString());
            }

            // in memory
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_INMEMORY).append("\" = \"");
            sb.append(olapTable.isInMemory()).append("\"");

            // storage type
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).append("\" = \"");
            sb.append(olapTable.getStorageFormat()).append("\"");

            // enable_persistent_index
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).append("\" = \"");
            sb.append(olapTable.enablePersistentIndex()).append("\"");

            // storage media
            Map<String, String> properties = olapTable.getTableProperty().getProperties();
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                sb.append("\n");
            } else {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM).append("\" = \"");
                sb.append(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)).append("\"");
                sb.append("\n");
            }

            if (table.getType() == TableType.OLAP_EXTERNAL) {
                ExternalOlapTable externalOlapTable = (ExternalOlapTable) table;
                // properties
                sb.append("\"host\" = \"").append(externalOlapTable.getSourceTableHost()).append("\",\n");
                sb.append("\"port\" = \"").append(externalOlapTable.getSourceTablePort()).append("\",\n");
                sb.append("\"user\" = \"").append(externalOlapTable.getSourceTableUser()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : externalOlapTable.getSourceTablePassword())
                        .append("\",\n");
                sb.append("\"database\" = \"").append(externalOlapTable.getSourceTableDbName()).append("\",\n");
                sb.append("\"table\" = \"").append(externalOlapTable.getSourceTableName()).append("\"\n");
            }
            sb.append(")");
        } else if (table.getType() == TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;
            addTableComment(sb, table);

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
            addTableComment(sb, table);

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
            addTableComment(sb, table);

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
            sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            sb.append("\"transport\" = \"").append(esTable.getTransport()).append("\",\n");
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isDocValueScanEnable()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.maxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isKeywordSniffEnable()).append("\",\n");
            sb.append("\"es.nodes.wan.only\" = \"").append(esTable.wanOnly()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;
            addTableComment(sb, table);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getHiveDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getTableName()).append("\",\n");
            sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\",\n");
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.HUDI) {
            HudiTable hudiTable = (HudiTable) table;
            addTableComment(sb, table);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hudiTable.getDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hudiTable.getTable()).append("\",\n");
            sb.append("\"resource\" = \"").append(hudiTable.getResourceName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.ICEBERG) {
            IcebergTable icebergTable = (IcebergTable) table;
            addTableComment(sb, table);

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
            addTableComment(sb, table);

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

    private static void addTableComment(StringBuilder sb, Table table) {
        if (!Strings.isNullOrEmpty(table.getComment())) {
            sb.append("\nCOMMENT \"").append(table.getDisplayComment()).append("\"");
        }
    }

    public void replayCreateTable(String dbName, Table table) {
        localMetastore.replayCreateTable(dbName, table);
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

    // Get the next available, need't lock because of nextId is atomic.
    public long getNextId() {
        return idGenerator.getNextId();
    }

    public List<String> getDbNames() {
        return localMetastore.listDbNames();
    }

    public List<String> getClusterDbNames(String clusterName) throws AnalysisException {
        return localMetastore.getClusterDbNames(clusterName);
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

    public MasterTaskExecutor getPendingLoadTaskScheduler() {
        return pendingLoadTaskScheduler;
    }

    public MasterTaskExecutor getLoadingLoadTaskScheduler() {
        return loadingLoadTaskScheduler;
    }

    public RoutineLoadManager getRoutineLoadManager() {
        return routineLoadManager;
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
        return this.editLog.getMaxJournalId();
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

    public Pair<String, Integer> getLeaderIpAndRpcPort() {
        return nodeMgr.getLeaderIpAndRpcPort();
    }

    public Pair<String, Integer> getLeaderIpAndHttpPort() {
        return nodeMgr.getLeaderIpAndHttpPort();
    }

    public String getMasterIp() {
        return nodeMgr.getMasterIp();
    }

    public EsRepository getEsRepository() {
        return this.esRepository;
    }

    public HiveRepository getHiveRepository() {
        return this.hiveRepository;
    }

    public IcebergRepository getIcebergRepository() {
        return this.icebergRepository;
    }

    public MetastoreEventsProcessor getMetastoreEventsProcessor() {
        return this.metastoreEventsProcessor;
    }

    public void setMaster(MasterInfo info) {
        nodeMgr.setMaster(info);
    }

    public boolean canRead() {
        return this.canRead.get();
    }

    public boolean isElectable() {
        return nodeMgr.isElectable();
    }

    public boolean isMaster() {
        return feType == FrontendNodeType.MASTER;
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
        localMetastore.setEditLog(editLog);
    }

    public void setNextId(long id) {
        idGenerator.setId(id);
    }

    public void setHaProtocol(HAProtocol protocol) {
        this.haProtocol = protocol;
    }

    public static short calcShortKeyColumnCount(List<Column> columns, Map<String, String> properties)
            throws DdlException {
        List<Column> indexColumns = new ArrayList<Column>();
        for (Column column : columns) {
            if (column.isKey()) {
                indexColumns.add(column);
            }
        }
        LOG.debug("index column size: {}", indexColumns.size());
        Preconditions.checkArgument(indexColumns.size() > 0);

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

            if (shortKeyColumnCount > indexColumns.size()) {
                throw new DdlException("Short key is too large. should less than: " + indexColumns.size());
            }

            for (int pos = 0; pos < shortKeyColumnCount; pos++) {
                if (indexColumns.get(pos).getPrimitiveType() == PrimitiveType.VARCHAR &&
                        pos != shortKeyColumnCount - 1) {
                    throw new DdlException("Varchar should not in the middle of short keys.");
                }
            }
        } else {
            /*
             * Calc short key column count. NOTE: short key column count is
             * calculated as follow: 1. All index column are taking into
             * account. 2. Max short key column count is Min(Num of
             * indexColumns, META_MAX_SHORT_KEY_NUM). 3. Short key list can
             * contains at most one VARCHAR column. And if contains, it should
             * be at the last position of the short key list.
             */
            shortKeyColumnCount = 0;
            int shortKeySizeByte = 0;
            int maxShortKeyColumnCount = Math.min(indexColumns.size(), FeConstants.shortkey_max_column_count);
            for (int i = 0; i < maxShortKeyColumnCount; i++) {
                Column column = indexColumns.get(i);
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

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
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

    public void replayRenameColumn(TableInfo tableInfo) throws DdlException {
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
    public void alterCluster(AlterSystemStmt stmt) throws UserException {
        this.alter.processAlterCluster(stmt);
    }

    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        this.alter.getClusterHandler().cancel(stmt);
    }

    // Change current catalog and database of this session.
    // We can support 'USE CATALOG.DB'
    public void changeCatalogDb(ConnectContext ctx, String identifier) throws DdlException {
        String currentCatalogName = ctx.getCurrentCatalog();
        String dbName = ctx.getDatabase();

        String[] parts = identifier.split("\\.");
        if (parts.length != 1 && parts.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
        } else if (parts.length == 1) {
            dbName = catalogMgr.isInternalCatalog(currentCatalogName) ?
                    ClusterNamespace.getFullName(ctx.getClusterName(), identifier) : identifier;
        } else {
            String newCatalogName = parts[0];
            if (catalogMgr.catalogExists(newCatalogName)) {
                dbName = catalogMgr.isInternalCatalog(newCatalogName) ?
                        ClusterNamespace.getFullName(ctx.getClusterName(), parts[1]) : parts[1];
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
            }
            ctx.setCurrentCatalog(newCatalogName);
        }

        // Check auth for internal catalog.
        // Here we check the request permission that sent by the mysql client or jdbc.
        // So we didn't check UseStmt permission in PrivilegeChecker.
        if (CatalogMgr.isInternalCatalog(ctx.getCurrentCatalog()) &&
                !auth.checkDbPriv(ctx, dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    ctx.getQualifiedUser(), dbName);
        }

        if (metadataMgr.getDb(ctx.getCurrentCatalog(), dbName) == null) {
            LOG.debug("Unknown catalog '%s' and db '%s'", ctx.getCurrentCatalog(), dbName);
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
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

    public void changeCluster(ConnectContext ctx, String clusterName) throws DdlException {
        localMetastore.changeCluster(ctx, clusterName);
    }

    public Cluster getCluster(String clusterName) {
        return localMetastore.getCluster(clusterName);
    }

    public List<String> getClusterNames() {
        return localMetastore.getClusterNames();
    }

    public Set<BaseParam> getMigrations() {
        return localMetastore.getMigrations();
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

    public void refreshExternalTable(TableName tableName, List<String> partitions) throws DdlException {
        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        Database db = metadataMgr.getDb(catalogName, tableName.getDb());
        if (db == null) {
            throw new DdlException("db: " + tableName.getDb() + " not exists");
        }
        HiveMetaStoreTable table;
        db.readLock();
        try {
            Table tbl = metadataMgr.getTable(catalogName, dbName, tblName);
            if (tbl == null || !(tbl instanceof HiveMetaStoreTable)) {
                throw new DdlException("table : " + tableName + " not exists, or is not hive/hudi external table");
            }
            table = (HiveMetaStoreTable) tbl;
        } finally {
            db.readUnlock();
        }

        if (partitions != null && partitions.size() > 0) {
            table.refreshPartCache(partitions);
        } else {
            table.refreshTableCache(dbName, tblName);
        }
    }

    public void initDefaultCluster() {
        localMetastore.initDefaultCluster();
    }

    public void replayUpdateDb(DatabaseInfo info) {
        localMetastore.replayUpdateDb(info);
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

        LOG.info("finished dumpping image to {}", dumpFilePath);
        return dumpFilePath;
    }

    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        localMetastore.truncateTable(truncateTableStmt);
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        localMetastore.replayTruncateTable(info);
    }

    public void createFunction(CreateFunctionStmt stmt) throws UserException {
        FunctionName name = stmt.getFunctionName();
        Database db = getDb(name.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
        }
        db.addFunction(stmt.getFunction());
    }

    public void replayCreateFunction(Function function) {
        String dbName = function.getFunctionName().getDb();
        Database db = getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayAddFunction(function);
    }

    public void dropFunction(DropFunctionStmt stmt) throws UserException {
        FunctionName name = stmt.getFunctionName();
        Database db = getDb(name.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
        }
        db.dropFunction(stmt.getFunction());
    }

    public void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        String dbName = functionSearchDesc.getName().getDb();
        Database db = getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayDropFunction(functionSearchDesc);
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

    public HashMap<Long, AgentBatchTask> onEraseOlapTable(OlapTable olapTable, boolean isReplay) {
        return localMetastore.onEraseOlapTable(olapTable, isReplay);
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
}
