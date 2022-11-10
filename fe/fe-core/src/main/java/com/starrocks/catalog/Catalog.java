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

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.sleepycat.je.rep.InsufficientLogException;
import com.starrocks.StarRocksFE;
import com.starrocks.alter.Alter;
import com.starrocks.alter.AlterJob;
import com.starrocks.alter.AlterJob.JobType;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.DecommissionBackendJob.DecommissionType;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminCheckTabletsStmt.CheckType;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterClusterStmt;
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
import com.starrocks.analysis.CreateClusterStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DecommissionBackendClause;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropClusterStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.MultiRangePartitionDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.RefreshExternalTableStmt;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.RollupRenameClause;
import com.starrocks.analysis.ShowAlterStmt.AlterType;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.backup.BackupHandler;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database.DbState;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.catalog.Table.TableType;
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
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.consistency.ConsistencyChecker;
import com.starrocks.external.elasticsearch.EsRepository;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.events.MetastoreEventsProcessor;
import com.starrocks.external.starrocks.StarRocksRepository;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.ha.MasterInfo;
import com.starrocks.http.meta.MetaBaseAction;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.ExportChecker;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.Load;
import com.starrocks.load.LoadErrorHub;
import com.starrocks.load.loadv2.LoadEtlChecker;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadLoadingChecker;
import com.starrocks.load.loadv2.LoadManager;
import com.starrocks.load.loadv2.LoadTimeoutChecker;
import com.starrocks.load.routineload.RoutineLoadManager;
import com.starrocks.load.routineload.RoutineLoadScheduler;
import com.starrocks.load.routineload.RoutineLoadTaskScheduler;
import com.starrocks.master.Checkpoint;
import com.starrocks.master.MetaHelper;
import com.starrocks.meta.MetaContext;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ClusterInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropLinkDbAndUpdateDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.Storage;
import com.starrocks.persist.StorageInfo;
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
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.AnalyzeManager;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.task.MasterTaskExecutor;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import java.util.stream.Collectors;

public class Catalog {
    private static final Logger LOG = LogManager.getLogger(Catalog.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int HTTP_TIMEOUT_SECOND = 5;
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

    private ConcurrentHashMap<Long, Database> idToDb;
    private ConcurrentHashMap<String, Database> fullNameToDb;

    private ConcurrentHashMap<Long, Cluster> idToCluster;
    private ConcurrentHashMap<String, Cluster> nameToCluster;

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
    private Daemon replayer;
    private Daemon timePrinter;
    private Daemon listener;
    private EsRepository esRepository;  // it is a daemon, so add it here
    private StarRocksRepository starRocksRepository;
    private HiveRepository hiveRepository;
    private MetastoreEventsProcessor metastoreEventsProcessor;

    private boolean isFirstTimeStartUp = false;
    private boolean isElectable;
    // set to true after finished replay all meta and ready to serve
    // set to false when catalog is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);
    private BlockingQueue<FrontendNodeType> typeTransferQueue;

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;

    // node name is used for bdbje NodeName.
    private String nodeName;
    private FrontendNodeType role;
    private FrontendNodeType feType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;
    private int masterRpcPort;
    private int masterHttpPort;
    private String masterIp;

    private CatalogIdGenerator idGenerator = new CatalogIdGenerator(NEXT_ID_INIT_VALUE);

    private EditLog editLog;
    private int clusterId;
    private String token;
    // For checkpoint and observer memory replayed marker
    private AtomicLong replayedJournalId;

    private static Catalog CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;
    private List<Pair<String, Integer>> helperNodes = Lists.newArrayList();
    private Pair<String, Integer> selfNode = null;

    // node name -> Frontend
    private ConcurrentHashMap<String, Frontend> frontends;
    // removed frontends' name. used for checking if name is duplicated in bdbje
    private ConcurrentLinkedQueue<String> removedFrontends;

    private HAProtocol haProtocol = null;

    private JournalObservable journalObservable;

    private SystemInfoService systemInfo;
    private Map<Integer, SystemInfoService> systemInfoMap;
    private HeartbeatMgr heartbeatMgr;
    private TabletInvertedIndex tabletInvertedIndex;
    private ColocateTableIndex colocateTableIndex;

    private CatalogRecycleBin recycleBin;
    private FunctionSet functionSet;

    private MetaReplayState metaReplayState;

    private BrokerMgr brokerMgr;
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

    public List<Frontend> getFrontends(FrontendNodeType nodeType) {
        if (nodeType == null) {
            // get all
            return Lists.newArrayList(frontends.values());
        }

        List<Frontend> result = Lists.newArrayList();
        for (Frontend frontend : frontends.values()) {
            if (frontend.getRole() == nodeType) {
                result.add(frontend);
            }
        }

        return result;
    }

    public List<String> getRemovedFrontendNames() {
        return Lists.newArrayList(removedFrontends);
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    public SystemInfoService getOrCreateSystemInfo(Integer clusterId) {
        SystemInfoService systemInfoService = systemInfoMap.get(clusterId);
        if (systemInfoService == null) {
            systemInfoService = new SystemInfoService();
            systemInfoMap.put(clusterId, systemInfoService);
        }
        return systemInfoService;
    }

    private SystemInfoService getClusterInfo() {
        return this.systemInfo;
    }

    private HeartbeatMgr getHeartbeatMgr() {
        return this.heartbeatMgr;
    }

    public TabletInvertedIndex getTabletInvertedIndex() {
        return this.tabletInvertedIndex;
    }

    // only for test
    public void setColocateTableIndex(ColocateTableIndex colocateTableIndex) {
        this.colocateTableIndex = colocateTableIndex;
    }

    public ColocateTableIndex getColocateTableIndex() {
        return this.colocateTableIndex;
    }

    private CatalogRecycleBin getRecycleBin() {
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
        private static final Catalog INSTANCE = new Catalog();
    }

    private Catalog() {
        this(false);
    }

    // if isCheckpointCatalog is true, it means that we should not collect thread pool metric
    private Catalog(boolean isCheckpointCatalog) {
        this.idToDb = new ConcurrentHashMap<>();
        this.fullNameToDb = new ConcurrentHashMap<>();
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
        this.isElectable = false;
        this.synchronizedTimeMs = 0;
        this.feType = FrontendNodeType.INIT;
        this.typeTransferQueue = Queues.newLinkedBlockingDeque();

        this.role = FrontendNodeType.UNKNOWN;
        this.frontends = new ConcurrentHashMap<>();
        this.removedFrontends = new ConcurrentLinkedQueue<>();

        this.journalObservable = new JournalObservable();
        this.masterRpcPort = 0;
        this.masterHttpPort = 0;
        this.masterIp = "";

        this.systemInfo = new SystemInfoService();
        this.systemInfoMap = new ConcurrentHashMap<Integer, SystemInfoService>();
        this.heartbeatMgr = new HeartbeatMgr(systemInfo, !isCheckpointCatalog);
        this.tabletInvertedIndex = new TabletInvertedIndex();
        this.colocateTableIndex = new ColocateTableIndex();
        this.recycleBin = new CatalogRecycleBin();
        this.functionSet = new FunctionSet();
        this.functionSet.init();

        this.metaReplayState = new MetaReplayState();

        this.idToCluster = new ConcurrentHashMap<>();
        this.nameToCluster = new ConcurrentHashMap<>();

        this.isDefaultClusterCreated = false;

        this.brokerMgr = new BrokerMgr();
        this.resourceMgr = new ResourceMgr();

        this.globalTransactionMgr = new GlobalTransactionMgr(this);
        this.tabletStatMgr = new TabletStatMgr();

        this.auth = new Auth();
        this.domainResolver = new DomainResolver(auth);

        this.workGroupMgr = new WorkGroupMgr(this);

        this.esRepository = new EsRepository();
        this.starRocksRepository = new StarRocksRepository();
        this.hiveRepository = new HiveRepository();
        this.metastoreEventsProcessor = new MetastoreEventsProcessor(hiveRepository);

        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();
        this.tabletScheduler = new TabletScheduler(this, systemInfo, tabletInvertedIndex, stat);
        this.tabletChecker = new TabletChecker(this, systemInfo, tabletScheduler, stat);

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
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public static Catalog getCurrentCatalog() {
        if (isCheckpointThread()) {
            // only checkpoint thread it self will goes here.
            // so no need to care about the thread safe.
            if (CHECKPOINT == null) {
                CHECKPOINT = new Catalog(true);
            }
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    // NOTICE: in most case, we should use getCurrentCatalog() to get the right catalog.
    // but in some cases, we should get the serving catalog explicitly.
    public static Catalog getServingCatalog() {
        return SingletonHolder.INSTANCE;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    public ResourceMgr getResourceMgr() {
        return resourceMgr;
    }

    public static GlobalTransactionMgr getCurrentGlobalTransactionMgr() {
        return getCurrentCatalog().globalTransactionMgr;
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
        return fullNameToDb;
    }

    public AuditEventProcessor getAuditEventProcessor() {
        return auditEventProcessor;
    }

    // use this to get correct ClusterInfoService instance
    public static SystemInfoService getCurrentSystemInfo() {
        return getCurrentCatalog().getClusterInfo();
    }

    public static HeartbeatMgr getCurrentHeartbeatMgr() {
        return getCurrentCatalog().getHeartbeatMgr();
    }

    // use this to get correct TabletInvertedIndex instance
    public static TabletInvertedIndex getCurrentInvertedIndex() {
        return getCurrentCatalog().getTabletInvertedIndex();
    }

    // use this to get correct ColocateTableIndex instance
    public static ColocateTableIndex getCurrentColocateIndex() {
        return getCurrentCatalog().getColocateTableIndex();
    }

    public static CatalogRecycleBin getCurrentRecycleBin() {
        return getCurrentCatalog().getRecycleBin();
    }

    // use this to get correct Catalog's journal version
    public static int getCurrentCatalogJournalVersion() {
        return MetaContext.get().getMetaVersion();
    }

    public static int getCurrentCatalogStarRocksJournalVersion() {
        return MetaContext.get().getStarRocksMetaVersion();
    }

    public static final boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static PluginMgr getCurrentPluginMgr() {
        return getCurrentCatalog().getPluginMgr();
    }

    public static AnalyzeManager getCurrentAnalyzeMgr() {
        return getCurrentCatalog().getAnalyzeManager();
    }

    public static StatisticStorage getCurrentStatisticStorage() {
        return getCurrentCatalog().statisticStorage;
    }

    // Only used in UT
    public void setStatisticStorage(StatisticStorage statisticStorage) {
        this.statisticStorage = statisticStorage;
    }

    public static AuditEventProcessor getCurrentAuditEventProcessor() {
        return getCurrentCatalog().getAuditEventProcessor();
    }

    // Use tryLock to avoid potential dead lock
    private boolean tryLock(boolean mustLock) {
        while (true) {
            try {
                if (!lock.tryLock(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    // to see which thread held this lock for long time.
                    Thread owner = lock.getOwner();
                    if (owner != null) {
                        LOG.warn("catalog lock is held by: {}", Util.dumpThread(owner, 50));
                    }

                    if (mustLock) {
                        continue;
                    } else {
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                LOG.warn("got exception while getting catalog lock", e);
                if (mustLock) {
                    continue;
                } else {
                    return lock.isHeldByCurrentThread();
                }
            }
        }
    }

    private void unlock() {
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
    }

    public void initialize(String[] args) throws Exception {
        // set meta dir first.
        // we already set these variables in constructor. but Catalog is a singleton class.
        // so they may be set before Config is initialized.
        // set them here again to make sure these variables use values in fe.conf.

        setMetaDir();

        // 0. get local node and helper node info
        getCheckedSelfHostPort();
        getHelperNodes(args);

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
        getClusterIdAndRoleOnStartup();

        // 3. Load image first and replay edits
        this.editLog = new EditLog(nodeName);
        loadImage(this.imageDir); // load image file

        editLog.open(); // open bdb env
        this.globalTransactionMgr.setEditLog(editLog);
        this.idGenerator.setEditLog(editLog);

        // 4. create load and export job label cleaner thread
        createLabelCleaner();

        // 5. create txn timeout checker thread
        createTxnTimeoutChecker();

        // 6. start state listener thread
        createStateListener();
        listener.start();
    }

    // wait until FE is ready.
    public void waitForReady() throws InterruptedException {
        while (true) {
            if (isReady()) {
                LOG.info("catalog is ready. FE type: {}", feType);
                feStartTime = System.currentTimeMillis();
                break;
            }

            Thread.sleep(2000);
            LOG.info("wait catalog to be ready. FE type: {}. is ready: {}", feType, isReady.get());
        }
    }

    public boolean isReady() {
        return isReady.get();
    }

    private void getClusterIdAndRoleOnStartup() throws IOException {
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);

        // if helper node is point to self, or there is ROLE and VERSION file in local.
        // get the node type from local
        if (isMyself() || (roleFile.exists() && versionFile.exists())) {

            if (!isMyself()) {
                LOG.info("find ROLE and VERSION file in local, ignore helper nodes: {}", helperNodes);
            }

            // check file integrity, if has.
            if ((roleFile.exists() && !versionFile.exists())
                    || (!roleFile.exists() && versionFile.exists())) {
                LOG.error("role file and version file must both exist or both not exist. "
                        + "please specific one helper node to recover. will exit.");
                System.exit(-1);
            }

            // ATTN:
            // If the version file and role file does not exist and the helper node is itself,
            // this should be the very beginning startup of the cluster, so we create ROLE and VERSION file,
            // set isFirstTimeStartUp to true, and add itself to frontends list.
            // If ROLE and VERSION file is deleted for some reason, we may arbitrarily start this node as
            // FOLLOWER, which may cause UNDEFINED behavior.
            // Everything may be OK if the origin role is exactly FOLLOWER,
            // but if not, FE process will exit somehow.
            Storage storage = new Storage(this.imageDir);
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                nodeName = genFeNodeName(selfNode.first, selfNode.second, false /* new style */);
                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }

                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true/* old style */);
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("forward compatibility. role: {}, node name: {}", role.name(), nodeName);
                }
            }

            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            if (!versionFile.exists()) {
                clusterId = Config.cluster_id == -1 ? Storage.newClusterID() : Config.cluster_id;
                token = Strings.isNullOrEmpty(Config.auth_token) ?
                        Storage.newToken() : Config.auth_token;
                storage = new Storage(clusterId, token, this.imageDir);
                storage.writeClusterIdAndToken();

                isFirstTimeStartUp = true;
                Frontend self = new Frontend(role, nodeName, selfNode.first, selfNode.second);
                // We don't need to check if frontends already contains self.
                // frontends must be empty cause no image is loaded and no journal is replayed yet.
                // And this frontend will be persisted later after opening bdbje environment.
                frontends.put(nodeName, self);
            } else {
                clusterId = storage.getClusterID();
                if (storage.getToken() == null) {
                    token = Strings.isNullOrEmpty(Config.auth_token) ?
                            Storage.newToken() : Config.auth_token;
                    LOG.info("new token={}", token);
                    storage.setToken(token);
                    storage.writeClusterIdAndToken();
                } else {
                    token = storage.getToken();
                }
                isFirstTimeStartUp = false;
            }
        } else {
            // try to get role and node name from helper node,
            // this loop will not end until we get certain role type and name
            while (true) {
                if (!getFeNodeTypeAndNameFromHelpers()) {
                    LOG.warn("current node is not added to the group. please add it first. "
                            + "sleep 5 seconds and retry, current helper nodes: {}", helperNodes);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }

                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }
                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            Pair<String, Integer> rightHelperNode = helperNodes.get(0);

            Storage storage = new Storage(this.imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    LOG.error("fail to download version file from " + rightHelperNode.first + " will exit.");
                    System.exit(-1);
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(this.imageDir);
                clusterId = storage.getClusterID();
                token = storage.getToken();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                }
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                clusterId = storage.getClusterID();
                token = storage.getToken();
                try {
                    URL idURL = new URL("http://" + rightHelperNode.first + ":" + Config.http_port + "/check");
                    HttpURLConnection conn = null;
                    conn = (HttpURLConnection) idURL.openConnection();
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);
                    String clusterIdString = conn.getHeaderField(MetaBaseAction.CLUSTER_ID);
                    int remoteClusterId = Integer.parseInt(clusterIdString);
                    if (remoteClusterId != clusterId) {
                        LOG.error("cluster id is not equal with helper node {}. will exit.", rightHelperNode.first);
                        System.exit(-1);
                    }
                    String remoteToken = conn.getHeaderField(MetaBaseAction.TOKEN);
                    if (token == null && remoteToken != null) {
                        LOG.info("get token from helper node. token={}.", remoteToken);
                        token = remoteToken;
                        storage.writeClusterIdAndToken();
                        storage.reload();
                    }
                    if (Config.enable_token_check) {
                        Preconditions.checkNotNull(token);
                        Preconditions.checkNotNull(remoteToken);
                        if (!token.equals(remoteToken)) {
                            LOG.error("token is not equal with helper node {}. will exit.", rightHelperNode.first);
                            System.exit(-1);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("fail to check cluster_id and token with helper node.", e);
                    System.exit(-1);
                }
            }
            getNewImageOnStartup(rightHelperNode);
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            LOG.error("cluster id is not equal with config item cluster_id. will exit.");
            System.exit(-1);
        }

        isElectable = role.equals(FrontendNodeType.FOLLOWER);

        systemInfoMap.put(clusterId, systemInfo);

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("finished to get cluster id: {}, role: {} and node name: {}",
                clusterId, role.name(), nodeName);
    }

    public static String genFeNodeName(String host, int port, boolean isOldStyle) {
        String name = host + "_" + port;
        if (isOldStyle) {
            return name;
        } else {
            return name + "_" + System.currentTimeMillis();
        }
    }

    // Get the role info and node name from helper node.
    // return false if failed.
    private boolean getFeNodeTypeAndNameFromHelpers() {
        // we try to get info from helper nodes, once we get the right helper node,
        // other helper nodes will be ignored and removed.
        Pair<String, Integer> rightHelperNode = null;
        for (Pair<String, Integer> helperNode : helperNodes) {
            try {
                URL url = new URL("http://" + helperNode.first + ":" + Config.http_port
                        + "/role?host=" + selfNode.first + "&port=" + selfNode.second);
                HttpURLConnection conn = null;
                conn = (HttpURLConnection) url.openConnection();
                if (conn.getResponseCode() != 200) {
                    LOG.warn("failed to get fe node type from helper node: {}. response code: {}",
                            helperNode, conn.getResponseCode());
                    continue;
                }

                String type = conn.getHeaderField("role");
                if (type == null) {
                    LOG.warn("failed to get fe node type from helper node: {}.", helperNode);
                    continue;
                }
                role = FrontendNodeType.valueOf(type);
                nodeName = conn.getHeaderField("name");

                // get role and node name before checking them, because we want to throw any exception
                // as early as we encounter.

                if (role == FrontendNodeType.UNKNOWN) {
                    LOG.warn("frontend {} is not added to cluster yet. role UNKNOWN", selfNode);
                    return false;
                }

                if (Strings.isNullOrEmpty(nodeName)) {
                    // For forward compatibility, we use old-style name: "ip_port"
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true /* old style */);
                }
            } catch (Exception e) {
                LOG.warn("failed to get fe node type from helper node: {}.", helperNode, e);
                continue;
            }

            LOG.info("get fe node type {}, name {} from {}:{}", role, nodeName, helperNode.first, Config.http_port);
            rightHelperNode = helperNode;
            break;
        }

        if (rightHelperNode == null) {
            return false;
        }

        helperNodes.clear();
        helperNodes.add(rightHelperNode);
        return true;
    }

    private void getCheckedSelfHostPort() {
        selfNode = new Pair<>(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
        /*
         * For the first time, if the master start up failed, it will also fail to restart.
         * Check port using before create meta files to avoid this problem.
         */
        try {
            if (NetUtils.isPortUsing(selfNode.first, selfNode.second)) {
                LOG.error("edit_log_port {} is already in use. will exit.", selfNode.second);
                System.exit(-1);
            }
        } catch (UnknownHostException e) {
            LOG.error(e);
            System.exit(-1);
        }
        LOG.debug("get self node: {}", selfNode);
    }

    private void getHelperNodes(String[] args) throws AnalysisException {
        String helpers = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-helper")) {
                if (i + 1 >= args.length) {
                    System.out.println("-helper need parameter host:port,host:port");
                    System.exit(-1);
                }
                helpers = args[i + 1];
                break;
            }
        }

        if (helpers != null) {
            String[] splittedHelpers = helpers.split(",");
            for (String helper : splittedHelpers) {
                Pair<String, Integer> helperHostPort = SystemInfoService.validateHostAndPort(helper);
                if (helperHostPort.equals(selfNode)) {
                    /*
                     * If user specified the helper node to this FE itself,
                     * we will stop the starting FE process and report an error.
                     * First, it is meaningless to point the helper to itself.
                     * Secondly, when some users add FE for the first time, they will mistakenly
                     * point the helper that should have pointed to the Master to themselves.
                     * In this case, some errors have caused users to be troubled.
                     * So here directly exit the program and inform the user to avoid unnecessary trouble.
                     */
                    throw new AnalysisException(
                            "Do not specify the helper node to FE itself. "
                                    + "Please specify it to the existing running Master or Follower FE");
                }
                helperNodes.add(helperHostPort);
            }
        } else {
            // If helper node is not designated, use local node as helper node.
            helperNodes.add(Pair.create(selfNode.first, Config.edit_log_port));
        }

        LOG.info("get helper nodes: {}", helperNodes);
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

        checkCurrentNodeExist();

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
            if (isFirstTimeStartUp) {
                // if isFirstTimeStartUp is true, frontends must contains this Node.
                Frontend self = frontends.get(nodeName);
                Preconditions.checkNotNull(self);
                // OP_ADD_FIRST_FRONTEND is emitted, so it can write to BDBJE even if canWrite is false
                editLog.logAddFirstFrontend(self);
            }

            if (!isDefaultClusterCreated) {
                initDefaultCluster();
            }

            // MUST set master ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-master FE to push image
            this.masterIp = FrontendOptions.getLocalHostAddress();
            this.masterRpcPort = Config.rpc_port;
            this.masterHttpPort = Config.http_port;
            MasterInfo info = new MasterInfo(this.masterIp, this.masterHttpPort, this.masterRpcPort);
            editLog.logMasterInfo(info);

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
        heartbeatMgr.setMaster(clusterId, token, epoch);
        heartbeatMgr.start();
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
        // catalog recycle bin
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
    }

    // start threads that should running on all FE
    private void startNonMasterDaemonThreads() {
        tabletStatMgr.start();
        // load and export job label cleaner thread
        labelCleaner.start();
        // ES state store
        esRepository.start();
        starRocksRepository.start();

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
            return;
        }

        // transfer from INIT/UNKNOWN to OBSERVER/FOLLOWER

        // add helper sockets
        if (Config.edit_log_type.equalsIgnoreCase("BDB")) {
            for (Frontend fe : frontends.values()) {
                if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                    ((BDBHA) getHaProtocol()).addHelperSocket(fe.getHost(), fe.getEditLogPort());
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

    /*
     * If the current node is not in the frontend list, then exit. This may
     * happen when this node is removed from frontend list, and the drop
     * frontend log is deleted because of checkpoint.
     */
    private void checkCurrentNodeExist() {
        if (Config.metadata_failure_recovery.equals("true")) {
            return;
        }

        Frontend fe = checkFeExist(selfNode.first, selfNode.second);
        if (fe == null) {
            LOG.error("current node is not added to the cluster, will exit");
            System.exit(-1);
        } else if (fe.getRole() != role) {
            LOG.error("current node role is {} not match with frontend recorded role {}. will exit", role,
                    fe.getRole());
            System.exit(-1);
        }
    }

    private boolean getVersionFileFromHelper(Pair<String, Integer> helperNode) throws IOException {
        try {
            String url = "http://" + helperNode.first + ":" + Config.http_port + "/version";
            File dir = new File(this.imageDir);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getOutputStream(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn(e);
        }

        return false;
    }


    /**
      * When a new node joins in the cluster for the first time, it will download image from the helper at the very beginning
      * Exception are free to raise on initialized phase
      */
    private void getNewImageOnStartup(Pair<String, Integer> helperNode) throws IOException {
        long localImageVersion = 0;
        Storage storage = new Storage(this.imageDir);
        localImageVersion = storage.getImageJournalId();

        URL infoUrl = new URL("http://" + helperNode.first + ":" + Config.http_port + "/info");
        StorageInfo info = getStorageInfo(infoUrl);
        long version = info.getImageJournalId();
        if (version > localImageVersion) {
            String url = "http://" + helperNode.first + ":" + Config.http_port
                    + "/image?version=" + version;
            String filename = Storage.IMAGE + "." + version;
            File dir = new File(this.imageDir);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000, MetaHelper.getOutputStream(filename, dir));
            MetaHelper.complete(filename, dir);
        }
    }

    private boolean isMyself() {
        Preconditions.checkNotNull(selfNode);
        Preconditions.checkNotNull(helperNodes);
        LOG.debug("self: {}. helpers: {}", selfNode, helperNodes);
        // if helper nodes contain it self, remove other helpers
        boolean containSelf = false;
        for (Pair<String, Integer> helperNode : helperNodes) {
            if (selfNode.equals(helperNode)) {
                containSelf = true;
            }
        }
        if (containSelf) {
            helperNodes.clear();
            helperNodes.add(selfNode);
        }

        return containSelf;
    }

    private StorageInfo getStorageInfo(URL url) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(HTTP_TIMEOUT_SECOND * 1000);
            connection.setReadTimeout(HTTP_TIMEOUT_SECOND * 1000);
            return mapper.readValue(connection.getInputStream(), StorageInfo.class);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public boolean hasReplayer() {
        return replayer != null;
    }

    public void loadImage(String imageDir) throws IOException, DdlException {
        Storage storage = new Storage(imageDir);
        clusterId = storage.getClusterID();
        File curFile = storage.getCurrentImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("image does not exist: {}", curFile.getAbsolutePath());
            return;
        }
        replayedJournalId.set(storage.getImageJournalId());
        LOG.info("start load image from {}. is ckpt: {}", curFile.getAbsolutePath(), Catalog.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));

        long checksum = 0;
        long remoteChecksum = -1;
        try {
            checksum = loadHeader(dis, checksum);
            checksum = loadMasterInfo(dis, checksum);
            checksum = loadFrontends(dis, checksum);
            checksum = Catalog.getCurrentSystemInfo().loadBackends(dis, checksum);
            checksum = loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            recreateTabletInvertIndex();
            // rebuild es state state
            esRepository.loadTableFromCatalog();
            starRocksRepository.loadTableFromCatalog();

            checksum = loadLoadJob(dis, checksum);
            checksum = loadAlterJob(dis, checksum);
            checksum = loadRecycleBin(dis, checksum);
            checksum = loadGlobalVariable(dis, checksum);
            checksum = loadCluster(dis, checksum);
            checksum = loadBrokers(dis, checksum);
            checksum = loadResources(dis, checksum);
            checksum = loadExportJob(dis, checksum);
            checksum = loadBackupHandler(dis, checksum);
            checksum = loadAuth(dis, checksum);
            // global transaction must be replayed before load jobs v2
            checksum = loadTransactionState(dis, checksum);
            checksum = loadColocateTableIndex(dis, checksum);
            checksum = loadRoutineLoadJobs(dis, checksum);
            checksum = loadLoadJobsV2(dis, checksum);
            checksum = loadSmallFiles(dis, checksum);
            checksum = loadPlugins(dis, checksum);
            checksum = loadDeleteHandler(dis, checksum);

            remoteChecksum = dis.readLong();
            checksum = loadAnalyze(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = loadWorkGroups(dis, checksum);
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

    private void recreateTabletInvertIndex() {
        if (isCheckpointThread()) {
            return;
        }

        // create inverted index
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (Database db : this.fullNameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                for (Partition partition : allPartitions) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                                if (MetaContext.get().getMetaVersion() < FeMetaVersion.VERSION_48) {
                                    // set replica's schema hash
                                    replica.setSchemaHash(schemaHash);
                                }
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
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

    public long loadMasterInfo(DataInputStream dis, long checksum) throws IOException {
        masterIp = Text.readString(dis);
        masterRpcPort = dis.readInt();
        long newChecksum = checksum ^ masterRpcPort;
        masterHttpPort = dis.readInt();
        newChecksum ^= masterHttpPort;

        LOG.info("finished replay masterInfo from image");
        return newChecksum;
    }

    public long loadFrontends(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            int size = dis.readInt();
            long newChecksum = checksum ^ size;
            for (int i = 0; i < size; i++) {
                Frontend fe = Frontend.read(dis);
                replayAddFrontend(fe);
            }

            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_41) {
                    Frontend fe = Frontend.read(dis);
                    removedFrontends.add(fe.getNodeName());
                } else {
                    removedFrontends.add(Text.readString(dis));
                }
            }
            return newChecksum;
        }
        LOG.info("finished replay frontends from image");
        return checksum;
    }

    public long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException {
        int dbCount = dis.readInt();
        long newChecksum = checksum ^ dbCount;
        for (long i = 0; i < dbCount; ++i) {
            Database db = new Database();
            db.readFields(dis);
            newChecksum ^= db.getId();
            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            if (db.getDbState() == DbState.LINK) {
                fullNameToDb.put(db.getAttachDb(), db);
            }
            globalTransactionMgr.addDatabaseTransactionMgr(db.getId());
        }
        LOG.info("finished replay databases from image");
        return newChecksum;
    }

    public long loadLoadJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        // load jobs
        int jobSize = dis.readInt();
        long newChecksum = checksum ^ jobSize;
        Preconditions.checkArgument(jobSize == 0, "Number of job jobs must be 0");

        // delete jobs
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_11) {
            jobSize = dis.readInt();
            newChecksum ^= jobSize;
            Preconditions.checkArgument(jobSize == 0, "Number of delete job infos must be 0");
        }

        // load error hub info
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_24) {
            LoadErrorHub.Param param = new LoadErrorHub.Param();
            param.readFields(dis);
            load.setLoadErrorHubInfo(param);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            // 4. load delete jobs
            int deleteJobSize = dis.readInt();
            newChecksum ^= deleteJobSize;
            Preconditions.checkArgument(deleteJobSize == 0, "Number of delete jobs must be 0");
        }

        LOG.info("finished replay loadJob from image");
        return newChecksum;
    }

    public long loadExportJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        long newChecksum = checksum;
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_32) {
            int size = dis.readInt();
            newChecksum = checksum ^ size;
            for (int i = 0; i < size; ++i) {
                long jobId = dis.readLong();
                newChecksum ^= jobId;
                ExportJob job = new ExportJob();
                job.readFields(dis);
                exportMgr.unprotectAddJob(job);
            }
        }
        LOG.info("finished replay exportJob from image");
        return newChecksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum) throws IOException {
        long newChecksum = checksum;
        for (JobType type : JobType.values()) {
            if (type == JobType.DECOMMISSION_BACKEND) {
                if (Catalog.getCurrentCatalogJournalVersion() >= 5) {
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

        if (Catalog.getCurrentCatalogJournalVersion() >= 2) {
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
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_61) {
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

    public long loadBackupHandler(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_42) {
            getBackupHandler().readFields(dis);
        }
        getBackupHandler().setCatalog(this);
        LOG.info("finished replay backupHandler from image");
        return checksum;
    }

    public long saveBackupHandler(DataOutputStream dos, long checksum) throws IOException {
        getBackupHandler().write(dos);
        return checksum;
    }

    public long loadDeleteHandler(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_82) {
            this.deleteHandler = DeleteHandler.read(dis);
        }
        LOG.info("finished replay deleteHandler from image");
        return checksum;
    }

    public long loadWorkGroups(DataInputStream dis, long checksum) throws IOException {
        try {
            this.getWorkGroupMgr().readFields(dis);
            LOG.info("finished replaying WorkGroups from image");
        } catch (EOFException e) {
            LOG.info("no WorkGroups to replay.");
        }
        return checksum;
    }

    public long saveDeleteHandler(DataOutputStream dos, long checksum) throws IOException {
        getDeleteHandler().write(dos);
        return checksum;
    }

    public long saveWorkGroups(DataOutputStream dos, long checksum) throws IOException {
        getWorkGroupMgr().write(dos);
        return checksum;
    }

    public long loadAuth(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_43) {
            // CAN NOT use Auth.read(), cause this auth instance is already passed to DomainResolver
            auth.readFields(dis);
        }
        LOG.info("finished replay auth from image");
        return checksum;
    }

    public long loadTransactionState(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            int size = dis.readInt();
            long newChecksum = checksum ^ size;
            globalTransactionMgr.readFields(dis);
            LOG.info("finished replay transactionState from image");
            return newChecksum;
        }
        return checksum;
    }

    public long loadRecycleBin(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_10) {
            recycleBin.readFields(dis);
            if (!isCheckpointThread()) {
                // add tablet in Recycle bin to TabletInvertedIndex
                recycleBin.addTabletToInvertedIndex();
            }
            // create DatabaseTransactionMgr for db in recycle bin.
            // these dbs do not exist in `idToDb` of the catalog.
            for (Long dbId : recycleBin.getAllDbIds()) {
                globalTransactionMgr.addDatabaseTransactionMgr(dbId);
            }
        }
        LOG.info("finished replay recycleBin from image");
        return checksum;
    }

    public long loadColocateTableIndex(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_46) {
            Catalog.getCurrentColocateIndex().readFields(dis);
        }
        LOG.info("finished replay colocateTableIndex from image");
        return checksum;
    }

    public long loadRoutineLoadJobs(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_49) {
            Catalog.getCurrentCatalog().getRoutineLoadManager().readFields(dis);
        }
        LOG.info("finished replay routineLoadJobs from image");
        return checksum;
    }

    public long loadLoadJobsV2(DataInputStream in, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_50) {
            loadManager.readFields(in);
        }
        LOG.info("finished replay loadJobsV2 from image");
        return checksum;
    }

    public long loadResources(DataInputStream in, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_87) {
            resourceMgr = ResourceMgr.read(in);
        }
        LOG.info("finished replay resources from image");
        return checksum;
    }

    public long loadSmallFiles(DataInputStream in, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_52) {
            smallFileMgr.readFields(in);
        }
        LOG.info("finished replay smallFiles from image");
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
        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), Catalog.isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(curFile))) {
            checksum = saveHeader(dos, replayedJournalId, checksum);
            checksum = saveMasterInfo(dos, checksum);
            checksum = saveFrontends(dos, checksum);
            checksum = Catalog.getCurrentSystemInfo().saveBackends(dos, checksum);
            checksum = saveDb(dos, checksum);
            checksum = saveLoadJob(dos, checksum);
            checksum = saveAlterJob(dos, checksum);
            checksum = saveRecycleBin(dos, checksum);
            checksum = saveGlobalVariable(dos, checksum);
            checksum = saveCluster(dos, checksum);
            checksum = saveBrokers(dos, checksum);
            checksum = saveResources(dos, checksum);
            checksum = saveExportJob(dos, checksum);
            checksum = saveBackupHandler(dos, checksum);
            checksum = saveAuth(dos, checksum);
            checksum = saveTransactionState(dos, checksum);
            checksum = saveColocateTableIndex(dos, checksum);
            checksum = saveRoutineLoadJobs(dos, checksum);
            checksum = saveLoadJobsV2(dos, checksum);
            checksum = saveSmallFiles(dos, checksum);
            checksum = savePlugins(dos, checksum);
            checksum = saveDeleteHandler(dos, checksum);

            dos.writeLong(checksum);
            checksum = saveAnalyze(dos, checksum);
            dos.writeLong(checksum);
            checksum = saveWorkGroups(dos, checksum);
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

    public long saveMasterInfo(DataOutputStream dos, long checksum) throws IOException {
        Text.writeString(dos, masterIp);

        checksum ^= masterRpcPort;
        dos.writeInt(masterRpcPort);

        checksum ^= masterHttpPort;
        dos.writeInt(masterHttpPort);

        return checksum;
    }

    public long saveFrontends(DataOutputStream dos, long checksum) throws IOException {
        int size = frontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (Frontend fe : frontends.values()) {
            fe.write(dos);
        }

        size = removedFrontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (String feName : removedFrontends) {
            Text.writeString(dos, feName);
        }

        return checksum;
    }

    public long saveDb(DataOutputStream dos, long checksum) throws IOException {
        int dbCount = idToDb.size() - nameToCluster.keySet().size();
        checksum ^= dbCount;
        dos.writeInt(dbCount);
        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            Database db = entry.getValue();
            String dbName = db.getFullName();
            // Don't write information_schema db meta
            if (!InfoSchemaDb.isInfoSchemaDb(dbName)) {
                checksum ^= entry.getKey();
                db.readLock();
                try {
                    db.write(dos);
                } finally {
                    db.readUnlock();
                }
            }
        }
        return checksum;
    }

    public long saveLoadJob(DataOutputStream dos, long checksum) throws IOException {
        // 1. save load.dbToLoadJob
        int jobSize = 0;
        checksum ^= jobSize;
        dos.writeInt(jobSize);

        // 2. save delete jobs
        jobSize = 0;
        checksum ^= jobSize;
        dos.writeInt(jobSize);

        // 3. load error hub info
        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        param.write(dos);

        // 4. save delete load job info
        int deleteJobSize = 0;
        checksum ^= deleteJobSize;
        dos.writeInt(deleteJobSize);

        return checksum;
    }

    public long saveExportJob(DataOutputStream dos, long checksum) throws IOException {
        Map<Long, ExportJob> idToJob = exportMgr.getIdToJob();
        int size = idToJob.size();
        checksum ^= size;
        dos.writeInt(size);
        for (ExportJob job : idToJob.values()) {
            long jobId = job.getId();
            checksum ^= jobId;
            dos.writeLong(jobId);
            job.write(dos);
        }

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

    public long saveAuth(DataOutputStream dos, long checksum) throws IOException {
        auth.write(dos);
        return checksum;
    }

    public long saveTransactionState(DataOutputStream dos, long checksum) throws IOException {
        int size = globalTransactionMgr.getTransactionNum();
        checksum ^= size;
        dos.writeInt(size);
        globalTransactionMgr.write(dos);
        return checksum;
    }

    public long saveRecycleBin(DataOutputStream dos, long checksum) throws IOException {
        CatalogRecycleBin recycleBin = Catalog.getCurrentRecycleBin();
        recycleBin.write(dos);
        return checksum;
    }

    public long saveColocateTableIndex(DataOutputStream dos, long checksum) throws IOException {
        Catalog.getCurrentColocateIndex().write(dos);
        return checksum;
    }

    public long saveRoutineLoadJobs(DataOutputStream dos, long checksum) throws IOException {
        Catalog.getCurrentCatalog().getRoutineLoadManager().write(dos);
        return checksum;
    }

    // global variable persistence
    public long loadGlobalVariable(DataInputStream in, long checksum) throws IOException, DdlException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            VariableMgr.read(in);
        }
        LOG.info("finished replay globalVariable from image");
        return checksum;
    }

    public long saveGlobalVariable(DataOutputStream out, long checksum) throws IOException {
        VariableMgr.write(out);
        return checksum;
    }

    public void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        VariableMgr.replayGlobalVariable(variable);
    }

    public void replayGlobalVariableV2(GlobalVarPersistInfo info) throws IOException, DdlException {
        VariableMgr.replayGlobalVariableV2(info);
    }

    public long saveLoadJobsV2(DataOutputStream out, long checksum) throws IOException {
        Catalog.getCurrentCatalog().getLoadManager().write(out);
        return checksum;
    }

    public long saveResources(DataOutputStream out, long checksum) throws IOException {
        Catalog.getCurrentCatalog().getResourceMgr().write(out);
        return checksum;
    }

    private long saveSmallFiles(DataOutputStream out, long checksum) throws IOException {
        smallFileMgr.write(out);
        return checksum;
    }

    public void createLabelCleaner() {
        labelCleaner = new MasterDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                clearExpiredJobs();
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
        if (currentTimeMs - synchronizedTimeMs > Config.meta_delay_toleration_second * 1000) {
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
                            switch (newType) {
                                case UNKNOWN: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
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
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            Frontend fe = unprotectCheckFeExist(host, editLogPort);
            if (fe != null) {
                throw new DdlException("frontend already exists " + fe);
            }

            String nodeName = genFeNodeName(host, editLogPort, false /* new name style */);

            if (removedFrontends.contains(nodeName)) {
                throw new DdlException("frontend name already exists " + nodeName + ". Try again");
            }

            fe = new Frontend(role, nodeName, host, editLogPort);
            frontends.put(nodeName, fe);
            BDBHA bdbha = (BDBHA) haProtocol;
            if (role == FrontendNodeType.FOLLOWER || role == FrontendNodeType.REPLICA) {
                bdbha.addHelperSocket(host, editLogPort);
                helperNodes.add(Pair.create(host, editLogPort));
                bdbha.addUnstableNode(host, getFollowerCnt());
            }

            // In some cases, for example, fe starts with the outdated meta, the node name that has been dropped
            // will remain in bdb.
            // So we should remove those nodes before joining the group,
            // or it will throws NodeConflictException (New or moved node:xxxx, is configured with the socket address:
            // xxx. It conflicts with the socket already used by the member: xxxx)
            bdbha.removeNodeIfExist(host, editLogPort, nodeName);

            editLog.logAddFrontend(fe);
        } finally {
            unlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (host.equals(selfNode.first) && port == selfNode.second && feType == FrontendNodeType.MASTER) {
            throw new DdlException("can not drop current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            Frontend fe = unprotectCheckFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" + host + ":" + port + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" + host + ":" + port + "]");
            }
            frontends.remove(fe.getNodeName());
            removedFrontends.add(fe.getNodeName());

            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                haProtocol.removeElectableNode(fe.getNodeName());
                helperNodes.remove(Pair.create(host, port));

                BDBHA ha = (BDBHA) haProtocol;
                ha.removeUnstableNode(host, getFollowerCnt());
                ha.removeHelperSocket(host, port);
            }
            editLog.logRemoveFrontend(fe);
        } finally {
            unlock();
        }
    }

    public Frontend checkFeExist(String host, int port) {
        tryLock(true);
        try {
            return unprotectCheckFeExist(host, port);
        } finally {
            unlock();
        }
    }

    public Frontend unprotectCheckFeExist(String host, int port) {
        for (Frontend fe : frontends.values()) {
            if (fe.getHost().equals(host) && fe.getEditLogPort() == port) {
                return fe;
            }
        }
        return null;
    }

    public Frontend getFeByHost(String host) {
        for (Frontend fe : frontends.values()) {
            if (fe.getHost().equals(host)) {
                return fe;
            }
        }
        return null;
    }

    public Frontend getFeByName(String name) {
        for (Frontend fe : frontends.values()) {
            if (fe.getNodeName().equals(name)) {
                return fe;
            }
        }
        return null;
    }

    public int getFollowerCnt() {
        int cnt = 0;
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                cnt++;
            }
        }
        return cnt;
    }

    // The interface which DdlExecutor needs.
    public void createDb(CreateDbStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        String fullDbName = stmt.getFullDbName();
        long id = 0L;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (!nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER, clusterName);
            }
            if (fullNameToDb.containsKey(fullDbName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create database[{}] which already exists", fullDbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                }
            } else {
                id = getNextId();
                Database db = new Database(id, fullDbName);
                db.setClusterName(clusterName);
                unprotectCreateDb(db);
                editLog.logCreateDb(db);
            }
        } finally {
            unlock();
        }
        LOG.info("createDb dbName = " + fullDbName + ", id = " + id);
    }

    // For replay edit log, needn't lock metadata
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        fullNameToDb.put(db.getFullName(), db);
        db.writeLock();
        db.setExist(true);
        db.writeUnlock();
        final Cluster cluster = nameToCluster.get(db.getClusterName());
        cluster.addDb(db.getFullName(), db.getId());
        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());
    }

    // for test
    public void addCluster(Cluster cluster) {
        nameToCluster.put(cluster.getName(), cluster);
        idToCluster.put(cluster.getId(), cluster);
    }

    public void replayCreateDb(Database db) {
        tryLock(true);
        try {
            unprotectCreateDb(db);
            LOG.info("finish replay create db, name: {}, id: {}", db.getFullName(), db.getId());
        } finally {
            unlock();
        }
    }

    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();

        // 1. check if database exists
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (!fullNameToDb.containsKey(dbName)) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop database[{}] which does not exist", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }

            // 2. drop tables in db
            Database db = this.fullNameToDb.get(dbName);
            HashMap<Long, AgentBatchTask> batchTaskMap;
            db.writeLock();
            try {
                if (!stmt.isForceDrop()) {
                    if (Catalog.getCurrentCatalog().getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), null, null)) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                        "The database [" + dbName +
                                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                        " please use \"DROP database FORCE\".");
                    }
                }
                if (db.getDbState() == DbState.LINK && dbName.equals(db.getAttachDb())) {
                    // We try to drop a hard link.
                    final DropLinkDbAndUpdateDbInfo info = new DropLinkDbAndUpdateDbInfo();
                    fullNameToDb.remove(db.getAttachDb());
                    db.setDbState(DbState.NORMAL);
                    info.setUpdateDbState(DbState.NORMAL);
                    final Cluster cluster = nameToCluster
                            .get(ClusterNamespace.getClusterNameFromFullName(db.getAttachDb()));
                    final BaseParam param = new BaseParam();
                    param.addStringParam(db.getAttachDb());
                    param.addLongParam(db.getId());
                    cluster.removeLinkDb(param);
                    info.setDropDbCluster(cluster.getName());
                    info.setDropDbId(db.getId());
                    info.setDropDbName(db.getAttachDb());
                    editLog.logDropLinkDb(info);
                    return;
                }

                if (db.getDbState() == DbState.LINK && dbName.equals(db.getFullName())) {
                    // We try to drop a db which other dbs attach to it,
                    // which is not allowed.
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                if (dbName.equals(db.getAttachDb()) && db.getDbState() == DbState.MOVE) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                // save table names for recycling
                Set<String> tableNames = db.getTableNamesWithLock();
                batchTaskMap = unprotectDropDb(db, stmt.isForceDrop(), false);
                if (!stmt.isForceDrop()) {
                    Catalog.getCurrentRecycleBin().recycleDatabase(db, tableNames);
                } else {
                    Catalog.getCurrentCatalog().onEraseDatabase(db.getId());
                }
                db.setExist(false);
            } finally {
                db.writeUnlock();
            }
            sendDropTabletTasks(batchTaskMap);

            // 3. remove db from catalog
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());
            DropDbInfo info = new DropDbInfo(dbName, stmt.isForceDrop());
            editLog.logDropDb(info);

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), stmt.isForceDrop());
        } finally {
            unlock();
        }
    }

    public HashMap<Long, AgentBatchTask> unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (Table table : db.getTables()) {
            HashMap<Long, AgentBatchTask> dropTasks = unprotectDropTable(db, table.getId(), isForeDrop, isReplay);
            if (!isReplay) {
                for (Long backendId : dropTasks.keySet()) {
                    AgentBatchTask batchTask = batchTaskMap.get(backendId);
                    if (batchTask == null) {
                        batchTask = new AgentBatchTask();
                        batchTaskMap.put(backendId, batchTask);
                    }
                    batchTask.addTasks(backendId, dropTasks.get(backendId).getAllTasks());
                }
            }
        }
        return batchTaskMap;
    }

    public void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        tryLock(true);
        try {
            final Database db = this.fullNameToDb.remove(info.getDropDbName());
            db.setDbState(info.getUpdateDbState());
            final Cluster cluster = nameToCluster
                    .get(info.getDropDbCluster());
            final BaseParam param = new BaseParam();
            param.addStringParam(db.getAttachDb());
            param.addLongParam(db.getId());
            cluster.removeLinkDb(param);
        } finally {
            unlock();
        }
    }

    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db, isForceDrop, true);
                if (!isForceDrop) {
                    Catalog.getCurrentRecycleBin().recycleDatabase(db, tableNames);
                } else {
                    Catalog.getCurrentCatalog().onEraseDatabase(db.getId());
                }
                db.setExist(false);
            } finally {
                db.writeUnlock();
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());

            LOG.info("finish replay drop db, name: {}, id: {}", dbName, db.getId());
        } finally {
            unlock();
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        // check is new db with same name already exist
        if (getDb(recoverStmt.getDbName()) != null) {
            throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
        }

        Database db = Catalog.getCurrentRecycleBin().recoverDatabase(recoverStmt.getDbName());

        // add db to catalog
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (fullNameToDb.containsKey(db.getFullName())) {
                throw new DdlException("Database[" + db.getFullName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover any more
            }

            fullNameToDb.put(db.getFullName(), db);
            idToDb.put(db.getId(), db);
            db.writeLock();
            db.setExist(true);
            db.writeUnlock();
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.addDb(db.getFullName(), db.getId());

            // log
            RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
            editLog.logRecoverDb(recoverInfo);
        } finally {
            unlock();
        }

        LOG.info("finish recover database, name: {}, id: {}", recoverStmt.getDbName(), db.getId());
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!Catalog.getCurrentRecycleBin().recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("table[" + tableName + "] is not OLAP table");
            }
            OlapTable olapTable = (OlapTable) table;

            String partitionName = recoverStmt.getPartitionName();
            if (olapTable.getPartition(partitionName) != null) {
                throw new DdlException("partition[" + partitionName + "] already exist in table[" + tableName + "]");
            }

            Catalog.getCurrentRecycleBin().recoverPartition(db.getId(), olapTable, partitionName);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayEraseDatabase(long dbId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = Catalog.getCurrentRecycleBin().replayRecoverDatabase(dbId);

        // add db to catalog
        replayCreateDb(db);

        LOG.info("replay recover db[{}], name: {}", dbId, db.getFullName());
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        QuotaType quotaType = stmt.getQuotaType();
        if (quotaType == QuotaType.DATA) {
            db.setDataQuotaWithLock(stmt.getQuota());
        } else if (quotaType == QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(stmt.getQuota());
        }
        long quota = stmt.getQuota();
        DatabaseInfo dbInfo = new DatabaseInfo(dbName, "", quota, quotaType);
        editLog.logAlterDb(dbInfo);
    }

    public void replayAlterDatabaseQuota(String dbName, long quota, QuotaType quotaType) {
        Database db = getDb(dbName);
        Preconditions.checkNotNull(db);
        if (quotaType == QuotaType.DATA) {
            db.setDataQuotaWithLock(quota);
        } else if (quotaType == QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(quota);
        }
    }

    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();
        String clusterName = stmt.getClusterName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db = null;
        Cluster cluster = null;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            if (db.getDbState() == DbState.LINK || db.getDbState() == DbState.MOVE) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_RENAME_DB_ERR, fullDbName);
            }
            // check if name is already used
            if (fullNameToDb.get(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }

            cluster.removeDb(db.getFullName(), db.getId());
            cluster.addDb(newFullDbName, db.getId());
            // 1. rename db
            db.setNameWithLock(newFullDbName);

            // 2. add to meta. check again
            fullNameToDb.remove(fullDbName);
            fullNameToDb.put(newFullDbName, db);

            DatabaseInfo dbInfo = new DatabaseInfo(fullDbName, newFullDbName, -1L, QuotaType.NONE);
            editLog.logDatabaseRename(dbInfo);
        } finally {
            unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, db.getId());
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(db.getFullName(), db.getId());
            db.setName(newDbName);
            cluster.addDb(newDbName, db.getId());
            fullNameToDb.remove(dbName);
            fullNameToDb.put(newDbName, db);

            LOG.info("replay rename database {} to {}, id: {}", dbName, newDbName, db.getId());
        } finally {
            unlock();
        }
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 6.4. storageFormat
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     */
    public void createTable(CreateTableStmt stmt) throws DdlException {
        String engineName = stmt.getEngineName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // only internal table should check quota and cluster capacity
        if (!stmt.isExternal()) {
            // check cluster capacity
            Catalog.getCurrentSystemInfo().checkClusterCapacity(stmt.getClusterName());
            // check db quota
            db.checkQuota();
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        if (engineName.equals("olap")) {
            createOlapTable(db, stmt);
            return;
        } else if (engineName.equals("mysql")) {
            createMysqlTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("elasticsearch") || engineName.equalsIgnoreCase("es")) {
            createEsTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("hive")) {
            createHiveTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("iceberg")) {
            createIcebergTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("hudi")) {
            createHudiTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("jdbc")) {
            createJDBCTable(db, stmt);
            return;
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, engineName);
        }
        Preconditions.checkState(false);
    }

    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        try {
            Database db = Catalog.getCurrentCatalog().getDb(stmt.getExistedDbName());
            List<String> createTableStmt = Lists.newArrayList();
            db.readLock();
            try {
                Table table = db.getTable(stmt.getExistedTableName());
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, stmt.getExistedTableName());
                }
                Catalog.getDdlStmt(stmt.getDbName(), table, createTableStmt, null, null, false, false);
                if (createTableStmt.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
                }
            } finally {
                db.readUnlock();
            }
            StatementBase statementBase =
                    SqlParserUtils.parseAndAnalyzeStmt(createTableStmt.get(0), ConnectContext.get());
            if (statementBase instanceof CreateTableStmt) {
                CreateTableStmt parsedCreateTableStmt =
                        (CreateTableStmt) SqlParserUtils
                                .parseAndAnalyzeStmt(createTableStmt.get(0), ConnectContext.get());
                parsedCreateTableStmt.setTableName(stmt.getTableName());
                if (stmt.isSetIfNotExists()) {
                    parsedCreateTableStmt.setIfNotExists();
                }
                createTable(parsedCreateTableStmt);
            } else if (statementBase instanceof CreateViewStmt) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_UNSUPPORTED_VIEW);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to execute CREATE TABLE LIKE " + stmt.getExistedTableName() + ". Reason: " +
                    e.getMessage());
        }
    }

    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        if (partitionDesc instanceof SingleRangePartitionDesc) {
            addPartitions(db, tableName, ImmutableList.of((SingleRangePartitionDesc) partitionDesc),
                    addPartitionClause);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            db.readLock();
            RangePartitionInfo rangePartitionInfo;
            Map<String, String> tableProperties;
            try {
                Table table = db.getTable(tableName);
                CatalogChecker.checkTableExist(db, tableName);
                CatalogChecker.checkTableTypeOLAP(db, table);
                OlapTable olapTable = (OlapTable) table;
                tableProperties = olapTable.getTableProperty().getProperties();
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            } finally {
                db.readUnlock();
            }

            if (rangePartitionInfo == null) {
                throw new DdlException("Alter batch get partition info failed.");
            }

            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                throw new DdlException("Alter batch build partition only support single range column.");
            }

            Column firstPartitionColumn = partitionColumns.get(0);
            MultiRangePartitionDesc multiRangePartitionDesc = (MultiRangePartitionDesc) partitionDesc;
            Map<String, String> properties = addPartitionClause.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
            }
            if (tableProperties != null && tableProperties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                properties.put(DynamicPartitionProperty.START_DAY_OF_WEEK,
                        tableProperties.get(DynamicPartitionProperty.START_DAY_OF_WEEK));
            }
            List<SingleRangePartitionDesc> singleRangePartitionDescs = multiRangePartitionDesc
                    .convertToSingle(firstPartitionColumn.getType(), properties);
            addPartitions(db, tableName, singleRangePartitionDescs, addPartitionClause);
        }
    }

    public void addPartitions(Database db, String tableName, List<SingleRangePartitionDesc> singleRangePartitionDescs,
                              AddPartitionClause addPartitionClause) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable;
        OlapTable copiedTable;

        boolean isTempPartition = addPartitionClause.isTempPartition();
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            CatalogChecker.checkTableExist(db, tableName);
            CatalogChecker.checkTableTypeOLAP(db, table);
            olapTable = (OlapTable) table;
            CatalogChecker.checkTableState(olapTable, tableName);
            // check partition type
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() != PartitionType.RANGE) {
                throw new DdlException("Only support adding partition to range partitioned table");
            }
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Set<String> existPartitionNameSet =
                    CatalogChecker.checkPartitionNameExistForAddPartitions(olapTable, singleRangePartitionDescs);
            // partition properties is prior to clause properties
            // clause properties is prior to table properties
            Map<String, String> properties = Maps.newHashMap();
            properties = getOrSetDefaultProperties(olapTable, properties);
            Map<String, String> clauseProperties = addPartitionClause.getProperties();
            if (clauseProperties != null && !clauseProperties.isEmpty()) {
                properties.putAll(clauseProperties);
            }
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                Map<String, String> cloneProperties = Maps.newHashMap(properties);
                Map<String, String> sourceProperties = singleRangePartitionDesc.getProperties();
                if (sourceProperties != null && !sourceProperties.isEmpty()) {
                    cloneProperties.putAll(sourceProperties);
                }
                singleRangePartitionDesc.analyze(rangePartitionInfo.getPartitionColumns().size(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    rangePartitionInfo.checkAndCreateRange(singleRangePartitionDesc, isTempPartition);
                }
            }

            // get distributionInfo
            List<Column> baseSchema = olapTable.getBaseSchema();
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();
            if (distributionDesc != null) {
                distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                // for now. we only support modify distribution's bucket num
                if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                    throw new DdlException("Cannot assign different distribution type. default is: "
                            + defaultDistributionInfo.getType());
                }

                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> newDistriCols = hashDistributionInfo.getDistributionColumns();
                    List<Column> defaultDistriCols = ((HashDistributionInfo) defaultDistributionInfo)
                            .getDistributionColumns();
                    if (!newDistriCols.equals(defaultDistriCols)) {
                        throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                                + "default is: " + defaultDistriCols);
                    }
                    if (hashDistributionInfo.getBucketNum() <= 0) {
                        throw new DdlException("Cannot assign hash distribution buckets less than 1");
                    }
                }
            } else {
                distributionInfo = defaultDistributionInfo;
            }

            // check colocation
            if (Catalog.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
                String fullGroupName = db.getId() + "_" + olapTable.getColocateGroup();
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkDistribution(distributionInfo);
                for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                    groupSchema.checkReplicationNum(singleRangePartitionDesc.getReplicationNum());
                }
            }

            copiedTable = olapTable.selectiveCopy(null, false, IndexExtState.VISIBLE);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            db.readUnlock();
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        // create partition outside db lock
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
            Preconditions.checkNotNull(dataProperty);
        }

        Set<Long> tabletIdSetForAll = Sets.newHashSet();
        HashMap<String, Set<Long>> partitionNameToTabletSet = Maps.newHashMap();
        try {
            List<Partition> partitionList = Lists.newArrayListWithCapacity(singleRangePartitionDescs.size());

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                long partitionId = getNextId();
                DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
                String partitionName = singleRangePartitionDesc.getPartitionName();
                Long version = singleRangePartitionDesc.getVersionInfo();
                Set<Long> tabletIdSet = Sets.newHashSet();

                copiedTable.getPartitionInfo().setDataProperty(partitionId, dataProperty);
                copiedTable.getPartitionInfo().setTabletType(partitionId, singleRangePartitionDesc.getTabletType());
                copiedTable.getPartitionInfo()
                        .setReplicationNum(partitionId, singleRangePartitionDesc.getReplicationNum());
                copiedTable.getPartitionInfo().setIsInMemory(partitionId, singleRangePartitionDesc.isInMemory());

                Partition partition =
                        createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet);

                partitionList.add(partition);
                tabletIdSetForAll.addAll(tabletIdSet);
                partitionNameToTabletSet.put(partitionName, tabletIdSet);
            }

            buildPartitions(db, copiedTable, partitionList);

            // check again
            if (!db.writeLockAndCheckExist()) {
                throw new DdlException("db " + db.getFullName()
                        + "(" + db.getId() + ") has been dropped");
            }
            Set<String> existPartitionNameSet = Sets.newHashSet();
            try {
                CatalogChecker.checkTableExist(db, tableName);
                Table table = db.getTable(tableName);
                CatalogChecker.checkTableTypeOLAP(db, table);
                olapTable = (OlapTable) table;
                CatalogChecker.checkTableState(olapTable, tableName);
                existPartitionNameSet = CatalogChecker.checkPartitionNameExistForAddPartitions(olapTable,
                        singleRangePartitionDescs);

                if (existPartitionNameSet.size() > 0) {
                    for (String partitionName : existPartitionNameSet) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                    }
                }

                // check if meta changed
                // rollup index may be added or dropped during add partition operation.
                // schema may be changed during add partition operation.
                boolean metaChanged = false;
                if (olapTable.getIndexNameToId().size() != copiedTable.getIndexNameToId().size()) {
                    metaChanged = true;
                } else {
                    // compare schemaHash
                    for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                        long indexId = entry.getKey();
                        if (!copiedTable.getIndexIdToMeta().containsKey(indexId)) {
                            metaChanged = true;
                            break;
                        }
                        if (copiedTable.getIndexIdToMeta().get(indexId).getSchemaHash() !=
                                entry.getValue().getSchemaHash()) {
                            metaChanged = true;
                            break;
                        }
                    }
                }

                if (metaChanged) {
                    throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
                }

                // check partition type
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() != PartitionType.RANGE) {
                    throw new DdlException("Only support adding partition to range partitioned table");
                }

                // update partition info
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                rangePartitionInfo.handleNewRangePartitionDescs(singleRangePartitionDescs,
                        partitionList, existPartitionNameSet, isTempPartition);

                if (isTempPartition) {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addTempPartition(partition);
                        }
                    }
                } else {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addPartition(partition);
                        }
                    }
                }

                // log
                int partitionLen = partitionList.size();
                // Forward compatible with previous log formats
                // Version 1.15 is compatible if users only use single-partition syntax.
                // Otherwise, the followers will be crash when reading the new log
                if (partitionLen == 1) {
                    Partition partition = partitionList.get(0);
                    if (existPartitionNameSet.contains(partition.getName())) {
                        LOG.info("add partition[{}] which already exists", partition.getName());
                        return;
                    }
                    long partitionId = partition.getId();
                    PartitionPersistInfo info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            rangePartitionInfo.getRange(partitionId),
                            singleRangePartitionDescs.get(0).getPartitionDataProperty(),
                            rangePartitionInfo.getReplicationNum(partitionId),
                            rangePartitionInfo.getIsInMemory(partitionId),
                            isTempPartition);
                    editLog.logAddPartition(info);

                    LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partitionId,
                            partition.getName(), isTempPartition);
                } else {
                    List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(partitionLen);
                    for (int i = 0; i < partitionLen; i++) {
                        Partition partition = partitionList.get(i);
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            PartitionPersistInfo info =
                                    new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                                            rangePartitionInfo.getRange(partition.getId()),
                                            singleRangePartitionDescs.get(i).getPartitionDataProperty(),
                                            rangePartitionInfo.getReplicationNum(partition.getId()),
                                            rangePartitionInfo.getIsInMemory(partition.getId()),
                                            isTempPartition);
                            partitionInfoList.add(info);
                        }
                    }

                    AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                    editLog.logAddPartitions(infos);

                    for (Partition partition : partitionList) {
                        LOG.info("succeed in creating partitions[{}], name: {}, temp: {}", partition.getId(),
                                partition.getName(), isTempPartition);
                    }
                }
            } finally {
                for (String partitionName : existPartitionNameSet) {
                    Set<Long> existPartitionTabletSet = partitionNameToTabletSet.get(partitionName);
                    if (existPartitionTabletSet == null) {
                        // should not happen
                        continue;
                    }
                    for (Long tabletId : existPartitionTabletSet) {
                        // createPartitionWithIndices create duplicate tablet that if not exists scenario
                        // so here need to clean up those created tablets which partition already exists from invert index
                        Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
                    }
                }
                db.writeUnlock();
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSetForAll) {
                Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
    }

    private Map<String, String> getOrSetDefaultProperties(OlapTable olapTable, Map<String, String> sourceProperties) {
        // partition properties should inherit table properties
        Short replicationNum = olapTable.getDefaultReplicationNum();
        if (sourceProperties == null) {
            sourceProperties = Maps.newConcurrentMap();
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_INMEMORY, olapTable.isInMemory().toString());
        }
        Map<String, String> tableProperty = olapTable.getTableProperty().getProperties();
        if (tableProperty != null && tableProperty.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                    tableProperty.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM));
        }
        return sourceProperties;
    }

    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = info.getPartition();

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(partition.getId(),
                    info.isTempPartition(), info.getRange(), info.getDataProperty(), info.getReplicationNum(),
                    info.isInMemory());

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                            index.getId(), schemaHash, info.getDataProperty().getStorageMedium());
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void dropPartition(Database db, OlapTable olapTable, DropPartitionClause clause) throws DdlException {
        CatalogChecker.checkTableExist(db, olapTable.getName());
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());

        String partitionName = clause.getPartitionName();
        boolean isTempPartition = clause.isTempPartition();

        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        if (!olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", partitionName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, partitionName);
            }
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() != PartitionType.RANGE) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }

        // drop
        if (isTempPartition) {
            olapTable.dropTempPartition(partitionName, true);
        } else {
            if (!clause.isForceDrop()) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition != null) {
                    if (Catalog.getCurrentCatalog().getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), olapTable.getId(), partition.getId())) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed." +
                                        " The partition [" + partitionName +
                                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                        " please use \"DROP partition FORCE\".");
                    }
                }
            }
            olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
        }

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName, isTempPartition,
                clause.isForceDrop());
        editLog.logDropPartition(info);

        LOG.info("succeed in droping partition[{}], is temp : {}, is force : {}", partitionName, isTempPartition,
                clause.isForceDrop());
    }

    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            if (info.isTempPartition()) {
                olapTable.dropTempPartition(info.getPartitionName(), true);
            } else {
                olapTable.dropPartition(info.getDbId(), info.getPartitionName(), info.isForceDrop());
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayErasePartition(long partitionId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Table table = db.getTable(info.getTableId());
            Catalog.getCurrentRecycleBin().replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            db.writeUnlock();
        }
    }

    private Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                                      Long version, Set<Long> tabletIdSet) throws DdlException {
        return createPartitionCommon(db, table, partitionId, partitionName,
                table.getPartitionInfo().getReplicationNum(partitionId),
                table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium(),
                version, tabletIdSet);
    }

    private Partition createPartitionCommon(Database db, OlapTable table, long partitionId, String partitionName,
                                            short replicationNum, TStorageMedium storageMedium,
                                            Long version, Set<Long> tabletIdSet) throws DdlException {
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexId : table.getIndexIdToMeta().keySet()) {
            MaterializedIndex rollup = new MaterializedIndex(indexId, IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Partition partition =
                new Partition(partitionId, partitionName, indexMap.get(table.getBaseIndexId()), distributionInfo);

        // version
        if (version != null) {
            partition.updateVisibleVersion(version);
        }

        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(indexId);

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), table.getId(), partitionId, indexId, indexMeta.getSchemaHash(),
                            storageMedium);
            createTablets(db.getClusterName(), index, ReplicaState.NORMAL, distributionInfo,
                    partition.getVisibleVersion(),
                    replicationNum, tabletMeta, tabletIdSet);
            if (index.getId() != table.getBaseIndexId()) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        }
        return partition;
    }

    private void buildPartitions(Database db, OlapTable table, List<Partition> partitions) throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveBackends = systemInfo.getBackendIds(true).size();
        int numReplicas = 0;
        for (Partition partition : partitions) {
            numReplicas += partition.getReplicaCount();
        }

        if (partitions.size() >= 3 && numAliveBackends >= 3 && numReplicas >= numAliveBackends * 500) {
            LOG.info("creating {} partitions of table {} concurrently", partitions.size(), table.getName());
            buildPartitionsConcurrently(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else if (numAliveBackends > 0) {
            buildPartitionsSequentially(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else {
            throw new DdlException("no alive backend");
        }
    }

    private int countMaxTasksPerBackend(List<CreateReplicaTask> tasks) {
        Map<Long, Integer> tasksPerBackend = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            tasksPerBackend.compute(task.getBackendId(), (k, v) -> (v == null) ? 1 : v + 1);
        }
        return Collections.max(tasksPerBackend.values());
    }

    private void buildPartitionsSequentially(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        // Try to bundle at least 200 CreateReplicaTask's in a single AgentBatchTask.
        // The number 200 is just an experiment value that seems to work without obvious problems, feel free to
        // change it if you have a better choice.
        int avgReplicasPerPartition = numReplicas / partitions.size();
        int partitionGroupSize = Math.max(1, numBackends * 200 / Math.max(1, avgReplicasPerPartition));
        for (int i = 0; i < partitions.size(); i += partitionGroupSize) {
            int endIndex = Math.min(partitions.size(), i + partitionGroupSize);
            List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partitions.subList(i, endIndex));
            int partitionCount = endIndex - i;
            int indexCountPerPartition = partitions.get(i).getVisibleMaterializedIndicesCount();
            int timeout = Config.tablet_create_timeout_second * countMaxTasksPerBackend(tasks);
            // Compatible with older versions, `Config.max_create_table_timeout_second` is the timeout time for a single index.
            // Here we assume that all partitions have the same number of indexes.
            int maxTimeout = partitionCount * indexCountPerPartition * Config.max_create_table_timeout_second;
            try {
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
                tasks.clear();
            } finally {
                for (CreateReplicaTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                }
            }
        }
    }

    private void buildPartitionsConcurrently(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        int timeout = numReplicas / numBackends * Config.tablet_create_timeout_second;
        int numIndexes = partitions.stream().mapToInt(Partition::getVisibleMaterializedIndicesCount).sum();
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(numReplicas);
        Thread t = new Thread(() -> {
            Map<Long, List<Long>> taskSignatures = new HashMap<>();
            try {
                int numFinishedTasks;
                int numSendedTasks = 0;
                for (Partition partition : partitions) {
                    if (!countDownLatch.getStatus().ok()) {
                        break;
                    }
                    List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partition);
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
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                        numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                    }
                }
                countDownLatch.await();
                if (countDownLatch.getStatus().ok()) {
                    taskSignatures.clear();
                }
            } catch (Exception e) {
                LOG.warn(e);
                countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.toString()));
            } finally {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.CREATE, signature);
                    }
                }
            }
        }, "partition-build");
        t.start();
        try {
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
        } catch (Exception e) {
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw e;
        }
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, List<Partition> partitions) {
        List<CreateReplicaTask> tasks = new ArrayList<>();
        for (Partition partition : partitions) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition) {
        ArrayList<CreateReplicaTask> tasks = new ArrayList<>((int) partition.getReplicaCount());
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition, index));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition,
                                                            MaterializedIndex index) {
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(index.getId());
        for (Tablet tablet : index.getTablets()) {
            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                CreateReplicaTask task = new CreateReplicaTask(
                        replica.getBackendId(),
                        dbId,
                        table.getId(),
                        partition.getId(),
                        index.getId(),
                        tablet.getId(),
                        indexMeta.getShortKeyColumnCount(),
                        indexMeta.getSchemaHash(),
                        partition.getVisibleVersion(),
                        indexMeta.getKeysType(),
                        indexMeta.getStorageType(),
                        table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                        indexMeta.getSchema(),
                        table.getBfColumns(),
                        table.getBfFpp(),
                        null,
                        table.getIndexes(),
                        table.getPartitionInfo().getIsInMemory(partition.getId()),
                        table.getPartitionInfo().getTabletType(partition.getId()));
                tasks.add(task);
            }
        }
        return tasks;
    }

    // NOTE: Unfinished tasks will NOT be removed from the AgentTaskQueue.
    private void sendCreateReplicaTasksAndWaitForFinished(List<CreateReplicaTask> tasks, long timeout)
            throws DdlException {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(tasks.size());
        sendCreateReplicaTasks(tasks, countDownLatch);
        waitForFinished(countDownLatch, timeout);
    }

    private void sendCreateReplicaTasks(List<CreateReplicaTask> tasks,
                                        MarkedCountDownLatch<Long, Long> countDownLatch) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTabletId());
            AgentBatchTask batchTask = batchTaskMap.get(task.getBackendId());
            if (batchTask == null) {
                batchTask = new AgentBatchTask();
                batchTaskMap.put(task.getBackendId(), batchTask);
            }
            batchTask.addTask(task);
        }
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentTaskQueue.addBatchTask(entry.getValue());
            AgentTaskExecutor.submit(entry.getValue());
        }
    }

    // REQUIRE: must set countDownLatch to error stat before throw an exception.
    private void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) throws DdlException {
        try {
            if (countDownLatch.await(timeout, TimeUnit.SECONDS)) {
                if (!countDownLatch.getStatus().ok()) {
                    String errMsg = "fail to create tablet: " + countDownLatch.getStatus().getErrorMsg();
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } else { // timed out
                List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Entry<Long, Long>> firstThree = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                StringBuilder sb = new StringBuilder("fail to create tablet: timed out. unfinished replicas");
                sb.append("(").append(firstThree.size()).append("/").append(unfinishedMarks.size()).append("): ");
                // Show details of the first 3 unfinished tablets.
                for (Entry<Long, Long> mark : firstThree) {
                    sb.append(mark.getValue()); // TabletId
                    sb.append('(');
                    Backend backend = systemInfo.getBackend(mark.getKey());
                    sb.append(backend != null ? backend.getHost() : "N/A");
                    sb.append(") ");
                }
                sb.append(" timeout=").append(timeout).append("s");
                String errMsg = sb.toString();
                LOG.warn(errMsg);
                countDownLatch.countDownToZero(new Status(TStatusCode.TIMEOUT, "timed out"));
                throw new DdlException(errMsg);
            }
        } catch (InterruptedException e) {
            LOG.warn(e);
            countDownLatch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        }
    }

    // Create olap table and related base index synchronously.
    private void createOlapTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        LOG.debug("begin create olap table: {}", tableName);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            // gen partition id first
            if (partitionDesc instanceof RangePartitionDesc) {
                RangePartitionDesc rangeDesc = (RangePartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : rangeDesc.getSingleRangePartitionDescs()) {
                    long partitionId = getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(stmt.getProperties())) {
                throw new DdlException("Only support dynamic partition properties on range partition table");
            }
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        // calc short key column count
        short shortKeyColumnCount = Catalog.calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);

        // indexes
        TableIndexes indexes = new TableIndexes(stmt.getIndexes());

        // create table
        long tableId = Catalog.getCurrentCatalog().getNextId();
        OlapTable olapTable = null;
        if (stmt.isExternal()) {
            olapTable = new ExternalOlapTable(db.getId(), tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes, stmt.getProperties());
        } else {
            olapTable = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes);
        }
        olapTable.setComment(stmt.getComment());

        // set base index id
        long baseIndexId = getNextId();
        olapTable.setBaseIndexId(baseIndexId);

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    olapTable.getKeysType() == KeysType.PRIMARY_KEYS);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // analyze replication_num
        short replicationNum = FeConstants.default_replication_num;
        try {
            boolean isReplicationNumSet =
                    properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
            if (isReplicationNumSet) {
                olapTable.setReplicationNum(replicationNum);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // set in memory
        boolean isInMemory =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        olapTable.setIsInMemory(isInMemory);

        TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
        try {
            tabletType = PropertyAnalyzer.analyzeTabletType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // if this is an unpartitioned table, we should analyze data property and replication num here.
            // if this is a partitioned table, there properties are already analyzed in RangePartitionDesc analyze phase.

            // use table name as this single partition name
            long partitionId = partitionNameToId.get(tableName);
            DataProperty dataProperty = null;
            try {
                boolean hasMedium = false;
                if (properties != null) {
                    hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                }
                dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, DataProperty.DEFAULT_DATA_PROPERTY);
                if (hasMedium) {
                    olapTable.setStorageMedium(dataProperty.getStorageMedium());
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicationNum(partitionId, replicationNum);
            partitionInfo.setIsInMemory(partitionId, isInMemory);
            partitionInfo.setTabletType(partitionId, tabletType);
        }

        // check colocation properties
        try {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (!Strings.isNullOrEmpty(colocateGroup)) {
                String fullGroupName = db.getId() + "_" + colocateGroup;
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
                if (groupSchema != null) {
                    // group already exist, check if this table can be added to this group
                    groupSchema.checkColocateSchema(olapTable);
                }
                // add table to this group, if group does not exist, create a new one
                getColocateTableIndex().addTableToGroup(db.getId(), olapTable, colocateGroup,
                        null /* generate group id inside */);
                olapTable.setColocateGroup(colocateGroup);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get base index storage type. default is COLUMN
        TStorageType baseIndexStorageType = null;
        try {
            baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(baseIndexStorageType);
        // set base index meta
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, bfColumns, bfFpp);
        olapTable.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, keysType);

        for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;

            Long baseRollupIndex = olapTable.getIndexIdByName(tableName);

            // get storage type for rollup index
            TStorageType rollupIndexStorageType = null;
            try {
                rollupIndexStorageType = PropertyAnalyzer.analyzeStorageType(addRollupClause.getProperties());
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(rollupIndexStorageType);
            // set rollup index meta to olap table
            List<Column> rollupColumns = getRollupHandler().checkAndPrepareMaterializedView(addRollupClause,
                    olapTable, baseRollupIndex, false);
            short rollupShortKeyColumnCount =
                    Catalog.calcShortKeyColumnCount(rollupColumns, alterClause.getProperties());
            int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupColumns, bfColumns, bfFpp);
            long rollupIndexId = getCurrentCatalog().getNextId();
            olapTable.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
                    rollupSchemaHash, rollupShortKeyColumnCount, rollupIndexStorageType, keysType);
        }

        // analyze version info
        Long version = null;
        try {
            version = PropertyAnalyzer.analyzeVersionInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(version);

        // get storage format
        TStorageFormat storageFormat = TStorageFormat.DEFAULT; // default means it's up to BE's config
        try {
            storageFormat = PropertyAnalyzer.analyzeStorageFormat(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setStorageFormat(storageFormat);

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<Long>();

        boolean createTblSuccess = false;
        boolean addToColocateGroupSuccess = false;
        // create partition
        try {
            // do not create partition for external table
            if (olapTable.getType() == TableType.OLAP) {
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    // this is a 1-level partitioned table, use table name as partition name
                    long partitionId = partitionNameToId.get(tableName);
                    Partition partition = createPartition(db, olapTable, partitionId, tableName, version, tabletIdSet);
                    buildPartitions(db, olapTable, Collections.singletonList(partition));
                    olapTable.addPartition(partition);
                } else if (partitionInfo.getType() == PartitionType.RANGE) {
                    try {
                        // just for remove entries in stmt.getProperties(),
                        // and then check if there still has unknown properties
                        boolean hasMedium = false;
                        if (properties != null) {
                            hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                        }
                        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                DataProperty.DEFAULT_DATA_PROPERTY);
                        DynamicPartitionUtil
                                .checkAndSetDynamicPartitionBuckets(properties, distributionDesc.getBuckets());
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(olapTable, properties);
                        if (olapTable.dynamicPartitionExists() && olapTable.getColocateGroup() != null) {
                            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                            if (info.getBucketNum() !=
                                    olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets()) {
                                throw new DdlException("dynamic_partition.buckets should equal the distribution buckets"
                                        + " if creating a colocate table");
                            }
                        }
                        if (hasMedium) {
                            olapTable.setStorageMedium(dataProperty.getStorageMedium());
                        }
                        if (properties != null && !properties.isEmpty()) {
                            // here, all properties should be checked
                            throw new DdlException("Unknown properties: " + properties);
                        }
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    // this is a 2-level partitioned tables
                    List<Partition> partitions = new ArrayList<>(partitionNameToId.size());
                    for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                        Partition partition = createPartition(db, olapTable, entry.getValue(), entry.getKey(), version,
                                tabletIdSet);
                        partitions.add(partition);
                    }
                    // It's ok if partitions is empty.
                    buildPartitions(db, olapTable, partitions);
                    for (Partition partition : partitions) {
                        olapTable.addPartition(partition);
                    }
                } else {
                    throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
                }
            }

            // check database exists again, because database can be dropped when creating table
            if (!tryLock(false)) {
                throw new DdlException("Failed to acquire catalog lock. Try again");
            }
            try {
                if (getDb(db.getId()) == null) {
                    throw new DdlException("database has been dropped when creating table");
                }
                createTblSuccess = db.createTableWithLock(olapTable, false);
                if (!createTblSuccess) {
                    if (!stmt.isSetIfNotExists()) {
                        ErrorReport
                                .reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                    } else {
                        LOG.info("Create table[{}] which already exists", tableName);
                        return;
                    }
                }
            } finally {
                unlock();
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.

            // we have added these index to memory, only need to persist here
            if (getColocateTableIndex().isColocateTable(tableId)) {
                GroupId groupId = getColocateTableIndex().getGroup(tableId);
                List<List<Long>> backendsPerBucketSeq = getColocateTableIndex().getBackendsPerBucketSeq(groupId);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForAddTable(groupId, tableId, backendsPerBucketSeq);
                editLog.logColocateAddTable(info);
                addToColocateGroupSuccess = true;
            }
            LOG.info("Successfully create table[{};{}]", tableName, tableId);
            // register or remove table from DynamicPartition after table created
            DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTable);
            dynamicPartitionScheduler.createOrUpdateRuntimeInfo(
                    tableName, DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
        } finally {
            if (!createTblSuccess) {
                for (Long tabletId : tabletIdSet) {
                    Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
                }
            }
            // only remove from memory, because we have not persist it
            if (getColocateTableIndex().isColocateTable(tableId) && !addToColocateGroupSuccess) {
                getColocateTableIndex().removeTable(tableId);
            }
        }
    }

    private void createMysqlTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = Catalog.getCurrentCatalog().getNextId();
        MysqlTable mysqlTable = new MysqlTable(tableId, tableName, columns, stmt.getProperties());
        mysqlTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(mysqlTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("Successfully create table[{}-{}]", tableName, tableId);
    }

    private void createEsTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        long tableId = Catalog.getCurrentCatalog().getNextId();
        EsTable esTable = new EsTable(tableId, tableName, baseSchema, stmt.getProperties(), partitionInfo);
        esTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(esTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create table{} with id {}", tableName, tableId);
    }

    private void createHiveTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        HiveTable hiveTable = new HiveTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hiveTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hiveTable.setComment(partitionCmt);
        } else {
            hiveTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hiveTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createIcebergTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        IcebergTable icebergTable = new IcebergTable(tableId, tableName, columns, stmt.getProperties());
        if (!Strings.isNullOrEmpty(stmt.getComment())) {
            icebergTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(icebergTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createHudiTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();

        Set<String> metaFields = new HashSet<>(Arrays.asList(
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                HoodieRecord.FILENAME_METADATA_FIELD));
        Set<String> includedMetaFields = columns.stream().map(Column::getName)
                .filter(metaFields::contains).collect(Collectors.toSet());
        metaFields.removeAll(includedMetaFields);
        metaFields.forEach(f -> columns.add(new Column(f, Type.STRING, true)));

        long tableId = getNextId();
        HudiTable hudiTable = new HudiTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hudiTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hudiTable.setComment(partitionCmt);
        } else {
            hudiTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getFullName()) == null) {
                throw new DdlException("Database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hudiTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("Successfully create table[{}-{}]", tableName, tableId);
    }

    private void createJDBCTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = getNextId();
        JDBCTable jdbcTable = new JDBCTable(tableId, tableName, columns, properties);

        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }

        try {
            if (getDb(db.getFullName()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(jdbcTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table [{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create jdbc table[{}-{}]", tableName, tableId);
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

            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (partitionInfo.getType() == PartitionType.RANGE) {
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
            sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            sb.append("\"transport\" = \"").append(esTable.getTransport()).append("\",\n");
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isDocValueScanEnable()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.maxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isKeywordSniffEnable()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getHiveDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getHiveTable()).append("\",\n");
            sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\",\n");
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.HUDI) {
            HudiTable hudiTable = (HudiTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hudiTable.getDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hudiTable.getTable()).append("\",\n");
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
        Database db = this.fullNameToDb.get(dbName);
        db.createTableWithLock(table, true);

        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.getType() == TableType.OLAP) {
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (Partition partition : olapTable.getAllPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                } // end for partitions
                DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(dbId, olapTable);
            }
        }
    }

    private void createTablets(String clusterName, MaterializedIndex index, ReplicaState replicaState,
                               DistributionInfo distributionInfo, long version, short replicationNum,
                               TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType == DistributionInfoType.HASH) {
            ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
            List<List<Long>> backendsPerBucketSeq = null;
            GroupId groupId = null;
            if (colocateIndex.isColocateTable(tabletMeta.getTableId())) {
                // if this is a colocate table, try to get backend seqs from colocation index.
                Database db = Catalog.getCurrentCatalog().getDb(tabletMeta.getDbId());
                groupId = colocateIndex.getGroup(tabletMeta.getTableId());
                // Use db write lock here to make sure the backendsPerBucketSeq is consistent when the backendsPerBucketSeq is updating.
                // This lock will release very fast.
                db.writeLock();
                try {
                    backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
                } finally {
                    db.writeUnlock();
                }
            }

            // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
            // or this is just a normal table, and we can choose backends arbitrary.
            // otherwise, backends should be chosen from backendsPerBucketSeq;
            boolean chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
            if (chooseBackendsArbitrary) {
                backendsPerBucketSeq = Lists.newArrayList();
            }
            for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
                // create a new tablet with random chosen backends
                LocalTablet tablet = new LocalTablet(getNextId());

                // add tablet to inverted index first
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());

                // get BackendIds
                List<Long> chosenBackendIds;
                if (chooseBackendsArbitrary) {
                    // This is the first colocate table in the group, or just a normal table,
                    // randomly choose backends
                    if (Config.enable_strict_storage_medium_check) {
                        chosenBackendIds =
                                chosenBackendIdBySeq(replicationNum, clusterName, tabletMeta.getStorageMedium());
                    } else {
                        chosenBackendIds = chosenBackendIdBySeq(replicationNum, clusterName);
                    }
                    backendsPerBucketSeq.add(chosenBackendIds);
                } else {
                    // get backends from existing backend sequence
                    chosenBackendIds = backendsPerBucketSeq.get(i);
                }

                // create replicas
                for (long backendId : chosenBackendIds) {
                    long replicaId = getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version,
                            tabletMeta.getOldSchemaHash());
                    tablet.addReplica(replica);
                }
                Preconditions.checkState(chosenBackendIds.size() == replicationNum,
                        chosenBackendIds.size() + " vs. " + replicationNum);
            }

            if (groupId != null && chooseBackendsArbitrary) {
                colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                editLog.logColocateBackendsPerBucketSeq(info);
            }

        } else {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }
    }

    // create replicas for tablet with random chosen backends
    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName, TStorageMedium storageMedium)
            throws DdlException {
        List<Long> chosenBackendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIdsByStorageMedium(replicationNum,
                true, true, clusterName, storageMedium);
        if (chosenBackendIds == null) {
            throw new DdlException(
                    "Failed to find enough host with storage medium is " + storageMedium + " in all backends. need: " +
                            replicationNum);
        }
        return chosenBackendIds;
    }

    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName) throws DdlException {
        List<Long> chosenBackendIds =
                Catalog.getCurrentSystemInfo().seqChooseBackendIds(replicationNum, true, true, clusterName);
        if (chosenBackendIds == null) {
            throw new DdlException("Failed to find enough host in all backends. need: " + replicationNum);
        }
        return chosenBackendIds;
    }

    // Drop table
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check database
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Table table;
        HashMap<Long, AgentBatchTask> batchTaskMap;
        db.writeLock();
        try {
            table = db.getTable(tableName);
            if (table == null) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop table[{}] which does not exist", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            }

            // Check if a view
            if (stmt.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "TABLE");
                }
            }

            if (!stmt.isForceDrop()) {
                if (Catalog.getCurrentCatalog().getGlobalTransactionMgr()
                        .existCommittedTxns(db.getId(), table.getId(), null)) {
                    throw new DdlException(
                            "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                    "The table [" + tableName +
                                    "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                    " please use \"DROP table FORCE\".");
                }
            }
            batchTaskMap = unprotectDropTable(db, table.getId(), stmt.isForceDrop(), false);
            DropInfo info = new DropInfo(db.getId(), table.getId(), -1L, stmt.isForceDrop());
            editLog.logDropTable(info);
        } finally {
            db.writeUnlock();
        }
        sendDropTabletTasks(batchTaskMap);
        LOG.info("finished dropping table: {} from db: {}, is force: {}", tableName, dbName, stmt.isForceDrop());
    }

    public void sendDropTabletTasks(HashMap<Long, AgentBatchTask> batchTaskMap) {
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
                if (partTask.getAllTasks().size() > 0) {
                    AgentTaskExecutor.submit(partTask);
                }
            } else {
                AgentTaskExecutor.submit(originTasks);
            }
        }
    }

    public HashMap<Long, AgentBatchTask> unprotectDropTable(Database db, long tableId, boolean isForceDrop, boolean isReplay) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        Table table = db.getTable(tableId);
        // delete from db meta
        if (table == null) {
            return batchTaskMap;
        }

        table.onDrop();

        db.dropTable(table.getName());
        if (!isForceDrop) {
            Table oldTable = Catalog.getCurrentRecycleBin().recycleTable(db.getId(), table);
            if (oldTable != null && oldTable.getType() == TableType.OLAP) {
                batchTaskMap = Catalog.getCurrentCatalog().onEraseOlapTable((OlapTable) oldTable, false);
            }
        } else {
            if (table.getType() == TableType.OLAP) {
                batchTaskMap = Catalog.getCurrentCatalog().onEraseOlapTable((OlapTable) table, isReplay);
            }
        }

        LOG.info("finished dropping table[{}] in db[{}], tableId: {}", table.getName(), db.getFullName(),
                table.getId());
        return batchTaskMap;
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        db.writeLock();
        try {
            unprotectDropTable(db, tableId, isForceDrop, true);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayEraseTable(long tableId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayEraseTable(tableId);
    }

    public void replayEraseMultiTables(MultiEraseTableInfo multiEraseTableInfo) throws DdlException {
        List<Long> tableIds = multiEraseTableInfo.getTableIds();
        for (Long tableId : tableIds) {
            Catalog.getCurrentRecycleBin().replayEraseTable(tableId);
        }
    }

    public void replayRecoverTable(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Catalog.getCurrentRecycleBin().replayRecoverTable(db, info.getTableId());
        } finally {
            db.writeUnlock();
        }
    }

    private void unprotectAddReplica(ReplicaPersistInfo info) {
        LOG.debug("replay add a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());

        // for compatibility
        int schemaHash = info.getSchemaHash();
        if (schemaHash == -1) {
            schemaHash = olapTable.getSchemaHashByIndexId(info.getIndexId());
        }

        Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(),
                schemaHash, info.getDataSize(), info.getRowCount(),
                ReplicaState.NORMAL,
                info.getLastFailedVersion(),
                info.getLastSuccessVersion());
        tablet.addReplica(replica);
    }

    private void unprotectUpdateReplica(ReplicaPersistInfo info) {
        LOG.debug("replay update a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
        Preconditions.checkNotNull(replica, info);
        replica.updateRowCount(info.getVersion(), info.getDataSize(), info.getRowCount());
        replica.setBad(false);
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectAddReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectUpdateReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void unprotectDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        tablet.deleteReplicaByBackendId(info.getBackendId());
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectDeleteReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAddFrontend(Frontend fe) {
        tryLock(true);
        try {
            Frontend existFe = unprotectCheckFeExist(fe.getHost(), fe.getEditLogPort());
            if (existFe != null) {
                LOG.warn("fe {} already exist.", existFe);
                if (existFe.getRole() != fe.getRole()) {
                    /*
                     * This may happen if:
                     * 1. first, add a FE as OBSERVER.
                     * 2. This OBSERVER is restarted with ROLE and VERSION file being DELETED.
                     *    In this case, this OBSERVER will be started as a FOLLOWER, and add itself to the frontends.
                     * 3. this "FOLLOWER" begin to load image or replay journal,
                     *    then find the origin OBSERVER in image or journal.
                     * This will cause UNDEFINED behavior, so it is better to exit and fix it manually.
                     */
                    System.err.println("Try to add an already exist FE with different role" + fe.getRole());
                    System.exit(-1);
                }
                return;
            }
            frontends.put(fe.getNodeName(), fe);
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                // DO NOT add helper sockets here, cause BDBHA is not instantiated yet.
                // helper sockets will be added after start BDBHA
                // But add to helperNodes, just for show
                helperNodes.add(Pair.create(fe.getHost(), fe.getEditLogPort()));
            }
        } finally {
            unlock();
        }
    }

    public void replayDropFrontend(Frontend frontend) {
        tryLock(true);
        try {
            Frontend removedFe = frontends.remove(frontend.getNodeName());
            if (removedFe == null) {
                LOG.error(frontend.toString() + " does not exist.");
                return;
            }
            if (removedFe.getRole() == FrontendNodeType.FOLLOWER
                    || removedFe.getRole() == FrontendNodeType.REPLICA) {
                helperNodes.remove(Pair.create(removedFe.getHost(), removedFe.getEditLogPort()));
            }

            removedFrontends.add(removedFe.getNodeName());
        } finally {
            unlock();
        }
    }

    public int getClusterId() {
        return this.clusterId;
    }

    public String getToken() {
        return token;
    }

    public Database getDb(String name) {
        if (fullNameToDb.containsKey(name)) {
            return fullNameToDb.get(name);
        } else {
            // This maybe a information_schema db request, and information_schema db name is case insensitive.
            // So, we first extract db name to check if it is information_schema.
            // Then we reassemble the origin cluster name with lower case db name,
            // and finally get information_schema db from the name map.
            String dbName = ClusterNamespace.getNameFromFullName(name);
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                String clusterName = ClusterNamespace.getClusterNameFromFullName(name);
                return fullNameToDb.get(ClusterNamespace.getFullName(clusterName, dbName.toLowerCase()));
            }
        }
        return null;
    }

    public Database getDb(long dbId) {
        return idToDb.get(dbId);
    }

    public Database getDbIncludeRecycleBin(long dbId) {
        Database db = idToDb.get(dbId);
        if (db == null) {
            db = recycleBin.getDatabase(dbId);
        }
        return db;
    }

    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        Table table = db.getTable(tableId);
        if (table == null) {
            table = recycleBin.getTable(db.getId(), tableId);
        }
        return table;
    }

    public List<Table> getTablesIncludeRecycleBin(Database db) {
        List<Table> tables = db.getTables();
        tables.addAll(recycleBin.getTables(db.getId()));
        return tables;
    }

    public Partition getPartitionIncludeRecycleBin(OlapTable table, long partitionId) {
        Partition partition = table.getPartition(partitionId);
        if (partition == null) {
            partition = recycleBin.getPartition(partitionId);
        }
        return partition;
    }

    public Collection<Partition> getPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = new ArrayList<>(table.getPartitions());
        partitions.addAll(recycleBin.getPartitions(table.getId()));
        return partitions;
    }

    public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = table.getAllPartitions();
        partitions.addAll(recycleBin.getPartitions(table.getId()));
        return partitions;
    }

    // NOTE: result can be null, cause partition erase is not in db lock
    public DataProperty getDataPropertyIncludeRecycleBin(PartitionInfo info, long partitionId) {
        DataProperty dataProperty = info.getDataProperty(partitionId);
        if (dataProperty == null) {
            dataProperty = recycleBin.getPartitionDataProperty(partitionId);
        }
        return dataProperty;
    }

    // NOTE: result can be -1, cause partition erase is not in db lock
    public short getReplicationNumIncludeRecycleBin(PartitionInfo info, long partitionId) {
        short replicaNum = info.getReplicationNum(partitionId);
        if (replicaNum == (short) -1) {
            replicaNum = recycleBin.getPartitionReplicationNum(partitionId);
        }
        return replicaNum;
    }

    public EditLog getEditLog() {
        return editLog;
    }

    // Get the next available, need't lock because of nextId is atomic.
    public long getNextId() {
        long id = idGenerator.getNextId();
        return id;
    }

    public List<String> getDbNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    public List<String> getClusterDbNames(String clusterName) throws AnalysisException {
        final Cluster cluster = nameToCluster.get(clusterName);
        if (cluster == null) {
            throw new AnalysisException("No cluster selected");
        }
        return Lists.newArrayList(cluster.getDbNames());
    }

    public List<Long> getDbIds() {
        return Lists.newArrayList(idToDb.keySet());
    }

    public List<Long> getDbIdsIncludeRecycleBin() {
        List<Long> dbIds = getDbIds();
        dbIds.addAll(recycleBin.getAllDbIds());
        return dbIds;
    }

    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        HashMap<Long, TStorageMedium> storageMediumMap = new HashMap<Long, TStorageMedium>();

        // record partition which need to change storage medium
        // dbId -> (tableId -> partitionId)
        HashMap<Long, Multimap<Long, Long>> changedPartitionsMap = new HashMap<Long, Multimap<Long, Long>>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = getDbIds();

        for (long dbId : dbIds) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }

            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    long tableId = table.getId();
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty,
                                partition.getName() + ", pId:" + partitionId + ", db: " + dbId + ", tbl: " + tableId);
                        // only normal state table can migrate.
                        // PRIMARY_KEYS table does not support local migration.
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs
                                && olapTable.getState() == OlapTableState.NORMAL
                                && olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                            // expire. change to HDD.
                            // record and change when holding write lock
                            Multimap<Long, Long> multimap = changedPartitionsMap.get(dbId);
                            if (multimap == null) {
                                multimap = HashMultimap.create();
                                changedPartitionsMap.put(dbId, multimap);
                            }
                            multimap.put(tableId, partitionId);
                        } else {
                            storageMediumMap.put(partitionId, dataProperty.getStorageMedium());
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            Multimap<Long, Long> tableIdToPartitionIds = changedPartitionsMap.get(dbId);

            // use try lock to avoid blocking a long time.
            // if block too long, backend report rpc will timeout.
            if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("try get db {} writelock but failed when hecking backend storage medium", dbId);
                continue;
            }
            Preconditions.checkState(db.isWriteLockHeldByCurrentThread());
            try {
                for (Long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                    Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            partitionInfo.setDataProperty(partition.getId(), new DataProperty(TStorageMedium.HDD));
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                            LOG.debug("partition[{}-{}-{}] storage medium changed from SSD to HDD",
                                    dbId, tableId, partitionId);

                            // log
                            ModifyPartitionInfo info =
                                    new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                            partition.getId(),
                                            DataProperty.DEFAULT_DATA_PROPERTY,
                                            (short) -1,
                                            partitionInfo.getIsInMemory(partition.getId()));
                            editLog.logModifyPartition(info);
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.writeUnlock();
            }
        } // end for dbs
        return storageMediumMap;
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
        return this.role;
    }

    public Pair<String, Integer> getHelperNode() {
        Preconditions.checkState(helperNodes.size() >= 1);
        return this.helperNodes.get(0);
    }

    public List<Pair<String, Integer>> getHelperNodes() {
        return Lists.newArrayList(helperNodes);
    }

    public Pair<String, Integer> getSelfNode() {
        return this.selfNode;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public FrontendNodeType getFeType() {
        return this.feType;
    }

    public int getMasterRpcPort() {
        if (!isReady()) {
            return 0;
        }
        return this.masterRpcPort;
    }

    public int getMasterHttpPort() {
        if (!isReady()) {
            return 0;
        }
        return this.masterHttpPort;
    }

    public String getMasterIp() {
        if (!isReady()) {
            return "";
        }
        return this.masterIp;
    }

    public EsRepository getEsRepository() {
        return this.esRepository;
    }

    public StarRocksRepository getStarRocksRepository() {
        return this.starRocksRepository;
    }

    public HiveRepository getHiveRepository() {
        return this.hiveRepository;
    }

    public MetastoreEventsProcessor getMetastoreEventsProcessor() {
        return this.metastoreEventsProcessor;
    }

    public void setMaster(MasterInfo info) {
        this.masterIp = info.getIp();
        this.masterHttpPort = info.getHttpPort();
        this.masterRpcPort = info.getRpcPort();
    }

    public boolean canRead() {
        return this.canRead.get();
    }

    public boolean isElectable() {
        return this.isElectable;
    }

    public boolean isMaster() {
        return feType == FrontendNodeType.MASTER;
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
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
    public void alterTable(AlterTableStmt stmt) throws DdlException, UserException {
        this.alter.processAlterTable(stmt);
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        this.alter.processAlterView(stmt, ConnectContext.get());
    }

    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        this.alter.processCreateMaterializedView(stmt);
    }

    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        this.alter.processDropMaterializedView(stmt);
    }

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        if (stmt.getAlterType() == AlterType.ROLLUP) {
            this.getRollupHandler().cancel(stmt);
        } else if (stmt.getAlterType() == AlterType.COLUMN) {
            this.getSchemaChangeHandler().cancel(stmt);
        } else if (stmt.getAlterType() == AlterType.MATERIALIZED_VIEW) {
            this.getRollupHandler().cancelMV(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
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
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String oldTableName = table.getName();
        String newTableName = tableRenameClause.getNewTableName();
        if (oldTableName.equals(newTableName)) {
            throw new DdlException("Same table name");
        }

        // check if name is already used
        if (db.getTable(newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }

        table.checkAndSetName(newTableName, false);

        db.dropTable(oldTableName);
        db.createTable(table);

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), table.getId(), newTableName);
        editLog.logTableRename(tableInfo);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, table.getId());
    }

    public void replayRenameTable(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String tableName = table.getName();
            db.dropTable(tableName);
            table.setName(newTableName);
            db.createTable(table);

            LOG.info("replay rename table[{}] to {}, tableId: {}", tableName, newTableName, table.getId());
        } finally {
            db.writeUnlock();
        }
    }

    // the invoker should keep db write lock
    public void modifyTableColocate(Database db, OlapTable table, String colocateGroup, boolean isReplay,
                                    GroupId assignedGroupId)
            throws DdlException {

        String oldGroup = table.getColocateGroup();
        GroupId groupId = null;
        if (!Strings.isNullOrEmpty(colocateGroup)) {
            String fullGroupName = db.getId() + "_" + colocateGroup;
            ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
            if (groupSchema == null) {
                // user set a new colocate group,
                // check if all partitions all this table has same buckets num and same replication number
                PartitionInfo partitionInfo = table.getPartitionInfo();
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    int bucketsNum = -1;
                    short replicationNum = -1;
                    for (Partition partition : table.getPartitions()) {
                        if (bucketsNum == -1) {
                            bucketsNum = partition.getDistributionInfo().getBucketNum();
                        } else if (bucketsNum != partition.getDistributionInfo().getBucketNum()) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different buckets number");
                        }

                        if (replicationNum == -1) {
                            replicationNum = partitionInfo.getReplicationNum(partition.getId());
                        } else if (replicationNum != partitionInfo.getReplicationNum(partition.getId())) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different replication number");
                        }
                    }
                }
            } else {
                // set to an already exist colocate group, check if this table can be added to this group.
                groupSchema.checkColocateSchema(table);
            }

            List<List<Long>> backendsPerBucketSeq = null;
            if (groupSchema == null) {
                // assign to a newly created group, set backends sequence.
                // we arbitrarily choose a tablet backends sequence from this table,
                // let the colocation balancer do the work.
                backendsPerBucketSeq = table.getArbitraryTabletBucketsSeq();
            }
            // change group after getting backends sequence(if has), in case 'getArbitraryTabletBucketsSeq' failed
            groupId = colocateTableIndex.changeGroup(db.getId(), table, oldGroup, colocateGroup, assignedGroupId);

            if (groupSchema == null) {
                Preconditions.checkNotNull(backendsPerBucketSeq);
                colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            }

            // set this group as unstable
            colocateTableIndex.markGroupUnstable(groupId, false /* edit log is along with modify table log */);
            table.setColocateGroup(colocateGroup);
        } else {
            // unset colocation group
            if (Strings.isNullOrEmpty(oldGroup)) {
                // this table is not a colocate table, do nothing
                return;
            }

            // when replayModifyTableColocate, we need the groupId info
            String fullGroupName = db.getId() + "_" + oldGroup;
            groupId = colocateTableIndex.getGroupSchema(fullGroupName).getGroupId();

            colocateTableIndex.removeTable(table.getId());
            table.setColocateGroup(null);
        }

        if (!isReplay) {
            Map<String, String> properties = Maps.newHashMapWithExpectedSize(1);
            properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, colocateGroup);
            TablePropertyInfo info = new TablePropertyInfo(table.getId(), groupId, properties);
            editLog.logModifyTableColocate(info);
        }
        LOG.info("finished modify table's colocation property. table: {}, is replay: {}",
                table.getName(), isReplay);
    }

    public void replayModifyTableColocate(TablePropertyInfo info) {
        long tableId = info.getTableId();
        Map<String, String> properties = info.getPropertyMap();

        Database db = getDb(info.getGroupId().dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            modifyTableColocate(db, table, properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH), true,
                    info.getGroupId());
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table colocate", e);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String rollupName = renameClause.getRollupName();
        // check if it is base table name
        if (rollupName.equals(table.getName())) {
            throw new DdlException("Using ALTER TABLE RENAME to change table name");
        }

        String newRollupName = renameClause.getNewRollupName();
        if (rollupName.equals(newRollupName)) {
            throw new DdlException("Same rollup name");
        }

        Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
        if (indexNameToIdMap.get(rollupName) == null) {
            throw new DdlException("Rollup index[" + rollupName + "] does not exists");
        }

        // check if name is already used
        if (indexNameToIdMap.get(newRollupName) != null) {
            throw new DdlException("Rollup name[" + newRollupName + "] is already used");
        }

        long indexId = indexNameToIdMap.remove(rollupName);
        indexNameToIdMap.put(newRollupName, indexId);

        // log
        TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexId, newRollupName);
        editLog.logRollupRename(tableInfo);
        LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
    }

    public void replayRenameRollup(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String rollupName = table.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renamePartition(Database db, OlapTable table, PartitionRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        if (table.getPartitionInfo().getType() != PartitionType.RANGE) {
            throw new DdlException("Table[" + table.getName() + "] is single partitioned. "
                    + "no need to rename partition name.");
        }

        String partitionName = renameClause.getPartitionName();
        String newPartitionName = renameClause.getNewPartitionName();
        if (partitionName.equalsIgnoreCase(newPartitionName)) {
            throw new DdlException("Same partition name");
        }

        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition[" + partitionName + "] does not exists");
        }

        // check if name is already used
        if (table.checkPartitionNameExist(newPartitionName)) {
            throw new DdlException("Partition name[" + newPartitionName + "] is already used");
        }

        table.renamePartition(partitionName, newPartitionName);

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), table.getId(), partition.getId(),
                newPartitionName);
        editLog.logPartitionRename(tableInfo);
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
    }

    public void replayRenamePartition(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            Partition partition = table.getPartition(partitionId);
            table.renamePartition(partition.getName(), newPartitionName);

            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void replayRenameColumn(TableInfo tableInfo) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Map<String, String> logProperties = new HashMap<>(properties);
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(table, properties);
        } else {
            Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(properties);
            tableProperty.modifyTableProperties(analyzedDynamicPartition);
            tableProperty.buildDynamicProperty();
        }

        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), table);
        dynamicPartitionScheduler.createOrUpdateRuntimeInfo(
                table.getName(), DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), logProperties);
        editLog.logDynamicPartition(info);
    }

    /**
     * Set replication number for unpartitioned table.
     * ATTN: only for unpartitioned table now.
     *
     * @param db
     * @param table
     * @param properties
     * @throws DdlException
     */
    // The caller need to hold the db write lock
    public void modifyTableReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ColocateTableIndex colocateTableIndex = Catalog.getCurrentColocateIndex();
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        String defaultReplicationNumName = "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE) {
            throw new DdlException(
                    "This is a range partitioned table, you should specify partitions with MODIFY PARTITION clause." +
                            " If you want to set default replication number, please use '" + defaultReplicationNumName +
                            "' instead of '" + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM + "' to escape misleading.");
        }

        // unpartitioned table
        // update partition replication num
        String partitionName = table.getName();
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition does not exist. name: " + partitionName);
        }

        short replicationNum = Short.valueOf(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        boolean isInMemory = partitionInfo.getIsInMemory(partition.getId());
        DataProperty newDataProperty = partitionInfo.getDataProperty(partition.getId());
        partitionInfo.setReplicationNum(partition.getId(), replicationNum);

        // update table default replication num
        table.setReplicationNum(replicationNum);

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), table.getId(), partition.getId(),
                newDataProperty, replicationNum, isInMemory);
        editLog.logModifyPartition(info);
        LOG.info("modify partition[{}-{}-{}] replication num to {}", db.getFullName(), table.getName(),
                partition.getName(), replicationNum);
    }

    /**
     * Set default replication number for a specified table.
     * You can see the default replication number by Show Create Table stmt.
     *
     * @param db
     * @param table
     * @param properties
     */
    // The caller need to hold the db write lock
    public void modifyTableDefaultReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ColocateTableIndex colocateTableIndex = Catalog.getCurrentColocateIndex();
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        // check unpartitioned table
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Partition partition = null;
        boolean isUnpartitionedTable = false;
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            isUnpartitionedTable = true;
            String partitionName = table.getName();
            partition = table.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
        }

        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildReplicationNum();

        // update partition replication num if this table is unpartitioned table
        if (isUnpartitionedTable) {
            Preconditions.checkNotNull(partition);
            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
        }

        // log
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        editLog.logModifyReplicationNum(info);
        LOG.info("modify table[{}] replication num to {}", table.getName(),
                properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

    // The caller need to hold the db write lock
    public void modifyTableInMemoryMeta(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildInMemory();

        // need to update partition info meta
        for (Partition partition : table.getPartitions()) {
            table.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
        }

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        editLog.logModifyInMemory(info);
    }

    public void setHasForbitGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
        Map<String, String> property = new HashMap<>();
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("the DB " + dbName + "isn't  exist");
        }

        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DdlException("the DB " + dbName + " table: " + tableName + "isn't  exist");
        }

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            olapTable.setHasForbitGlobalDict(isForbit);
            if (isForbit) {
                property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.DISABLE_LOW_CARD_DICT);
            } else {
                property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.ABLE_LOW_CARD_DICT);
            }
            ModifyTablePropertyOperationLog info =
                    new ModifyTablePropertyOperationLog(db.getId(), table.getId(), property);
            editLog.logSetHasForbitGlobalDict(info);
        }
    }

    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info) {
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<String, String> properties = info.getProperties();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (opCode == OperationType.OP_SET_FORBIT_GLOBAL_DICT) {
                String enAble = properties.get(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE);
                Preconditions.checkState(enAble != null);
                if (olapTable != null) {
                    if (enAble == PropertyAnalyzer.DISABLE_LOW_CARD_DICT) {
                        olapTable.setHasForbitGlobalDict(true);
                    } else {
                        olapTable.setHasForbitGlobalDict(false);
                    }
                }
            } else {
                TableProperty tableProperty = olapTable.getTableProperty();
                if (tableProperty == null) {
                    olapTable.setTableProperty(new TableProperty(properties).buildProperty(opCode));
                } else {
                    tableProperty.modifyTableProperties(properties);
                    tableProperty.buildProperty(opCode);
                }

                // need to replay partition info meta
                if (opCode == OperationType.OP_MODIFY_IN_MEMORY) {
                    for (Partition partition : olapTable.getPartitions()) {
                        olapTable.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
                    }
                } else if (opCode == OperationType.OP_MODIFY_REPLICATION_NUM) {
                    // update partition replication num if this table is unpartitioned table
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                        String partitionName = olapTable.getName();
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition != null) {
                            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    /*
     * used for handling AlterClusterStmt
     * (for client is the ALTER CLUSTER command).
     */
    public void alterCluster(AlterSystemStmt stmt) throws DdlException, UserException {
        this.alter.processAlterCluster(stmt);
    }

    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        this.alter.getClusterHandler().cancel(stmt);
    }

    /*
     * generate and check columns' order and key's existence
     */
    private void validateColumns(List<Column> columns) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    // Change current database of this session.
    public void changeDb(ConnectContext ctx, String qualifiedDb) throws DdlException {
        if (!auth.checkDbPriv(ctx, qualifiedDb, PrivPredicate.SHOW)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED, ctx.getQualifiedUser(), qualifiedDb);
        }

        if (getDb(qualifiedDb) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, qualifiedDb);
        }

        ctx.setDatabase(qualifiedDb);
    }

    // for test only
    public void clear() {
        if (SingletonHolder.INSTANCE.idToDb != null) {
            SingletonHolder.INSTANCE.idToDb.clear();
        }
        if (SingletonHolder.INSTANCE.fullNameToDb != null) {
            SingletonHolder.INSTANCE.fullNameToDb.clear();
        }

        SingletonHolder.INSTANCE.getRollupHandler().unprotectedGetAlterJobs().clear();
        SingletonHolder.INSTANCE.getSchemaChangeHandler().unprotectedGetAlterJobs().clear();
        System.gc();
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();

        // check if db exists
        Database db = this.getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        List<Column> columns = stmt.getColumns();

        long tableId = Catalog.getCurrentCatalog().getNextId();
        View newView = new View(tableId, tableName, columns);
        newView.setComment(stmt.getComment());
        newView.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                ConnectContext.get().getSessionVariable().getSqlMode());
        // init here in case the stmt string from view.toSql() has some syntax error.
        try {
            newView.init();
        } catch (UserException e) {
            throw new DdlException("failed to init view stmt", e);
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating view");
            }
            if (!db.createTableWithLock(newView, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create view[" + tableName + "-" + newView.getId() + "]");
    }

    /**
     * Returns the function that best matches 'desc' that is registered with the
     * catalog using 'mode' to check for matching. If desc matches multiple
     * functions in the catalog, it will return the function with the strictest
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

    /**
     * create cluster
     *
     * @param stmt
     * @throws DdlException
     */
    public void createCluster(CreateClusterStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_HAS_EXIST, clusterName);
            } else {
                List<Long> backendList = systemInfo.createCluster(clusterName, stmt.getInstanceNum());
                // 1: BE returned is less than requested, throws DdlException.
                // 2: BE returned is more than or equal to 0, succeeds.
                if (backendList != null || stmt.getInstanceNum() == 0) {
                    final long id = getNextId();
                    final Cluster cluster = new Cluster(clusterName, id);
                    cluster.setBackendIdList(backendList);
                    unprotectCreateCluster(cluster);
                    if (clusterName.equals(SystemInfoService.DEFAULT_CLUSTER)) {
                        for (Database db : idToDb.values()) {
                            if (db.getClusterName().equals(SystemInfoService.DEFAULT_CLUSTER)) {
                                cluster.addDb(db.getFullName(), db.getId());
                            }
                        }
                    }
                    editLog.logCreateCluster(cluster);
                    LOG.info("finish to create cluster: {}", clusterName);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BE_NOT_ENOUGH);
                }
            }
        } finally {
            unlock();
        }

        // create super user for this cluster
        UserIdentity adminUser = new UserIdentity(Auth.ADMIN_USER, "%");
        try {
            adminUser.analyze(stmt.getClusterName());
        } catch (AnalysisException e) {
            LOG.error("should not happen", e);
        }
        auth.createUser(new CreateUserStmt(new UserDesc(adminUser, "", true)));
    }

    private void unprotectCreateCluster(Cluster cluster) {
        final Iterator<Long> iterator = cluster.getBackendIdList().iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = systemInfo.getBackend(id);
            backend.setOwnerClusterName(cluster.getName());
            backend.setBackendState(BackendState.using);
        }

        idToCluster.put(cluster.getId(), cluster);
        nameToCluster.put(cluster.getName(), cluster);

        // create info schema db
        final InfoSchemaDb infoDb = new InfoSchemaDb(cluster.getName());
        infoDb.setClusterName(cluster.getName());
        unprotectCreateDb(infoDb);

        // only need to create default cluster once.
        if (cluster.getName().equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            isDefaultClusterCreated = true;
        }
    }

    /**
     * replay create cluster
     *
     * @param cluster
     */
    public void replayCreateCluster(Cluster cluster) {
        tryLock(true);
        try {
            unprotectCreateCluster(cluster);
        } finally {
            unlock();
        }
    }

    /**
     * drop cluster and cluster's db must be have deleted
     *
     * @param stmt
     * @throws DdlException
     */
    public void dropCluster(DropClusterStmt stmt) throws DdlException {
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            final String clusterName = stmt.getClusterName();
            final Cluster cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            final List<Backend> backends = systemInfo.getClusterBackends(clusterName);
            for (Backend backend : backends) {
                if (backend.isDecommissioned()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_DECOMMISSION, clusterName);
                }
            }

            // check if there still have databases undropped, except for information_schema db
            if (cluster.getDbNames().size() > 1) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DELETE_DB_EXIST, clusterName);
            }

            systemInfo.releaseBackends(clusterName, false /* is not replay */);
            final ClusterInfo info = new ClusterInfo(clusterName, cluster.getId());
            unprotectDropCluster(info, false /* is not replay */);
            editLog.logDropCluster(info);
        } finally {
            unlock();
        }

        // drop user of this cluster
        // set is replay to true, not write log
        auth.dropUserOfCluster(stmt.getClusterName(), true /* is replay */);
    }

    private void unprotectDropCluster(ClusterInfo info, boolean isReplay) {
        systemInfo.releaseBackends(info.getClusterName(), isReplay);
        idToCluster.remove(info.getClusterId());
        nameToCluster.remove(info.getClusterName());
        final Database infoSchemaDb = fullNameToDb.get(InfoSchemaDb.getFullInfoSchemaDbName(info.getClusterName()));
        fullNameToDb.remove(infoSchemaDb.getFullName());
        idToDb.remove(infoSchemaDb.getId());
    }

    public void replayDropCluster(ClusterInfo info) {
        tryLock(true);
        try {
            unprotectDropCluster(info, true/* is replay */);
        } finally {
            unlock();
        }

        auth.dropUserOfCluster(info.getClusterName(), true /* is replay */);
    }

    public void replayExpandCluster(ClusterInfo info) {
        tryLock(true);
        try {
            final Cluster cluster = nameToCluster.get(info.getClusterName());
            cluster.addBackends(info.getBackendIdList());

            for (Long beId : info.getBackendIdList()) {
                Backend be = Catalog.getCurrentSystemInfo().getBackend(beId);
                if (be == null) {
                    continue;
                }
                be.setOwnerClusterName(info.getClusterName());
                be.setBackendState(BackendState.using);
            }
        } finally {
            unlock();
        }
    }

    /**
     * modify cluster: Expansion or shrink
     *
     * @param stmt
     * @throws DdlException
     */
    public void processModifyCluster(AlterClusterStmt stmt) throws UserException {
        final String clusterName = stmt.getAlterClusterName();
        final int newInstanceNum = stmt.getInstanceNum();
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            Cluster cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }

            // check if this cluster has backend in decommission
            final List<Long> backendIdsInCluster = cluster.getBackendIdList();
            for (Long beId : backendIdsInCluster) {
                Backend be = systemInfo.getBackend(beId);
                if (be.isDecommissioned()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_DECOMMISSION, clusterName);
                }
            }

            final int oldInstanceNum = backendIdsInCluster.size();
            if (newInstanceNum > oldInstanceNum) {
                // expansion
                final List<Long> expandBackendIds = systemInfo.calculateExpansionBackends(clusterName,
                        newInstanceNum - oldInstanceNum);
                if (expandBackendIds == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BE_NOT_ENOUGH);
                }
                cluster.addBackends(expandBackendIds);
                final ClusterInfo info = new ClusterInfo(clusterName, cluster.getId(), expandBackendIds);
                editLog.logExpandCluster(info);
            } else if (newInstanceNum < oldInstanceNum) {
                // shrink
                final List<Long> decomBackendIds = systemInfo.calculateDecommissionBackends(clusterName,
                        oldInstanceNum - newInstanceNum);
                if (decomBackendIds == null || decomBackendIds.size() == 0) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BACKEND_ERROR);
                }

                List<String> hostPortList = Lists.newArrayList();
                for (Long id : decomBackendIds) {
                    final Backend backend = systemInfo.getBackend(id);
                    hostPortList.add(new StringBuilder().append(backend.getHost()).append(":")
                            .append(backend.getHeartbeatPort()).toString());
                }

                // here we reuse the process of decommission backends. but set backend's decommission type to
                // ClusterDecommission, which means this backend will not be removed from the system
                // after decommission is done.
                final DecommissionBackendClause clause = new DecommissionBackendClause(hostPortList);
                try {
                    clause.analyze(null);
                    clause.setType(DecommissionType.ClusterDecommission);
                    AlterSystemStmt alterStmt = new AlterSystemStmt(clause);
                    alterStmt.setClusterName(clusterName);
                    this.alter.processAlterCluster(alterStmt);
                } catch (AnalysisException e) {
                    Preconditions.checkState(false, "should not happend: " + e.getMessage());
                }
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_NO_CHANGE, newInstanceNum);
            }

        } finally {
            unlock();
        }
    }

    /**
     * @param ctx
     * @param clusterName
     * @throws DdlException
     */
    public void changeCluster(ConnectContext ctx, String clusterName) throws DdlException {
        if (!Catalog.getCurrentCatalog().getAuth().checkCanEnterCluster(ConnectContext.get(), clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY,
                    ConnectContext.get().getQualifiedUser(), "enter");
        }

        if (!nameToCluster.containsKey(clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
        }

        ctx.setCluster(clusterName);
    }

    public Cluster getCluster(String clusterName) {
        return nameToCluster.get(clusterName);
    }

    public List<String> getClusterNames() {
        return new ArrayList<String>(nameToCluster.keySet());
    }

    /**
     * get migrate progress , when finish migration, next clonecheck will reset dbState
     *
     * @return
     */
    public Set<BaseParam> getMigrations() {
        final Set<BaseParam> infos = Sets.newHashSet();
        for (Database db : fullNameToDb.values()) {
            db.readLock();
            try {
                if (db.getDbState() == DbState.MOVE) {
                    int tabletTotal = 0;
                    int tabletQuorum = 0;
                    final Set<Long> beIds = Sets.newHashSet(systemInfo.getClusterBackendIds(db.getClusterName()));
                    final Set<String> tableNames = db.getTableNamesWithLock();
                    for (String tableName : tableNames) {

                        Table table = db.getTable(tableName);
                        if (table == null || table.getType() != TableType.OLAP) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        for (Partition partition : olapTable.getPartitions()) {
                            final short replicationNum = olapTable.getPartitionInfo()
                                    .getReplicationNum(partition.getId());
                            for (MaterializedIndex materializedIndex : partition
                                    .getMaterializedIndices(IndexExtState.ALL)) {
                                if (materializedIndex.getState() != IndexState.NORMAL) {
                                    continue;
                                }
                                for (Tablet tablet : materializedIndex.getTablets()) {
                                    int replicaNum = 0;
                                    int quorum = replicationNum / 2 + 1;
                                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                        if (replica.getState() != ReplicaState.CLONE
                                                && beIds.contains(replica.getBackendId())) {
                                            replicaNum++;
                                        }
                                    }
                                    if (replicaNum > quorum) {
                                        replicaNum = quorum;
                                    }

                                    tabletQuorum = tabletQuorum + replicaNum;
                                    tabletTotal = tabletTotal + quorum;
                                }
                            }
                        }
                    }
                    final BaseParam info = new BaseParam();
                    info.addStringParam(db.getClusterName());
                    info.addStringParam(db.getAttachDb());
                    info.addStringParam(db.getFullName());
                    final float percentage = tabletTotal > 0 ? (float) tabletQuorum / (float) tabletTotal : 0f;
                    info.addFloatParam(percentage);
                    infos.add(info);
                }
            } finally {
                db.readUnlock();
            }
        }

        return infos;
    }

    public long loadCluster(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            int clusterCount = dis.readInt();
            checksum ^= clusterCount;
            for (long i = 0; i < clusterCount; ++i) {
                final Cluster cluster = Cluster.read(dis);
                checksum ^= cluster.getId();

                List<Long> latestBackendIds = systemInfo.getClusterBackendIds(cluster.getName());
                if (latestBackendIds.size() != cluster.getBackendIdList().size()) {
                    LOG.warn("Cluster:" + cluster.getName() + ", backends in Cluster is "
                            + cluster.getBackendIdList().size() + ", backends in SystemInfoService is "
                            + cluster.getBackendIdList().size());
                }
                // The number of BE in cluster is not same as in SystemInfoService, when perform 'ALTER
                // SYSTEM ADD BACKEND TO ...' or 'ALTER SYSTEM ADD BACKEND ...', because both of them are 
                // for adding BE to some Cluster, but loadCluster is after loadBackend.
                cluster.setBackendIdList(latestBackendIds);

                String dbName = InfoSchemaDb.getFullInfoSchemaDbName(cluster.getName());
                InfoSchemaDb db;
                // Use real Catalog instance to avoid InfoSchemaDb id continuously increment
                // when checkpoint thread load image.
                if (Catalog.getCurrentCatalog().getFullNameToDb().containsKey(dbName)) {
                    db = (InfoSchemaDb) Catalog.getCurrentCatalog().getFullNameToDb().get(dbName);
                } else {
                    db = new InfoSchemaDb(cluster.getName());
                    db.setClusterName(cluster.getName());
                }
                String errMsg = "InfoSchemaDb id shouldn't larger than 10000, please restart your FE server";
                // Every time we construct the InfoSchemaDb, which id will increment.
                // When InfoSchemaDb id larger than 10000 and put it to idToDb,
                // which may be overwrite the normal db meta in idToDb,
                // so we ensure InfoSchemaDb id less than 10000.
                Preconditions.checkState(db.getId() < NEXT_ID_INIT_VALUE, errMsg);
                idToDb.put(db.getId(), db);
                fullNameToDb.put(db.getFullName(), db);
                cluster.addDb(dbName, db.getId());
                idToCluster.put(cluster.getId(), cluster);
                nameToCluster.put(cluster.getName(), cluster);
            }
        }
        LOG.info("finished replay cluster from image");
        return checksum;
    }

    public void refreshExternalTable(RefreshExternalTableStmt stmt) throws DdlException {
        refreshExternalTable(stmt.getDbName(), stmt.getTableName(), stmt.getPartitions());

        List<Frontend> allFrontends = Catalog.getCurrentCatalog().getFrontends(null);
        Map<String, Future<TStatus>> resultMap = Maps.newHashMapWithExpectedSize(allFrontends.size() - 1);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(Catalog.getCurrentCatalog().getSelfNode().first)) {
                continue;
            }

            resultMap.put(fe.getHost(), refreshOtherFesTable(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                    stmt.getDbName(), stmt.getTableName(), stmt.getPartitions()));
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

    public Future<TStatus> refreshOtherFesTable(TNetworkAddress thriftAddress, String dbName, String tableName,
                                                List<String> partitions) {
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000
                + Config.thrift_rpc_timeout_ms;
        FutureTask<TStatus> task = new FutureTask<TStatus>(() -> {
            TRefreshTableRequest request = new TRefreshTableRequest();
            request.setDb_name(dbName);
            request.setTable_name(tableName);
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

    public void refreshExternalTable(String dbName, String tableName, List<String> partitions) throws DdlException {
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("db: " + dbName + " not exists");
        }
        HiveMetaStoreTable table;
        db.readLock();
        try {
            Table tbl = db.getTable(tableName);
            if (!(tbl instanceof HiveMetaStoreTable)) {
                throw new DdlException("table : " + tableName + " not exists, or is not hive/hudi external table");
            }
            table = (HiveMetaStoreTable) tbl;
        } finally {
            db.readUnlock();
        }

        if (partitions != null && partitions.size() > 0) {
            table.refreshPartCache(partitions);
        } else {
            table.refreshTableCache(dbName, tableName);
        }
    }

    public void initDefaultCluster() {
        final List<Long> backendList = Lists.newArrayList();
        final List<Backend> defaultClusterBackends = systemInfo.getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend backend : defaultClusterBackends) {
            backendList.add(backend.getId());
        }

        final long id = getNextId();
        final Cluster cluster = new Cluster(SystemInfoService.DEFAULT_CLUSTER, id);

        // make sure one host hold only one backend.
        Set<String> beHost = Sets.newHashSet();
        for (Backend be : defaultClusterBackends) {
            if (beHost.contains(be.getHost())) {
                // we can not handle this situation automatically.
                LOG.error("found more than one backends in same host: {}", be.getHost());
                System.exit(-1);
            } else {
                beHost.add(be.getHost());
            }
        }

        // we create default_cluster to meet the need for ease of use, because
        // most users hava no multi tenant needs.
        cluster.setBackendIdList(backendList);
        unprotectCreateCluster(cluster);
        for (Database db : idToDb.values()) {
            db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
            cluster.addDb(db.getFullName(), db.getId());
        }

        // no matter default_cluster is created or not,
        // mark isDefaultClusterCreated as true
        isDefaultClusterCreated = true;
        editLog.logCreateCluster(cluster);
    }

    public void replayUpdateDb(DatabaseInfo info) {
        final Database db = fullNameToDb.get(info.getDbName());
        db.setClusterName(info.getClusterName());
        db.setDbState(info.getDbState());
    }

    public long saveCluster(DataOutputStream dos, long checksum) throws IOException {
        final int clusterCount = idToCluster.size();
        checksum ^= clusterCount;
        dos.writeInt(clusterCount);
        for (Map.Entry<Long, Cluster> entry : idToCluster.entrySet()) {
            long clusterId = entry.getKey();
            if (clusterId >= NEXT_ID_INIT_VALUE) {
                checksum ^= clusterId;
                final Cluster cluster = entry.getValue();
                cluster.write(dos);
            }
        }
        return checksum;
    }

    public long saveBrokers(DataOutputStream dos, long checksum) throws IOException {
        Map<String, List<FsBroker>> addressListMap = brokerMgr.getBrokerListMap();
        int size = addressListMap.size();
        checksum ^= size;
        dos.writeInt(size);

        for (Map.Entry<String, List<FsBroker>> entry : addressListMap.entrySet()) {
            Text.writeString(dos, entry.getKey());
            final List<FsBroker> addrs = entry.getValue();
            size = addrs.size();
            checksum ^= size;
            dos.writeInt(size);
            for (FsBroker addr : addrs) {
                addr.write(dos);
            }
        }

        return checksum;
    }

    public long loadBrokers(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (MetaContext.get().getMetaVersion() >= FeMetaVersion.VERSION_31) {
            int count = dis.readInt();
            checksum ^= count;
            for (long i = 0; i < count; ++i) {
                String brokerName = Text.readString(dis);
                int size = dis.readInt();
                checksum ^= size;
                List<FsBroker> addrs = Lists.newArrayList();
                for (int j = 0; j < size; j++) {
                    FsBroker addr = FsBroker.readIn(dis);
                    addrs.add(addr);
                }
                brokerMgr.replayAddBrokers(brokerName, addrs);
            }
            LOG.info("finished replay brokerMgr from image");
        }
        return checksum;
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        for (long id : info.getBackendList()) {
            final Backend backend = systemInfo.getBackend(id);
            final Cluster cluster = nameToCluster.get(backend.getOwnerClusterName());
            cluster.removeBackend(id);
            backend.setDecommissioned(false);
            backend.clearClusterName();
            backend.setBackendState(BackendState.free);
        }
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

    /*
     * Truncate specified table or partitions.
     * The main idea is:
     *
     * 1. using the same schema to create new table(partitions)
     * 2. use the new created table(partitions) to replace the old ones.
     *
     * if no partition specified, it will truncate all partitions of this table, including all temp partitions,
     * otherwise, it will only truncate those specified partitions.
     *
     */
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();

        // check, and save some info which need to be checked again later
        Map<String, Partition> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl;
        Database db = getDb(dbTbl.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbTbl.getDb());
        }

        boolean truncateEntireTable = tblRef.getPartitionNames() == null;
        db.readLock();
        try {
            Table table = db.getTable(dbTbl.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTbl.getTbl());
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Only support truncate OLAP table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }

                    origPartitions.put(partName, partition);
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition);
                }
            }

            copiedTbl = olapTable.selectiveCopy(origPartitions.keySet(), true, IndexExtState.VISIBLE);
        } finally {
            db.readUnlock();
        }

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(origPartitions.size());
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                long oldPartitionId = entry.getValue().getId();
                long newPartitionId = getNextId();
                String newPartitionName = entry.getKey();

                PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
                partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(oldPartitionId));
                partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(oldPartitionId));
                partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(oldPartitionId));
                partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(oldPartitionId));

                copiedTbl.setDefaultDistributionInfo(entry.getValue().getDistributionInfo());

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet);
                newPartitions.add(newPartition);
            }
            buildPartitions(db, copiedTbl, newPartitions);
        } catch (DdlException e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        // Things may be changed outside the database lock.
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(copiedTbl.getId());
            if (olapTable == null) {
                throw new DdlException("Table[" + copiedTbl.getName() + "] is dropped");
            }

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

            // check partitions
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                Partition partition = copiedTbl.getPartition(entry.getValue().getId());
                if (partition == null || !partition.getName().equalsIgnoreCase(entry.getKey())) {
                    throw new DdlException("Partition [" + entry.getKey() + "] is changed");
                }
            }

            // check if meta changed
            // rollup index may be added or dropped, and schema may be changed during creating partition operation.
            boolean metaChanged = false;
            if (olapTable.getIndexNameToId().size() != copiedTbl.getIndexNameToId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexIdToSchemaHash = copiedTbl.getIndexIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                    long indexId = entry.getKey();
                    if (!copiedIndexIdToSchemaHash.containsKey(indexId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexIdToSchemaHash.get(indexId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // replace
            truncateTableInternal(olapTable, newPartitions, truncateEntireTable);

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            editLog.logTruncateTable(info);
        } finally {
            db.writeUnlock();
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    private void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions, boolean isEntireTable) {
        // use new partitions to replace the old ones.
        Set<Long> oldTabletIds = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            // save old tablets to be removed
            for (MaterializedIndex index : oldPartition.getMaterializedIndices(IndexExtState.ALL)) {
                index.getTablets().stream().forEach(t -> {
                    oldTabletIds.add(t.getId());
                });
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Long tabletId : oldTabletIds) {
            Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
        }
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        Database db = getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTblId());
            truncateTableInternal(olapTable, info.getPartitions(), info.isEntireTable());

            if (!Catalog.isCheckpointThread()) {
                // add tablet to inverted index
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (Partition partition : info.getPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(),
                                partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
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
        Map<String, String> configs = stmt.getConfigs();
        Preconditions.checkState(configs.size() == 1);

        setFrontendConfig(configs);

        List<Frontend> allFrontends = Catalog.getCurrentCatalog().getFrontends(null);
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000
                + Config.thrift_rpc_timeout_ms;
        StringBuilder errMsg = new StringBuilder();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(Catalog.getCurrentCatalog().getSelfNode().first)) {
                continue;
            }

            TSetConfigRequest request = new TSetConfigRequest();
            request.setKeys(new ArrayList<>(configs.keySet()));
            request.setValues(new ArrayList<>(configs.values()));
            try {
                TSetConfigResponse response = FrontendServiceProxy
                        .call(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                                timeout,
                                Config.thrift_rpc_retry_times,
                                client -> client.setConfig(request));
                TStatus status = response.getStatus();
                if (status.getStatus_code() != TStatusCode.OK) {
                    errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ");
                    if (status.getError_msgs() != null && status.getError_msgs().size() > 0) {
                        errMsg.append(String.join(",", status.getError_msgs()));
                    }
                    errMsg.append(";");
                }
            } catch (Exception e) {
                LOG.warn("set remote fe: {} config failed", fe.getHost(), e);
                errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ").append(e.getMessage());
            }
        }
        if (errMsg.length() > 0) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_SET_CONFIG_FAILED, errMsg.toString());
        }
    }

    public void setFrontendConfig(Map<String, String> configs) throws DdlException {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            ConfigBase.setMutableConfig(entry.getKey(), entry.getValue());
        }
    }

    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        List<Pair<Long, Integer>> tabletsWithSchemaHash = backendTabletsInfo.getTabletSchemaHash();
        if (!tabletsWithSchemaHash.isEmpty()) {
            // In previous version, we save replica info in `tabletsWithSchemaHash`,
            // but it is wrong because we can not get replica from `tabletInvertedIndex` when doing checkpoint,
            // because when doing checkpoint, the tabletInvertedIndex is not initialized at all.
            //
            // So we can only discard this information, in this case, it is equivalent to losing the record of these operations.
            // But it doesn't matter, these records are currently only used to record whether a replica is in a bad state.
            // This state has little effect on the system, and it can be restored after the system has processed the bad state replica.
            for (Pair<Long, Integer> tabletInfo : tabletsWithSchemaHash) {
                LOG.warn("find an old backendTabletsInfo for tablet {}, ignore it", tabletInfo.first);
            }
            return;
        }

        // in new version, replica info is saved here.
        // but we need to get replica from db->tbl->partition->...
        List<ReplicaPersistInfo> replicaPersistInfos = backendTabletsInfo.getReplicaPersistInfos();
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            long dbId = info.getDbId();
            Database db = getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(info.getTableId());
                if (tbl == null) {
                    continue;
                }
                Partition partition = tbl.getPartition(info.getPartitionId());
                if (partition == null) {
                    continue;
                }
                MaterializedIndex mindex = partition.getIndex(info.getIndexId());
                if (mindex == null) {
                    continue;
                }
                LocalTablet tablet = (LocalTablet) mindex.getTablet(info.getTabletId());
                if (tablet == null) {
                    continue;
                }
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                if (replica != null) {
                    replica.setBad(true);
                    LOG.debug("get replica {} of tablet {} on backend {} to bad when replaying",
                            info.getReplicaId(), info.getTabletId(), info.getBackendId());
                }
            } finally {
                db.writeUnlock();
            }
        }
    }

    // Convert table's distribution type from random to hash.
    // random distribution is no longer supported.
    public void convertDistributionType(Database db, OlapTable tbl) throws DdlException {
        db.writeLock();
        try {
            if (!tbl.convertRandomDistributionToHashDistribution()) {
                throw new DdlException("Table " + tbl.getName() + " is not random distributed");
            }
            TableInfo tableInfo = TableInfo.createForModifyDistribution(db.getId(), tbl.getId());
            editLog.logModifyDistributionType(tableInfo);
            LOG.info("finished to modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
    }

    public void replayConvertDistributionType(TableInfo tableInfo) {
        Database db = getDb(tableInfo.getDbId());
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableInfo.getTableId());
            tbl.convertRandomDistributionToHashDistribution();
            LOG.info("replay modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
    }

    /*
     * The entry of replacing partitions with temp partitions.
     */
    public void replaceTempPartition(Database db, String tableName, ReplacePartitionClause clause) throws DdlException {
        List<String> partitionNames = clause.getPartitionNames();
        // duplicate temp partition will cause Incomplete transaction
        List<String> tempPartitionNames =
                clause.getTempPartitionNames().stream().distinct().collect(Collectors.toList());

        boolean isStrictRange = clause.isStrictRange();
        boolean useTempPartitionName = clause.useTempPartitionName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table");
            }

            OlapTable olapTable = (OlapTable) table;
            // check partition exist
            for (String partName : partitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, false)) {
                    throw new DdlException("Partition[" + partName + "] does not exist");
                }
            }
            for (String partName : tempPartitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, true)) {
                    throw new DdlException("Temp partition[" + partName + "] does not exist");
                }
            }

            olapTable.replaceTempPartitions(partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), olapTable.getId(),
                    partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);
            editLog.logReplaceTempPartition(info);
            LOG.info("finished to replace partitions {} with temp partitions {} from table: {}",
                    clause.getPartitionNames(), clause.getTempPartitionNames(), tableName);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog) {
        Database db = getDb(replaceTempPartitionLog.getDbId());
        if (db == null) {
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(replaceTempPartitionLog.getTblId());
            if (olapTable == null) {
                return;
            }
            olapTable.replaceTempPartitions(replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(),
                    replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName());
        } catch (DdlException e) {
            LOG.warn("should not happen. {}", e);
        } finally {
            db.writeUnlock();
        }
    }

    public void installPlugin(InstallPluginStmt stmt) throws UserException, IOException {
        pluginMgr.installPlugin(stmt);
    }

    public long savePlugins(DataOutputStream dos, long checksum) throws IOException {
        Catalog.getCurrentPluginMgr().write(dos);
        return checksum;
    }

    public long loadPlugins(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_78) {
            Catalog.getCurrentPluginMgr().readFields(dis);
        }
        LOG.info("finished replay plugins from image");
        return checksum;
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

    public void replayModifyHiveTableColumn(ModifyTableColumnOperationLog info) {
        if (info.getDbName() == null) {
            return;
        }
        String hiveExternalDb = info.getDbName();
        String hiveExternalTable = info.getTableName();
        LOG.info("replayModifyTableColumn hiveDb:{},hiveTable:{}", hiveExternalDb, hiveExternalTable);
        List<Column> columns = info.getColumns();
        Database db = getDb(hiveExternalDb);
        HiveTable table;
        db.writeLock();
        try {
            Table tbl = db.getTable(hiveExternalTable);
            table = (HiveTable) tbl;
            table.setNewFullSchema(columns);
        } finally {
            db.writeUnlock();
        }
    }

    public long saveAnalyze(DataOutputStream dos, long checksum) throws IOException {
        Catalog.getCurrentAnalyzeMgr().write(dos);
        return checksum;
    }

    public long loadAnalyze(DataInputStream dis, long checksum) throws IOException {
        try {
            Catalog.getCurrentAnalyzeMgr().readFields(dis);
            LOG.info("finished replay analyze job from image");
        } catch (EOFException e) {
            LOG.info("none analyze job replay.");
        }
        return checksum;
    }

    // entry of checking tablets operation
    public void checkTablets(AdminCheckTabletsStmt stmt) {
        CheckType type = stmt.getType();
        switch (type) {
            case CONSISTENCY:
                consistencyChecker.addTabletsToCheck(stmt.getTabletIds());
                break;
            default:
                break;
        }
    }

    // Set specified replica's status. If replica does not exist, just ignore it.
    public void setReplicaStatus(AdminSetReplicaStatusStmt stmt) {
        long tabletId = stmt.getTabletId();
        long backendId = stmt.getBackendId();
        ReplicaStatus status = stmt.getStatus();
        setReplicaStatusInternal(tabletId, backendId, status, false);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        setReplicaStatusInternal(log.getTabletId(), log.getBackendId(), log.getReplicaStatus(), true);
    }

    private void setReplicaStatusInternal(long tabletId, long backendId, ReplicaStatus status, boolean isReplay) {
        TabletMeta meta = tabletInvertedIndex.getTabletMeta(tabletId);
        if (meta == null) {
            LOG.info("tablet {} does not exist", tabletId);
            return;
        }
        long dbId = meta.getDbId();
        Database db = getDb(dbId);
        if (db == null) {
            LOG.info("database {} of tablet {} does not exist", dbId, tabletId);
            return;
        }
        db.writeLock();
        try {
            Replica replica = tabletInvertedIndex.getReplica(tabletId, backendId);
            if (replica == null) {
                LOG.info("replica of tablet {} does not exist", tabletId);
                return;
            }
            if (status == ReplicaStatus.BAD || status == ReplicaStatus.OK) {
                if (replica.setBadForce(status == ReplicaStatus.BAD)) {
                    if (!isReplay) {
                        SetReplicaStatusOperationLog log =
                                new SetReplicaStatusOperationLog(backendId, tabletId, status);
                        getEditLog().logSetReplicaStatus(log);
                    }
                    LOG.info("set replica {} of tablet {} on backend {} as {}. is replay: {}",
                            replica.getId(), tabletId, backendId, status, isReplay);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void onEraseDatabase(long dbId) {
        // remove jobs
        Catalog.getCurrentCatalog().getSchemaChangeHandler().removeDbAlterJob(dbId);
        Catalog.getCurrentCatalog().getRollupHandler().removeDbAlterJob(dbId);

        // remove database transaction manager
        Catalog.getCurrentCatalog().getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
    }

    public HashMap<Long, AgentBatchTask> onEraseOlapTable(OlapTable olapTable, boolean isReplay) {
        // inverted index
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        if (!isReplay) {
            // drop all replicas

            for (Partition partition : olapTable.getAllPartitions()) {
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    long indexId = materializedIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        List<Replica> replicas = ((LocalTablet) tablet).getReplicas();
                        for (Replica replica : replicas) {
                            long backendId = replica.getBackendId();
                            DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                            AgentBatchTask batchTask = batchTaskMap.get(backendId);
                            if (batchTask == null) {
                                batchTask = new AgentBatchTask();
                                batchTaskMap.put(backendId, batchTask);
                            }
                            batchTask.addTask(dropTask);
                        } // end for replicas
                    } // end for tablets
                } // end for indices
            } // end for partitions
        }
        // colocation
        Catalog.getCurrentColocateIndex().removeTable(olapTable.getId());
        return batchTaskMap;
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                invertedIndex.deleteTablet(tablet.getId());
            }
        }
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
    }
}
