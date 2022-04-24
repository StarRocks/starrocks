package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.starrocks.StarRocksFE;
import com.starrocks.alter.Alter;
import com.starrocks.alter.AlterJob;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.backup.BackupHandler;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MetaReplayState;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.StarOSAgent;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.catalog.WorkGroupMgr;
import com.starrocks.clone.ColocateTableBalancer;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.clone.TabletChecker;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.SmallFileMgr;
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
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.Storage;
import com.starrocks.persist.TablePropertyInfo;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.JournalObservable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.AnalyzeManager;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.MasterTaskExecutor;
import com.starrocks.thrift.TMasterInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalStateMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalStateMgr.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final int STATE_CHANGE_CHECK_INTERVAL_MS = 100;
    private static final String BDB_DIR = "/bdb";
    private static final String IMAGE_DIR = "/image";

    private String metaDir;
    private String bdbDir;
    private String imageDir;

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;
    private FrontendNodeType feType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;
    // set to true after finished replay all meta and ready to serve
    // set to false when catalog is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);
    private BlockingQueue<FrontendNodeType> typeTransferQueue;
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
    private Daemon replayer;
    private Daemon timePrinter;
    private Daemon listener;
    private EsRepository esRepository;  // it is a daemon, so add it here
    private StarRocksRepository starRocksRepository;
    private HiveRepository hiveRepository;
    private MetastoreEventsProcessor metastoreEventsProcessor;

    private CatalogIdGenerator idGenerator = new CatalogIdGenerator(NEXT_ID_INIT_VALUE);
    private EditLog editLog;

    private HAProtocol haProtocol = null;
    private JournalObservable journalObservable;
    // For checkpoint and observer memory replayed marker
    private AtomicLong replayedJournalId;
    private static GlobalStateMgr CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;

    private TabletInvertedIndex tabletInvertedIndex;
    private ColocateTableIndex colocateTableIndex;

    private CatalogRecycleBin recycleBin;
    private FunctionSet functionSet;
    private MetaReplayState metaReplayState;

    private NodeMgr nodeMgr;
    private LocalMetastore localMetastore;
    private BrokerMgr brokerMgr;
    private ResourceMgr resourceMgr;

    private GlobalTransactionMgr globalTransactionMgr;

    private TabletStatMgr tabletStatMgr;

    private Auth auth;

    private DomainResolver domainResolver;

    private TabletSchedulerStat stat;

    private TabletScheduler tabletScheduler;

    private TabletChecker tabletChecker;
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

    private long imageJournalId;

    private AnalyzeManager analyzeManager;

    private StatisticStorage statisticStorage;

    private long feStartTime;

    private WorkGroupMgr workGroupMgr;

    private StarOSAgent starOSAgent;

    private ConnectorManager connectorManager;

    private CatalogManager catalogManager;

    private MetadataManager metadataManager;

    private static class SingletonHolder {
        private static final GlobalStateMgr INSTANCE = new GlobalStateMgr();
    }

    public long getFeStartTime() {
        return feStartTime;
    }

    private GlobalStateMgr() {
        this(false);
    }

    private GlobalStateMgr(boolean isCheckpoint) {
        this.load = new Load();
        this.exportMgr = new ExportMgr();
        this.routineLoadManager = new RoutineLoadManager();
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
        this.nodeMgr = new NodeMgr(isCheckpoint);
        this.localMetastore = new LocalMetastore();
        this.workGroupMgr = new WorkGroupMgr(this);
        this.esRepository = new EsRepository();
        this.starRocksRepository = new StarRocksRepository();
        this.hiveRepository = new HiveRepository();

        this.metastoreEventsProcessor = new MetastoreEventsProcessor(hiveRepository);

        this.replayedJournalId = new AtomicLong(0L);
        this.isDefaultClusterCreated = false;
        this.brokerMgr = new BrokerMgr();
        this.resourceMgr = new ResourceMgr();
        this.globalTransactionMgr = new GlobalTransactionMgr(this);
        this.tabletStatMgr = new TabletStatMgr();
        this.tabletInvertedIndex = new TabletInvertedIndex();
        this.colocateTableIndex = new ColocateTableIndex();
        this.recycleBin = new CatalogRecycleBin();
        this.functionSet = new FunctionSet();
        this.functionSet.init();

        this.metaReplayState = new MetaReplayState();

        this.feType = FrontendNodeType.INIT;
        this.synchronizedTimeMs = 0;
        this.typeTransferQueue = Queues.newLinkedBlockingDeque();
        this.journalObservable = new JournalObservable();
        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();
        this.tabletScheduler = new TabletScheduler(this, nodeMgr.getSystemInfo(), tabletInvertedIndex, stat);
        this.tabletChecker = new TabletChecker(this, nodeMgr.getSystemInfo(), tabletScheduler, stat);

        this.pendingLoadTaskScheduler =
                new MasterTaskExecutor("pending_load_task_scheduler", Config.async_load_task_pool_size,
                        Config.desired_max_waiting_jobs, !isCheckpoint);
        // One load job will be split into multiple loading tasks, the queue size is not determined, so set Integer.MAX_VALUE.
        this.loadingLoadTaskScheduler =
                new MasterTaskExecutor("loading_load_task_scheduler", Config.async_load_task_pool_size,
                        Integer.MAX_VALUE, !isCheckpoint);

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
        this.auth = new Auth();
        this.domainResolver = new DomainResolver(auth);
        this.analyzeManager = new AnalyzeManager();
        this.starOSAgent = new StarOSAgent();

        this.metadataManager = new MetadataManager(localMetastore);
        this.catalogManager = new CatalogManager();
        this.connectorManager = new ConnectorManager(metadataManager);
        connectorManager.init();
    }

    // Use tryLock to avoid potential dead lock
    public boolean tryLock(boolean mustLock) {
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

    public void unlock() {
        if (lock.isHeldByCurrentThread()) {
            this.lock.unlock();
        }
    }

    public ConnectorManager getConnectorManager() {
        return connectorManager;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public static int getCurrentCatalogStarRocksJournalVersion() {
        return MetaContext.get().getStarRocksMetaVersion();
    }



    public List<Long> getDbIds() {
        return Lists.newArrayList(localMetastore.getDbIds());
    }

    public void setHaProtocol(HAProtocol protocol) {
        this.haProtocol = protocol;
    }
    public static AuditEventProcessor getCurrentAuditEventProcessor() {
        return getCurrentState().getAuditEventProcessor();
    }

    public static AnalyzeManager getCurrentAnalyzeMgr() {
        return getCurrentState().getAnalyzeManager();
    }

    private void setMetaDir() {
        this.metaDir = Config.meta_dir;
        this.bdbDir = this.metaDir + BDB_DIR;
        this.imageDir = this.metaDir + IMAGE_DIR;
        nodeMgr.setImageDir(imageDir);
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public static GlobalStateMgr getServingCatalog() {
        return GlobalStateMgr.SingletonHolder.INSTANCE;
    }

    // use this to get correct Catalog's journal version
    public static int getCurrentCatalogJournalVersion() {
        return MetaContext.get().getMetaVersion();
    }

    public void installPlugin(InstallPluginStmt stmt) throws UserException, IOException {
        pluginMgr.installPlugin(stmt);
    }

    public void backup(BackupStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    // entry of checking tablets operation
    public void checkTablets(AdminCheckTabletsStmt stmt) {
        AdminCheckTabletsStmt.CheckType type = stmt.getType();
        if (type == AdminCheckTabletsStmt.CheckType.CONSISTENCY) {
            consistencyChecker.addTabletsToCheck(stmt.getTabletIds());
        }
    }

    public List<String> getClusterNames() {
        return localMetastore.getClusterNames();
    }

    public static GlobalTransactionMgr getCurrentGlobalTransactionMgr() {
        return getCurrentState().globalTransactionMgr;
    }

    public static StatisticStorage getCurrentStatisticStorage() {
        return getCurrentState().statisticStorage;
    }


    public void initialize(String[] args) throws Exception {
        // set meta dir first.
        // we already set these variables in constructor. but Catalog is a singleton class.
        // so they may be set before Config is initialized.
        // set them here again to make sure these variables use values in fe.conf.

        setMetaDir();

        // 0. get local node and helper node info
        nodeMgr.getCheckedSelfHostPort();
        nodeMgr.getHelperNodes(args);

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
        nodeMgr.getClusterIdAndRole();

        // 3. Load image first and replay edits
        this.editLog = new EditLog(nodeMgr.getNodeName());
        loadImage(imageDir);

        editLog.open();
        this.globalTransactionMgr.setEditLog(editLog);
        this.idGenerator.setEditLog(editLog);
        this.localMetastore.setEditLog(editLog);

        // 4. create load and export job label cleaner thread
        createLabelCleaner();

        // 5. create txn timeout checker thread
        createTxnTimeoutChecker();

        // 6. start state listener thread
        createStateListener();
        listener.start();
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
            return GlobalStateMgr.SingletonHolder.INSTANCE;
        }
    }

    public static final boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
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

    public HAProtocol getHaProtocol() {
        return this.haProtocol;
    }

    public Pair<String, Integer> getSelfNode() {
        return nodeMgr.getSelfNode();
    }

    public void setImageJournalId(long imageJournalId) {
        this.imageJournalId = imageJournalId;
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
            nodeMgr.addHelperSockets();
        }

        if (replayer == null) {
            createReplayer();
            replayer.start();
        }

        startNonMasterDaemonThreads();

        MetricRepo.init();
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
                    // Copy the missing log files from a member of the
                    // replication group who owns the files
                    LOG.error("catch insufficient log exception. please restart.", insufficientLogEx);
                    NetworkRestore restore = new NetworkRestore();
                    NetworkRestoreConfig config = new NetworkRestoreConfig();
                    config.setRetainLogFiles(false);
                    restore.execute(insufficientLogEx, config);
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
                localMetastore.initDefaultCluster();
            }

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
        consistencyChecker.start();
        // Backup handler
        backupHandler.start();
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

    public Long getMaxJournalId() {
        return this.editLog.getMaxJournalId();
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

    public void setNextId(long id) {
        idGenerator.setId(id);
    }

    public Load getLoadInstance() {
        return this.load;
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
        LOG.info("start load image from {}. is ckpt: {}", curFile.getAbsolutePath(), isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));

        long checksum = 0;
        long remoteChecksum = 0;

        try {
            checksum = loadHeader(dis, checksum);
            checksum = nodeMgr.loadMasterInfo(dis, checksum);
            checksum = nodeMgr.loadFrontends(dis, checksum);
            checksum = nodeMgr.getSystemInfo().loadBackends(dis, checksum);
            checksum = localMetastore.loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            localMetastore.recreateTabletInvertIndex();
            // rebuild es state state
            esRepository.loadTableFromCatalog();
            starRocksRepository.loadTableFromCatalog();

            checksum = load.loadJob(dis, checksum);
            checksum = loadAlterJob(dis, checksum);
            checksum = recycleBin.loadRecycleBin(dis, checksum);
            checksum = VariableMgr.loadGlobalVariable(dis, checksum);
            checksum = localMetastore.loadCluster(dis, checksum);
            checksum = brokerMgr.loadBrokers(dis, checksum);
            checksum = loadResources(dis, checksum);
            checksum = exportMgr.loadExportJob(dis, checksum);
            checksum = loadBackupHandler(dis, checksum);
            checksum = auth.loadAuth(dis, checksum);
            // global transaction must be replayed before load jobs v2
            checksum = globalTransactionMgr.loadTransactionState(dis, checksum);
            checksum = colocateTableIndex.loadColocateTableIndex(dis, checksum);
            checksum = routineLoadManager.loadRoutineLoadJobs(dis, checksum);
            checksum = loadManager.loadLoadJobsV2(dis, checksum);
            checksum = smallFileMgr.loadSmallFiles(dis, checksum);
            checksum = pluginMgr.loadPlugins(dis, checksum);
            checksum = deleteHandler.loadDeleteHandler(dis, checksum);

            remoteChecksum = dis.readLong();
            checksum = analyzeManager.loadAnalyze(dis, checksum);
            remoteChecksum = dis.readLong();
            checksum = workGroupMgr.loadWorkGroups(dis, checksum);
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
        for (AlterJob.JobType type : AlterJob.JobType.values()) {
            if (type == AlterJob.JobType.DECOMMISSION_BACKEND) {
                if (getCurrentCatalogJournalVersion() >= 5) {
                    newChecksum = loadAlterJob(dis, newChecksum, type);
                }
            } else {
                newChecksum = loadAlterJob(dis, newChecksum, type);
            }
        }
        LOG.info("finished replay alterJob from image");
        return newChecksum;
    }

    public boolean canRead() {
        return this.canRead.get();
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
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

    public boolean isElectable() {
        return nodeMgr.isElectable();
    }

    public long loadAlterJob(DataInputStream dis, long checksum, AlterJob.JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = null;
        Map<Long, AlterJobV2> alterJobsV2 = Maps.newHashMap();
        if (type == AlterJob.JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == AlterJob.JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        } else if (type == AlterJob.JobType.DECOMMISSION_BACKEND) {
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
            Database db = localMetastore.getDb(job.getDbId());
            // should check job state here because the job is finished but not removed from alter jobs list
            if (db != null && (job.getState() == com.starrocks.alter.AlterJob.JobState.PENDING
                    || job.getState() == com.starrocks.alter.AlterJob.JobState.RUNNING)) {
                job.replayInitJob(db);
            }
        }

        if (getCurrentCatalogJournalVersion() >= 2) {
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
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_61) {
            size = dis.readInt();
            newChecksum ^= size;
            for (int i = 0; i < size; i++) {
                AlterJobV2 alterJobV2 = AlterJobV2.read(dis);
                if (type == AlterJob.JobType.ROLLUP || type == AlterJob.JobType.SCHEMA_CHANGE) {
                    if (type == AlterJob.JobType.ROLLUP) {
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

    public SchemaChangeHandler getSchemaChangeHandler() {
        return (SchemaChangeHandler) this.alter.getSchemaChangeHandler();
    }

    public MaterializedViewHandler getRollupHandler() {
        return (MaterializedViewHandler) this.alter.getMaterializedViewHandler();
    }

    public SystemHandler getClusterHandler() {
        return (SystemHandler) this.alter.getClusterHandler();
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
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

    // the invoker should keep db write lock
    public void modifyTableColocate(Database db, OlapTable table, String colocateGroup, boolean isReplay,
                                    ColocateTableIndex.GroupId assignedGroupId)
            throws DdlException {

        String oldGroup = table.getColocateGroup();
        ColocateTableIndex.GroupId groupId = null;
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




    public void replayGlobalVariableV2(GlobalVarPersistInfo info) throws IOException, DdlException {
        VariableMgr.replayGlobalVariableV2(info);
    }

    public void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        VariableMgr.replayGlobalVariable(variable);
    }

    public boolean isMaster() {
        return feType == FrontendNodeType.MASTER;
    }

    public LocalMetastore getLocalMetastore() {
        return localMetastore;
    }

    public GlobalTransactionMgr getGlobalTransactionMgr() {
        return globalTransactionMgr;
    }

    public NodeMgr getNodeMgr() {
        return nodeMgr;
    }

    // Get the next available, need't lock because of nextId is atomic.
    public long getNextId() {
        return idGenerator.getNextId();
    }

    public TabletInvertedIndex getTabletInvertedIndex() {
        return tabletInvertedIndex;
    }

    public static CatalogRecycleBin getCurrentRecycleBin() {
        return getCurrentState().getRecycleBin();
    }

    private CatalogRecycleBin getRecycleBin() {
        return this.recycleBin;
    }

    // use this to get correct ColocateTableIndex instance
    public static ColocateTableIndex getCurrentColocateIndex() {
        return getCurrentState().getColocateTableIndex();
    }

    public ColocateTableIndex getColocateTableIndex() {
        return this.colocateTableIndex;
    }

    public DynamicPartitionScheduler getDynamicPartitionScheduler() {
        return this.dynamicPartitionScheduler;
    }

    public SystemInfoService getClusterInfo() {
        return nodeMgr.getSystemInfo();
    }

    public StarOSAgent getStarOSAgent() {
        return starOSAgent;
    }

    // use this to get correct ClusterInfoService instance
    public static SystemInfoService getCurrentSystemInfo() {
        return getCurrentState().getClusterInfo();
    }

    // use this to get correct TabletInvertedIndex instance
    public static TabletInvertedIndex getCurrentInvertedIndex() {
        return getCurrentState().getTabletInvertedIndex();
    }

    public Alter getAlterInstance() {
        return this.alter;
    }

    public Auth getAuth() {
        return auth;
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

    public void setDefaultClusterCreated(boolean defaultClusterCreated) {
        isDefaultClusterCreated = defaultClusterCreated;
    }

    public long loadResources(DataInputStream in, long checksum) throws IOException {
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_87) {
            resourceMgr = ResourceMgr.read(in);
        }
        LOG.info("finished replay resources from image");
        return checksum;
    }

    public long loadCatalogs(DataInputStream in, long checksum) throws IOException {
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_87) {
            catalogManager = CatalogManager.read(in);
        }
        LOG.info("finished replay resources from image");
        return checksum;
    }

    public long loadBackupHandler(DataInputStream dis, long checksum) throws IOException {
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_42) {
            backupHandler.readFields(dis);
        }
        backupHandler.setStateMgr(this);
        LOG.info("finished replay backupHandler from image");
        return checksum;
    }

    public DeleteHandler getDeleteHandler() {
        return deleteHandler;
    }

    public void setDeleteHandler(DeleteHandler deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    public void createLabelCleaner() {
        labelCleaner = new MasterDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                clearExpiredJobs();
            }
        };
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
        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(curFile))) {
            checksum = saveHeader(dos, replayedJournalId, checksum);
            checksum = nodeMgr.saveMasterInfo(dos, checksum);
            checksum = nodeMgr.saveFrontends(dos, checksum);
            checksum = nodeMgr.getSystemInfo().saveBackends(dos, checksum);
            checksum = localMetastore.saveDb(dos, checksum);
            checksum = load.saveLoadJob(dos, checksum);
            checksum = saveAlterJob(dos, checksum);
            checksum = recycleBin.saveRecycleBin(dos, checksum);
            checksum = saveGlobalVariable(dos, checksum);
            checksum = localMetastore.saveCluster(dos, checksum);
            checksum = nodeMgr.saveBrokers(dos, checksum);
            checksum = saveResources(dos, checksum);
            checksum = exportMgr.saveExportJob(dos, checksum);
            checksum = saveBackupHandler(dos, checksum);
            checksum = saveAuth(dos, checksum);
            checksum = globalTransactionMgr.saveTransactionState(dos, checksum);
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

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public long saveWorkGroups(DataOutputStream dos, long checksum) throws IOException {
        workGroupMgr.write(dos);
        return checksum;
    }

    public long saveAnalyze(DataOutputStream dos, long checksum) throws IOException {
        analyzeManager.write(dos);
        return checksum;
    }

    public long saveDeleteHandler(DataOutputStream dos, long checksum) throws IOException {
        getDeleteHandler().write(dos);
        return checksum;
    }

    public long savePlugins(DataOutputStream dos, long checksum) throws IOException {
        pluginMgr.write(dos);
        return checksum;
    }

    private long saveSmallFiles(DataOutputStream out, long checksum) throws IOException {
        smallFileMgr.write(out);
        return checksum;
    }

    public long saveLoadJobsV2(DataOutputStream out, long checksum) throws IOException {
        getLoadManager().write(out);
        return checksum;
    }

    public long saveRoutineLoadJobs(DataOutputStream dos, long checksum) throws IOException {
        getRoutineLoadManager().write(dos);
        return checksum;
    }

    public long saveColocateTableIndex(DataOutputStream dos, long checksum) throws IOException {
        colocateTableIndex.write(dos);
        return checksum;
    }

    public long saveAuth(DataOutputStream dos, long checksum) throws IOException {
        auth.write(dos);
        return checksum;
    }

    public long saveBackupHandler(DataOutputStream dos, long checksum) throws IOException {
        getBackupHandler().write(dos);
        return checksum;
    }

    public long saveGlobalVariable(DataOutputStream out, long checksum) throws IOException {
        VariableMgr.write(out);
        return checksum;
    }

    public long saveResources(DataOutputStream out, long checksum) throws IOException {
        resourceMgr.write(out);
        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum) throws IOException {
        for (AlterJob.JobType type : AlterJob.JobType.values()) {
            checksum = saveAlterJob(dos, checksum, type);
        }
        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum, AlterJob.JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = null;
        Map<Long, AlterJobV2> alterJobsV2 = Maps.newHashMap();
        if (type == AlterJob.JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getRollupHandler().getAlterJobsV2();
        } else if (type == AlterJob.JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        } else if (type == AlterJob.JobType.DECOMMISSION_BACKEND) {
            alterJobs = this.getClusterHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getClusterHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        }

        // alter jobs
        int size = alterJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (Map.Entry<Long, AlterJob> entry : alterJobs.entrySet()) {
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

    public Database getDbIncludeRecycleBin(long dbId) {
        Database db = localMetastore.getDb(dbId);
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

    public int getClusterId() {
        return nodeMgr.getClusterId();
    }

    public Cluster getCluster(String clusterName) {
        return localMetastore.getCluster(clusterName);
    }

    public String dumpImage() {
        LOG.info("begin to dump meta data");
        String dumpFilePath;
        Map<Long, Database> lockedDbMap = Maps.newTreeMap();
        tryLock(true);
        try {
            // sort all dbs
            for (long dbId : localMetastore.getDbIds()) {
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

    public void createTxnTimeoutChecker() {
        txnTimeoutChecker = new MasterDaemon("txnTimeoutChecker", Config.transaction_clean_interval_second) {
            @Override
            protected void runAfterCatalogReady() {
                globalTransactionMgr.abortTimeoutTxns();
            }
        };
    }

    public Database getDb(String name) {
        return localMetastore.getDb(name);
    }

    public Database getDb(Long id) {
        return localMetastore.getDb(id);
    }

    public EditLog getEditLog() {
        return editLog;
    }

    public String getMetaDir() {
        return metaDir;
    }

    public String getBdbDir() {
        return bdbDir;
    }

    public String getImageDir() {
        return imageDir;
    }

    public boolean isDefaultClusterCreated() {
        return isDefaultClusterCreated;
    }

    public FrontendNodeType getFeType() {
        return feType;
    }

    public long getSynchronizedTimeMs() {
        return synchronizedTimeMs;
    }

    public AtomicBoolean getIsReady() {
        return isReady;
    }

    public AtomicBoolean getCanRead() {
        return canRead;
    }

    public BlockingQueue<FrontendNodeType> getTypeTransferQueue() {
        return typeTransferQueue;
    }

    public MetaContext getMetaContext() {
        return metaContext;
    }

    public long getEpoch() {
        return epoch;
    }

    public QueryableReentrantLock getLock() {
        return lock;
    }

    public Load getLoad() {
        return load;
    }

    public LoadManager getLoadManager() {
        return loadManager;
    }

    public RoutineLoadManager getRoutineLoadManager() {
        return routineLoadManager;
    }

    public ExportMgr getExportMgr() {
        return exportMgr;
    }

    public Alter getAlter() {
        return alter;
    }

    public ConsistencyChecker getConsistencyChecker() {
        return consistencyChecker;
    }

    public BackupHandler getBackupHandler() {
        return backupHandler;
    }

    public PublishVersionDaemon getPublishVersionDaemon() {
        return publishVersionDaemon;
    }

    public UpdateDbUsedDataQuotaDaemon getUpdateDbUsedDataQuotaDaemon() {
        return updateDbUsedDataQuotaDaemon;
    }

    public MasterDaemon getLabelCleaner() {
        return labelCleaner;
    }

    public MasterDaemon getTxnTimeoutChecker() {
        return txnTimeoutChecker;
    }

    public Daemon getReplayer() {
        return replayer;
    }

    public Daemon getTimePrinter() {
        return timePrinter;
    }

    public Daemon getListener() {
        return listener;
    }

    public EsRepository getEsRepository() {
        return esRepository;
    }

    public StarRocksRepository getStarRocksRepository() {
        return starRocksRepository;
    }

    public HiveRepository getHiveRepository() {
        return hiveRepository;
    }

    public MetastoreEventsProcessor getMetastoreEventsProcessor() {
        return metastoreEventsProcessor;
    }

    public CatalogIdGenerator getIdGenerator() {
        return idGenerator;
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    public long getReplayedJournalId() {
        return this.replayedJournalId.get();
    }

    public static GlobalStateMgr getCHECKPOINT() {
        return CHECKPOINT;
    }

    public static long getCheckpointThreadId() {
        return checkpointThreadId;
    }

    public Checkpoint getCheckpointer() {
        return checkpointer;
    }

    public FunctionSet getFunctionSet() {
        return functionSet;
    }

    public MetaReplayState getMetaReplayState() {
        return metaReplayState;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    public ResourceMgr getResourceMgr() {
        return resourceMgr;
    }

    public TabletStatMgr getTabletStatMgr() {
        return tabletStatMgr;
    }

    public DomainResolver getDomainResolver() {
        return domainResolver;
    }

    public TabletSchedulerStat getStat() {
        return stat;
    }

    public TabletScheduler getTabletScheduler() {
        return tabletScheduler;
    }

    public TabletChecker getTabletChecker() {
        return tabletChecker;
    }

    public MasterTaskExecutor getPendingLoadTaskScheduler() {
        return pendingLoadTaskScheduler;
    }

    public MasterTaskExecutor getLoadingLoadTaskScheduler() {
        return loadingLoadTaskScheduler;
    }

    public LoadJobScheduler getLoadJobScheduler() {
        return loadJobScheduler;
    }

    public LoadTimeoutChecker getLoadTimeoutChecker() {
        return loadTimeoutChecker;
    }

    public LoadEtlChecker getLoadEtlChecker() {
        return loadEtlChecker;
    }

    public LoadLoadingChecker getLoadLoadingChecker() {
        return loadLoadingChecker;
    }

    public RoutineLoadScheduler getRoutineLoadScheduler() {
        return routineLoadScheduler;
    }

    public RoutineLoadTaskScheduler getRoutineLoadTaskScheduler() {
        return routineLoadTaskScheduler;
    }

    public SmallFileMgr getSmallFileMgr() {
        return smallFileMgr;
    }

    public PluginMgr getPluginMgr() {
        return pluginMgr;
    }

    public AuditEventProcessor getAuditEventProcessor() {
        return auditEventProcessor;
    }

    public StatisticsMetaManager getStatisticsMetaManager() {
        return statisticsMetaManager;
    }

    public StatisticAutoCollector getStatisticAutoCollector() {
        return statisticAutoCollector;
    }

    public long getImageJournalId() {
        return imageJournalId;
    }

    public AnalyzeManager getAnalyzeManager() {
        return analyzeManager;
    }

    public StatisticStorage getStatisticStorage() {
        return statisticStorage;
    }

    public WorkGroupMgr getWorkGroupMgr() {
        return workGroupMgr;
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

    public void restore(RestoreStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    public void cancelBackup(CancelBackupStmt stmt) throws DdlException {
        getBackupHandler().cancel(stmt);
    }


}
