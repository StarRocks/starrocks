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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.backup.BackupHandler;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.binlog.BinlogManager;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.BrokerTable;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DomainResolver;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HiveView;
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
import com.starrocks.catalog.RefreshDictionaryCacheTaskDaemon;
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
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.common.util.concurrent.QueryableReentrantLock;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.elasticsearch.EsRepository;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.connector.hive.events.MetastoreEventsProcessor;
import com.starrocks.consistency.ConsistencyChecker;
import com.starrocks.consistency.LockChecker;
import com.starrocks.credential.CredentialUtil;
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
import com.starrocks.load.pipe.PipeListener;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.load.pipe.PipeScheduler;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.routineload.RoutineLoadScheduler;
import com.starrocks.load.routineload.RoutineLoadTaskScheduler;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.meta.MetaContext;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthUpgrader;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AuthUpgradeInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.ImageHeader;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PhysicalPartitionPersistInfoV2;
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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockLoader;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.AuditEventProcessor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.JournalObservable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.VariableMgr;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.qe.scheduler.slot.SlotProvider;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MVJobExecutor;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt.QuotaType;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
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
import com.starrocks.sql.ast.DistributionDesc;
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
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.HeartbeatMgr;
import com.starrocks.system.PortConnectivityChecker;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTaskExecutor;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.UpdateDbUsedDataQuotaDaemon;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_TYPE_COLUMN;

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

    private Load load;
    private LoadMgr loadMgr;
    private RoutineLoadMgr routineLoadMgr;
    private StreamLoadMgr streamLoadMgr;
    private ExportMgr exportMgr;

    private ConsistencyChecker consistencyChecker;
    private BackupHandler backupHandler;
    private PublishVersionDaemon publishVersionDaemon;
    private DeleteMgr deleteMgr;
    private UpdateDbUsedDataQuotaDaemon updateDbUsedDataQuotaDaemon;

    private FrontendDaemon labelCleaner; // To clean old LabelInfo, ExportJobInfos
    private FrontendDaemon txnTimeoutChecker; // To abort timeout txns
    private FrontendDaemon taskCleaner;   // To clean expire Task/TaskRun
    private JournalWriter journalWriter; // leader only: write journal log
    private Daemon replayer;
    private Daemon timePrinter;
    private EsRepository esRepository;  // it is a daemon, so add it here
    private MetastoreEventsProcessor metastoreEventsProcessor;
    private ConnectorTableMetadataProcessor connectorTableMetadataProcessor;

    // set to true after finished replay all meta and ready to serve
    // set to false when globalStateMgr is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;

    // false if default_warehouse is not created.
    private boolean isDefaultWarehouseCreated = false;

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

    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

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
    private LockChecker lockChecker;

    private RoutineLoadScheduler routineLoadScheduler;
    private RoutineLoadTaskScheduler routineLoadTaskScheduler;

    private MVJobExecutor mvMVJobExecutor;

    private SmallFileMgr smallFileMgr;

    private DynamicPartitionScheduler dynamicPartitionScheduler;

    private PluginMgr pluginMgr;

    private AuditEventProcessor auditEventProcessor;

    private final StatisticsMetaManager statisticsMetaManager;

    private final StatisticAutoCollector statisticAutoCollector;

    private final SafeModeChecker safeModeChecker;

    private AnalyzeMgr analyzeMgr;

    private StatisticStorage statisticStorage;

    private long imageJournalId;

    private long feStartTime;

    private boolean isSafeMode = false;

    private ResourceGroupMgr resourceGroupMgr;

    private StarOSAgent starOSAgent;

    private StarMgrMetaSyncer starMgrMetaSyncer;

    private MetadataMgr metadataMgr;
    private CatalogMgr catalogMgr;
    private ConnectorMgr connectorMgr;
    private ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr;

    private TaskManager taskManager;
    private InsertOverwriteJobMgr insertOverwriteJobMgr;

    private LocalMetastore localMetastore;
    private GlobalFunctionMgr globalFunctionMgr;

    @Deprecated
    private ShardManager shardManager;

    private StateChangeExecution execution;

    private TaskRunStateSynchronizer taskRunStateSynchronizer;

    private BinlogManager binlogManager;

    // For LakeTable
    private CompactionMgr compactionMgr;

    private WarehouseManager warehouseMgr;

    private ConfigRefreshDaemon configRefreshDaemon;

    private StorageVolumeMgr storageVolumeMgr;

    private AutovacuumDaemon autovacuumDaemon;

    private PipeManager pipeManager;
    private PipeListener pipeListener;
    private PipeScheduler pipeScheduler;
    private MVActiveChecker mvActiveChecker;

    private ReplicationMgr replicationMgr;

    private final ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
    private final SlotManager slotManager = new SlotManager(resourceUsageMonitor);
    private final SlotProvider slotProvider = new SlotProvider();

    private final DictionaryMgr dictionaryMgr = new DictionaryMgr();
    private RefreshDictionaryCacheTaskDaemon refreshDictionaryCacheTaskDaemon;

    public NodeMgr getNodeMgr() {
        return nodeMgr;
    }

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

    public TNodesInfo createNodesInfo(Integer clusterId) {
        TNodesInfo nodesInfo = new TNodesInfo();
        SystemInfoService systemInfoService = getOrCreateSystemInfo(clusterId);
        // use default warehouse
        Warehouse warehouse = warehouseMgr.getDefaultWarehouse();
        // TODO: need to refactor after be split into cn + dn
        if (warehouse != null && RunMode.isSharedDataMode()) {
            com.starrocks.warehouse.Cluster cluster = warehouse.getAnyAvailableCluster();
            for (Long cnId : cluster.getComputeNodeIds()) {
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

    public SystemInfoService getClusterInfo() {
        return nodeMgr.getClusterInfo();
    }

    private HeartbeatMgr getHeartbeatMgr() {
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

    private GlobalStateMgr() {
        this(false);
    }

    // if isCkptGlobalState is true, it means that we should not collect thread pool metric
    private GlobalStateMgr(boolean isCkptGlobalState) {
        if (!isCkptGlobalState) {
            RunMode.detectRunMode();
        }

        if (RunMode.isSharedDataMode()) {
            this.starOSAgent = new StarOSAgent();
        }

        // System Manager
        this.nodeMgr = new NodeMgr();
        this.heartbeatMgr = new HeartbeatMgr(!isCkptGlobalState);
        this.portConnectivityChecker = new PortConnectivityChecker();

        // Alter Job Manager
        this.alterJobMgr = new AlterJobMgr();

        this.load = new Load();
        this.streamLoadMgr = new StreamLoadMgr();
        this.routineLoadMgr = new RoutineLoadMgr();
        this.exportMgr = new ExportMgr();

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

        this.isDefaultClusterCreated = false;

        this.resourceMgr = new ResourceMgr();

        this.globalTransactionMgr = new GlobalTransactionMgr(this);
        this.tabletStatMgr = new TabletStatMgr();
        initAuth(USING_NEW_PRIVILEGE);

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
        this.warehouseMgr = new WarehouseManager();
        this.connectorMgr = new ConnectorMgr();
        this.connectorTblMetaInfoMgr = new ConnectorTblMetaInfoMgr();
        this.metadataMgr = new MetadataMgr(localMetastore, connectorMgr, connectorTblMetaInfoMgr);
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

        getConfigRefreshDaemon().registerListener(() -> {
            try {
                if (Config.max_broker_load_job_concurrency != loadingLoadTaskScheduler.getCorePoolSize()) {
                    loadingLoadTaskScheduler.setPoolSize(Config.max_broker_load_job_concurrency);
                }
            } catch (Exception e) {
                LOG.warn("check config failed", e);
            }
        });

        this.replicationMgr = new ReplicationMgr();
        nodeMgr.registerLeaderChangeListener(slotProvider::leaderChangeListener);
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

    public AnalyzeMgr getAnalyzeMgr() {
        return analyzeMgr;
    }

    public Auth getAuth() {
        return auth;
    }

    public AuthenticationMgr getAuthenticationMgr() {
        return authenticationMgr;
    }

    public AuthorizationMgr getAuthorizationMgr() {
        return authorizationMgr;
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

    public static StarMgrMetaSyncer getCurrentStarMgrMetaSyncer() {
        return getCurrentState().getStarMgrMetaSyncer();
    }

    public static WarehouseManager getCurrentWarehouseMgr() {
        return getCurrentState().getWarehouseMgr();
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

    public static int getCurrentStateStarRocksMetaVersion() {
        return MetaContext.get().getStarRocksMetaVersion();
    }

    public static boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static PluginMgr getCurrentPluginMgr() {
        return getCurrentState().getPluginMgr();
    }

    public static AnalyzeMgr getCurrentAnalyzeMgr() {
        return getCurrentState().getAnalyzeMgr();
    }

    public static StatisticStorage getCurrentStatisticStorage() {
        return getCurrentState().statisticStorage;
    }

    public static TabletStatMgr getCurrentTabletStatMgr() {
        return getCurrentState().tabletStatMgr;
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

    public InsertOverwriteJobMgr getInsertOverwriteJobMgr() {
        return insertOverwriteJobMgr;
    }

    public WarehouseManager getWarehouseMgr() {
        return warehouseMgr;
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

    // Use tryLock to avoid potential deadlock
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
        this.imageDir = Config.meta_dir + IMAGE_DIR;
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
        if (RunMode.isSharedDataMode() && !starOSAgent.init(null)) {
            LOG.error("init starOSAgent failed");
            System.exit(-1);
        }
    }

    // set usingNewPrivilege = true in UT
    public void initAuth(boolean usingNewPrivilege) {
        this.auth = new Auth();
        this.usingNewPrivilege = new AtomicBoolean(usingNewPrivilege);
        if (usingNewPrivilege) {
            this.authenticationMgr = new AuthenticationMgr();
            this.domainResolver = new DomainResolver(authenticationMgr);
            this.authorizationMgr = new AuthorizationMgr(this, null);
            LOG.info("using new privilege framework..");
        } else {
            this.domainResolver = new DomainResolver(auth);
            this.authenticationMgr = null;
            this.authorizationMgr = null;
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
        return !authorizationMgr.isLoaded() || !authenticationMgr.isLoaded();
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

                // For follower/observer, defer setting auth to null when we have replayed all the journal,
                // because we may encounter old auth journal when replaying log in which case we still
                // need the auth object.
                if (isUsingNewPrivilege() && !needUpgradedToNewPrivilege()) {
                    // already upgraded, set auth = null
                    auth = null;
                }

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

            if (!isDefaultClusterCreated) {
                initDefaultCluster();
            }

            // MUST set leader ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-leader FE to push image
            nodeMgr.setLeaderInfo();

            if (USING_NEW_PRIVILEGE) {
                if (needUpgradedToNewPrivilege()) {
                    reInitializeNewPrivilegeOnUpgrade();
                    AuthUpgrader upgrader = new AuthUpgrader(auth, authenticationMgr, authorizationMgr, this);
                    // upgrade metadata in old privilege framework to the new one
                    upgrader.upgradeAsLeader();
                    this.domainResolver.setAuthenticationManager(authenticationMgr);
                }
                LOG.info("set usingNewPrivilege to true after transfer to leader");
                usingNewPrivilege.set(true);
                auth = null;  // remove references to useless objects to release memory
            }

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

        // heartbeat mgr
        heartbeatMgr.setLeader(nodeMgr.getClusterId(), nodeMgr.getToken(), epoch);
        heartbeatMgr.start();
        // New load scheduler
        pendingLoadTaskScheduler.start();
        loadingLoadTaskScheduler.start();
        loadMgr.prepareJobs();
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

        Map<SRMetaBlockID, SRMetaBlockLoader> loadImages = ImmutableMap.<SRMetaBlockID, SRMetaBlockLoader>builder()
                .put(SRMetaBlockID.NODE_MGR, nodeMgr::load)
                .put(SRMetaBlockID.LOCAL_META_STORE, localMetastore::load)
                .put(SRMetaBlockID.ALTER_MGR, alterJobMgr::load)
                .put(SRMetaBlockID.CATALOG_RECYCLE_BIN, recycleBin::load)
                .put(SRMetaBlockID.VARIABLE_MGR, VariableMgr::load)
                .put(SRMetaBlockID.RESOURCE_MGR, resourceMgr::loadResourcesV2)
                .put(SRMetaBlockID.EXPORT_MGR, exportMgr::loadExportJobV2)
                .put(SRMetaBlockID.BACKUP_MGR, backupHandler::loadBackupHandlerV2)
                .put(SRMetaBlockID.AUTH, auth::load)
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
                .put(SRMetaBlockID.MATERIALIZED_VIEW_MGR, MaterializedViewMgr.getInstance()::load)
                .put(SRMetaBlockID.GLOBAL_FUNCTION_MGR, globalFunctionMgr::load)
                .put(SRMetaBlockID.STORAGE_VOLUME_MGR, storageVolumeMgr::load)
                .put(SRMetaBlockID.DICTIONARY_MGR, dictionaryMgr::load)
                .put(SRMetaBlockID.REPLICATION_MGR, replicationMgr::load)
                .build();

        Set<SRMetaBlockID> metaMgrMustExists = new HashSet<>(loadImages.keySet());
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(curFile.toPath())))) {
            loadHeader(dis);
            while (true) {
                SRMetaBlockReader reader = new SRMetaBlockReader(dis);
                SRMetaBlockID srMetaBlockID = reader.getHeader().getSrMetaBlockID();

                try {
                    SRMetaBlockLoader imageLoader = loadImages.get(srMetaBlockID);
                    if (imageLoader == null) {
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

                    imageLoader.apply(reader);
                    metaMgrMustExists.remove(srMetaBlockID);
                    LOG.info("Success load StarRocks meta block " + srMetaBlockID + " from image");
                } catch (SRMetaBlockEOFException srMetaBlockEOFException) {
                    /*
                     * The number of json expected to be read is more than the number of json actually stored in the image
                     */
                    metaMgrMustExists.remove(srMetaBlockID);
                    LOG.warn("Got EOF exception, ignore, ", srMetaBlockEOFException);
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
        }

        if (isUsingNewPrivilege() && needUpgradedToNewPrivilege() && !isLeader() && !isCheckpointThread()) {
            LOG.warn("follower has to wait for leader to upgrade the privileges, set usingNewPrivilege = false for now");
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
                List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
                updateBaseTableRelatedMv(db.getId(), mv, baseTableInfos);
            }
        }

        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("finish processing all tables' related materialized views in {}ms", duration);
    }

    public void updateBaseTableRelatedMv(Long dbId, MaterializedView mv, List<BaseTableInfo> baseTableInfos) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table table;
            try {
                table = baseTableInfo.getTable();
            } catch (Exception e) {
                LOG.warn("there is an exception during get table from mv base table. exception:", e);
                continue;
            }
            if (table == null) {
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                        "the table {} was not exist.", mv.getName(), mv.getId(), baseTableInfo.getTableName());
                mv.setInactiveAndReason("base table dropped: " + baseTableInfo.getTableId());
                continue;
            }
            if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                MaterializedView baseMv = (MaterializedView) table;
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                "the materialized view{}({}) is invalid.", mv.getName(), mv.getId(),
                        baseMv.getName(), baseMv.getId());
                mv.setInactiveAndReason("base mv is not active: " + baseMv.getName());
                continue;
            }
            MvId mvId = new MvId(dbId, mv.getId());
            table.addRelatedMaterializedView(mvId);
            if (!table.isNativeTableOrMaterializedView()) {
                connectorTblMetaInfoMgr.addConnectorTableInfo(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), baseTableInfo.getTableIdentifier(),
                        ConnectorTableInfo.builder().setRelatedMaterializedViews(
                                Sets.newHashSet(mvId)).build());
            }
        }
    }

    private void checkOpTypeValid() throws IOException {
        try {
            for (Field field : OperationType.class.getDeclaredFields()) {
                short id = field.getShort(null);
                if (id > OperationType.OP_TYPE_EOF) {
                    throw new IOException("OperationType cannot use a value exceeding 20000, " +
                            "and an error will be reported if it exceeds : " + field.getName() + " = " + id);
                }
            }
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
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
        checkOpTypeValid();
        ImageHeader header = GsonUtils.GSON.fromJson(Text.readString(dis), ImageHeader.class);
        idGenerator.setId(header.getBatchEndId());
        isDefaultClusterCreated = header.isDefaultClusterCreated();
        LOG.info("finished to replay header from image");
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

        long saveImageStartTime = System.currentTimeMillis();
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(curFile.toPath()))) {
            try {
                saveHeader(dos);
                nodeMgr.save(dos);
                localMetastore.save(dos);
                alterJobMgr.save(dos);
                recycleBin.save(dos);
                VariableMgr.save(dos);
                resourceMgr.saveResourcesV2(dos);
                exportMgr.saveExportJobV2(dos);
                backupHandler.saveBackupHandlerV2(dos);
                auth.save(dos);
                globalTransactionMgr.saveTransactionStateV2(dos);
                colocateTableIndex.saveColocateTableIndexV2(dos);
                routineLoadMgr.saveRoutineLoadJobsV2(dos);
                loadMgr.saveLoadJobsV2JsonFormat(dos);
                smallFileMgr.saveSmallFilesV2(dos);
                pluginMgr.save(dos);
                deleteMgr.save(dos);
                analyzeMgr.save(dos);
                resourceGroupMgr.save(dos);
                authenticationMgr.saveV2(dos);
                authorizationMgr.saveV2(dos);
                taskManager.saveTasksV2(dos);
                catalogMgr.save(dos);
                insertOverwriteJobMgr.save(dos);
                compactionMgr.save(dos);
                streamLoadMgr.save(dos);
                MaterializedViewMgr.getInstance().save(dos);
                globalFunctionMgr.save(dos);
                storageVolumeMgr.save(dos);
                dictionaryMgr.save(dos);
                replicationMgr.save(dos);
            } catch (SRMetaBlockException e) {
                LOG.error("Save meta block failed ", e);
                throw new IOException("Save meta block failed ", e);
            }

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
        header.setDefaultClusterCreated(isDefaultClusterCreated);
        Text.writeString(dos, GsonUtils.GSON.toJson(header));
    }

    public void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        VariableMgr.replayGlobalVariable(variable);
    }

    public void replayGlobalVariableV2(GlobalVarPersistInfo info) throws IOException, DdlException {
        VariableMgr.replayGlobalVariableV2(info);
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
            }
        };
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
                EditLog.loadJournal(this, entity);
            } catch (Throwable e) {
                if (canSkipBadReplayedJournal()) {
                    LOG.error("!!! DANGER: SKIP JOURNAL {}: {} !!!",
                            replayedJournalId.incrementAndGet(),
                            entity == null ? null : entity.getData(),
                            e);
                    if (!readSucc) {
                        cursor.skipNext();
                    }
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
        timePrinter = new FrontendDaemon("timePrinter", 10 * 1000L) {
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

    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return localMetastore.createTable(stmt);
    }

    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        localMetastore.createTable(stmt.getCreateTableStmt());
    }

    public void addSubPartitions(Database db, String tableName, Partition partition, int num) throws DdlException {
        localMetastore.addSubPartitions(db, tableName, partition, num);
    }

    public void replayAddSubPartition(PhysicalPartitionPersistInfoV2 info) throws DdlException {
        localMetastore.replayAddSubPartition(info);
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
        if (table.isMaterializedView()) {
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
                colSb.append("`" + column.getName() + "`");
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
            if (table.isOlapOrCloudNativeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
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
        if (table.isOlapOrCloudNativeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }

        sb.append("\n) ENGINE=");
        sb.append(table.getType() == TableType.CLOUD_NATIVE ? "OLAP" : table.getType().name()).append(" ");

        if (table.isOlapOrCloudNativeTable() || table.getType() == TableType.OLAP_EXTERNAL) {
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

            String partitionDuration =
                    olapTable.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION);
            if (partitionDuration != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)
                        .append("\" = \"")
                        .append(partitionDuration).append("\"");
            }

            if (olapTable.getAutomaticBucketSize() > 0) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE)
                        .append("\" = \"")
                        .append(olapTable.getAutomaticBucketSize()).append("\"");
            }

            if (table.isCloudNativeTable()) {
                Map<String, String> storageProperties = olapTable.getProperties();

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE)).append("\"");

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)).append("\"");

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK)
                        .append("\" = \"");
                sb.append(storageProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK)).append("\"");

                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)
                        .append("\" = \"");
                sb.append(olapTable.enablePersistentIndex()).append("\"");

                if (olapTable.enablePersistentIndex() && !Strings.isNullOrEmpty(olapTable.getPersistentIndexTypeString())) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE)
                            .append("\" = \"");
                    sb.append(olapTable.getPersistentIndexTypeString()).append("\"");
                }
            } else {
                // in memory
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_INMEMORY)
                        .append("\" = \"");
                sb.append(olapTable.isInMemory()).append("\"");

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

                // show fastSchemaEvolution only when it is set true
                if (olapTable.getUseFastSchemaEvolution()) {
                    sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_USE_FAST_SCHEMA_EVOLUTION).append("\" = \"");
                    sb.append(olapTable.getUseFastSchemaEvolution()).append("\"");
                }

                // storage media
                Map<String, String> properties = olapTable.getTableProperty().getProperties();

                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
                            .append("\" = \"");
                    sb.append(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)).append("\"");
                }

                String storageCoolDownTTL =
                        olapTable.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
                if (storageCoolDownTTL != null) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                            .append(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)
                            .append("\" = \"")
                            .append(storageCoolDownTTL).append("\"");
                }

                // partition live number
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)
                            .append("\" = \"");
                    sb.append(properties.get(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)).append("\"");
                }

                // unique constraint
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                        && !Strings.isNullOrEmpty(properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT))) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                            .append("\" = \"");
                    sb.append(properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)).append("\"");
                }

                // foreign key constraint
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                        && !Strings.isNullOrEmpty(properties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT))) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                            .append("\" = \"");
                    sb.append(ForeignKeyConstraint.getShowCreateTableConstraintDesc(olapTable.getForeignKeyConstraints()))
                            .append("\"");
                }

                // store type
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
                    if (olapTable.storageType() != null &&
                            !PROPERTIES_STORAGE_TYPE_COLUMN.equalsIgnoreCase(olapTable.storageType())) {
                        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                                .append(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)
                                .append("\" = \"");

                        sb.append(olapTable.storageType()).append("\"");
                    }
                }
            }

            if (olapTable.primaryIndexCacheExpireSec() > 0) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC)
                        .append("\" = \"");
                sb.append(olapTable.primaryIndexCacheExpireSec()).append("\"");
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
            addTableComment(sb, table);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getDbName()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getTableName()).append("\",\n");
            sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\"");
            if (!hiveTable.getProperties().isEmpty()) {
                sb.append(",\n");
            }
            sb.append(new PrintableMap<>(hiveTable.getProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.FILE) {
            FileTable fileTable = (FileTable) table;
            Map<String, String> clonedFileProperties = new HashMap<>(fileTable.getFileProperties());
            CredentialUtil.maskCredential(clonedFileProperties);
            addTableComment(sb, table);

            sb.append("\nPROPERTIES (\n");
            sb.append(new PrintableMap<>(clonedFileProperties, " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.HUDI) {
            HudiTable hudiTable = (HudiTable) table;
            addTableComment(sb, table);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hudiTable.getDbName()).append("\",\n");
            sb.append("\"table\" = \"").append(hudiTable.getTableName()).append("\",\n");
            sb.append("\"resource\" = \"").append(hudiTable.getResourceName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.ICEBERG) {
            IcebergTable icebergTable = (IcebergTable) table;
            addTableComment(sb, table);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(icebergTable.getRemoteDbName()).append("\",\n");
            sb.append("\"table\" = \"").append(icebergTable.getRemoteTableName()).append("\",\n");
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
                && ((OlapTable) table).getPartitionInfo().isRangePartition()
                && table.getPartitions().size() > 1) {
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

    public void replayCreateTable(CreateTableInfo info) {
        localMetastore.replayCreateTable(info);
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

    public void replayBatchDeleteReplica(BatchDeleteReplicaInfo info) {
        localMetastore.replayBatchDeleteReplica(info);
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

    public Optional<Table> mayGetTable(long dbId, long tableId) {
        return mayGetDb(dbId).flatMap(db -> db.tryGetTable(tableId));
    }

    public Optional<Database> mayGetDb(String name) {
        return Optional.ofNullable(localMetastore.getDb(name));
    }

    public Optional<Database> mayGetDb(long dbId) {
        return Optional.ofNullable(localMetastore.getDb(dbId));
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

    // Get the next available, lock-free because nextId is atomic.
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

    public AlterJobMgr getAlterJobMgr() {
        return this.alterJobMgr;
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return (SchemaChangeHandler) this.alterJobMgr.getSchemaChangeHandler();
    }

    public MaterializedViewHandler getRollupHandler() {
        return (MaterializedViewHandler) this.alterJobMgr.getMaterializedViewHandler();
    }

    public BackupHandler getBackupHandler() {
        return this.backupHandler;
    }

    public DeleteMgr getDeleteMgr() {
        return this.deleteMgr;
    }

    public Load getLoadInstance() {
        return this.load;
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

    public Pair<String, Integer> getLeaderIpAndRpcPort() {
        return nodeMgr.getLeaderIpAndRpcPort();
    }

    public Pair<String, Integer> getLeaderIpAndHttpPort() {
        return nodeMgr.getLeaderIpAndHttpPort();
    }

    public String getLeaderIp() {
        return nodeMgr.getLeaderIp();
    }

    public EsRepository getEsRepository() {
        return this.esRepository;
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
        this.alterJobMgr.replayRenameMaterializedView(log);
    }

    public void replayChangeMaterializedViewRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        this.alterJobMgr.replayChangeMaterializedViewRefreshScheme(log);
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        this.alterJobMgr.replayAlterMaterializedViewProperties(opCode, log);
    }

    public void replayAlterMaterializedViewStatus(AlterMaterializedViewStatusLog log) {
        this.alterJobMgr.replayAlterMaterializedViewStatus(log);
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

    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        localMetastore.alterTableComment(db, table, clause);
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

    public void replayRenameColumn(ColumnRenameInfo columnRenameInfo) throws DdlException {
        localMetastore.replayRenameColumn(columnRenameInfo);
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        localMetastore.renameColumn(db, table, renameClause);
    }

    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.modifyTableDynamicPartition(db, table, properties);
    }

    public void modifyTableReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.modifyTableReplicationNum(db, table, properties);
    }

    public void alterTableProperties(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        localMetastore.alterTableProperties(db, table, properties);
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

    public void modifyTableConstraint(Database db, String tableName, Map<String, String> properties) throws DdlException {
        localMetastore.modifyTableConstraint(db, tableName, properties);
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
        return this.alterJobMgr.processAlterCluster(stmt);
    }

    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        this.alterJobMgr.getClusterHandler().cancel(stmt);
    }

    // Change current warehouse of this session.
    public void changeWarehouse(ConnectContext ctx, String newWarehouseName) throws AnalysisException {
        if (!warehouseMgr.warehouseExists(newWarehouseName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_WAREHOUSE_ERROR, newWarehouseName);
        }
        ctx.setCurrentWarehouse(newWarehouseName);
    }

    // Change current catalog of this session, and reset current database.
    // We can support "use 'catalog <catalog_name>'" from mysql client or "use catalog <catalog_name>" from jdbc.
    public void changeCatalog(ConnectContext ctx, String newCatalogName) throws DdlException {
        if (!catalogMgr.catalogExists(newCatalogName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
        }
        if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
            try {
                Authorizer.checkAnyActionOnCatalog(ctx.getCurrentUserIdentity(),
                        ctx.getCurrentRoleIds(), newCatalogName);
            } catch (AccessDeniedException e) {
                ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USE CATALOG");
            }
        }
        ctx.setCurrentCatalog(newCatalogName);
        ctx.setDatabase("");
    }

    // Change current catalog and database of this session.
    // identifier could be "CATALOG.DB" or "DB".
    // For "CATALOG.DB", we change the current catalog database.
    // For "DB", we keep the current catalog and change the current database.
    public void changeCatalogDb(ConnectContext ctx, String identifier) throws DdlException {
        String dbName;

        String[] parts = identifier.split("\\.", 2); // at most 2 parts
        if (parts.length != 1 && parts.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
        }

        if (parts.length == 1) { // use database
            dbName = identifier;
        } else { // use catalog.database
            String newCatalogName = parts[0];
            if (!catalogMgr.catalogExists(newCatalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
            }
            if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
                try {
                    Authorizer.checkAnyActionOnCatalog(ctx.getCurrentUserIdentity(),
                            ctx.getCurrentRoleIds(), newCatalogName);
                } catch (AccessDeniedException e) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USE CATALOG");
                }
            }
            ctx.setCurrentCatalog(newCatalogName);
            dbName = parts[1];
        }

        if (!Strings.isNullOrEmpty(dbName) && metadataMgr.getDb(ctx.getCurrentCatalog(), dbName) == null) {
            LOG.debug("Unknown catalog {} and db {}", ctx.getCurrentCatalog(), dbName);
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // Here we check the request permission that sent by the mysql client or jdbc.
        // So we didn't check UseDbStmt permission in PrivilegeCheckerV2.
        try {
            Authorizer.checkAnyActionOnOrInDb(ctx.getCurrentUserIdentity(),
                    ctx.getCurrentRoleIds(), ctx.getCurrentCatalog(), dbName);
        } catch (AccessDeniedException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    ctx.getCurrentUserIdentity().getUser(), dbName);
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
        TableName tableName = stmt.getTableName();
        List<String> partitionNames = stmt.getPartitions();
        refreshExternalTable(tableName, partitionNames);
        refreshOthersFeTable(tableName, partitionNames, true);
    }

    public void refreshOthersFeTable(TableName tableName, List<String> partitions, boolean isSync) throws DdlException {
        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getFrontends(null);
        Map<String, Future<TStatus>> resultMap = Maps.newHashMapWithExpectedSize(allFrontends.size() - 1);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getSelfNode().first)) {
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

        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            table = metadataMgr.getTable(catalogName, dbName, tblName);
            if (!(table instanceof HiveMetaStoreTable) && !(table instanceof HiveView)
                    && !(table instanceof IcebergTable) && !(table instanceof JDBCTable)) {
                throw new StarRocksConnectorException(
                        "table : " + tableName + " not exists, or is not hive/hudi/iceberg/odps/jdbc external table/view");
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            Preconditions.checkState(table instanceof HiveMetaStoreTable);
            catalogName = ((HiveMetaStoreTable) table).getCatalogName();
        }

        metadataMgr.refreshTable(catalogName, dbName, table, partitions, true);
    }

    // TODO [meta-format-change] deprecated
    public void initDefaultCluster() {
        localMetastore.initDefaultCluster();
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
            for (long dbId : getDbIds()) {
                Database db = getDb(dbId);
                Preconditions.checkNotNull(db);
                lockedDbMap.put(dbId, db);
            }

            // lock all dbs
            for (Database db : lockedDbMap.values()) {
                locker.lockDatabase(db, LockType.READ);
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
                locker.unLockDatabase(db, LockType.READ);
            }
            unlock();
        }

        LOG.info("finished dumping image to {}", dumpFilePath);
        return dumpFilePath;
    }

    public List<Partition> createTempPartitionsFromPartitions(Database db, Table table,
                                                              String namePostfix, List<Long> sourcePartitionIds,
                                                              List<Long> tmpPartitionIds, DistributionDesc distributionDesc) {
        return localMetastore.createTempPartitionsFromPartitions(db, table, namePostfix, sourcePartitionIds,
                tmpPartitionIds, distributionDesc);
    }

    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        localMetastore.truncateTable(truncateTableStmt);
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        localMetastore.replayTruncateTable(info);
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

    public Long allocateAutoIncrementId(Long tableId, Long rows) {
        return localMetastore.allocateAutoIncrementId(tableId, rows);
    }

    public void removeAutoIncrementIdByTableId(Long tableId, boolean isReplay) {
        localMetastore.removeAutoIncrementIdByTableId(tableId, isReplay);
    }

    public Long getCurrentAutoIncrementIdByTableId(Long tableId) {
        return localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
    }

    public void addOrReplaceAutoIncrementIdByTableId(Long tableId, Long id) {
        localMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, id);
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
            // If we still need to replay old auth journal, it means that,
            // 1. either no new privilege image has been generated, and some old auth journal haven't been compacted
            //    into old auth image
            // 2. or new privilege image has already been generated, and we roll back to old version, make some user or
            //    privilege operation, then generate old auth journal
            // in both cases, we need a definite upgrade, so we mark the managers of
            // new privilege framework as unloaded to trigger upgrade process.
            LOG.info("set authenticationManager and authorizationManager as unloaded because of old auth journal");
            authenticationMgr.setLoaded(false);
            authorizationMgr.setLoaded(false);
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

    private void reInitializeNewPrivilegeOnUpgrade() {
        // In the case where we upgrade again, i.e. upgrade->rollback->upgrade,
        // we may already load the image from last upgrade, in this case we should
        // discard the privilege data from last upgrade and only use the data from
        // current image to upgrade, so we initialize a new AuthorizationManager and AuthenticationManger
        // instance here
        LOG.info("reinitialize privilege info before upgrade");
        this.authenticationMgr = new AuthenticationMgr();
        this.authorizationMgr = new AuthorizationMgr(this, null);
    }

    public void replayAuthUpgrade(AuthUpgradeInfo info) throws AuthUpgrader.AuthUpgradeUnrecoverableException {
        reInitializeNewPrivilegeOnUpgrade();
        AuthUpgrader upgrader = new AuthUpgrader(auth, authenticationMgr, authorizationMgr, this);
        upgrader.replayUpgrade(info.getRoleNameToId());
        LOG.info("set usingNewPrivilege to true after auth upgrade log replayed");
        usingNewPrivilege.set(true);
        domainResolver.setAuthenticationManager(authenticationMgr);
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

    public SlotProvider getSlotProvider() {
        return slotProvider;
    }

    public ResourceUsageMonitor getResourceUsageMonitor() {
        return resourceUsageMonitor;
    }

    public DictionaryMgr getDictionaryMgr() {
        return dictionaryMgr;
    }
}
