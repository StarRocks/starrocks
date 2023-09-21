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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/ExportJob.java

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

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BaseTableRef;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.MysqlScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.UnlockTabletMetadataRequest;
import com.starrocks.qe.Coordinator;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentClient;
import com.starrocks.thrift.TAgentResult;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// NOTE: we must be carefully if we send next request
//       as soon as receiving one instance's report from one BE,
//       because we may change job's member concurrently.
//
// export file name format:
// <prefix>_<task-number>_<instance-number>_<file-number>.csv  (if include_query_id is false)
// <prefix>_<query-id>_<task-number>_<instance-number>_<file-number>.csv
public class ExportJob implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);
    // descriptor used to register all column and table need
    private final DescriptorTable desc;
    private final Set<String> exportedTempFiles = Sets.newConcurrentHashSet();
    private Set<String> exportedFiles = Sets.newConcurrentHashSet();
    private final Analyzer analyzer;
    private final List<Coordinator> coordList = Lists.newArrayList();
    private final AtomicInteger nextId = new AtomicInteger(0);
    // backedn_address => snapshot path
    private List<Pair<TNetworkAddress, String>> snapshotPaths = Lists.newArrayList();
    // backend id => backend lastStartTime 
    private final Map<Long, Long> beLastStartTime = Maps.newHashMap();

    @SerializedName("id")
    private long id;
    private UUID queryId;
    @SerializedName("qd")
    private String queryIdString;
    @SerializedName("dd")
    private long dbId;
    @SerializedName("td")
    private long tableId;
    @SerializedName("bd")
    private BrokerDesc brokerDesc;
    // exportPath has "/" suffix
    @SerializedName("ep")
    private String exportPath;
    private String exportTempPath;
    private String fileNamePrefix;
    @SerializedName("cs")
    private String columnSeparator;
    @SerializedName("rd")
    private String rowDelimiter;
    private boolean includeQueryId;
    @SerializedName("pt")
    private Map<String, String> properties = Maps.newHashMap();
    @SerializedName("ps")
    private List<String> partitions;
    @SerializedName("tn")
    private TableName tableName;
    private List<String> columnNames;
    private String sql = "";
    @SerializedName("se")
    private JobState state;
    @SerializedName("ct")
    private long createTimeMs;
    @SerializedName("st")
    private long startTimeMs;
    @SerializedName("ft")
    private long finishTimeMs;
    @SerializedName("pg")
    private int progress;
    @SerializedName("fm")
    private ExportFailMsg failMsg;
    private TupleDescriptor exportTupleDesc;
    private Table exportTable;
    // when set to true, means this job instance is created by replay thread(FE restarted or master changed)
    private boolean isReplayed = false;
    private Thread doExportingThread;
    private List<TScanRangeLocations> tabletLocations = Lists.newArrayList();

    public ExportJob() {
        this.id = -1;
        this.queryId = null;
        this.dbId = -1;
        this.tableId = -1;
        this.state = JobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.startTimeMs = -1;
        this.finishTimeMs = -1;
        this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
        this.analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), null);
        this.desc = analyzer.getDescTbl();
        this.exportPath = "";
        this.exportTempPath = "";
        this.fileNamePrefix = "";
        this.columnSeparator = "\t";
        this.rowDelimiter = "\n";
        this.includeQueryId = true;
    }

    public ExportJob(long jobId, UUID queryId) {
        this();
        this.id = jobId;
        this.queryId = queryId;
        this.queryIdString = queryId.toString();
    }

    public void setJob(ExportStmt stmt) throws UserException {
        String dbName = stmt.getTblName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        this.brokerDesc = stmt.getBrokerDesc();
        Preconditions.checkNotNull(brokerDesc);

        this.columnSeparator = stmt.getColumnSeparator();
        this.rowDelimiter = stmt.getRowDelimiter();
        this.includeQueryId = stmt.isIncludeQueryId();
        this.properties = stmt.getProperties();

        exportPath = stmt.getPath();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(exportPath));
        exportTempPath = this.exportPath + "__starrocks_export_tmp_" + queryId.toString();
        fileNamePrefix = stmt.getFileNamePrefix();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileNamePrefix));
        if (includeQueryId) {
            fileNamePrefix += queryId.toString() + "_";
        }

        this.partitions = stmt.getPartitions();
        this.columnNames = stmt.getColumnNames();

        db.readLock();
        try {
            this.dbId = db.getId();
            this.exportTable = db.getTable(stmt.getTblName().getTbl());
            if (exportTable == null) {
                throw new DdlException("Table " + stmt.getTblName().getTbl() + " does not exist");
            }
            this.tableId = exportTable.getId();
            this.tableName = stmt.getTblName();
            genExecFragment(stmt);
        } finally {
            db.readUnlock();
        }

        this.sql = stmt.toSql();
    }

    private void genExecFragment(ExportStmt stmt) throws UserException {
        registerToDesc();
        plan(stmt);
    }

    private void registerToDesc() throws UserException {
        TableRef ref = new TableRef(tableName, null, partitions == null ? null : new PartitionNames(false, partitions));
        BaseTableRef tableRef = new BaseTableRef(ref, exportTable, tableName);
        exportTupleDesc = desc.createTupleDescriptor();
        exportTupleDesc.setTable(exportTable);
        exportTupleDesc.setRef(tableRef);

        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        List<Column> tableColumns = exportTable.getBaseSchema();
        List<Column> exportColumns = Lists.newArrayList();
        for (Column column : tableColumns) {
            nameToColumn.put(column.getName(), column);
        }
        if (columnNames == null) {
            exportColumns.addAll(tableColumns);
        } else {
            for (String columnName : columnNames) {
                if (!nameToColumn.containsKey(columnName)) {
                    throw new UserException("Column [" + columnName + "] does not exist in table.");
                }
                exportColumns.add(nameToColumn.get(columnName));
            }
        }

        for (Column col : exportColumns) {
            SlotDescriptor slot = desc.addSlotDescriptor(exportTupleDesc);
            slot.setIsMaterialized(true);
            slot.setColumn(col);
            slot.setIsNullable(col.isAllowNull());
        }
        desc.computeMemLayout();
    }

    private void plan(ExportStmt stmt) throws UserException {
        List<PlanFragment> fragments = Lists.newArrayList();
        List<ScanNode> scanNodes = Lists.newArrayList();

        ScanNode scanNode = genScanNode();
        tabletLocations = scanNode.getScanRangeLocations(0);
        if (tabletLocations == null) {
            // not olap scan node
            PlanFragment fragment = genPlanFragment(exportTable.getType(), scanNode, 0);
            scanNodes.add(scanNode);
            fragments.add(fragment);
        } else {
            for (TScanRangeLocations tablet : tabletLocations) {
                List<TScanRangeLocation> locations = tablet.getLocations();
                Collections.shuffle(locations);
                tablet.setLocations(locations.subList(0, 1));
            }

            long maxBytesPerBe = Config.export_max_bytes_per_be_per_task;
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            List<TScanRangeLocations> copyTabletLocations = Lists.newArrayList(tabletLocations);
            int taskIdx = 0;
            while (!copyTabletLocations.isEmpty()) {
                Map<Long, Long> bytesPerBe = Maps.newHashMap();
                List<TScanRangeLocations> taskTabletLocations = Lists.newArrayList();
                Iterator<TScanRangeLocations> iter = copyTabletLocations.iterator();
                while (iter.hasNext()) {
                    TScanRangeLocations scanRangeLocations = iter.next();
                    long tabletId = scanRangeLocations.getScan_range().getInternal_scan_range().getTablet_id();
                    long backendId = scanRangeLocations.getLocations().get(0).getBackend_id();
                    Replica replica = invertedIndex.getReplica(tabletId, backendId);
                    long dataSize = replica != null ? replica.getDataSize() : 0L;

                    Long assignedBytes = bytesPerBe.get(backendId);
                    if (assignedBytes == null || assignedBytes < maxBytesPerBe) {
                        taskTabletLocations.add(scanRangeLocations);
                        bytesPerBe.put(backendId, assignedBytes != null ? assignedBytes + dataSize : dataSize);
                        iter.remove();
                    }
                }

                OlapScanNode taskScanNode = genOlapScanNodeByLocation(taskTabletLocations);
                scanNodes.add(taskScanNode);
                PlanFragment fragment = genPlanFragment(exportTable.getType(), taskScanNode, taskIdx++);
                fragments.add(fragment);
            }

            LOG.info("total {} tablets of export job {}, and assign them to {} coordinators",
                    tabletLocations.size(), id, fragments.size());
        }

        genCoordinators(stmt, fragments, scanNodes);
    }

    private ScanNode genScanNode() throws UserException {
        ScanNode scanNode = null;
        switch (exportTable.getType()) {
            case OLAP:
            case CLOUD_NATIVE:
                scanNode = new OlapScanNode(new PlanNodeId(0), exportTupleDesc, "OlapScanNodeForExport");
                scanNode.setColumnFilters(Maps.newHashMap());
                ((OlapScanNode) scanNode).setIsPreAggregation(false, "This an export operation");
                ((OlapScanNode) scanNode).setCanTurnOnPreAggr(false);
                scanNode.init(analyzer);
                ((OlapScanNode) scanNode).selectBestRollupByRollupSelector();
                break;
            case MYSQL:
                scanNode = new MysqlScanNode(new PlanNodeId(0), exportTupleDesc, (MysqlTable) this.exportTable);
                break;
            default:
                throw new UserException("Unsupported table type: " + exportTable.getType());
        }

        scanNode.finalizeStats(analyzer);
        return scanNode;
    }

    private OlapScanNode genOlapScanNodeByLocation(List<TScanRangeLocations> locations) {
        return OlapScanNode.createOlapScanNodeByLocation(
                new PlanNodeId(nextId.getAndIncrement()),
                exportTupleDesc,
                "OlapScanNodeForExport",
                locations);
    }

    private PlanFragment genPlanFragment(Table.TableType type, ScanNode scanNode, int taskIdx) throws UserException {
        PlanFragment fragment = null;
        switch (exportTable.getType()) {
            case OLAP:
            case CLOUD_NATIVE:
                fragment = new PlanFragment(
                        new PlanFragmentId(nextId.getAndIncrement()), scanNode, DataPartition.RANDOM);
                break;
            case MYSQL:
                fragment = new PlanFragment(
                        new PlanFragmentId(nextId.getAndIncrement()), scanNode, DataPartition.UNPARTITIONED);
                break;
            default:
                break;
        }
        if (fragment == null) {
            throw new UserException("invalid table type:" + exportTable.getType());
        }
        fragment.setOutputExprs(createOutputExprs());

        scanNode.setFragmentId(fragment.getFragmentId());
        THdfsProperties hdfsProperties = new THdfsProperties();
        if (!brokerDesc.hasBroker()) {
            HdfsUtil.getTProperties(exportTempPath, brokerDesc, hdfsProperties);
        }
        fragment.setSink(new ExportSink(exportTempPath, fileNamePrefix + taskIdx + "_", columnSeparator,
                rowDelimiter, brokerDesc, hdfsProperties));
        try {
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        } catch (Exception e) {
            LOG.info("Fragment finalize failed. e=", e);
            throw new UserException("Fragment finalize failed");
        }

        return fragment;
    }

    private List<Expr> createOutputExprs() {
        List<Expr> outputExprs = Lists.newArrayList();
        for (int i = 0; i < exportTupleDesc.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = exportTupleDesc.getSlots().get(i);
            SlotRef slotRef = new SlotRef(slotDesc);
            if (slotDesc.getType().getPrimitiveType() == PrimitiveType.CHAR) {
                slotRef.setType(Type.CHAR);
            }
            outputExprs.add(slotRef);
        }

        return outputExprs;
    }

    private void genCoordinators(ExportStmt stmt, List<PlanFragment> fragments, List<ScanNode> nodes) {
        UUID uuid = UUID.randomUUID();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            ScanNode scanNode = nodes.get(i);
            TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits() + i, uuid.getLeastSignificantBits());
            Coordinator coord = new Coordinator(
                    id, queryId, desc, Lists.newArrayList(fragment), Lists.newArrayList(scanNode),
                    TimeUtils.DEFAULT_TIME_ZONE, stmt.getExportStartTime(), Maps.newHashMap());
            coord.setExecMemoryLimit(getMemLimit());
            this.coordList.add(coord);
            LOG.info("split export job to tasks. job id: {}, job query id: {}, task idx: {}, task query id: {}",
                    id, DebugUtil.printId(this.queryId), i, DebugUtil.printId(queryId));
        }
        LOG.info("create {} coordintors for export job: {}", coordList.size(), id);
    }

    // For olap table, it may have multiple replica, 
    // rebalance process may schedule tablet from one BE to another BE.
    // In such case, coord will return 'Not found tablet xxx' error. To solve this, 
    // we need to find a new replica for that tablet and generate a new coord.
    // Also, if the version has been compacted in one BE's tablet, coord will return 
    // 'version already been compacted' error msg, find a new replica may be able to 
    // alleviate this problem.
    public Coordinator resetCoord(int taskIndex, TUniqueId newQueryId) throws UserException {
        Coordinator coord = coordList.get(taskIndex);
        OlapScanNode olapScanNode = (OlapScanNode) coord.getScanNodes().get(0);
        List<TScanRangeLocations> locations = olapScanNode.getScanRangeLocations(0);
        if (locations.size() == 0) {
            throw new UserException("SubExportTask " + taskIndex + " scan range is empty");
        }

        OlapScanNode newOlapScanNode = new OlapScanNode(new PlanNodeId(0), exportTupleDesc, "OlapScanNodeForExport");
        Analyzer tmpAnalyzer = new Analyzer(GlobalStateMgr.getCurrentState(), null);
        newOlapScanNode.setColumnFilters(Maps.newHashMap());
        newOlapScanNode.setIsPreAggregation(false, "This an export operation");
        newOlapScanNode.setCanTurnOnPreAggr(false);
        newOlapScanNode.init(tmpAnalyzer);
        newOlapScanNode.selectBestRollupByRollupSelector();
        List<TScanRangeLocations> newLocations = newOlapScanNode.updateScanRangeLocations(locations);

        // random select a new location for each TScanRangeLocations
        for (TScanRangeLocations tablet : newLocations) {
            List<TScanRangeLocation> tabletLocations = tablet.getLocations();
            Collections.shuffle(tabletLocations);
            tablet.setLocations(tabletLocations.subList(0, 1));
        }

        OlapScanNode newTaskScanNode = genOlapScanNodeByLocation(newLocations);
        PlanFragment newFragment = genPlanFragment(exportTable.getType(), newTaskScanNode, taskIndex);

        Coordinator newCoord = new Coordinator(
                id, newQueryId, desc, Lists.newArrayList(newFragment), Lists.newArrayList(newTaskScanNode),
                TimeUtils.DEFAULT_TIME_ZONE, coord.getStartTime(), Maps.newHashMap());
        newCoord.setExecMemoryLimit(getMemLimit());
        this.coordList.set(taskIndex, newCoord);
        LOG.info("reset coordinator for export job: {}, taskIdx: {}", id, taskIndex);
        return newCoord;
    }

    public boolean needResetCoord() {
        return exportTable.isOlapTable();
    }

    public void setSnapshotPaths(List<Pair<TNetworkAddress, String>> snapshotPaths) {
        this.snapshotPaths = snapshotPaths;
    }

    public void setExportTempPath(String exportTempPath) {
        this.exportTempPath = exportTempPath;
    }

    public void setExportedFiles(Set<String> exportedFiles) {
        this.exportedFiles = exportedFiles;
    }

    public void setBeStartTime(long beId, long lastStartTime) {
        this.beLastStartTime.put(beId, lastStartTime);
    }

    public void setFailMsg(ExportFailMsg failMsg) {
        this.failMsg = failMsg;
    }

    public Map<Long, Long> getBeStartTimeMap() {
        return this.beLastStartTime;
    }

    public long getId() {
        return id;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public JobState getState() {
        return state;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void setBrokerDesc(BrokerDesc brokerDesc) {
        this.brokerDesc = brokerDesc;
    }

    public String getExportPath() {
        return exportPath;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getRowDelimiter() {
        return this.rowDelimiter;
    }

    public long getMemLimit() {
        // The key is exec_mem_limit before version 1.18, check first
        if (properties.containsKey(LoadStmt.LOAD_MEM_LIMIT)) {
            return Long.parseLong(properties.get(LoadStmt.LOAD_MEM_LIMIT));
        } else {
            return 0;
        }
    }

    public int getTimeoutSecond() {
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
        } else {
            // for compatibility, some export job in old version does not have this property. use default.
            return Config.export_task_default_timeout_second;
        }
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public synchronized int getProgress() {
        return progress;
    }

    public synchronized void setProgress(int progress) {
        this.progress = progress;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public ExportFailMsg getFailMsg() {
        return failMsg;
    }

    public Set<String> getExportedTempFiles() {
        return this.exportedTempFiles;
    }

    public String getExportedTempPath() {
        return this.exportTempPath;
    }

    public Set<String> getExportedFiles() {
        return this.exportedFiles;
    }

    public synchronized void addExportedTempFiles(List<String> files) {
        exportedTempFiles.addAll(files);
        LOG.debug("exported temp files: {}", this.exportedTempFiles);
    }

    public synchronized void clearExportedTempFiles() {
        exportedTempFiles.clear();
    }

    public synchronized void addExportedFile(String file) {
        exportedFiles.add(file);
        LOG.debug("exported files: {}", this.exportedFiles);
    }

    public synchronized Thread getDoExportingThread() {
        return doExportingThread;
    }

    public synchronized void setDoExportingThread(Thread isExportingThread) {
        this.doExportingThread = isExportingThread;
    }

    public List<Coordinator> getCoordList() {
        return coordList;
    }

    public List<TScanRangeLocations> getTabletLocations() {
        return tabletLocations;
    }

    public List<Pair<TNetworkAddress, String>> getSnapshotPaths() {
        return this.snapshotPaths;
    }

    public void addSnapshotPath(Pair<TNetworkAddress, String> snapshotPath) {
        this.snapshotPaths.add(snapshotPath);
    }

    public String getSql() {
        return sql;
    }

    public TableName getTableName() {
        return tableName;
    }

    public synchronized boolean updateState(JobState newState) {
        return this.updateState(newState, false, System.currentTimeMillis());
    }

    public synchronized boolean updateState(JobState newState, boolean isReplay, long stateChangeTime) {
        if (isExportDone()) {
            LOG.warn("export job state is finished or cancelled");
            return false;
        }

        state = newState;
        switch (newState) {
            case PENDING:
                progress = 0;
                break;
            case EXPORTING:
                startTimeMs = stateChangeTime;
                break;
            case FINISHED:
            case CANCELLED:
                finishTimeMs = stateChangeTime;
                progress = 100;
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logExportUpdateState(id, newState, stateChangeTime,
                    snapshotPaths, exportTempPath, exportedFiles, failMsg);
        }
        return true;
    }

    public Status releaseSnapshots() {
        switch (exportTable.getType()) {
            case OLAP:
            case MYSQL:
                return releaseSnapshotPaths();
            case CLOUD_NATIVE:
                return releaseMetadataLocks();
            default:
                return Status.OK;
        }
    }

    public Status releaseSnapshotPaths() {
        List<Pair<TNetworkAddress, String>> snapshotPaths = getSnapshotPaths();
        LOG.debug("snapshotPaths:{}", snapshotPaths);
        for (Pair<TNetworkAddress, String> snapshotPath : snapshotPaths) {
            TNetworkAddress address = snapshotPath.first;
            String host = address.getHostname();
            int port = address.getPort();

            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(host, port);
            if (backend == null) {
                continue;
            }
            long backendId = backend.getId();
            if (!GlobalStateMgr.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                continue;
            }

            AgentClient client = new AgentClient(host, port);
            TAgentResult result = client.releaseSnapshot(snapshotPath.second);
            if (result == null || result.getStatus().getStatus_code() != TStatusCode.OK) {
                continue;
            }
        }
        snapshotPaths.clear();
        return Status.OK;
    }

    public Status releaseMetadataLocks() {
        for (TScanRangeLocations tablet : tabletLocations) {
            TScanRange scanRange = tablet.getScan_range();
            if (!scanRange.isSetInternal_scan_range()) {
                continue;
            }

            TInternalScanRange internalScanRange = scanRange.getInternal_scan_range();
            List<TScanRangeLocation> locations = tablet.getLocations();
            for (TScanRangeLocation location : locations) {
                TNetworkAddress address = location.getServer();
                String host = address.getHostname();
                int port = address.getPort();
                ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNodeWithBePort(host, port);
                if (!GlobalStateMgr.getCurrentSystemInfo().checkNodeAvailable(node)) {
                    continue;
                }
                try {
                    LakeService lakeService = BrpcProxy.getLakeService(host, port);
                    UnlockTabletMetadataRequest request = new UnlockTabletMetadataRequest();
                    request.tabletId = internalScanRange.getTablet_id();
                    request.version = Long.parseLong(internalScanRange.getVersion());
                    request.expireTime = (getCreateTimeMs() / 1000) + getTimeoutSecond();
                    lakeService.unlockTabletMetadata(request);
                } catch (Throwable e) {
                    LOG.error("Fail to release metadata lock, job id {}, tablet id {}, version {}", id,
                            tableId, internalScanRange.getVersion());
                }
            }
        }
        return Status.OK;
    }

    public synchronized boolean isExportDone() {
        return state == JobState.FINISHED || state == JobState.CANCELLED;
    }

    public synchronized void cancel(ExportFailMsg.CancelType type, String msg) throws UserException {
        if (isExportDone()) {
            throw new UserException("Export job [" + queryId.toString() + "] is already finished or cancelled");
        }

        cancelInternal(type, msg);
    }

    public synchronized void cancelInternal(ExportFailMsg.CancelType type, String msg) {
        if (isExportDone()) {
            LOG.warn("export job state is finished or cancelled");
            return;
        }

        try {
            if (msg != null && failMsg.getCancelType() == ExportFailMsg.CancelType.UNKNOWN) {
                failMsg = new ExportFailMsg(type, msg);
            }

            // cancel all running coordinators
            for (Coordinator coord : coordList) {
                coord.cancel();
            }

            // try to remove exported temp files
            try {
                if (!brokerDesc.hasBroker()) {
                    HdfsUtil.deletePath(exportTempPath, brokerDesc);
                } else {
                    BrokerUtil.deletePath(exportTempPath, brokerDesc);
                }
                LOG.info("remove export temp path success, path: {}", exportTempPath);
            } catch (UserException e) {
                LOG.warn("remove export temp path fail, path: {}", exportTempPath);
            }
            // try to remove exported files
            for (String exportedFile : exportedFiles) {
                try {
                    if (!brokerDesc.hasBroker()) {
                        HdfsUtil.deletePath(exportedFile, brokerDesc);
                    } else {
                        BrokerUtil.deletePath(exportedFile, brokerDesc);
                    }
                    LOG.info("remove exported file success, path: {}", exportedFile);
                } catch (UserException e) {
                    LOG.warn("remove exported file fail, path: {}", exportedFile);
                }
            }

            // release snapshot
            releaseSnapshots();
        } finally {
            updateState(ExportJob.JobState.CANCELLED);
            LOG.info("export job cancelled. job: {}", this);
        }
    }

    public synchronized void finish() {
        if (isExportDone()) {
            LOG.warn("export job state is finished or cancelled");
            return;
        }

        try {
            // release snapshot
            releaseSnapshots();

            // try to remove exported temp files
            try {
                if (!brokerDesc.hasBroker()) {
                    HdfsUtil.deletePath(exportTempPath, brokerDesc);
                } else {
                    BrokerUtil.deletePath(exportTempPath, brokerDesc);
                }
                LOG.info("remove export temp path success, path: {}", exportTempPath);
            } catch (UserException e) {
                LOG.warn("remove export temp path fail, path: {}", exportTempPath);
            }
        } finally {
            updateState(JobState.FINISHED);
            LOG.info("export job finished. job: {}", this);
        }
    }

    @Override
    public String toString() {
        return "ExportJob [jobId=" + id
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", state=" + state
                + ", path=" + exportPath
                + ", partitions=(" + StringUtils.join(partitions, ",") + ")"
                + ", progress=" + progress
                + ", createTimeMs=" + TimeUtils.longToTimeString(createTimeMs)
                + ", exportStartTimeMs=" + TimeUtils.longToTimeString(startTimeMs)
                + ", exportFinishTimeMs=" + TimeUtils.longToTimeString(finishTimeMs)
                + ", failMsg=" + failMsg
                + ", tmp files=(" + StringUtils.join(exportedTempFiles, ",") + ")"
                + ", files=(" + StringUtils.join(exportedFiles, ",") + ")"
                + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // base infos
        out.writeLong(id);
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, exportPath);
        Text.writeString(out, columnSeparator);
        Text.writeString(out, rowDelimiter);
        out.writeInt(properties.size());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            Text.writeString(out, property.getKey());
            Text.writeString(out, property.getValue());
        }

        // partitions
        boolean hasPartition = (partitions != null);
        if (hasPartition) {
            out.writeBoolean(true);
            int partitionSize = partitions.size();
            out.writeInt(partitionSize);
            for (String partitionName : partitions) {
                Text.writeString(out, partitionName);
            }
        } else {
            out.writeBoolean(false);
        }

        // task info
        Text.writeString(out, state.name());
        out.writeLong(createTimeMs);
        out.writeLong(startTimeMs);
        out.writeLong(finishTimeMs);
        out.writeInt(progress);
        failMsg.write(out);

        if (brokerDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            brokerDesc.write(out);
        }

        tableName.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        isReplayed = true;
        id = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        exportPath = Text.readString(in);
        columnSeparator = Text.readString(in);
        rowDelimiter = Text.readString(in);

        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        Database db = null;
        if (stateMgr.getMetadata() != null) {
            db = stateMgr.getDb(dbId);
        }
        if (db != null) {
            exportTable = db.getTable(tableId);
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            this.properties.put(propertyKey, propertyValue);
        }

        boolean hasPartition = in.readBoolean();
        if (hasPartition) {
            partitions = Lists.newArrayList();
            int partitionSize = in.readInt();
            for (int i = 0; i < partitionSize; ++i) {
                String partitionName = Text.readString(in);
                partitions.add(partitionName);
            }
        }

        state = JobState.valueOf(Text.readString(in));
        createTimeMs = in.readLong();
        startTimeMs = in.readLong();
        finishTimeMs = in.readLong();
        progress = in.readInt();
        failMsg.readFields(in);

        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        tableName = new TableName();
        tableName.readFields(in);
    }

    /**
     * for ut only
     */
    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ExportJob)) {
            return false;
        }

        ExportJob job = (ExportJob) obj;

        return this.id == job.id;
    }

    public boolean isReplayed() {
        return isReplayed;
    }

    public boolean exportLakeTable() {
        return exportTable.isCloudNativeTableOrMaterializedView();
    }

    public boolean exportOlapTable() {
        return exportTable.isOlapTable();
    }

    public enum JobState {
        PENDING,
        EXPORTING,
        FINISHED,
        CANCELLED,
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (!Strings.isNullOrEmpty(queryIdString)) {
            queryId = UUID.fromString(queryIdString);
        }
        isReplayed = true;
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        Database db = null;
        if (stateMgr.getMetadata() != null) {
            db = stateMgr.getDb(dbId);
        }
        if (db != null) {
            exportTable = db.getTable(tableId);
        }
    }

    // for only persist op when switching job state.
    public static class StateTransfer implements Writable {
        long jobId;
        JobState state;

        public StateTransfer() {
            this.jobId = -1;
            this.state = JobState.CANCELLED;
        }

        public StateTransfer(long jobId, JobState state) {
            this.jobId = jobId;
            this.state = state;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(jobId);
            Text.writeString(out, state.name());
        }

        public void readFields(DataInput in) throws IOException {
            jobId = in.readLong();
            state = JobState.valueOf(Text.readString(in));
        }
    }

    public static class ExportUpdateInfo implements Writable {
        @SerializedName("jobId")
        long jobId;
        @SerializedName("state")
        JobState state;
        @SerializedName("stateChangeTime")
        long stateChangeTime;
        @SerializedName("snapshotPaths")
        List<Pair<NetworkAddress, String>> snapshotPaths;
        @SerializedName("exportTempPath")
        String exportTempPath;
        @SerializedName("exportedFiles")
        Set<String> exportedFiles;
        @SerializedName("failMsg")
        ExportFailMsg failMsg;

        public ExportUpdateInfo() {
            this.jobId = -1;
            this.state = JobState.CANCELLED;
            this.snapshotPaths =  Lists.newArrayList();
            this.exportTempPath = "";
            this.exportedFiles = Sets.newConcurrentHashSet();
            this.failMsg = new ExportFailMsg();
        }

        public ExportUpdateInfo(long jobId, JobState state, long stateChangeTime,
                                List<Pair<TNetworkAddress, String>> snapshotPaths,
                                String exportTempPath, Set<String> exportedFiles, ExportFailMsg failMsg) {
            this.jobId = jobId;
            this.state = state;
            this.stateChangeTime = stateChangeTime;
            this.snapshotPaths = serialize(snapshotPaths);
            this.exportTempPath = exportTempPath;
            this.exportedFiles = exportedFiles;
            this.failMsg = failMsg;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this, ExportUpdateInfo.class);
            Text.writeString(out, json);

            // Due to TNetworkAddress unsupport to_json, snapshotPaths can not be seralized to GSON automatically,
            // here we manually seralize it
            out.writeInt(snapshotPaths.size());
            for (Pair<NetworkAddress, String> entry : snapshotPaths) {
                Text.writeString(out, entry.first.hostname);
                out.writeInt(entry.first.port);
                Text.writeString(out, entry.second);
            }
        }

        public static ExportUpdateInfo read(DataInput input) throws IOException {
            ExportUpdateInfo info = GsonUtils.GSON.fromJson(Text.readString(input), ExportUpdateInfo.class);

            int snapshotPathsLen = input.readInt();
            for (int i = 0; i < snapshotPathsLen; i++) {
                String hostName = Text.readString(input);
                int port = input.readInt();
                String path = Text.readString(input);
                Pair<NetworkAddress, String> entry = Pair.create(new NetworkAddress(hostName, port), path);
                info.snapshotPaths.set(i, entry);
            }

            return info;
        }

        public List<Pair<NetworkAddress, String>> serialize(List<Pair<TNetworkAddress, String>> snapshotPaths) {
            return snapshotPaths
                    .stream()
                    .map(snapshotPath
                            -> Pair.create(new NetworkAddress(snapshotPath.first.hostname, snapshotPath.first.port),
                            snapshotPath.second))
                    .collect(Collectors.toList());
        }

        public List<Pair<TNetworkAddress, String>> deserialize(List<Pair<NetworkAddress, String>> snapshotPaths) {
            return snapshotPaths
                    .stream()
                    .map(snapshotPath
                            -> Pair.create(new TNetworkAddress(snapshotPath.first.hostname, snapshotPath.first.port),
                            snapshotPath.second))
                    .collect(Collectors.toList());
        }
    }

    public static class NetworkAddress {
        @SerializedName("h")
        String hostname;
        @SerializedName("p")
        int port;

        public NetworkAddress() {

        }

        public NetworkAddress(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NetworkAddress
                    && this.hostname.equals(((NetworkAddress) obj).hostname)
                    && this.port == ((NetworkAddress) obj).port;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostname, port);
        }

        @Override
        public String toString() {
            return hostname + ":" + port;
        }
    }
}
