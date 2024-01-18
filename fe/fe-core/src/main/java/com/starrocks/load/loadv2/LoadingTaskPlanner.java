// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadingTaskPlanner.java

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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.Load;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LoadingTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(LoadingTaskPlanner.class);

    // Input params
    private final long loadJobId;
    private final long txnId;
    private final long dbId;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final boolean strictMode;
    private final long timeoutS;    // timeout of load job, in second
    private final boolean partialUpdate;
    private final int parallelInstanceNum;
    private final long startTime;
    private String mergeConditionStr;

    // Something useful
    // ConnectContext here is just a dummy object to avoid some NPE problem, like ctx.getDatabase()
    private Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
    private DescriptorTable descTable = analyzer.getDescTbl();

    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    private TupleDescriptor tupleDesc;
    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    private Map<String, String> sessionVariables = null;
    ConnectContext context = null;

    public LoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups,
            boolean strictMode, String timezone, long timeoutS,
            long startTime, boolean partialUpdate, Map<String, String> sessionVariables, String mergeConditionStr) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
        this.analyzer.setTimezone(timezone);
        this.timeoutS = timeoutS;
        this.partialUpdate = partialUpdate;
        this.mergeConditionStr = mergeConditionStr;
        this.parallelInstanceNum = Config.load_parallel_instance_num;
        this.startTime = startTime;
        this.sessionVariables = sessionVariables;
    }

    public void setConnectContext(ConnectContext context) {
        this.context = context;
    }

    public void plan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        // Generate tuple descriptor
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        List<Column> destColumns = Lists.newArrayList();
        boolean isPrimaryKey = table.getKeysType() == KeysType.PRIMARY_KEYS;
        if (isPrimaryKey && partialUpdate) {
            if (fileGroups.size() > 1) {
                throw new DdlException("partial update only support single filegroup.");
            } else if (fileGroups.size() == 1) {
                if (fileGroups.get(0).isNegative()) {
                    throw new DdlException("Primary key table does not support negative load");
                }
                destColumns = Load.getPartialUpateColumns(table, fileGroups.get(0).getColumnExprList());
            } else {
                throw new DdlException("filegroup number=" + fileGroups.size() + " is illegal");
            }
        } else if (!isPrimaryKey && partialUpdate) {
            throw new DdlException("Only primary key table support partial update");
        } else {
            destColumns = table.getFullSchema();
        }
        for (Column col : destColumns) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());

            if (col.getType().isVarchar() && IDictManager.getInstance().hasGlobalDict(table.getId(),
                    col.getName())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(table.getId(), col.getName());
                dict.ifPresent(columnDict -> globalDicts.add(new Pair<>(slotDesc.getId().asInt(), columnDict)));
            }
        }
        if (isPrimaryKey) {
            // add op type column
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(new Column(Load.LOAD_OP_COLUMN, Type.TINYINT));
            slotDesc.setIsNullable(false);
        }

        if (Config.enable_shuffle_load && needShufflePlan()) {
            if (Config.eliminate_shuffle_load_by_replicated_storage) {
                buildDirectPlan(loadId, fileStatusesList, filesAdded, true);
            } else {
                buildShufflePlan(loadId, fileStatusesList, filesAdded);
            }
        } else {
            buildDirectPlan(loadId, fileStatusesList, filesAdded, false);
        }
    }

    public void buildDirectPlan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList,
            int filesAdded, boolean forceReplicatedStorage)
            throws UserException {
        // Generate plan trees
        // 1. Broker scan node
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(nextNodeId++), tupleDesc, "FileScanNode",
                fileStatusesList, filesAdded);
        scanNode.setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups, strictMode, parallelInstanceNum);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);
        scanNodes.add(scanNode);
        descTable.computeMemLayout();

        // 2. Olap table sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, tupleDesc, partitionIds, true,
                table.writeQuorum(), forceReplicatedStorage ? true : table.enableReplicatedStorage());
        olapTableSink.init(loadId, txnId, dbId, timeoutS);
        Load.checkMergeCondition(mergeConditionStr, table);
        olapTableSink.complete(mergeConditionStr);

        // 3. Plan fragment
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        sinkFragment.setSink(olapTableSink);

        if (this.context != null) {
            if (this.context.getSessionVariable().isEnablePipelineEngine() && Config.enable_pipeline_load) {
                sinkFragment.setPipelineDop(parallelInstanceNum);
                sinkFragment.setParallelExecNum(1);
                sinkFragment.setHasOlapTableSink();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
            } else {
                sinkFragment.setPipelineDop(1);
                sinkFragment.setParallelExecNum(parallelInstanceNum);
            }
        } else {
            sinkFragment.setPipelineDop(1);
            sinkFragment.setParallelExecNum(parallelInstanceNum);
        }
        // After data loading, we need to check the global dict for low cardinality string column
        // whether update.
        sinkFragment.setLoadGlobalDicts(globalDicts);

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        }
        Collections.reverse(fragments);
    }

    public void buildShufflePlan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        // Generate plan trees
        // 1. Broker scan node
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(nextNodeId++), tupleDesc, "FileScanNode",
                fileStatusesList, filesAdded);
        scanNode.setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups, strictMode, parallelInstanceNum);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);
        scanNodes.add(scanNode);
        descTable.computeMemLayout();

        // 3. Scan plan fragment
        PlanFragment scanFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);

        fragments.add(scanFragment);

        // 5. Exchange node
        List<Column> columns = table.getFullSchema();

        List<Column> keyColumns = table.getKeyColumnsByIndexId(table.getBaseIndexId());
        List<Expr> partitionExprs = Lists.newArrayList();
        keyColumns.forEach(column -> {
            partitionExprs.add(new SlotRef(tupleDesc.getColumnSlot(column.getName())));
        });

        DataPartition dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs); 
        ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(nextNodeId++), scanFragment.getPlanRoot(),
                dataPartition);
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(1), exchangeNode, dataPartition);
        exchangeNode.setFragment(sinkFragment);
        scanFragment.setDestination(exchangeNode);
        scanFragment.setOutputPartition(dataPartition);

        // 4. Olap table sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, tupleDesc, partitionIds, true,
                table.writeQuorum(), table.enableReplicatedStorage());
        Load.checkMergeCondition(mergeConditionStr, table);
        olapTableSink.complete(mergeConditionStr);

        // 6. Sink plan fragment
        sinkFragment.setSink(olapTableSink);
        // For shuffle broker load, we only support tablet sink dop = 1
        // because for tablet sink dop > 1, local passthourgh exchange will influence the order of sending,
        // which may lead to inconsistent replica for primary key.
        // If you want to set tablet sink dop > 1, please enable single tablet loading and disable shuffle service
        if (this.context != null) {
            if (this.context.getSessionVariable().isEnablePipelineEngine() && Config.enable_pipeline_load) {
                sinkFragment.setHasOlapTableSink();
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
            }
            sinkFragment.setPipelineDop(1);
            sinkFragment.setParallelExecNum(parallelInstanceNum);
        }
        // After data loading, we need to check the global dict for low cardinality string column
        // whether update.
        sinkFragment.setLoadGlobalDicts(globalDicts);

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        }
        Collections.reverse(fragments);
    }

    public Boolean needShufflePlan() {
        if (KeysType.DUP_KEYS.equals(table.getKeysType())) {
            return false;
        }

        if (table.getDefaultReplicationNum() <= 1) {
            return false;
        }

        if (table.enableReplicatedStorage()) {
            return false;
        }

        if (KeysType.AGG_KEYS.equals(table.getKeysType())) {
            for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
                List<Column> schema = entry.getValue();
                for (Column column : schema) {
                    if (column.getAggregationType() == AggregateType.REPLACE
                            || column.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                        return true;
                    }
                }
            }
            return false;
        }

        return true;
    }

    public long getStartTime() {
        return startTime;
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public String getTimezone() {
        return analyzer.getTimezone();
    }

    private List<Long> getAllPartitionIds() throws LoadException, MetaNotFoundException {
        Set<Long> partitionIds = Sets.newHashSet();
        for (BrokerFileGroup brokerFileGroup : fileGroups) {
            if (brokerFileGroup.getPartitionIds() != null) {
                partitionIds.addAll(brokerFileGroup.getPartitionIds());
            }
            // all file group in fileGroups should have same partitions, so only need to get partition ids
            // from one of these file groups
            break;
        }

        if (partitionIds.isEmpty()) {
            for (Partition partition : table.getPartitions()) {
                partitionIds.add(partition.getId());
            }
        }

        // If this is a dynamic partitioned table, it will take some time to create the partition after the
        // table is created, a exception needs to be thrown here
        if (partitionIds.isEmpty()) {
            throw new LoadException("data cannot be inserted into table with empty partition. " +
                    "Use `SHOW PARTITIONS FROM " + table.getName() +
                    "` to see the currently partitions of this table. ");
        }

        return Lists.newArrayList(partitionIds);
    }

    /**
     * Update load info when task retry
     * 1. new olap table sink load id
     * 2. check be/broker and replace new alive be/broker if original be/broker in locations is dead
     */
    public void updateLoadInfo(TUniqueId loadId) {
        for (PlanFragment planFragment : fragments) {
            if (!(planFragment.getSink() instanceof OlapTableSink
                    && planFragment.getPlanRoot() instanceof FileScanNode)) {
                continue;
            }

            // when retry load by reusing this plan in load process, the load_id should be changed
            OlapTableSink olapTableSink = (OlapTableSink) planFragment.getSink();
            olapTableSink.updateLoadId(loadId);
            LOG.info("update olap table sink's load id to {}, job: {}", DebugUtil.printId(loadId), loadJobId);

            // update backend and broker
            FileScanNode fileScanNode = (FileScanNode) planFragment.getPlanRoot();
            fileScanNode.updateScanRangeLocations();
        }
    }
}
