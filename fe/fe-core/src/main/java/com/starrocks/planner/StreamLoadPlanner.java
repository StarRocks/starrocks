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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/StreamLoadPlanner.java

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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.load.Load;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// Used to generate a plan fragment for a streaming load.
// we only support OlapTable now.
// TODO(zc): support other type table
public class StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(StreamLoadPlanner.class);
    private final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private StreamLoadInfo streamLoadInfo;

    private Analyzer analyzer;
    private DescriptorTable descTable;

    public StreamLoadPlanner(Database db, OlapTable destTable, StreamLoadInfo streamLoadInfo) {
        this.db = db;
        this.destTable = destTable;
        this.streamLoadInfo = streamLoadInfo;
    }

    private void resetAnalyzer() {
        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), null);
        descTable = analyzer.getDescTbl();
    }

    // can only be called after "plan()", or it will return null
    public OlapTable getDestTable() {
        return destTable;
    }

    // create the plan. the plan's query id and load id are same, using the parameter 'loadId'
    public TExecPlanFragmentParams plan(TUniqueId loadId) throws UserException {
        boolean isPrimaryKey = destTable.getKeysType() == KeysType.PRIMARY_KEYS;
        resetAnalyzer();
        // construct tuple descriptor, used for scanNode and dataSink
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        boolean negative = streamLoadInfo.getNegative();
        if (isPrimaryKey) {
            if (negative) {
                throw new DdlException("Primary key table does not support negative load");
            }
        } else {
            if (streamLoadInfo.isPartialUpdate()) {
                throw new DdlException("Only primary key table support partial update");
            }
        }
        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        List<Column> destColumns;
        List<Boolean> missAutoIncrementColumn = Lists.newArrayList();
        if (streamLoadInfo.isPartialUpdate()) {
            destColumns = Load.getPartialUpateColumns(destTable, streamLoadInfo.getColumnExprDescs(), missAutoIncrementColumn);
        } else {
            destColumns = destTable.getFullSchema();
        }
        for (Column col : destColumns) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggreateType. column:" + col.getName());
            }

            if (col.getType().isVarchar() && Config.enable_dict_optimize_stream_load &&
                    IDictManager.getInstance().hasGlobalDict(destTable.getId(),
                            col.getName())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(destTable.getId(), col.getName());
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

        // create scan node
        StreamLoadScanNode scanNode =
                new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable, streamLoadInfo);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        descTable.computeMemLayout();

        // create dest sink
        TWriteQuorumType writeQuorum = destTable.writeQuorum();

        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(destTable, tupleDesc, partitionIds, writeQuorum,
                destTable.enableReplicatedStorage(), scanNode.nullExprInAutoIncrement());
        if (missAutoIncrementColumn.size() == 1 && missAutoIncrementColumn.get(0) == Boolean.TRUE) {
            olapTableSink.setMissAutoIncrementColumn();
        }
        olapTableSink.init(loadId, streamLoadInfo.getTxnId(), db.getId(), streamLoadInfo.getTimeout());
        Load.checkMergeCondition(streamLoadInfo.getMergeConditionStr(), destTable, olapTableSink.missAutoIncrementColumn());
        olapTableSink.complete(streamLoadInfo.getMergeConditionStr());

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);
        // At present, we only support dop=1 for olap table sink.
        // because tablet writing needs to know the number of senders in advance
        // and guaranteed order of data writing
        // It can be parallel only in some scenes, for easy use 1 dop now.
        fragment.setPipelineDop(1);
        // After data loading, we need to check the global dict for low cardinality string column
        // whether update.
        fragment.setLoadGlobalDicts(globalDicts);

        fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);

        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocol_version(InternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDesc_tbl(analyzer.getDescTbl().toThrift());

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // user load id (streamLoadInfo.id) as query id
        execParams.setQuery_id(loadId);
        execParams.setFragment_instance_id(new TUniqueId(loadId.hi, loadId.lo + 1));
        execParams.per_exch_num_senders = Maps.newHashMap();
        execParams.destinations = Lists.newArrayList();
        Map<Integer, List<TScanRangeParams>> perNodeScanRange = Maps.newHashMap();
        List<TScanRangeParams> scanRangeParams = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            scanRangeParams.add(new TScanRangeParams(locations.getScan_range()));
        }
        // For stream load, only one sender
        execParams.setSender_id(0);
        execParams.setNum_senders(1);
        perNodeScanRange.put(scanNode.getId().asInt(), scanRangeParams);
        execParams.setPer_node_scan_ranges(perNodeScanRange);
        params.setParams(execParams);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQuery_type(TQueryType.LOAD);
        queryOptions.setQuery_timeout(streamLoadInfo.getTimeout());
        queryOptions.setLoad_transmission_compression_type(streamLoadInfo.getTransmisionCompressionType());

        // Disable load_dop for LakeTable temporary, because BE's `LakeTabletsChannel` does not support
        // parallel send from a single sender.
        if (streamLoadInfo.getLoadParallelRequestNum() != 0 && !destTable.isCloudNativeTable()) {
            // only dup_keys can use parallel write since other table's the order of write is important
            if (destTable.getKeysType() == KeysType.DUP_KEYS) {
                queryOptions.setLoad_dop(streamLoadInfo.getLoadParallelRequestNum());
            } else {
                queryOptions.setLoad_dop(1);
            }
        }
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setMem_limit(streamLoadInfo.getExecMemLimit());
        queryOptions.setLoad_mem_limit(streamLoadInfo.getLoadMemLimit());
        params.setQuery_options(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        queryGlobals.setTimestamp_ms(new Date().getTime());
        queryGlobals.setTime_zone(streamLoadInfo.getTimezone());
        params.setQuery_globals(queryGlobals);


        LOG.info("load job id: {} tx id {} parallel {} compress {} replicated {} quorum {}", DebugUtil.printId(loadId),
                streamLoadInfo.getTxnId(),
                queryOptions.getLoad_dop(),
                queryOptions.getLoad_transmission_compression_type(), destTable.enableReplicatedStorage(),
                writeQuorum);
        return params;
    }

    // get all specified partition ids.
    // if no partition specified, return all partitions
    private List<Long> getAllPartitionIds() throws DdlException {
        List<Long> partitionIds = Lists.newArrayList();

        PartitionNames partitionNames = streamLoadInfo.getPartitions();
        if (partitionNames != null) {
            for (String partName : partitionNames.getPartitionNames()) {
                Partition part = destTable.getPartition(partName, partitionNames.isTemp());
                if (part == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_PARTITION, partName, destTable.getName());
                }
                partitionIds.add(part.getId());
            }
        } else {
            for (Partition partition : destTable.getPartitions()) {
                partitionIds.add(partition.getId());
            }
            if (partitionIds.isEmpty()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_EMPTY_PARTITION_IN_TABLE, destTable.getName());
            }
        }

        return partitionIds;
    }
}
