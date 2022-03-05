// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.load.Load;
import com.starrocks.load.LoadErrorHub;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.task.StreamLoadTask;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadErrorHubInfo;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
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
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private StreamLoadTask streamLoadTask;

    private Analyzer analyzer;
    private DescriptorTable descTable;

    public StreamLoadPlanner(Database db, OlapTable destTable, StreamLoadTask streamLoadTask) {
        this.db = db;
        this.destTable = destTable;
        this.streamLoadTask = streamLoadTask;
    }

    private void resetAnalyzer() {
        analyzer = new Analyzer(Catalog.getCurrentCatalog(), null);
        // TODO(cmy): currently we do not support UDF in stream load command.
        // Because there is no way to check the privilege of accessing UDF..
        analyzer.setUDFAllowed(false);
        descTable = analyzer.getDescTbl();
    }

    // can only be called after "plan()", or it will return null
    public OlapTable getDestTable() {
        return destTable;
    }

    // create the plan. the plan's query id and load id are same, using the parameter 'loadId'
    public TExecPlanFragmentParams plan(TUniqueId loadId) throws UserException {
        resetAnalyzer();
        // construct tuple descriptor, used for scanNode and dataSink
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        boolean negative = streamLoadTask.getNegative();
        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        // here we should be full schema to fill the descriptor table
        for (Column col : destTable.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggreateType. column:" + col.getName());
            }

            if (col.getType().isVarchar() && IDictManager.getInstance().hasGlobalDict(destTable.getId(),
                    col.getName())) {
                Optional<ColumnDict>  dict = IDictManager.getInstance().getGlobalDict(destTable.getId(), col.getName());
                if (dict != null && dict.isPresent()) {
                    globalDicts.add(new Pair<>(slotDesc.getId().asInt(), dict.get()));
                }
            }
        }
        if (destTable.getKeysType() == KeysType.PRIMARY_KEYS) {
            // add op type column
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(new Column(Load.LOAD_OP_COLUMN, Type.TINYINT));
            slotDesc.setIsNullable(false);
        }

        // create scan node
        StreamLoadScanNode scanNode =
                new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable, streamLoadTask);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);

        LOG.info("use vectorized load: {}, load job id: {}", scanNode.isUseVectorized(), loadId);
        descTable.computeMemLayout();

        // create dest sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(destTable, tupleDesc, partitionIds);
        olapTableSink.init(loadId, streamLoadTask.getTxnId(), db.getId(), streamLoadTask.getTimeout());
        olapTableSink.complete();

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);
        // After data loading, we need to check the global dict for low cardinality string column
        // whether update.
        fragment.setLoadGlobalDicts(globalDicts);

        fragment.finalize(null, false);

        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocol_version(InternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDesc_tbl(analyzer.getDescTbl().toThrift());

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // user load id (streamLoadTask.id) as query id
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
        queryOptions.setQuery_timeout(streamLoadTask.getTimeout());
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setMem_limit(streamLoadTask.getExecMemLimit());
        queryOptions.setLoad_mem_limit(streamLoadTask.getLoadMemLimit());
        params.setQuery_options(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        queryGlobals.setTimestamp_ms(new Date().getTime());
        queryGlobals.setTime_zone(streamLoadTask.getTimezone());
        params.setQuery_globals(queryGlobals);

        // set load error hub if exist
        LoadErrorHub.Param param = Catalog.getCurrentCatalog().getLoadInstance().getLoadErrorHubInfo();
        if (param != null) {
            TLoadErrorHubInfo info = param.toThrift();
            if (info != null) {
                params.setLoad_error_hub_info(info);
            }
        }

        // LOG.debug("stream load txn id: {}, plan: {}", streamLoadTask.getTxnId(), params);
        return params;
    }

    // get all specified partition ids.
    // if no partition specified, return all partitions
    private List<Long> getAllPartitionIds() throws DdlException {
        List<Long> partitionIds = Lists.newArrayList();

        PartitionNames partitionNames = streamLoadTask.getPartitions();
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
