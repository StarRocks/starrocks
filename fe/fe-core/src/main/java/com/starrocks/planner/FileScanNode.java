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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/FileScanNode.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BrokerTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.Load;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileScanNode;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.DefaultExpr.SUPPORTED_DEFAULT_FNS;

// Broker scan node
public class FileScanNode extends LoadScanNode {
    private static final Logger LOG = LogManager.getLogger(FileScanNode.class);
    private static final TBrokerFileStatusComparator T_BROKER_FILE_STATUS_COMPARATOR
            = new TBrokerFileStatusComparator();

    public static class TBrokerFileStatusComparator implements Comparator<TBrokerFileStatus> {
        @Override
        public int compare(TBrokerFileStatus o1, TBrokerFileStatus o2) {
            if (o1.size < o2.size) {
                return -1;
            } else if (o1.size > o2.size) {
                return 1;
            }
            return 0;
        }
    }

    private static final Comparator SCAN_RANGE_LOCATIONS_COMPARATOR =
            Comparator.comparingLong(
                    (Pair<TScanRangeLocations, Long> o) -> o.second);

    private final Random random = new Random(System.currentTimeMillis());

    // File groups need to
    private List<TScanRangeLocations> locationsList;
    private PriorityQueue<Pair<TScanRangeLocations, Long>> locationsHeap;

    // used both for load statement and select statement
    private int parallelInstanceNum;
    private long bytesPerInstance;

    // Parameters need to process
    private long loadJobId = -1; // -1 means this scan node is not for a load job
    private long txnId = -1;
    private Table targetTable;
    private BrokerDesc brokerDesc;
    private List<BrokerFileGroup> fileGroups;
    private boolean strictMode = true;

    private List<List<TBrokerFileStatus>> fileStatusesList;
    // file num
    private int filesAdded;
    
    private List<ComputeNode> nodes;
    private int nextBe = 0;

    private Analyzer analyzer;

    // Use vectorized load for improving load performance
    // 1. now for orcfile only
    // 2. remove cast string, and transform data from orig datatype directly
    // 3. use vectorized engine
    private boolean useVectorizedLoad;

    private LoadJob.JSONOptions jsonOptions = new LoadJob.JSONOptions();

    private boolean nullExprInAutoIncrement;
    private static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TBrokerScanRangeParams params;
        public TupleDescriptor tupleDescriptor;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
        public String timezone;
    }

    private List<ParamCreateContext> paramCreateContexts;

    public FileScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                        List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, desc, planNodeName);
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
        this.parallelInstanceNum = 1;
        this.useVectorizedLoad = false;
        this.nullExprInAutoIncrement = true;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        this.analyzer = analyzer;
        if (desc.getTable() != null) {
            Table tbl = desc.getTable();
            if (tbl instanceof BrokerTable) {
                BrokerTable brokerTable = (BrokerTable) tbl;
                try {
                    fileGroups = Lists.newArrayList(new BrokerFileGroup(brokerTable));
                } catch (AnalysisException e) {
                    throw new UserException(e.getMessage());
                }
                brokerDesc = new BrokerDesc(brokerTable.getBrokerName(), brokerTable.getBrokerProperties());
            } else if (tbl instanceof TableFunctionTable) {
                TableFunctionTable funcTable = (TableFunctionTable) tbl;
                try {
                    fileGroups = Lists.newArrayList(new BrokerFileGroup(funcTable));
                } catch (AnalysisException e) {
                    throw new UserException(e.getMessage());
                }
                brokerDesc = new BrokerDesc(funcTable.getProperties());
            }
            targetTable = tbl;
        }

        // Get all broker file status
        assignBackends();
        getFileStatusAndCalcInstance();

        paramCreateContexts = Lists.newArrayList();
        for (BrokerFileGroup fileGroup : fileGroups) {
            ParamCreateContext context = new ParamCreateContext();
            context.fileGroup = fileGroup;
            context.timezone = analyzer.getTimezone();
            // csv/json/parquet load is controlled by Config::enable_vectorized_file_load
            // if Config::enable_vectorized_file_load is set true,
            // vectorized load will been enabled
            TFileFormatType format = formatType(context.fileGroup.getFileFormat(), "");
            initParams(context);
            paramCreateContexts.add(context);
        }
    }

    private boolean isLoad() {
        return desc.getTable() == null;
    }

    @Deprecated
    public void setLoadInfo(Table targetTable,
                            BrokerDesc brokerDesc,
                            List<BrokerFileGroup> fileGroups) {
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
    }

    public void setLoadInfo(long loadJobId,
                            long txnId,
                            Table targetTable,
                            BrokerDesc brokerDesc,
                            List<BrokerFileGroup> fileGroups,
                            boolean strictMode,
                            int parallelInstanceNum) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.strictMode = strictMode;
        this.parallelInstanceNum = parallelInstanceNum;
    }

    public void setUseVectorizedLoad(boolean useVectorizedLoad) {
        this.useVectorizedLoad = useVectorizedLoad;
    }

    public void setJSONOptions(LoadJob.JSONOptions options) {
        this.jsonOptions = options;
    }

    public boolean nullExprInAutoIncrement() {
        return nullExprInAutoIncrement;
    }

    // Called from init, construct source tuple information
    private void initParams(ParamCreateContext context)
            throws UserException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setHdfs_read_buffer_size_kb(Config.hdfs_read_buffer_size_kb);
        context.params = params;

        BrokerFileGroup fileGroup = context.fileGroup;
        List<String> filePaths = fileGroup.getFilePaths();
        if (!brokerDesc.hasBroker()) {
            if (filePaths.size() == 0) {
                throw new DdlException("filegroup number=" + fileGroups.size() + " is illegal");
            }
            THdfsProperties hdfsProperties = new THdfsProperties();
            HdfsUtil.getTProperties(filePaths.get(0), brokerDesc, hdfsProperties); 
            params.setHdfs_properties(hdfsProperties);
        }
        byte[] column_separator = fileGroup.getColumnSeparator().getBytes(StandardCharsets.UTF_8);
        byte[] row_delimiter = fileGroup.getRowDelimiter().getBytes(StandardCharsets.UTF_8);
        if (column_separator.length != 1) {
            if (column_separator.length > 50) {
                throw new UserException("the column separator is limited to a maximum of 50 bytes");
            }
            params.setMulti_column_separator(fileGroup.getColumnSeparator());
        }
        if (row_delimiter.length != 1) {
            if (row_delimiter.length > 50) {
                throw new UserException("the row delimiter is limited to a maximum of 50 bytes");
            }
            params.setMulti_row_delimiter(fileGroup.getRowDelimiter());
        }

        params.setColumn_separator(column_separator[0]);
        params.setRow_delimiter(row_delimiter[0]);
        params.setStrict_mode(strictMode);
        params.setProperties(brokerDesc.getProperties());
        params.setUse_broker(brokerDesc.hasBroker());
        params.setSkip_header(fileGroup.getSkipHeader());
        params.setTrim_space(fileGroup.isTrimspace());
        params.setEnclose(fileGroup.getEnclose());
        params.setEscape(fileGroup.getEscape());
        params.setJson_file_size_limit(Config.json_file_size_limit);
        initColumns(context);
        initWhereExpr(fileGroup.getWhereExpr(), analyzer);
    }

    /**
     * This method is used to calculate the slotDescByName and exprMap.
     * The expr in exprMap is analyzed in this function.
     * The smap of slot which belongs to expr will be analyzed by src desc.
     * slotDescByName: the single slot from columns in load stmt
     * exprMap: the expr from column mapping in load stmt.
     *
     * @param context
     * @throws UserException
     */
    private void initColumns(ParamCreateContext context) throws UserException {
        context.tupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        // columns in column list is case insensitive
        context.slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        // for load job, column exprs is got from file group
        // for query, there is no column exprs, they will be got from table's schema in "Load.initColumns"
        List<ImportColumnDesc> columnExprs = Lists.newArrayList();
        List<String> columnsFromPath = Lists.newArrayList();
        if (isLoad()) {
            columnExprs = context.fileGroup.getColumnExprList();
            columnsFromPath = context.fileGroup.getColumnsFromPath();
        }

        Load.initColumns(targetTable, columnExprs,
                context.fileGroup.getColumnToHadoopFunction(), context.exprMap, analyzer,
                context.tupleDescriptor, context.slotDescByName, context.params, true,
                useVectorizedLoad, columnsFromPath);
    }

    private void finalizeParams(ParamCreateContext context) throws UserException, AnalysisException {
        Map<String, SlotDescriptor> slotDescByName = context.slotDescByName;
        Map<String, Expr> exprMap = context.exprMap;
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();

        boolean isNegative = context.fileGroup.isNegative();
        for (SlotDescriptor destSlotDesc : desc.getSlots()) {
            if (!destSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprMap != null) {
                expr = exprMap.get(destSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(destSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                    SlotRef slotRef = new SlotRef(srcSlotDesc);
                    slotRef.setColumnName(destSlotDesc.getColumn().getName());
                    expr = slotRef;
                } else {
                    Column column = destSlotDesc.getColumn();
                    Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                    if (defaultValueType == Column.DefaultValueType.CONST) {
                        expr = new StringLiteral(column.calculatedDefaultValue());
                    } else if (defaultValueType == Column.DefaultValueType.VARY) {
                        if (SUPPORTED_DEFAULT_FNS.contains(column.getDefaultExpr().getExpr())) {
                            expr = column.getDefaultExpr().obtainExpr();
                        } else {
                            throw new UserException("Column(" + column + ") has unsupported default value:"
                                    + column.getDefaultExpr().getExpr());
                        }
                    } else if (defaultValueType == Column.DefaultValueType.NULL) {
                        if (column.isAllowNull() || column.isAutoIncrement()) {
                            expr = NullLiteral.create(column.getType());
                            if (column.isAutoIncrement()) {
                                nullExprInAutoIncrement = false;
                            }
                        } else {
                            throw new UserException("Unknown slot ref("
                                    + destSlotDesc.getColumn().getName() + ") in source file");
                        }
                    }
                }
            }

            // check hll_hash
            if (destSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + destSlotDesc.getColumn().getName() + "=hll_hash(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.HLL_HASH) &&
                        !fn.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + destSlotDesc.getColumn().getName() + "=hll_hash(xxx) or " +
                            destSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(Type.HLL);
            }

            checkBitmapCompatibility(analyzer, destSlotDesc, expr);

            // analyze negative
            if (isNegative && destSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr = Expr.analyzeAndCastFold(expr);
            }
            expr = castToSlot(destSlotDesc, expr);
            context.params.putToExpr_of_dest_slot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        context.params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
        context.params.setSrc_tuple_id(context.tupleDescriptor.getId().asInt());
        context.params.setDest_tuple_id(desc.getId().asInt());
        context.params.setStrict_mode(strictMode);
        context.params.setJson_file_size_limit(Config.json_file_size_limit);
        // Need re compute memory layout after set some slot descriptor to nullable
        context.tupleDescriptor.computeMemLayout();
    }

    private TScanRangeLocations newLocations(TBrokerScanRangeParams params, String brokerName, boolean hasBroker)
            throws UserException {
        ComputeNode selectedBackend = nodes.get(nextBe++);
        nextBe = nextBe % nodes.size();

        // Generate on broker scan range
        TBrokerScanRange brokerScanRange = new TBrokerScanRange();
        brokerScanRange.setParams(params);

        if (hasBroker) {
            FsBroker broker = null;
            try {
                broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(brokerName, selectedBackend.getHost());
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            brokerScanRange.addToBroker_addresses(new TNetworkAddress(broker.ip, broker.port));
        } else {
            brokerScanRange.addToBroker_addresses(new TNetworkAddress("", 0));
        }

        // Scan range
        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScan_range(scanRange);

        TScanRangeLocation location = new TScanRangeLocation();
        location.setBackend_id(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
    }

    private TBrokerScanRange brokerScanRange(TScanRangeLocations locations) {
        return locations.scan_range.broker_scan_range;
    }

    private void getFileStatusAndCalcInstance() throws UserException {
        if (fileStatusesList == null || filesAdded == -1) {
            // FIXME(cmy): fileStatusesList and filesAdded can be set out of db lock when doing pull load,
            // but for now it is very difficult to set them out of db lock when doing broker query.
            // So we leave this code block here.
            // This will be fixed later.
            fileStatusesList = Lists.newArrayList();
            filesAdded = 0;
            for (BrokerFileGroup fileGroup : fileGroups) {
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    if (brokerDesc.hasBroker()) {
                        BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
                    } else {
                        HdfsUtil.parseFile(path, brokerDesc, fileStatuses);
                    }
                }
                fileStatusesList.add(fileStatuses);
                filesAdded += fileStatuses.size();
                for (TBrokerFileStatus fstatus : fileStatuses) {
                    LOG.info("Add file status is {}", fstatus);
                }
            }
        }
        Preconditions.checkState(fileStatusesList.size() == fileGroups.size());

        if (isLoad() && filesAdded == 0) {
            throw new UserException("No source file in this table(" + targetTable.getName() + ").");
        }

        long totalBytes = 0;
        for (List<TBrokerFileStatus> fileStatuses : fileStatusesList) {
            Collections.sort(fileStatuses, T_BROKER_FILE_STATUS_COMPARATOR);
            for (TBrokerFileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.size;
            }
        }

        // numInstances:
        // min(totalBytes / min_bytes_per_broker_scanner,
        //     backends_size * parallelInstanceNum)
        numInstances = (int) (totalBytes / Config.min_bytes_per_broker_scanner);
        numInstances = Math.min(nodes.size() * parallelInstanceNum, numInstances);
        numInstances = Math.max(1, numInstances);

        bytesPerInstance = (totalBytes + numInstances - 1) / (numInstances != 0 ? numInstances : 1);
    }

    private void assignBackends() throws UserException {
        nodes = Lists.newArrayList();

        // TODO: need to refactor after be split into cn + dn
        if (RunMode.isSharedDataMode()) {
            Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getDefaultWarehouse();
            for (long cnId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                ComputeNode cn = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(cnId);
                if (cn != null && cn.isAvailable()) {
                    nodes.add(cn);
                }
            }
        } else {
            for (ComputeNode be : GlobalStateMgr.getCurrentSystemInfo().getIdToBackend().values()) {
                if (be.isAvailable()) {
                    nodes.add(be);
                }
            }
        }

        if (nodes.isEmpty()) {
            throw new UserException("No available backends");
        }
        Collections.shuffle(nodes, random);
    }

    private TFileFormatType formatType(String fileFormat, String path) {
        if (fileFormat != null) {
            if (fileFormat.toLowerCase().equals("parquet")) {
                return TFileFormatType.FORMAT_PARQUET;
            } else if (fileFormat.toLowerCase().equals("orc")) {
                return TFileFormatType.FORMAT_ORC;
            } else if (fileFormat.toLowerCase().equals("json")) {
                return TFileFormatType.FORMAT_JSON;
            }
            // Attention: The compression type of csv format is from the suffix of filename.
        }

        String lowerCasePath = path.toLowerCase();
        if (lowerCasePath.endsWith(".parquet") || lowerCasePath.endsWith(".parq")) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCasePath.endsWith(".orc")) {
            return TFileFormatType.FORMAT_ORC;
        } else if (lowerCasePath.endsWith(".gz")) {
            return TFileFormatType.FORMAT_CSV_GZ;
        } else if (lowerCasePath.endsWith(".bz2")) {
            return TFileFormatType.FORMAT_CSV_BZ2;
        } else if (lowerCasePath.endsWith(".lz4")) {
            return TFileFormatType.FORMAT_CSV_LZ4_FRAME;
        } else if (lowerCasePath.endsWith(".deflate")) {
            return TFileFormatType.FORMAT_CSV_DEFLATE;
        } else if (lowerCasePath.endsWith(".zst")) {
            return TFileFormatType.FORMAT_CSV_ZSTD;
        } else {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void processFileGroup(
            ParamCreateContext context,
            List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (fileStatuses == null || fileStatuses.isEmpty()) {
            return;
        }

        // Create locations for file group
        createScanRangeLocations(context, fileStatuses);

        // Add files to locations with less allocated data
        Pair<TScanRangeLocations, Long> smallestLocations = null;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size(); ) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormat(), fileStatus.path);
            List<String> columnsFromPath = HdfsUtil.parseColumnsFromPath(fileStatus.path,
                    context.fileGroup.getColumnsFromPath());
            int numberOfColumnsFromFile = context.slotDescByName.size() - columnsFromPath.size();

            smallestLocations = locationsHeap.poll();
            long leftBytes = fileStatus.size - curFileOffset;
            long rangeBytes = 0;
            // The rest of the file belongs to one range
            boolean isEndOfFile = false;
            if (smallestLocations.second + leftBytes > bytesPerInstance &&
                    ((formatType == TFileFormatType.FORMAT_CSV_PLAIN || formatType == TFileFormatType.FORMAT_PARQUET)
                            && fileStatus.isSplitable)) {
                rangeBytes = bytesPerInstance - smallestLocations.second;
            } else {
                rangeBytes = leftBytes;
                isEndOfFile = true;
                i++;
            }

            TBrokerRangeDesc rangeDesc =
                    createBrokerRangeDesc(curFileOffset, fileStatus, formatType, rangeBytes, columnsFromPath,
                            numberOfColumnsFromFile);

            rangeDesc.setStrip_outer_array(jsonOptions.stripOuterArray);
            rangeDesc.setJsonpaths(jsonOptions.jsonPaths);
            rangeDesc.setJson_root(jsonOptions.jsonRoot);

            brokerScanRange(smallestLocations.first).addToRanges(rangeDesc);
            smallestLocations.second += rangeBytes;
            locationsHeap.add(smallestLocations);

            curFileOffset = isEndOfFile ? 0 : curFileOffset + rangeBytes;
        }

        // Put locations with valid scan ranges to locationsList
        while (!locationsHeap.isEmpty()) {
            TScanRangeLocations locations = locationsHeap.poll().first;
            if (brokerScanRange(locations).isSetRanges()) {
                locationsList.add(locations);
            }
        }
    }

    private TBrokerRangeDesc createBrokerRangeDesc(long curFileOffset, TBrokerFileStatus fileStatus,
                                                   TFileFormatType formatType, long rangeBytes,
                                                   List<String> columnsFromPath, int numberOfColumnsFromFile) {
        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFile_type(TFileType.FILE_BROKER);
        rangeDesc.setFormat_type(formatType);
        rangeDesc.setPath(fileStatus.path);
        rangeDesc.setSplittable(fileStatus.isSplitable);
        rangeDesc.setStart_offset(curFileOffset);
        rangeDesc.setSize(rangeBytes);
        rangeDesc.setFile_size(fileStatus.size);
        rangeDesc.setNum_of_columns_from_file(numberOfColumnsFromFile);
        rangeDesc.setColumns_from_path(columnsFromPath);
        return rangeDesc;
    }

    private void createScanRangeLocations(ParamCreateContext context, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        Preconditions.checkState(locationsHeap.isEmpty(), "Locations heap is not empty");

        long totalBytes = 0;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            totalBytes += fileStatus.size;
        }
        long numInstances = bytesPerInstance == 0 ? 1 : (totalBytes + bytesPerInstance - 1) / bytesPerInstance;
        // totalBytes may be 0, so numInstances may be 0
        numInstances = Math.max(numInstances, (long) 1);

        for (int i = 0; i < numInstances; ++i) {
            locationsHeap.add(Pair.create(newLocations(context.params, brokerDesc.getName(), brokerDesc.hasBroker()), 0L));
        }
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        locationsList = Lists.newArrayList();
        locationsHeap = new PriorityQueue<>(SCAN_RANGE_LOCATIONS_COMPARATOR);

        for (int i = 0; i < fileGroups.size(); ++i) {
            List<TBrokerFileStatus> fileStatuses = fileStatusesList.get(i);
            if (fileStatuses.isEmpty()) {
                continue;
            }
            ParamCreateContext context = paramCreateContexts.get(i);
            try {
                finalizeParams(context);
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            processFileGroup(context, fileStatuses);
        }

        // update numInstances
        numInstances = locationsList.size();

        if (LOG.isDebugEnabled()) {
            for (TScanRangeLocations locations : locationsList) {
                LOG.debug("Scan range is {}", locations);
            }
        }
        if (loadJobId != -1) {
            LOG.info("broker load job {} with txn {} has {} scan range: {}",
                    loadJobId, txnId, locationsList.size(),
                    locationsList.stream().map(loc -> loc.locations.get(0).backend_id).toArray());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.FILE_SCAN_NODE;
        TFileScanNode fileScanNode = new TFileScanNode(desc.getId().asInt());
        fileScanNode.setEnable_pipeline_load(true);
        msg.setFile_scan_node(fileScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return locationsList;
    }

    /**
     * check be/broker and replace with new alive be/broker if original be/broker in locations is dead
     */
    public void updateScanRangeLocations() {
        try {
            assignBackends();
        } catch (UserException e) {
            LOG.warn("assign backends failed.", e);
            // Just return, retry by LoadTask
            return;
        }

        Set<Long> aliveBes = nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet());
        nextBe = 0;
        for (TScanRangeLocations locations : locationsList) {
            TScanRangeLocation scanRangeLocation = locations.getLocations().get(0);
            TBrokerScanRange brokerScanRange = locations.getScan_range().getBroker_scan_range();
            TNetworkAddress address = brokerScanRange.getBroker_addresses().get(0);
            FsBroker fsBroker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(brokerDesc.getName(),
                    address.hostname, address.port);
            if (aliveBes.contains(scanRangeLocation.getBackend_id()) && (fsBroker != null && fsBroker.isAlive)) {
                continue;
            }

            TScanRangeLocations newLocations = null;
            try {
                // Get new alive be and broker here, and params is not used, so set null
                newLocations = newLocations(null, brokerDesc.getName(), brokerDesc.hasBroker());
            } catch (UserException e) {
                LOG.warn("new locations failed.", e);
                // Just return, retry by LoadTask
                return;
            }
            // update backend
            locations.unsetLocations();
            locations.addToLocations(newLocations.getLocations().get(0));
            // update broker
            brokerScanRange.unsetBroker_addresses();
            TBrokerScanRange newBrokerScanRange = newLocations.getScan_range().getBroker_scan_range();
            brokerScanRange.addToBroker_addresses(newBrokerScanRange.getBroker_addresses().get(0));
            LOG.info("broker load job {} with txn {} updates locations. backend from {} to {}, broker from {} to {}",
                    loadJobId, txnId, scanRangeLocation, locations.getLocations().get(0),
                    address, brokerScanRange.getBroker_addresses().get(0));
        }
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            BrokerTable brokerTable = (BrokerTable) targetTable;
            output.append(prefix).append("TABLE: ").append(brokerTable.getName()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(Joiner.on(",").join(brokerTable.getPaths())).append("\",\n");
        }
        if (brokerDesc != null) {
            output.append(prefix).append("BROKER: ").append(brokerDesc.getName()).append("\n");
        }
        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }


}
