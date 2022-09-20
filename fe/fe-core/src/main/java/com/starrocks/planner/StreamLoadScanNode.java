// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/StreamLoadScanNode.java

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
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.common.Config;
import com.starrocks.load.Load;
import com.starrocks.task.StreamLoadTask;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.DefaultExpr.SUPPORTED_DEFAULT_FNS;

/**
 * used to scan from stream
 */
public class StreamLoadScanNode extends LoadScanNode {
    private static final Logger LOG = LogManager.getLogger(StreamLoadScanNode.class);

    private TUniqueId loadId;
    // TODO(zc): now we use scanRange
    // input parameter
    private Table dstTable;
    private StreamLoadTask streamLoadTask;

    // helper
    private Analyzer analyzer;
    private TupleDescriptor srcTupleDesc;
    private TBrokerScanRange brokerScanRange;

    // columns in column list is case insensitive
    private Map<String, SlotDescriptor> slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    private Map<String, Expr> exprsByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    // Use vectorized load for improving load performance
    // 1. now for orcfile only
    // 2. remove cast string, and transform data from orig datatype directly
    // 3. use vectorized engine
    private boolean useVectorizedLoad;

    // used to construct for streaming loading
    public StreamLoadScanNode(
            TUniqueId loadId, PlanNodeId id, TupleDescriptor tupleDesc, Table dstTable, StreamLoadTask streamLoadTask) {
        super(id, tupleDesc, "StreamLoadScanNode");
        this.loadId = loadId;
        this.dstTable = dstTable;
        this.streamLoadTask = streamLoadTask;
        this.useVectorizedLoad = false;
    }

    public void setUseVectorizedLoad(boolean useVectorizedLoad) {
        this.useVectorizedLoad = useVectorizedLoad;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        // can't call super.init(), because after super.init, conjuncts would be null
        assignConjuncts(analyzer);

        this.analyzer = analyzer;
        brokerScanRange = new TBrokerScanRange();

        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFile_type(streamLoadTask.getFileType());
        rangeDesc.setFormat_type(streamLoadTask.getFormatType());
        if (rangeDesc.format_type == TFileFormatType.FORMAT_JSON) {
            if (!streamLoadTask.getJsonPaths().isEmpty()) {
                rangeDesc.setJsonpaths(streamLoadTask.getJsonPaths());
            }
            if (!streamLoadTask.getJsonRoot().isEmpty()) {
                rangeDesc.setJson_root(streamLoadTask.getJsonRoot());
            }
            rangeDesc.setStrip_outer_array(streamLoadTask.isStripOuterArray());
        }
        rangeDesc.setSplittable(false);
        switch (streamLoadTask.getFileType()) {
            case FILE_LOCAL:
                rangeDesc.setPath(streamLoadTask.getPath());
                break;
            case FILE_STREAM:
                rangeDesc.setPath("Invalid Path");
                rangeDesc.setLoad_id(loadId);
                break;
            default:
                throw new UserException("unsupported file type, type=" + streamLoadTask.getFileType());
        }
        rangeDesc.setStart_offset(0);
        rangeDesc.setSize(-1);

        srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor("StreamLoadScanNode");

        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setStrict_mode(streamLoadTask.isStrictMode());

        Load.initColumns(dstTable, streamLoadTask.getColumnExprDescs(), null /* no hadoop function */,
                exprsByName, analyzer, srcTupleDesc, slotDescByName,
                params, true, useVectorizedLoad, Lists.newArrayList(),
                streamLoadTask.getFormatType() == TFileFormatType.FORMAT_JSON, streamLoadTask.isPartialUpdate());

        rangeDesc.setNum_of_columns_from_file(srcTupleDesc.getSlots().size());
        brokerScanRange.addToRanges(rangeDesc);

        // analyze where statement
        initWhereExpr(streamLoadTask.getWhereExpr(), analyzer);

        if (streamLoadTask.getColumnSeparator() != null) {
            String sep = streamLoadTask.getColumnSeparator().getColumnSeparator();
            byte[] setBytes = sep.getBytes(StandardCharsets.UTF_8);
            params.setColumn_separator(setBytes[0]);
            if (setBytes.length > 50) {
                throw new UserException("the column separator is limited to a maximum of 50 bytes");
            }
            if (setBytes.length > 1) {
                params.setMulti_column_separator(sep);
            }
        } else {
            params.setColumn_separator((byte) '\t');
        }
        if (streamLoadTask.getRowDelimiter() != null) {
            String sep = streamLoadTask.getRowDelimiter().getRowDelimiter();
            byte[] sepBytes = sep.getBytes(StandardCharsets.UTF_8);
            params.setRow_delimiter(sepBytes[0]);
            if (sepBytes.length > 50) {
                throw new UserException("the row delimiter is limited to a maximum of 50 bytes");
            }
            if (sepBytes.length > 1) {
                params.setMulti_row_delimiter(sep);
            }
        } else {
            params.setRow_delimiter((byte) '\n');
        }
        params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
        params.setDest_tuple_id(desc.getId().asInt());
        brokerScanRange.setParams(params);

        brokerScanRange.setBroker_addresses(Lists.newArrayList());
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException, UserException {
        finalizeParams();
    }

    private void finalizeParams() throws UserException {
        boolean negative = streamLoadTask.getNegative();
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        for (SlotDescriptor dstSlotDesc : desc.getSlots()) {
            if (!dstSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprsByName != null) {
                expr = exprsByName.get(dstSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(dstSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    destSidToSrcSidWithoutTrans.put(dstSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                    // If dest is allowed null, we set source to nullable
                    if (dstSlotDesc.getColumn().isAllowNull()) {
                        srcSlotDesc.setIsNullable(true);
                    }
                    SlotRef slotRef = new SlotRef(srcSlotDesc);
                    slotRef.setColumnName(dstSlotDesc.getColumn().getName());
                    expr = slotRef;
                } else {
                    Column column = dstSlotDesc.getColumn();
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
                        if (column.isAllowNull()) {
                            expr = NullLiteral.create(column.getType());
                        } else {
                            throw new AnalysisException("column has no source field, column=" + column.getName());
                        }
                    }
                }
            }

            // check hll_hash
            if (dstSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                            + dstSlotDesc.getColumn().getName() + "=" + FunctionSet.HLL_HASH + "(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.HLL_HASH)
                        && !fn.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                            + dstSlotDesc.getColumn().getName() + "=" + FunctionSet.HLL_HASH
                            + "(xxx) or " + dstSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(Type.HLL);
            }

            checkBitmapCompatibility(analyzer, dstSlotDesc, expr);

            if (negative && dstSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr = Expr.analyzeAndCastFold(expr);
            }
            expr = castToSlot(dstSlotDesc, expr);

            brokerScanRange.params.putToExpr_of_dest_slot(dstSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        brokerScanRange.params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
        brokerScanRange.params.setDest_tuple_id(desc.getId().asInt());
        // LOG.info("brokerScanRange is {}", brokerScanRange);

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeMemLayout();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNode_type(TPlanNodeType.FILE_SCAN_NODE);
        TFileScanNode fileScanNode = new TFileScanNode(desc.getId().asInt());
        fileScanNode.setEnable_pipeline_load(Config.enable_pipeline_load);
        planNode.setFile_scan_node(fileScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        TScanRangeLocations locations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);
        locations.setScan_range(scanRange);
        locations.setLocations(Lists.newArrayList());
        return Lists.newArrayList(locations);
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "StreamLoadScanNode";
    }

    @Override
    public boolean canUsePipeLine() {
        return Config.enable_pipeline_load;
    }
}
