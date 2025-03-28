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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/LoadScanNode.java

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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Map;

public abstract class LoadScanNode extends ScanNode {

    public LoadScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
    }

    protected void initWhereExpr(Expr whereExpr, Analyzer analyzer) throws StarRocksException {
        if (whereExpr == null) {
            return;
        }

        Map<String, SlotDescriptor> dstDescMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            dstDescMap.put(slotDescriptor.getColumn().getName(), slotDescriptor);
        }

        List<SlotRef> slots = Lists.newArrayList();
        whereExpr.collect(SlotRef.class, slots);

        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        for (SlotRef slot : slots) {
            SlotDescriptor slotDesc = dstDescMap.get(slot.getColumnName());
            if (slotDesc == null) {
                throw new StarRocksException("unknown column in where statement. "
                        + "the column '" + slot.getColumnName() + "' in where clause must be in the target table.");
            }
            smap.getLhs().add(slot);
            SlotRef slotRef = new SlotRef(slotDesc);
            slotRef.setColumnName(slot.getColumnName());
            smap.getRhs().add(slotRef);
        }
        whereExpr = whereExpr.clone(smap);
        whereExpr = Expr.analyzeAndCastFold(whereExpr);

        if (!whereExpr.getType().isBoolean()) {
            throw new StarRocksException("where statement is not a valid statement return bool");
        }
        addConjuncts(AnalyzerUtils.extractConjuncts(whereExpr));
    }

    protected void checkBitmapCompatibility(Analyzer analyzer, SlotDescriptor slotDesc, Expr expr)
            throws AnalysisException {
        if (slotDesc.getColumn().getAggregationType() == AggregateType.BITMAP_UNION) {
            expr.analyze(analyzer);
            if (!expr.getType().isBitmapType()) {
                String errorMsg = String.format("bitmap column %s require the function return type is BITMAP",
                        slotDesc.getColumn().getName());
                throw new AnalysisException(errorMsg);
            }
        }
    }

    // Return all available nodes under the warehouse to run load scan. Should consider different deployment modes
    // 1. Share-nothing: only backends can be used for scan
    // 2. Share-data: both backends and compute nodes can be used for scan
    public static List<ComputeNode> getAvailableComputeNodes(long warehouseId) {
        List<ComputeNode> nodes = Lists.newArrayList();
        // TODO: need to refactor after be split into cn + dn
        if (RunMode.isSharedDataMode()) {
            List<Long> computeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
            for (long cnId : computeNodeIds) {
                ComputeNode cn = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(cnId);
                if (cn != null && cn.isAvailable()) {
                    nodes.add(cn);
                }
            }
        } else {
            for (ComputeNode be : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend().values()) {
                if (be.isAvailable()) {
                    nodes.add(be);
                }
            }
        }
        return nodes;
    }

}
