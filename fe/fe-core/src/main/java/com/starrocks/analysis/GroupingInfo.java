// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/GroupingInfo.java

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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Deprecated
public class GroupingInfo {
    public static final String COL_GROUPING_ID = "GROUPING_ID";

    private VirtualSlotRef groupingIDSlot;
    private TupleDescriptor virtualTuple;
    private Set<VirtualSlotRef> groupingSlots;
    private List<BitSet> groupingIdList;
    private GroupByClause.GroupingType groupingType;
    private BitSet bitSetAll;

    public GroupingInfo(Analyzer analyzer, GroupByClause.GroupingType groupingType) throws AnalysisException {
        this.groupingType = groupingType;
        groupingSlots = new LinkedHashSet<>();
        virtualTuple = analyzer.getDescTbl().createTupleDescriptor("VIRTUAL_TUPLE");
        groupingIDSlot = new VirtualSlotRef(COL_GROUPING_ID, Type.BIGINT, virtualTuple, new ArrayList<>());
        groupingIDSlot.analyze(analyzer);
        groupingSlots.add(groupingIDSlot);
    }

    public Set<VirtualSlotRef> getGroupingSlots() {
        return groupingSlots;
    }

    public TupleDescriptor getVirtualTuple() {
        return virtualTuple;
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

    // generate virtual slots for grouping or grouping_id functions
    public VirtualSlotRef addGroupingSlots(List<Expr> realSlots, Analyzer analyzer) throws AnalysisException {
        String colName = realSlots.stream().map(expr -> expr.toSql()).collect(Collectors.joining(
                "_"));
        colName = "GROUPING_PREFIX_" + colName;
        VirtualSlotRef virtualSlot = new VirtualSlotRef(colName, Type.BIGINT, virtualTuple, realSlots);
        virtualSlot.analyze(analyzer);
        if (groupingSlots.contains(virtualSlot)) {
            for (VirtualSlotRef vs : groupingSlots) {
                if (vs.equals(virtualSlot)) {
                    return vs;
                }
            }
        }
        groupingSlots.add(virtualSlot);
        return virtualSlot;
    }

    // generate the bitmap that repeated node will repeat rows according to
    public void buildRepeat(ArrayList<Expr> groupingExprs, List<ArrayList<Expr>> groupingSetList) {
        groupingIdList = new ArrayList<>();
        bitSetAll = new BitSet();
        bitSetAll.set(0, groupingExprs.size(), true);
        switch (groupingType) {
            case CUBE:
                // it will generate the full permutation bitmap of cube item
                // e.g. cube (k1,k2,k3) the result is ["{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 1, 2}",
                // "{0, 2}"]
                for (int i = 0; i < (1 << groupingExprs.size()); i++) {
                    BitSet bitSet = new BitSet();
                    for (int j = 0; j < groupingExprs.size(); j++) {
                        if ((i & (1 << j)) > 0) {
                            bitSet.set(j, true);
                        }
                    }
                    groupingIdList.add(bitSet);
                }
                break;

            case ROLLUP:
                for (int i = 0; i <= groupingExprs.size(); i++) {
                    BitSet bitSet = new BitSet();
                    bitSet.set(0, i);
                    groupingIdList.add(bitSet);
                }
                break;

            case GROUPING_SETS:
                for (ArrayList<Expr> list : groupingSetList) {
                    BitSet bitSet = new BitSet();
                    for (int i = 0; i < groupingExprs.size(); i++) {
                        bitSet.set(i, list.contains(groupingExprs.get(i)));
                    }
                    if (!groupingIdList.contains(bitSet)) {
                        groupingIdList.add(bitSet);
                    }
                }
                break;

            default:
                Preconditions.checkState(false);
        }
        groupingExprs.addAll(groupingSlots);
    }

    // generate grouping function's value
    public List<List<Long>> genGroupingList(ArrayList<Expr> groupingExprs) {
        List<List<Long>> groupingList = new ArrayList<>();
        for (SlotRef slot : groupingSlots) {
            List<Long> glist = new ArrayList<>();
            for (BitSet bitSet : groupingIdList) {
                long l = 0L;
                // for all column, using for group by
                if (slot.getColumnName().equalsIgnoreCase(COL_GROUPING_ID)) {
                    BitSet newBitSet = new BitSet();
                    for (int i = 0; i < bitSetAll.length(); ++i) {
                        newBitSet.set(i, bitSet.get(bitSetAll.length() - i - 1));
                    }
                    newBitSet.flip(0, bitSetAll.length());
                    newBitSet.and(bitSetAll);
                    for (int i = 0; i < newBitSet.length(); ++i) {
                        l += newBitSet.get(i) ? (1L << i) : 0L;
                    }
                } else {
                    // for grouping[_id] functions
                    int slotSize = ((VirtualSlotRef) slot).getRealSlots().size();
                    for (int i = 0; i < slotSize; ++i) {
                        int j = groupingExprs.indexOf(((VirtualSlotRef) slot).getRealSlots().get(i));
                        l += bitSet.get(j) ? 0L : (1L << (slotSize - i - 1));
                    }
                }
                glist.add(l);
            }
            groupingList.add(glist);
        }
        return groupingList;
    }
}
