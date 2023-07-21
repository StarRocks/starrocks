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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SortInfo.java

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
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 * TODO: reorganize this completely, this doesn't really encapsulate anything; this
 * should move into planner/ and encapsulate the implementation of the sort of a
 * particular input row (materialize all row slots)
 */
public class SortInfo {
    // All ordering exprs with cost greater than this will be materialized. Since we don't
    // currently have any information about actual function costs, this value is intended to
    // ensure that all expensive functions will be materialized while still leaving simple
    // operations unmaterialized, for example 'SlotRef + SlotRef' should have a cost below
    // this threshold.
    // TODO: rethink this when we have a better cost model.
    private static final float SORT_MATERIALIZATION_COST_THRESHOLD = Expr.FUNCTION_CALL_COST;

    // Only used in local partition topn
    private List<Expr> partitionExprs_;
    private long partitionLimit_;
    private List<Expr> orderingExprs_;
    private final List<Boolean> isAscOrder_;
    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private final List<Boolean> nullsFirstParams_;
    // Subset of ordering exprs that are materialized. Populated in
    // createMaterializedOrderExprs(), used for EXPLAIN output.
    private List<Expr> materializedOrderingExprs_;
    // The single tuple that is materialized, sorted, and output by a sort operator
    // (i.e. SortNode or TopNNode)
    private TupleDescriptor sortTupleDesc_;
    // Input expressions materialized into sortTupleDesc_. One expr per slot in
    // sortTupleDesc_.
    private List<Expr> sortTupleSlotExprs_;

    public SortInfo(List<Expr> partitionExprs, long partitionLimit, List<Expr> orderingExprs, List<Boolean> isAscOrder,
                    List<Boolean> nullsFirstParams) {
        Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
        Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
        partitionExprs_ = partitionExprs;
        partitionLimit_ = partitionLimit;
        orderingExprs_ = orderingExprs;
        isAscOrder_ = isAscOrder;
        nullsFirstParams_ = nullsFirstParams;
        materializedOrderingExprs_ = Lists.newArrayList();
    }

    /**
     * C'tor for cloning.
     */
    private SortInfo(SortInfo other) {
        partitionExprs_ = Expr.cloneList(other.partitionExprs_);
        partitionLimit_ = other.partitionLimit_;
        orderingExprs_ = Expr.cloneList(other.orderingExprs_);
        isAscOrder_ = Lists.newArrayList(other.isAscOrder_);
        nullsFirstParams_ = Lists.newArrayList(other.nullsFirstParams_);
        materializedOrderingExprs_ = Expr.cloneList(other.materializedOrderingExprs_);
        sortTupleDesc_ = other.sortTupleDesc_;
        if (other.sortTupleSlotExprs_ != null) {
            sortTupleSlotExprs_ = Expr.cloneList(other.sortTupleSlotExprs_);
        }
    }

    /**
     * Sets sortTupleDesc_, which is the internal row representation to be materialized and
     * sorted. The source exprs of the slots in sortTupleDesc_ are changed to those in
     * tupleSlotExprs.
     */
    public void setMaterializedTupleInfo(
            TupleDescriptor tupleDesc, List<Expr> tupleSlotExprs) {
        Preconditions.checkState(tupleDesc.getSlots().size() == tupleSlotExprs.size());
        sortTupleDesc_ = tupleDesc;
        sortTupleSlotExprs_ = tupleSlotExprs;
        for (int i = 0; i < sortTupleDesc_.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = sortTupleDesc_.getSlots().get(i);
            slotDesc.setSourceExpr(sortTupleSlotExprs_.get(i));
        }
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs_;
    }

    public long getPartitionLimit() {
        return partitionLimit_;
    }

    public List<Expr> getOrderingExprs() {
        return orderingExprs_;
    }

    public List<Boolean> getIsAscOrder() {
        return isAscOrder_;
    }

    public List<Expr> getSortTupleSlotExprs() {
        return sortTupleSlotExprs_;
    }

    public TupleDescriptor getSortTupleDescriptor() {
        return sortTupleDesc_;
    }

    /**
     * Gets the list of booleans indicating whether nulls come first or last, independent
     * of asc/desc.
     */
    public List<Boolean> getNullsFirst() {
        Preconditions.checkState(orderingExprs_.size() == nullsFirstParams_.size());
        List<Boolean> nullsFirst = Lists.newArrayList();
        for (int i = 0; i < orderingExprs_.size(); ++i) {
            nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i)
            ));
        }
        return nullsFirst;
    }

    public void substituteOrderingExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
        orderingExprs_ = Expr.substituteList(orderingExprs_, smap, analyzer, false);
    }

    @Override
    public SortInfo clone() {
        return new SortInfo(this);
    }
}

