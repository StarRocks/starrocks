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


package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.PartitionKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HDFSScanNodePredicates {
    // id -> partition key
    private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private final List<Expr> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private final List<Expr> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private final List<Expr> nonPartitionConjuncts = Lists.newArrayList();

    // List of conjuncts for min/max values that are used to skip data when scanning Parquet files.
    private final List<Expr> minMaxConjuncts = new ArrayList<>();
    private TupleDescriptor minMaxTuple;

    public TupleDescriptor getMinMaxTuple() {
        return minMaxTuple;
    }

    public void setMinMaxTuple(TupleDescriptor minMaxTuple) {
        this.minMaxTuple = minMaxTuple;
    }

    public Map<Long, PartitionKey> getIdToPartitionKey() {
        return idToPartitionKey;
    }

    public void setIdToPartitionKey(Map<Long, PartitionKey> idToPartitionKey) {
        this.idToPartitionKey = idToPartitionKey;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public List<Expr> getPartitionConjuncts() {
        return partitionConjuncts;
    }

    public List<Expr> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    public List<Expr> getNonPartitionConjuncts() {
        return nonPartitionConjuncts;
    }

    public List<Expr> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }
}
