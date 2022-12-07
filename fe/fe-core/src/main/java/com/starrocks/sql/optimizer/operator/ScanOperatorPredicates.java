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


package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ScanOperatorPredicates {
    // id -> partition key
    private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private List<ScalarOperator> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private List<ScalarOperator> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private List<ScalarOperator> nonPartitionConjuncts = Lists.newArrayList();
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();

    public Map<Long, PartitionKey> getIdToPartitionKey() {
        return idToPartitionKey;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    public List<PartitionKey> getSelectedPartitionKeys() {
        List<PartitionKey> partitions = Lists.newArrayList();
        for (long partitionId : selectedPartitionIds) {
            partitions.add(idToPartitionKey.get(partitionId));
        }
        return partitions;
    }

    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public List<ScalarOperator> getPartitionConjuncts() {
        return partitionConjuncts;
    }

    public List<ScalarOperator> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    public List<ScalarOperator> getNonPartitionConjuncts() {
        return nonPartitionConjuncts;
    }

    public List<ScalarOperator> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public Map<ColumnRefOperator, Column> getMinMaxColumnRefMap() {
        return minMaxColumnRefMap;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        ScanOperatorPredicates targetPredicts = (ScanOperatorPredicates) o;
        return Objects.equal(getSelectedPartitionIds(), targetPredicts.getSelectedPartitionIds()) &&
                Objects.equal(getIdToPartitionKey(), targetPredicts.getIdToPartitionKey()) &&
                Objects.equal(getNoEvalPartitionConjuncts(), targetPredicts.getNoEvalPartitionConjuncts()) &&
                Objects.equal(getPartitionConjuncts(), targetPredicts.getPartitionConjuncts()) &&
                Objects.equal(getMinMaxConjuncts(), targetPredicts.getMinMaxConjuncts()) &&
                Objects.equal(getMinMaxColumnRefMap(), targetPredicts.getMinMaxColumnRefMap());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(idToPartitionKey, selectedPartitionIds, partitionConjuncts,
                noEvalPartitionConjuncts, nonPartitionConjuncts, minMaxConjuncts, minMaxColumnRefMap);
    }
}
