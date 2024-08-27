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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ScanOperatorPredicates {
    // id -> partition key
    private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private List<ScalarOperator> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be sent to backend.
    private List<ScalarOperator> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private List<ScalarOperator> nonPartitionConjuncts = Lists.newArrayList();
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();
    // flag to indicate whether if has pruned partition
    private boolean hasPrunedPartition = false;

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
        this.hasPrunedPartition = true;
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public List<ScalarOperator> getPartitionConjuncts() {
        return partitionConjuncts;
    }

    public List<ScalarOperator> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    /**
     * TODO: it's better to record pruned partition predicates directly.
     * @return: Return pruned partition conjuncts after OptPartitionPruner.
     */
    public List<ScalarOperator> getPrunedPartitionConjuncts() {
        return partitionConjuncts.stream()
                .filter(x -> !noEvalPartitionConjuncts.contains(x)).collect(Collectors.toList());
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

    public boolean hasPrunedPartition() {
        return hasPrunedPartition;
    }

    public void clear() {
        idToPartitionKey.clear();
        selectedPartitionIds.clear();
        partitionConjuncts.clear();
        noEvalPartitionConjuncts.clear();
        nonPartitionConjuncts.clear();
        minMaxConjuncts.clear();
        minMaxColumnRefMap.clear();
    }

    @Override
    public ScanOperatorPredicates clone() {
        ScanOperatorPredicates other = new ScanOperatorPredicates();
        other.idToPartitionKey.putAll(this.idToPartitionKey);
        other.selectedPartitionIds.addAll(this.selectedPartitionIds);
        other.partitionConjuncts.addAll(this.partitionConjuncts);
        other.noEvalPartitionConjuncts.addAll(this.noEvalPartitionConjuncts);
        other.nonPartitionConjuncts.addAll(this.nonPartitionConjuncts);
        other.minMaxConjuncts.addAll(this.minMaxConjuncts);
        other.minMaxColumnRefMap.putAll(this.minMaxColumnRefMap);
        other.hasPrunedPartition = this.hasPrunedPartition;

        return other;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScanOperatorPredicates that = (ScanOperatorPredicates) o;
        return Objects.equals(idToPartitionKey, that.idToPartitionKey) &&
                Objects.equals(selectedPartitionIds, that.selectedPartitionIds) &&
                Objects.equals(partitionConjuncts, that.partitionConjuncts) &&
                Objects.equals(noEvalPartitionConjuncts, that.noEvalPartitionConjuncts) &&
                Objects.equals(nonPartitionConjuncts, that.nonPartitionConjuncts) &&
                Objects.equals(minMaxConjuncts, that.minMaxConjuncts) &&
                Objects.equals(minMaxColumnRefMap, that.minMaxColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idToPartitionKey, selectedPartitionIds, partitionConjuncts, noEvalPartitionConjuncts,
                nonPartitionConjuncts, minMaxConjuncts, minMaxColumnRefMap);
    }

    @Override
    public String toString() {
        List<String> strings = Lists.newArrayList();
        if (!selectedPartitionIds.isEmpty()) {
            strings.add(String.format("selectedPartitionIds=%s", selectedPartitionIds));
        }
        if (!partitionConjuncts.isEmpty()) {
            strings.add(String.format("partitionConjuncts=%s", partitionConjuncts));
        }
        if (!noEvalPartitionConjuncts.isEmpty()) {
            strings.add(String.format("noEvalPartitionConjuncts=%s", noEvalPartitionConjuncts));
        }
        if (!nonPartitionConjuncts.isEmpty()) {
            strings.add(String.format("nonPartitionConjuncts=%s", nonPartitionConjuncts));
        }
        if (!minMaxConjuncts.isEmpty()) {
            strings.add(String.format("minMaxConjuncts=%s", minMaxConjuncts));
        }
        return Joiner.on(", ").join(strings);
    }

    /**
     * Duplicate and rewrite all columnRefOperator in predicates by rewriter.
     */
    public void duplicate(ReplaceColumnRefRewriter rewriter) {
        this.partitionConjuncts = this.partitionConjuncts.stream().map(x -> rewriter.rewrite(x)).collect(Collectors.toList());
        this.noEvalPartitionConjuncts = this.noEvalPartitionConjuncts.stream()
                .map(x -> rewriter.rewrite(x)).collect(Collectors.toList());
        this.nonPartitionConjuncts = nonPartitionConjuncts.stream().map(x -> rewriter.rewrite(x)).collect(Collectors.toList());
        this.minMaxConjuncts = minMaxConjuncts.stream().map(x -> rewriter.rewrite(x)).collect(Collectors.toList());
        Map<ColumnRefOperator, Column> newMinMaxColumnRefMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Column> e : this.minMaxColumnRefMap.entrySet()) {
            newMinMaxColumnRefMap.put((ColumnRefOperator) rewriter.rewrite(e.getKey()), e.getValue());
        }
        this.minMaxColumnRefMap = newMinMaxColumnRefMap;
    }
}
