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


package com.starrocks.sql.optimizer.base;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HashDistributionDesc {
    public enum SourceType {
        LOCAL, // hash property from scan node
        ICEBERG_LOCAL, // hash property from iceberg scan node
        // SHUFFLE AGG required contains column, like:
        // e.g. required SHUFFLE_AGG(a, b, c), child SHUFFLE_AGG(a), satisfy
        // e.g. required SHUFFLE_AGG(a, b, c), child SHUFFLE_AGG(b, a), satisfy
        // e.g. required SHUFFLE_AGG(a, b, c), child SHUFFLE_AGG(c, b), satisfy
        // e.g. required SHUFFLE_AGG(a, b, c), child SHUFFLE_AGG(b, c, a), satisfy
        SHUFFLE_AGG, // hash property from shuffle agg
        // SHUFFLE JOIN required must equals and order same, it's much stricter than SHUFFLE_AGG
        // e.g. required SHUFFLE_JOIN(a, b, c), child SHUFFLE_JOIN(a), not satisfy
        // e.g. required SHUFFLE_JOIN(a, b, c), child SHUFFLE_JOIN(a, b), not satisfy
        // e.g. required SHUFFLE_JOIN(a, b, c), child SHUFFLE_JOIN(b, c, a), not satisfy
        // e.g. required SHUFFLE_JOIN(a, b, c), child SHUFFLE_JOIN(a, b, c), satisfy
        SHUFFLE_JOIN, // hash property from shuffle join
        BUCKET, // hash property from bucket
        SHUFFLE_ENFORCE // parent node which can not satisfy the requirement will enforce child this hash property
    }

    private List<DistributionCol> distributionCols;

    // Which operator produce this hash DistributionDesc
    private final SourceType sourceType;

    // only used to build scan initial distribution info
    public HashDistributionDesc(List<Integer> columns, SourceType sourceType) {
        this.distributionCols = columns.stream().map(e -> new DistributionCol(e, true))
                .collect(Collectors.toList());
        this.sourceType = sourceType;
        Preconditions.checkState(!distributionCols.isEmpty());
    }

    public HashDistributionDesc(Collection<DistributionCol> distributionCols, SourceType sourceType) {
        this.distributionCols = Lists.newArrayList(distributionCols);
        this.sourceType = sourceType;
        Preconditions.checkState(!distributionCols.isEmpty());
    }

    public List<DistributionCol> getDistributionCols() {
        return distributionCols;
    }

    public List<Integer> getExplainInfo() {
        return distributionCols.stream().map(DistributionCol::getColId).collect(Collectors.toList());
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public boolean isSatisfy(HashDistributionDesc item) {
        if (item == this) {
            return true;
        }

        if (this.distributionCols.size() > item.distributionCols.size()) {
            return false;
        }

        if (this.sourceType == SourceType.SHUFFLE_AGG && item.sourceType == SourceType.SHUFFLE_JOIN) {
            return distributionColsEquals(item.distributionCols);
        } else if (this.sourceType == SourceType.SHUFFLE_JOIN && (item.sourceType == SourceType.SHUFFLE_AGG ||
                item.sourceType == SourceType.SHUFFLE_JOIN)) {
            return distributionColsContainsAll(item.distributionCols);
        } else if (!this.sourceType.equals(item.sourceType) &&
                this.sourceType != HashDistributionDesc.SourceType.LOCAL) {
            return false;
        }

        // different columns size is allowed if this sourceType is LOCAL or SHUFFLE_AGG
        if (SourceType.LOCAL.equals(sourceType) || SourceType.SHUFFLE_AGG.equals(sourceType)) {
            return distributionColsContainsAll(item.distributionCols);
        }
        return distributionColsEquals(item.distributionCols);
    }

    public boolean distributionColsEquals(List<DistributionCol> requiredCols) {
        if (this.distributionCols.size() != requiredCols.size()) {
            return false;
        }
        for (int i = 0; i < distributionCols.size(); i++) {
            DistributionCol col = distributionCols.get(i);
            DistributionCol requiredCol = requiredCols.get(i);
            if (!col.isSatisfy(requiredCol)) {
                return false;
            }
        }
        return true;
    }

    public boolean distributionColsContainsAll(List<DistributionCol> requiredCols) {
        for (DistributionCol col : distributionCols) {
            boolean find = false;
            for (DistributionCol requiredCol : requiredCols) {
                if (col.isSatisfy(requiredCol)) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                return false;
            }
        }
        return true;
    }



    public boolean isLocal() {
        return this.sourceType == SourceType.LOCAL;
    }

    public boolean isIcebergLocal() {
        return this.sourceType == SourceType.ICEBERG_LOCAL;
    }

    public boolean isShuffle() {
        return this.sourceType == SourceType.SHUFFLE_AGG || this.sourceType == SourceType.SHUFFLE_JOIN;
    }

    public boolean isAggShuffle() {
        return this.sourceType == SourceType.SHUFFLE_AGG;
    }

    public boolean isShuffleEnforce() {
        return this.sourceType == SourceType.SHUFFLE_ENFORCE;
    }

    public boolean isBucketJoin() {
        return this.sourceType == SourceType.BUCKET;
    }

    public HashDistributionDesc getNullRelaxDesc() {
        List<DistributionCol> newDistributionCols = distributionCols.stream()
                .map(e -> new DistributionCol(e.getColId(), false)).collect(Collectors.toList());
        return new HashDistributionDesc(newDistributionCols, sourceType);
    }

    public HashDistributionDesc getNullStrictDesc() {
        List<DistributionCol> newDistributionCols = distributionCols.stream()
                .map(e -> new DistributionCol(e.getColId(), true)).collect(Collectors.toList());
        return new HashDistributionDesc(newDistributionCols, sourceType);
    }

    public boolean isAllNullStrict() {
        return distributionCols.stream().allMatch(DistributionCol::isNullStrict);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof HashDistributionDesc)) {
            return false;
        }

        HashDistributionDesc other = (HashDistributionDesc) o;
        if (!sourceType.equals(other.sourceType)) {
            return false;
        }

        return Objects.equals(distributionCols, other.distributionCols) && Objects.equals(sourceType, other.sourceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distributionCols, sourceType);
    }

    @Override
    public String toString() {
        return distributionCols.toString();
    }

}
