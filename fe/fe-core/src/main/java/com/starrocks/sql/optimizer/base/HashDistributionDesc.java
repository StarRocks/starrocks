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

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class HashDistributionDesc {
    public enum SourceType {
        LOCAL, // hash property from scan node
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

    private final List<Integer> columns;
    // Which operator produce this hash DistributionDesc
    private final SourceType sourceType;

    public HashDistributionDesc(List<Integer> columns, SourceType sourceType) {
        this.columns = columns;
        this.sourceType = sourceType;
        Preconditions.checkState(!columns.isEmpty());
    }

    public List<Integer> getColumns() {
        return columns;
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public boolean isSatisfy(HashDistributionDesc item) {
        if (item == this) {
            return true;
        }

        if (this.columns.size() > item.columns.size()) {
            return false;
        }

        if (this.sourceType == SourceType.SHUFFLE_AGG && item.sourceType == SourceType.SHUFFLE_JOIN) {
            return this.columns.size() == item.columns.size() && this.columns.equals(item.columns);
        } else if (this.sourceType == SourceType.SHUFFLE_JOIN && (item.sourceType == SourceType.SHUFFLE_AGG ||
                item.sourceType == SourceType.SHUFFLE_JOIN)) {
            return item.columns.containsAll(this.columns);
        } else if (!this.sourceType.equals(item.sourceType) &&
                this.sourceType != HashDistributionDesc.SourceType.LOCAL) {
            return false;
        }

        // different columns size is allowed if this sourceType is LOCAL or SHUFFLE_AGG
        if (SourceType.LOCAL.equals(sourceType) || SourceType.SHUFFLE_AGG.equals(sourceType)) {
            return item.columns.containsAll(this.columns);
        }

        if (this.columns.size() != item.columns.size()) {
            return false;
        }

        return this.columns.equals(item.columns);
    }

    public boolean isLocal() {
        return this.sourceType == SourceType.LOCAL;
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

        return Objects.equals(columns, other.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return columns.toString();
    }
}
