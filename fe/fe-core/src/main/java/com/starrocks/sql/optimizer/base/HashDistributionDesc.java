// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class HashDistributionDesc {
    public enum SourceType {
        LOCAL, // hash property from scan node
        // @Todo: Should modify Coordinator and PlanFragmentBuilder.
        //  For sql: select * from t1 join[shuffle] (select x1, sum(1) from t1 group by x1) s on t1.x2 = s.x1;
        //  Join check isn't bucket shuffle/shuffle depend on child is exchange node.
        //  Join will mistake shuffle as bucket shuffle in PlanFragmentBuilder if aggregate node
        //  use SHUFFLE_JOIN, and the hash algorithm is different which use for shuffle and bucket shuffle, then
        //  the result will be error
        SHUFFLE_AGG, // hash property from shuffle agg
        SHUFFLE_JOIN, // hash property from shuffle join
        BUCKET_JOIN, // hash property from bucket join
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

        // different columns size is allowed if this sourceType is LOCAL or SHUFFLE_AGG
        if (!(SourceType.LOCAL.equals(sourceType) || SourceType.SHUFFLE_AGG.equals(sourceType)) &&
                item.columns.size() != this.columns.size()) {
            return false;
        }

        for (int i = 0; i < this.columns.size(); i++) {
            if (!Objects.equals(this.columns.get(i), item.columns.get(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean isLocalShuffle() {
        return this.sourceType == SourceType.LOCAL;
    }

    public boolean isJoinShuffle() {
        return this.sourceType == SourceType.SHUFFLE_JOIN;
    }

    public boolean isShuffleEnforce() {
        return this.sourceType == SourceType.SHUFFLE_ENFORCE;
    }

    public boolean isBucketJoin() {
        return this.sourceType == SourceType.BUCKET_JOIN;
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
