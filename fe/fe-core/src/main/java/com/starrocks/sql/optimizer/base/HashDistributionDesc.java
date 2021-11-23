// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
        // @Todo: It's a temporary solution
        FORCE_SHUFFLE_JOIN, // hash property from shuffle join if contains expression in on clause
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

        if (!sourceType.equals(item.sourceType)) {
            return false;
        }

        return isColumnsSatisfy(this.columns, item.columns);
    }

    public static boolean isColumnsSatisfy(List<Integer> left, List<Integer> right) {
        return right.containsAll(left);
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
