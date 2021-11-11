// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class HashDistributionDesc {
    public enum SourceType {
        LOCAL, // hash property from scan node
        SHUFFLE_JOIN, // hash property from shuffle join
        BUCKET_JOIN, // hash property from bucket join
        // @Todo: It's a temporary solution
        FORCE_SHUFFLE_JOIN, // hash property from shuffle join if contains expression in on clause
        SHUFFLE_AGG, // hash property from shuffle agg,
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
