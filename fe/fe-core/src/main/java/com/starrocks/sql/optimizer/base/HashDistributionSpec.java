// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;

import java.util.List;
import java.util.Objects;

public class HashDistributionSpec extends DistributionSpec {
    private final HashDistributionDesc hashDistributionDesc;

    public HashDistributionSpec(HashDistributionDesc distributionDesc) {
        super(DistributionType.SHUFFLE);
        this.hashDistributionDesc = distributionDesc;
    }

    public HashDistributionSpec(HashDistributionDesc distributionDesc, PropertyInfo propertyInfo) {
        super(DistributionType.SHUFFLE, propertyInfo);
        this.hashDistributionDesc = distributionDesc;
    }

    public HashDistributionDesc getHashDistributionDesc() {
        return this.hashDistributionDesc;
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        if (!spec.type.equals(DistributionType.SHUFFLE)) {
            return false;
        }

        HashDistributionSpec other = (HashDistributionSpec) spec;
        HashDistributionDesc.SourceType thisSourceType = hashDistributionDesc.getSourceType();
        HashDistributionDesc.SourceType otherSourceType = other.hashDistributionDesc.getSourceType();
        // shuffle_local may satisfy shuffle_agg or shuffle_join
        if (!thisSourceType.equals(otherSourceType) &&
                thisSourceType != HashDistributionDesc.SourceType.LOCAL) {
            return false;
        }
        // check shuffle_local PropertyInfo
        if (thisSourceType == HashDistributionDesc.SourceType.LOCAL) {
            ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
            long tableId = propertyInfo.tableId;
            // Disable use colocate/bucket join when table with empty partition
            boolean satisfyColocate = (colocateIndex.isColocateTable(tableId) &&
                    !colocateIndex.isGroupUnstable(colocateIndex.getGroup(tableId)) &&
                    !propertyInfo.isEmptyPartition())
                    || propertyInfo.isSinglePartition();
            if (!satisfyColocate) {
                return false;
            }
        }
        // Outer join will produce NULL rows in different node, do aggregate may output multi null rows
        // if satisfy required shuffle directly
        if (otherSourceType == HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            ColumnRefSet otherColumns = new ColumnRefSet();
            other.hashDistributionDesc.getColumns().forEach(otherColumns::union);
            if (propertyInfo.nullableColumns.isIntersect(otherColumns)) {
                return false;
            }
        }
        return hashDistributionDesc.isSatisfy(other.hashDistributionDesc);
    }

    public List<Integer> getShuffleColumns() {
        return hashDistributionDesc.getColumns();
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashDistributionDesc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof HashDistributionSpec)) {
            return false;
        }

        final HashDistributionSpec spec = (HashDistributionSpec) obj;
        return this.hashDistributionDesc.equals(spec.hashDistributionDesc);
    }

    @Override
    public String toString() {
        return hashDistributionDesc.getSourceType().toString() + hashDistributionDesc.toString();
    }
}
