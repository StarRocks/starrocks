// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    private boolean isJoinEqColumnsCompatible(HashDistributionSpec requiredSpec) {
        List<Integer> requiredShuffleColumns = requiredSpec.getShuffleColumns();
        List<Integer> shuffleColumns = getShuffleColumns();

        // Local shuffle, including bucket shuffle and colocate shuffle, only need to verify the shuffleColumns part,
        // no need to care about the extra part in requiredShuffleColumns
        if (requiredShuffleColumns.size() < shuffleColumns.size()) {
            return false;
        }

        if (getHashDistributionDesc().isLocalShuffle()) {
            // Minority meets majority
            List<ColumnRefSet> requiredEquivalentColumns = requiredShuffleColumns.stream()
                    .map(c -> propertyInfo.getEquivalentColumns(c)).collect(Collectors.toList());
            List<ColumnRefSet> shuffleEquivalentColumns = shuffleColumns.stream()
                    .map(s -> propertyInfo.getEquivalentColumns(s)).collect(Collectors.toList());
            return requiredEquivalentColumns.containsAll(shuffleEquivalentColumns);
        } else if (requiredShuffleColumns.size() == shuffleColumns.size()) {
            // must keep same
            for (int i = 0; i < shuffleColumns.size(); i++) {
                int requiredShuffleColumn = requiredShuffleColumns.get(i);
                int outputShuffleColumn = shuffleColumns.get(i);
                /*
                 * Given the following example：
                 *      SELECT * FROM A JOIN B ON A.a = B.b
                 *      JOIN C ON B.b = C.c
                 *      JOIN D ON C.c = D.d
                 *      JOIN E ON D.d = E.e
                 * We focus on the third join `.. join D ON C.c = D.d`
                 * requiredColumn: D.d
                 * outputColumn: A.a
                 * joinEquivalentColumns: [A.a, B.b, C.c, D.d]
                 *
                 * A.a can be mapped to D.d through joinEquivalentColumns
                 */
                if (!propertyInfo.isEquivalentJoinOnColumns(requiredShuffleColumn, outputShuffleColumn)) {
                    return false;
                }
            }
            return true;
        }

        return false;
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
        // Left outer join will cause right table produce NULL in different node, also right outer join
        if (thisSourceType == HashDistributionDesc.SourceType.LOCAL &&
                otherSourceType == HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            ColumnRefSet otherColumns = new ColumnRefSet();
            other.hashDistributionDesc.getColumns().forEach(otherColumns::union);
            if (propertyInfo.nullableColumns.isIntersect(otherColumns)) {
                return false;
            }
        }
        return hashDistributionDesc.isSatisfy(other.hashDistributionDesc) || isJoinEqColumnsCompatible(other);
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
