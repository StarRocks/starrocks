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

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.server.GlobalStateMgr;

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
        // keep same logical with `isSatisfy` method of HashDistributionDesc
        List<Integer> requiredShuffleColumns = requiredSpec.getShuffleColumns();
        List<Integer> shuffleColumns = getShuffleColumns();

        // Local shuffle, including bucket shuffle and colocate shuffle, only need to verify the shuffleColumns part,
        // no need to care about the extra part in requiredShuffleColumns
        if (requiredShuffleColumns.size() < shuffleColumns.size()) {
            return false;
        }

        HashDistributionDesc.SourceType requiredShuffleType = requiredSpec.getHashDistributionDesc().getSourceType();
        HashDistributionDesc.SourceType thisShuffleType = getHashDistributionDesc().getSourceType();

        if (thisShuffleType == HashDistributionDesc.SourceType.SHUFFLE_AGG &&
                requiredShuffleType == HashDistributionDesc.SourceType.SHUFFLE_JOIN) {
            return satisfySameColumns(requiredShuffleColumns, shuffleColumns);
        } else if (thisShuffleType == HashDistributionDesc.SourceType.SHUFFLE_JOIN &&
                requiredShuffleType == HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            return satisfyContainAll(requiredShuffleColumns, shuffleColumns);
        } else if (!thisShuffleType.equals(requiredShuffleType) &&
                thisShuffleType != HashDistributionDesc.SourceType.LOCAL) {
            return false;
        }

        // different columns size is allowed if this sourceType is LOCAL or SHUFFLE_AGG
        if (HashDistributionDesc.SourceType.LOCAL.equals(thisShuffleType) ||
                HashDistributionDesc.SourceType.SHUFFLE_AGG.equals(thisShuffleType)) {
            return satisfyContainAll(requiredShuffleColumns, shuffleColumns);
        }

        return satisfySameColumns(requiredShuffleColumns, shuffleColumns);
    }

    private boolean satisfyContainAll(List<Integer> requiredShuffleColumns, List<Integer> shuffleColumns) {
        // Minority meets majority
        List<ColumnRefSet> requiredEquivalentColumns = requiredShuffleColumns.stream()
                .map(c -> propertyInfo.getEquivalentColumns(c)).collect(Collectors.toList());
        List<ColumnRefSet> shuffleEquivalentColumns = shuffleColumns.stream()
                .map(s -> propertyInfo.getEquivalentColumns(s)).collect(Collectors.toList());
        return requiredEquivalentColumns.containsAll(shuffleEquivalentColumns);
    }

    private boolean satisfySameColumns(List<Integer> requiredShuffleColumns, List<Integer> shuffleColumns) {
        // must keep same
        if (requiredShuffleColumns.size() != shuffleColumns.size()) {
            return false;
        }

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

        // check shuffle_local PropertyInfo
        if (thisSourceType == HashDistributionDesc.SourceType.LOCAL) {
            ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
            long tableId = propertyInfo.tableId;
            // Disable use colocate/bucket join when table with empty partition
            boolean satisfyColocate = propertyInfo.isSinglePartition() || (colocateIndex.isColocateTable(tableId) &&
                    !colocateIndex.isGroupUnstable(colocateIndex.getGroup(tableId)) &&
                    !propertyInfo.isEmptyPartition());
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
        return hashDistributionDesc.getSourceType().toString() + hashDistributionDesc;
    }
}
