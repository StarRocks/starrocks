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

public class HashDistributionSpec extends DistributionSpec {
    private final HashDistributionDesc hashDistributionDesc;
    private final EquivalentDescriptor equivDesc;

    public HashDistributionSpec(HashDistributionDesc distributionDesc) {
        super(DistributionType.SHUFFLE);
        this.hashDistributionDesc = distributionDesc;
        this.equivDesc = new EquivalentDescriptor();
        equivDesc.initDistributionUnionFind(distributionDesc.getDistributionCols());
    }

    public HashDistributionSpec(HashDistributionDesc distributionDesc, EquivalentDescriptor descriptor) {
        super(DistributionType.SHUFFLE);
        this.hashDistributionDesc = distributionDesc;
        this.equivDesc = descriptor;
    }

    public HashDistributionDesc getHashDistributionDesc() {
        return this.hashDistributionDesc;
    }

    private boolean isJoinEqColumnsCompatible(HashDistributionSpec requiredSpec) {
        // keep same logical with `isSatisfy` method of HashDistributionDesc
        List<DistributionCol> requiredShuffleColumns = requiredSpec.getShuffleColumns();
        List<DistributionCol> shuffleColumns = getShuffleColumns();

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

    private boolean satisfyContainAll(List<DistributionCol> requiredShuffleColumns, List<DistributionCol> shuffleColumns) {
        // Minority meets majority
        for (int i = 0; i < shuffleColumns.size(); i++) {
            DistributionCol shuffleCol = shuffleColumns.get(i);
            int idx = 0;
            for (; idx < requiredShuffleColumns.size(); idx++) {
                DistributionCol requiredCol = requiredShuffleColumns.get(idx);
                if (equivDesc.isConnected(requiredCol, shuffleCol)) {
                    break;
                }
            }
            if (idx == requiredShuffleColumns.size()) {
                return false;
            }
        }
        return true;
    }

    private boolean satisfySameColumns(List<DistributionCol> requiredShuffleColumns, List<DistributionCol> shuffleColumns) {
        // must keep same
        if (requiredShuffleColumns.size() != shuffleColumns.size()) {
            return false;
        }

        for (int i = 0; i < shuffleColumns.size(); i++) {
            DistributionCol requiredCol = requiredShuffleColumns.get(i);
            DistributionCol shuffleCol = shuffleColumns.get(i);
            if (!equivDesc.isConnected(requiredCol, shuffleCol)) {
                return false;
            }

        }
        return true;
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        if (spec.type.equals(DistributionType.ROUND_ROBIN)) {
            return true;
        }

        if (!spec.type.equals(DistributionType.SHUFFLE)) {
            return false;
        }

        HashDistributionSpec other = (HashDistributionSpec) spec;
        HashDistributionDesc.SourceType thisSourceType = hashDistributionDesc.getSourceType();

        // check shuffle_local equivalentDescriptor
        if (thisSourceType == HashDistributionDesc.SourceType.LOCAL) {
            ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
            long tableId = equivDesc.getTableId();
            // Disable use colocate/bucket join when table with empty partition
            boolean satisfyColocate = equivDesc.isSinglePartition() || (colocateIndex.isColocateTable(tableId) &&
                    !colocateIndex.isGroupUnstable(colocateIndex.getGroup(tableId)) &&
                    !equivDesc.isEmptyPartition());
            if (!satisfyColocate) {
                return false;
            }
        }

        return hashDistributionDesc.isSatisfy(other.hashDistributionDesc) || isJoinEqColumnsCompatible(other);
    }

    public List<DistributionCol> getShuffleColumns() {
        return hashDistributionDesc.getDistributionCols();
    }

    public HashDistributionSpec getNullRelaxSpec(EquivalentDescriptor descriptor) {
        return new HashDistributionSpec(hashDistributionDesc.getNullRelaxDesc(), descriptor);
    }

    public HashDistributionSpec getNullStrictSpec(EquivalentDescriptor descriptor) {
        if (!hashDistributionDesc.isAllNullStrict()) {
            return new HashDistributionSpec(hashDistributionDesc.getNullStrictDesc(), descriptor);
        } else {
            return this;
        }
    }

    public boolean isAllNullStrict() {
        return hashDistributionDesc.isAllNullStrict();
    }

    public EquivalentDescriptor getEquivDesc() {
        return equivDesc;
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
