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

import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

// Property information for hash distribution desc, it used for check DistributionSpec satisfy condition.
public final class DistributionPropertyInfo {
    public DistributionPropertyInfo() {}

    public long tableId = -1;

    public List<Long> partitionIds = Lists.newArrayList();

    private DistributionDisjointSet nullStrictDisjointSet = new DistributionDisjointSet();

    private DistributionDisjointSet nullRelaxDisjointSet = new DistributionDisjointSet();

    public boolean isSinglePartition() {
        return partitionIds.size() == 1;
    }

    public boolean isEmptyPartition() {
        return partitionIds.size() == 0;
    }

    public void initDistributionDisjointSet(List<DistributionCol> distributionCols) {
        for (DistributionCol col : distributionCols) {
            if (col.isNullStrict()) {
                nullStrictDisjointSet.add(col);
                nullRelaxDisjointSet.add(new DistributionCol(col.getColId(), false));
            } else {
                nullRelaxDisjointSet.add(col);
            }
        }
    }

    public DistributionDisjointSet getNullStrictDisjointSet() {
        return nullStrictDisjointSet;
    }

    public DistributionDisjointSet getNullRelaxDisjointSet() {
        return nullRelaxDisjointSet;
    }

    public void setNullStrictDisjointSet(DistributionDisjointSet nullStrictDisjointSet) {
        this.nullStrictDisjointSet = nullStrictDisjointSet;
    }

    public void setNullRelaxDisjointSet(DistributionDisjointSet nullRelaxDisjointSet) {
        this.nullRelaxDisjointSet = nullRelaxDisjointSet;
    }

    public void unionDistributionCols(DistributionCol leftCol, DistributionCol rightCol) {
        checkState(leftCol.isNullStrict() == rightCol.isNullStrict(),
                "%s and %s should have same nullStrict value", leftCol, rightCol);
        if (leftCol.isNullStrict()) {
            nullStrictDisjointSet.union(leftCol, rightCol);
            nullRelaxDisjointSet.union(leftCol.getNullRelaxCol(), rightCol.getNullRelaxCol());
        } else {
            nullRelaxDisjointSet.union(leftCol, rightCol);
            nullStrictDisjointSet.union(leftCol.getNullStrictCol(), rightCol.getNullStrictCol());
        }
    }

    public void unionNullRelaxCols(DistributionCol leftCol, DistributionCol rightCol) {
        nullRelaxDisjointSet.union(leftCol.getNullRelaxCol(), rightCol.getNullRelaxCol());
    }

    public void clearNullStrictDisjointSet() {
        nullStrictDisjointSet = new DistributionDisjointSet();
    }

    public boolean isConnected(DistributionCol requiredCol, DistributionCol existDistributionCol) {
        if (requiredCol.isNullStrict() && !existDistributionCol.isNullStrict()) {
            return false;
        }

        if (requiredCol.isNullStrict()) {
            return nullStrictDisjointSet.isEquivalent(requiredCol, existDistributionCol);
        } else {
            return nullRelaxDisjointSet.isEquivalent(requiredCol, existDistributionCol.getNullRelaxCol());
        }
    }

    public DistributionPropertyInfo copy() {
        DistributionPropertyInfo copy = new DistributionPropertyInfo();
        copy.tableId = tableId;
        copy.partitionIds = partitionIds;
        copy.nullRelaxDisjointSet = nullRelaxDisjointSet.copy();
        copy.nullStrictDisjointSet = nullStrictDisjointSet.copy();
        return copy;
    }
}
