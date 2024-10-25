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

package com.starrocks.sql.common;

import com.google.api.client.util.Lists;

import java.util.List;
import java.util.Objects;

/**
 * {@link PListCell} means a list partition with multiple values.
 * eg: partition p1 values in ((1, 'a'), (2, 'b')) is a partition items which contains multi values with multi partition columns
 *  partitionName: p1
 *  partitionItems: ((1, 'a'), (2, 'b'))
 */
public final class PListCellPlus {
    // multi values: the order is not important
    private final PListCell cell;
    private final String partitionName;

    public PListCellPlus(String partitionName,
                         PListCell cell) {
        this.partitionName = partitionName;
        this.cell = cell;
    }

    public PListCell getCell() {
        return cell;
    }

    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Convert a list of partition items to a map with partition name as key
     */
    public List<PListAtom> toAtoms() {
        List<PListAtom> partitionValues = Lists.newArrayList();
        for (List<String> item : cell.getPartitionItems()) {
            partitionValues.add(new PListAtom(item));
        }
        return partitionValues;
    }

    public List<PListAtom> toAtoms(List<Integer> refIds) {
        List<PListAtom> partitionValues = Lists.newArrayList();
        for (List<String> partitionKey : cell.getPartitionItems()) {
            List<String> selectedPartitionKey = Lists.newArrayList();
            for (Integer i : refIds) {
                selectedPartitionKey.add(partitionKey.get(i));
            }
            partitionValues.add(new PListAtom(selectedPartitionKey));
        }
        return partitionValues;
    }

    @Override
    public int hashCode() {
        // only consider partition key
        return Objects.hash(partitionName, cell);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof PListCellPlus)) {
            return false;
        }
        return partitionName.equals(((PListCellPlus) o).partitionName)
                && cell.equals(((PListCellPlus) o).cell);
    }

    @Override
    public String toString() {
        return "PListCell{" +
                "cell=" + cell +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }
}
