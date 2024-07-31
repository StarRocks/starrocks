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
 * {@code PListCell} means a list partition's multiple values.
 * eg: partition p1 values in ((1, 'a'), (2, 'b')) is a partition items which contains multi values
 * with multi partition columns
 *  partitionItems  : ((1, 'a'), (2, 'b'))
 */
public final class PListCell extends PCell implements Comparable<PListCell> {
    // multi values: the order is only associated comparing.
    private final List<List<String>> partitionItems;

    public PListCell(List<List<String>> items) {
        Objects.requireNonNull(items);
        this.partitionItems = items;
    }

    public List<List<String>> getPartitionItems() {
        return partitionItems;
    }

    /**
     * Add a list of partition items as the partition values
     * @param items new partition items
     */
    public void addItems(List<List<String>> items) {
        partitionItems.addAll(items);
    }

    /**
     * Construct a new partition cell by using selected idx
     */
    public PListCell toPListCell(List<Integer> selectColIds) {
        List<List<String>> partitionItems = Lists.newArrayList();
        for (List<String> partitionKey : this.partitionItems) {
            List<String> selectedPartitionKey = Lists.newArrayList();
            for (Integer i : selectColIds) {
                selectedPartitionKey.add(partitionKey.get(i));
            }
            partitionItems.add(selectedPartitionKey);
        }
        return new PListCell(partitionItems);
    }

    @Override
    public int compareTo(PListCell o) {
        int len1 = partitionItems.size();
        int len2 = o.partitionItems.size();
        int ans = Integer.compare(len1, len2);
        if (ans != 0) {
            return ans;
        }
        for (int i = 0; i < len1; i++) {
            List<String> atom1 = partitionItems.get(i);
            List<String> atom2 = o.partitionItems.get(i);
            if (atom1.size() != atom2.size()) {
                return Integer.compare(atom1.size(), atom2.size());
            }
            for (int j = 0; j < atom1.size(); j++) {
                ans = atom1.get(j).compareTo(atom2.get(j));
                if (ans != 0) {
                    return ans;
                }
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        // only consider partition items
        return Objects.hash(partitionItems);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof PListCell)) {
            return false;
        }
        return partitionItems.equals(((PListCell) o).partitionItems);
    }

    @Override
    public String toString() {
        return "PListCell{" +
                "items=" + partitionItems +
                '}';
    }
}