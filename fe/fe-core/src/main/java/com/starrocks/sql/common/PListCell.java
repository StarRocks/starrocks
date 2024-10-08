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
import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.PartitionValue;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code PListCell} means a list partition's multiple values.
 * eg: partition p1 values in ((1, 'a'), (2, 'b')) is a partition items which contains multi values
 * with multi partition columns
 *  partitionItems  : ((1, 'a'), (2, 'b'))
 */
public final class PListCell extends PCell implements Comparable<PListCell> {
    // default partition values which may contain null value, and should be compared in the end
    public static Set<String> DEFAULT_PARTITION_VALUES = new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
            .add(PartitionValue.STARROCKS_DEFAULT_PARTITION_VALUE)
            .add(PartitionUtil.ICEBERG_DEFAULT_PARTITION)
            .add(HiveMetaClient.PARTITION_NULL_VALUE)
            .add(HiveMetaClient.HUDI_PARTITION_NULL_VALUE)
            .build();

    // multi values: the order is only associated comparing.
    @SerializedName(("partitionItems"))
    private final List<List<String>> partitionItems;

    public PListCell(List<List<String>> items) {
        Objects.requireNonNull(items);
        this.partitionItems = items;
    }

    // single value with single partition column cell
    public PListCell(String item) {
        Objects.requireNonNull(item);
        this.partitionItems = ImmutableList.of(ImmutableList.of(item));
    }

    public List<List<String>> getPartitionItems() {
        return partitionItems;
    }

    public int getItemSize() {
        if (partitionItems == null) {
            return 0;
        }
        return partitionItems.size();
    }

    public Set<PListCell> toSingleValueCells() {
        if (partitionItems == null) {
            return Sets.newHashSet();
        }
        return partitionItems.stream()
                .map(item -> new PListCell(ImmutableList.of(item)))
                .collect(Collectors.toSet());
    }

    public Set<PListAtom> toAtoms() {
        if (partitionItems == null) {
            return Sets.newHashSet();
        }
        return partitionItems.stream()
                .map(item -> new PListAtom(item))
                .collect(Collectors.toSet());
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
        int len = Math.min(len1, len2);
        int ans = 0;
        // compare each partition item by item's value
        // eg:
        // partitionItems1: '20240101'
        // partitionItems2: '20240102', '20240103'
        // 1. compare '20240101' and '20240102'
        // 2. then compare lengths of partitionItems1 and partitionItems2
        // compare each partition item by item's value
        for (int i = 0; i < len; i++) {
            // prefer the partition item with greater values
            List<String> atom1 = partitionItems.get(i);
            List<String> atom2 = o.partitionItems.get(i);
            if (atom1.size() != atom2.size()) {
                return Integer.compare(atom1.size(), atom2.size());
            }
            for (int j = 0; j < atom1.size(); j++) {
                String value1 = atom1.get(j);
                String value2 = atom2.get(j);
                // if one of the partition item is default partition value, prefer the other one
                if (isDefaultPartitionValue(value1)) {
                    return -1;
                }
                if (isDefaultPartitionValue(value2)) {
                    return 1;
                }
                ans = atom1.get(j).compareTo(atom2.get(j));
                if (ans != 0) {
                    return ans;
                }
            }
        }
        // compare len if all partition items are equal
        return Integer.compare(len1, len2);
    }

    private boolean isDefaultPartitionValue(String val) {
        return DEFAULT_PARTITION_VALUES.contains(val);
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

    /**
     * Serialize the PListCell to string
     */
    public String serialize() {
        return GsonUtils.GSON.toJson(this);
    }

    /**
     * Deserialize PListCell items from string
     * @param str serialized partition items string
     */
    public static PListCell deserialize(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return GsonUtils.GSON.fromJson(str, PListCell.class);
    }

    /**
     * PListCellBatchRecord represents a batch of PListCell which is used to serialize and deserialize PListCells.
     */
    private static class PListCellBatchRecord {
        @SerializedName("data")
        private final Set<PListCell> pListCells;

        public PListCellBatchRecord(Set<PListCell> pListCells) {
            this.pListCells = pListCells;
        }

        public Set<PListCell> getPListCells() {
            return pListCells;
        }

        public static PListCellBatchRecord fromJson(String json) {
            return GsonUtils.GSON.fromJson(json, PListCellBatchRecord.class);
        }
    }

    /**
     * Serialize the PListCell values to string
     * @param partitionValues list partition values
     */
    public static String batchSerialize(Set<PListCell> partitionValues) {
        if (CollectionUtils.isEmpty(partitionValues)) {
            return null;
        }
        PListCellBatchRecord batch = new PListCellBatchRecord(partitionValues);
        return GsonUtils.GSON.toJson(batch);
    }

    /**
     * Deserialize the PListCell values from string
     * @param partitionValues serialized partition values string
     */
    public static Set<PListCell> batchDeserialize(String partitionValues) {
        if (StringUtils.isEmpty(partitionValues)) {
            return null;
        }
        PListCellBatchRecord batch = PListCellBatchRecord.fromJson(partitionValues);
        return batch.getPListCells();
    }
}