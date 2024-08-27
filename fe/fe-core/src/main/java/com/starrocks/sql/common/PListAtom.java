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

import java.util.List;
import java.util.Objects;

/**
 * {@link PListAtom} represents a partition atom with a value which is used in the List Partition tables.
 * eg: partition p1 values in (1, 'a') is a partition item which contains one value with multi partition columns
 */
public final class PListAtom {
    // a partition key may contain multi columns
    private final List<String> partitionItem;

    public PListAtom(List<String> partitionKeys) {
        this.partitionItem = partitionKeys;
    }

    public List<String> getPartitionItem() {
        return partitionItem;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof PListAtom)) {
            return false;
        }
        PListAtom other = (PListAtom) o;
        List<String> otherSelectedPartitionKeys = other.getPartitionItem();
        if (otherSelectedPartitionKeys.size() != partitionItem.size()) {
            return false;
        }
        int len = partitionItem.size();
        for (int i = 0; i < len; i++) {
            if (!partitionItem.get(i).equals(otherSelectedPartitionKeys.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // only consider partition key
        return Objects.hash(partitionItem);
    }

    @Override
    public String toString() {
        return "PListAtom{" +
                "partitionItems=" + partitionItem +
                '}';
    }
}