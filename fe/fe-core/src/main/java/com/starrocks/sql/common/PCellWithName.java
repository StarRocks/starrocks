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

import java.util.Objects;

/**
 * PCellWithName is a record that associates a partition name with a PCell.
 */
public record PCellWithName(String name, PCell cell) implements Comparable<PCellWithName> {
    public static PCellWithName of(String partitionName, PCell cell) {
        return new PCellWithName(partitionName, cell);
    }

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", cell=" + cell;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, cell);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PCellWithName)) {
            return false;
        }
        PCellWithName that = (PCellWithName) o;
        return name.equals(that.name) && cell.equals(that.cell);
    }

    @Override
    public int compareTo(PCellWithName o) {
        // compare by pcell
        int cellComparison = this.cell.compareTo(o.cell);
        if (cellComparison != 0) {
            return cellComparison;
        }
        // if pcell is equal, compare by partition name
        return this.name.compareTo(o.name);
    }
}
