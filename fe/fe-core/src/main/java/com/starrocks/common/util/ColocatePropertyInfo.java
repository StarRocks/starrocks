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

package com.starrocks.common.util;

import com.google.common.base.Splitter;

import java.util.List;
import java.util.Objects;

/**
 * Parsed result of "colocate_with" property value.
 *
 * <p>Syntax:
 * <ul>
 *   <li>{@code "group_name"} -> colocateGroupName="group_name", colocateColumnNames=null</li>
 *   <li>{@code "group_name:col1,col2"} -> colocateGroupName="group_name", colocateColumnNames=["col1","col2"]</li>
 * </ul>
 *
 * <p>When colocateColumnNames is null, default colocate columns are all sort key columns.
 *
 * <p>{@link #of(String)} and {@link #toString()} are inverse operations:
 * {@code ColocatePropertyInfo.of(info.toString())} produces an equivalent instance.
 */
public class ColocatePropertyInfo {
    private final String colocateGroupName;
    private final List<String> colocateColumnNames; // null = default (all sort key)

    public ColocatePropertyInfo(String colocateGroupName, List<String> colocateColumnNames) {
        this.colocateGroupName = Objects.requireNonNull(colocateGroupName,
                "colocateGroupName must not be null");
        this.colocateColumnNames = colocateColumnNames;
    }

    public String getColocateGroupName() {
        return colocateGroupName;
    }

    public List<String> getColocateColumnNames() {
        return colocateColumnNames;
    }

    /**
     * Parses a "colocate_with" property value into a ColocatePropertyInfo.
     *
     * @param value the property value, e.g. "group1" or "group1:col1,col2"
     * @return the parsed info, or null if value is null or empty
     */
    public static ColocatePropertyInfo of(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        int colonIndex = value.indexOf(':');
        if (colonIndex < 0) {
            return new ColocatePropertyInfo(value, null);
        }
        String groupName = value.substring(0, colonIndex).trim();
        List<String> columnNames = Splitter.on(',').trimResults()
                .splitToList(value.substring(colonIndex + 1));
        return new ColocatePropertyInfo(groupName, columnNames);
    }

    /**
     * Returns the original property string format.
     * Inverse of {@link #of(String)}.
     */
    @Override
    public String toString() {
        if (colocateColumnNames == null || colocateColumnNames.isEmpty()) {
            return colocateGroupName;
        }
        return colocateGroupName + ":" + String.join(",", colocateColumnNames);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ColocatePropertyInfo other = (ColocatePropertyInfo) object;
        return Objects.equals(colocateGroupName, other.colocateGroupName)
                && Objects.equals(colocateColumnNames, other.colocateColumnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colocateGroupName, colocateColumnNames);
    }
}
