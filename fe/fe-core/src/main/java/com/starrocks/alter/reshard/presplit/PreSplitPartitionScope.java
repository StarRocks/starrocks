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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionRef;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Restricts a multi-partition pre-split to partitions named by the INSERT.
 * Logical names are the deterministic auto-partition names derived from sampled values; catalog
 * names are the real or temporary partitions that the load will write. Static overwrite supplies
 * an explicit source-to-temp mapping, while a regular explicit INSERT starts with an identity map
 * and can fall back to range matching for custom temporary names.
 */
final class PreSplitPartitionScope {

    private static final PreSplitPartitionScope UNRESTRICTED =
            new PreSplitPartitionScope(false, false, Map.of());

    private final boolean specified;
    private final boolean temporary;
    private final Map<String, String> logicalToCatalogName;

    private PreSplitPartitionScope(
            boolean specified, boolean temporary, Map<String, String> logicalToCatalogName) {
        this.specified = specified;
        this.temporary = temporary;
        this.logicalToCatalogName = Map.copyOf(logicalToCatalogName);
    }

    static PreSplitPartitionScope unrestricted() {
        return UNRESTRICTED;
    }

    static PreSplitPartitionScope fromInsert(InsertStmt insertStmt) {
        if (!insertStmt.isSpecifyPartitionNames()) {
            return unrestricted();
        }
        PartitionRef partitionRef = insertStmt.getTargetPartitionNames();
        Map<String, String> identity = new LinkedHashMap<>();
        for (String partitionName : partitionRef.getPartitionNames()) {
            identity.put(normalize(partitionName), partitionName);
        }
        return new PreSplitPartitionScope(true, partitionRef.isTemp(), identity);
    }

    static PreSplitPartitionScope staticOverwrite(
            List<String> sourcePartitionNames, List<String> temporaryPartitionNames) {
        if (sourcePartitionNames.size() != temporaryPartitionNames.size()) {
            throw new IllegalArgumentException("source and temporary partition counts differ");
        }
        Map<String, String> mapping = new LinkedHashMap<>();
        for (int i = 0; i < sourcePartitionNames.size(); i++) {
            mapping.put(normalize(sourcePartitionNames.get(i)), temporaryPartitionNames.get(i));
        }
        return new PreSplitPartitionScope(true, true, mapping);
    }

    boolean isSpecified() {
        return specified;
    }

    boolean isTemporary() {
        return temporary;
    }

    String mappedCatalogName(String logicalPartitionName) {
        return logicalToCatalogName.get(normalize(logicalPartitionName));
    }

    List<String> catalogPartitionNames() {
        return logicalToCatalogName.values().stream().distinct().toList();
    }

    private static String normalize(String partitionName) {
        return partitionName.toLowerCase(Locale.ROOT);
    }
}
