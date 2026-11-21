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

package com.starrocks.persist.image;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * The declared differences between a table loaded from a v2 image and the same table loaded from a v3 image, as
 * {@link GsonEquivalence} path patterns.
 */
public final class CatalogEquivalenceRules {
    private CatalogEquivalenceRules() {
    }

    /**
     * The physical-partition members Partition still declares. They are written at construction time, never read,
     * duplicate the default physical partition and are not persisted by the v3 format (design decision).
     */
    public static final Set<String> LEGACY_PARTITION_FIELDS = ImmutableSet.of(
            "^\\$(\\.tempPartitions)?(\\.idToPartition\\.\\d+)?\\.(shardGroupId|isImmutable|baseIndex"
                    + "|idToVisibleRollupIndex|idToShadowIndex|visibleVersion|visibleVersionTime|nextVersion"
                    + "|dataVersion|nextDataVersion|versionEpoch|versionTxnType)$");

    /**
     * A type built in memory is an IntegerType / VarcharType / ... object, a subclass GSON does not register, so its
     * GSON view carries no class label; a load (through either format) builds the registered ScalarType instead,
     * which carries one. The label is the only difference, so a comparison of a live object with a loaded one skips
     * it wherever a type can appear.
     */
    public static final Set<String> TYPE_LABELS_ADDED_ON_LOAD = ImmutableSet.of(
            "^.*\\.(type|argTypes\\[\\d+\\]|retType|itemType|keyType|valueType)\\.clazz$");

    /** The rules for comparing a live table with a loaded one: the legacy partition fields plus the type labels. */
    public static final Set<String> LIVE_TABLE_FIELDS = ImmutableSet.<String>builder()
            .addAll(LEGACY_PARTITION_FIELDS).addAll(TYPE_LABELS_ADDED_ON_LOAD).build();

    /** Maps with complex keys serialize as arrays whose order follows a hash map; compare them as sets. */
    public static final Set<String> UNORDERED_PATHS = ImmutableSet.of(
            "^\\$\\.refreshScheme\\.asyncRefreshContext\\.(baseTableInfoVisibleVersionMap"
                    + "|baseTableInfoTvrVersionRangeMap|tempBaseTableInfoTvrVersionRangeMap)$");
}
