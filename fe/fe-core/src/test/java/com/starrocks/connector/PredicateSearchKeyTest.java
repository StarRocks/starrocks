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

package com.starrocks.connector;

import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PredicateSearchKeyTest {
    private static PredicateSearchKey key(boolean enableColumnStats, Set<String> statsColumnNames) {
        return key(enableColumnStats, statsColumnNames, false);
    }

    private static PredicateSearchKey key(boolean enableColumnStats, Set<String> statsColumnNames,
                                          boolean optionalColumnStats) {
        return PredicateSearchKey.of("db", "t1", GetRemoteFilesParams.newBuilder()
                .setPredicate(ConstantOperator.TRUE)
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L)))
                .setEnableColumnStats(enableColumnStats)
                .setFieldNames(List.of("c1", "c2", "c3"))
                .setStatsColumnNames(statsColumnNames)
                .setOptionalColumnStats(optionalColumnStats)
                .build());
    }

    @Test
    public void testStatsColumnsSeparateKeys() {
        // the min/max scan (statistics of its columns) and the column-statistics path (all) plan different files
        PredicateSearchKey minMax = key(true, Set.of("c1", "c2"));
        PredicateSearchKey allStats = key(true, null);
        Assertions.assertNotEquals(minMax, allStats);
        Assertions.assertNotEquals(minMax, key(true, Set.of("c1")));
        Assertions.assertEquals(minMax, key(true, Set.of("c2", "c1")));
        Assertions.assertEquals(minMax.hashCode(), key(true, Set.of("c2", "c1")).hashCode());
        Assertions.assertTrue(minMax.toString().contains("statsColumns="));

        // without statistics the stats columns do not matter
        Assertions.assertEquals(key(false, Set.of("c1")), key(false, Set.of("c2")));
        Assertions.assertEquals(key(false, Set.of("c1")).hashCode(), key(false, null).hashCode());
        Assertions.assertNotEquals(key(false, null), allStats);
    }

    @Test
    public void testOptionalStatsSeparateKeys() {
        // optional statistics may be missing from the planned files, so they never serve a scan that needs them
        Assertions.assertNotEquals(key(true, Set.of("c1")), key(true, Set.of("c1"), true));
        Assertions.assertEquals(key(true, Set.of("c1"), true), key(true, Set.of("c1"), true));
        Assertions.assertEquals(key(true, Set.of("c1"), true).hashCode(), key(true, Set.of("c1"), true).hashCode());
        // without statistics the flag does not matter
        Assertions.assertEquals(key(false, null), key(false, null, true));
    }
}
