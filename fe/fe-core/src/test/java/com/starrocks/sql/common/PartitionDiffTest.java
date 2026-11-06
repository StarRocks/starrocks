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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionDiffTest {

    /**
     * Only the range differ prunes by retention, so every other producer keeps the two-argument form. An
     * absent set has to read as "nothing refused" rather than as null, or each consumer grows its own guard.
     */
    @Test
    public void testRetentionPrunedDefaultsToEmpty() {
        PartitionDiff diff = new PartitionDiff(PCellSortedSet.of(), PCellSortedSet.of());
        Assertions.assertNotNull(diff.getRetentionPruned());
        Assertions.assertTrue(diff.getRetentionPruned().isEmpty());
    }
}
