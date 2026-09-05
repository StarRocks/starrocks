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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HashDistributionDescTest {

    private static HashDistributionDesc descOf(HashDistributionDesc.SourceType sourceType) {
        return new HashDistributionDesc(Lists.newArrayList(1), sourceType);
    }

    @Test
    void testIsShuffleLike() {
        // Exhaustive truth table over every SourceType: true for the shuffle variants
        // (SHUFFLE_AGG / SHUFFLE_JOIN / SHUFFLE_ENFORCE), false otherwise (LOCAL / BUCKET).
        assertTrue(descOf(HashDistributionDesc.SourceType.SHUFFLE_AGG).isShuffleLike());
        assertTrue(descOf(HashDistributionDesc.SourceType.SHUFFLE_JOIN).isShuffleLike());
        assertTrue(descOf(HashDistributionDesc.SourceType.SHUFFLE_ENFORCE).isShuffleLike());
        assertFalse(descOf(HashDistributionDesc.SourceType.LOCAL).isShuffleLike());
        assertFalse(descOf(HashDistributionDesc.SourceType.BUCKET).isShuffleLike());
    }
}
