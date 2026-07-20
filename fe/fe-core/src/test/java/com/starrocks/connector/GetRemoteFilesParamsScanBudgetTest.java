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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Unit tests for the bounded-cost scan-budget fields on GetRemoteFilesParams (design 2.2/2.4).
public class GetRemoteFilesParamsScanBudgetTest {

    @Test
    public void testDefaultsNoBudget() {
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().build();
        Assertions.assertEquals(-1, params.getScanBytesCap());
        Assertions.assertEquals(-1, params.getScanFilesCap());
        Assertions.assertEquals(-1, params.getScanRowsCap());
        Assertions.assertFalse(params.hasScanBudget());
    }

    @Test
    public void testHasScanBudgetWhenAnyCapPositive() {
        Assertions.assertTrue(GetRemoteFilesParams.newBuilder().setScanBytesCap(1).build().hasScanBudget());
        Assertions.assertTrue(GetRemoteFilesParams.newBuilder().setScanFilesCap(1).build().hasScanBudget());
        Assertions.assertTrue(GetRemoteFilesParams.newBuilder().setScanRowsCap(1).build().hasScanBudget());
        // Zero or negative means unlimited -> no budget.
        Assertions.assertFalse(GetRemoteFilesParams.newBuilder()
                .setScanBytesCap(0).setScanFilesCap(0).setScanRowsCap(-1).build().hasScanBudget());
    }

    @Test
    public void testCopyPreservesCaps() {
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setScanBytesCap(100).setScanFilesCap(200).setScanRowsCap(300).build();
        GetRemoteFilesParams copy = params.copy();
        Assertions.assertEquals(100, copy.getScanBytesCap());
        Assertions.assertEquals(200, copy.getScanFilesCap());
        Assertions.assertEquals(300, copy.getScanRowsCap());
        Assertions.assertTrue(copy.hasScanBudget());
    }
}
