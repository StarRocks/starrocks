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

package com.starrocks.connector.paimon;

import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.paimon.index.PaimonGlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaimonGetRemoteFilesParamsTest {
    @Test
    public void testCopiesPreserveRequestSpecificIndexOptions() {
        PaimonGlobalIndexResult result = new PaimonGlobalIndexResult(
                "file:/table", "main", 7, GlobalIndexResult.create(new RoaringNavigableMap64()));
        PaimonGetRemoteFilesParams.Builder builder = PaimonGetRemoteFilesParams.newBuilder()
                .setIndexResult(result).setDisableGlobalIndex(true);
        TvrTableSnapshot snapshot = TvrTableSnapshot.of(7L);
        builder.setPartitionNames(List.of("p0", "p1")).setTableVersionRange(snapshot)
                .setIsRecursive(false).setScanRowsCap(64);
        PaimonGetRemoteFilesParams params = builder.build();

        PaimonGetRemoteFilesParams copy = params.copy();
        PaimonGetRemoteFilesParams sub = (PaimonGetRemoteFilesParams) params.sub(0, 1);
        assertEquals(List.of("p0", "p1"), copy.getPartitionNames());
        assertEquals(List.of("p0"), sub.getPartitionNames());
        for (PaimonGetRemoteFilesParams derived : List.of(copy, sub)) {
            assertSame(result, derived.getIndexResult());
            assertTrue(derived.isDisableGlobalIndex());
            assertSame(snapshot, derived.getTableVersionRange());
            assertEquals(Optional.of(false), derived.getIsRecursive());
            assertEquals(64, derived.getScanRowsCap());
        }
        copy.setIndexResult(null);
        assertSame(result, params.getIndexResult());
        assertFalse(PaimonGetRemoteFilesParams.newBuilder().build().isDisableGlobalIndex());
    }
}
