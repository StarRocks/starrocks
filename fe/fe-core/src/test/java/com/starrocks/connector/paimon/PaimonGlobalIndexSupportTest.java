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

import com.starrocks.connector.index.ConnectorIndexResult;
import mockit.Mocked;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaimonGlobalIndexSupportTest {
    @Test
    public void testSnapshotBoundResult() {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        rows.add(3L);
        GlobalIndexResult result = GlobalIndexResult.create(rows);
        PaimonGlobalIndexResult indexResult = new PaimonGlobalIndexResult(7L, result);

        assertSame(result, PaimonMetadata.resolvePaimonGlobalIndexResult(indexResult, 7L).orElseThrow());
        assertFalse(PaimonMetadata.resolvePaimonGlobalIndexResult(indexResult, 8L).isPresent());
        assertFalse(PaimonMetadata.resolvePaimonGlobalIndexResult(indexResult, -1L).isPresent());

        ConnectorIndexResult otherConnectorResult = () -> 7L;
        assertFalse(PaimonMetadata.resolvePaimonGlobalIndexResult(otherConnectorResult, 7L).isPresent());
        assertThrows(IllegalArgumentException.class, () -> new PaimonGlobalIndexResult(-1L, result));
    }

    @Test
    public void testUnwrapIndexedSplit(@Mocked DataSplit dataSplit) {
        IndexedSplit indexedSplit = new IndexedSplit(dataSplit, List.of(new Range(3L, 5L)), null);

        assertTrue(PaimonSplitUtils.isGlobalIndexSplit(indexedSplit));
        assertSame(dataSplit, PaimonSplitUtils.getDataSplit(indexedSplit).orElseThrow());
        assertFalse(PaimonSplitUtils.isGlobalIndexSplit(dataSplit));
        assertSame(dataSplit, PaimonSplitUtils.getDataSplit(dataSplit).orElseThrow());
    }
}
