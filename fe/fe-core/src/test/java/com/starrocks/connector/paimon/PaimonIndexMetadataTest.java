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

import com.starrocks.connector.index.ConnectorIndexType;
import org.apache.paimon.utils.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PaimonIndexMetadataTest {
    @Test
    public void testPaimonProviderMapping() {
        Assertions.assertEquals(ConnectorIndexType.BITMAP, PaimonMetadata.toConnectorIndexType("bitmap"));
        Assertions.assertEquals(ConnectorIndexType.RANGE, PaimonMetadata.toConnectorIndexType("btree"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR, PaimonMetadata.toConnectorIndexType("lumina"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR,
                PaimonMetadata.toConnectorIndexType("lumina-vector-ann"));
        Assertions.assertEquals(ConnectorIndexType.FULL_TEXT,
                PaimonMetadata.toConnectorIndexType("tantivy-fulltext"));
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType(null));
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType("unknown"));

        Assertions.assertFalse(PaimonMetadata.isPaimonCppReadable(ConnectorIndexType.BITMAP));
        Assertions.assertTrue(PaimonMetadata.isPaimonCppReadable(ConnectorIndexType.RANGE));
        Assertions.assertFalse(PaimonMetadata.isPaimonCppReadable(ConnectorIndexType.VECTOR));
        Assertions.assertFalse(PaimonMetadata.isPaimonCppReadable(ConnectorIndexType.FULL_TEXT));
    }

    @Test
    public void testCoverageRequiresEveryDataRange() {
        List<Range> merged = PaimonMetadata.mergeRanges(List.of(
                new Range(10L, 12L), new Range(0L, 4L), new Range(5L, 9L), new Range(20L, 21L)));

        Assertions.assertEquals(2, merged.size());
        Assertions.assertEquals(0L, merged.get(0).from);
        Assertions.assertEquals(12L, merged.get(0).to);
        Assertions.assertEquals(20L, merged.get(1).from);
        Assertions.assertEquals(21L, merged.get(1).to);
        Assertions.assertTrue(PaimonMetadata.covers(merged,
                List.of(new Range(2L, 8L), new Range(20L, 20L))));
        Assertions.assertFalse(PaimonMetadata.covers(merged,
                List.of(new Range(2L, 13L))));
        Assertions.assertFalse(PaimonMetadata.covers(
                List.of(new Range(0L, 4L), new Range(6L, 12L)), List.of(new Range(0L, 12L))));

        List<Range> executionShards = PaimonMetadata.mergeRanges(List.of(
                new Range(5L, 9L), new Range(0L, 4L), new Range(8L, 12L), new Range(20L, 21L)), false);
        Assertions.assertEquals(3, executionShards.size());
        Assertions.assertEquals(new Range(0L, 4L), executionShards.get(0));
        Assertions.assertEquals(new Range(5L, 12L), executionShards.get(1));
        Assertions.assertEquals(new Range(20L, 21L), executionShards.get(2));
    }

    @Test
    public void testIndexShardIntersectionPreservesManifestBoundaries() {
        List<Range> dataFileRanges = PaimonMetadata.mergeRanges(
                List.of(new Range(0L, 9L), new Range(10L, 19L)), false);
        List<Range> firstIndexRanges = PaimonMetadata.mergeRanges(
                List.of(new Range(0L, 4L), new Range(5L, 14L), new Range(15L, 19L)), false);
        List<Range> secondIndexRanges = PaimonMetadata.mergeRanges(
                List.of(new Range(0L, 9L), new Range(10L, 19L)), false);

        List<Range> shards = PaimonMetadata.intersectRangesPreservingBoundaries(
                firstIndexRanges, secondIndexRanges);

        Assertions.assertNotEquals(dataFileRanges, shards);
        Assertions.assertEquals(List.of(
                new Range(0L, 4L),
                new Range(5L, 9L),
                new Range(10L, 14L),
                new Range(15L, 19L)), shards);
    }
}
