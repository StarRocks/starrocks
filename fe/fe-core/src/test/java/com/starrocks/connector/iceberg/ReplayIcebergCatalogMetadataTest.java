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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link ReplayIcebergCatalogMetadata#buildPartitionSpec}, the piece that turns the partition
 * transforms captured in a query dump back into a real iceberg {@link PartitionSpec} at replay time.
 *
 * <p>The captured strings are exactly what {@code IcebergApiConverter.toPartitionField(spec, field, false)}
 * emits: an identity partition is the backtick-escaped bare column name ({@code `dt`}); temporal / bucket /
 * truncate transforms are {@code fn(`col`[, n])}. A partitioned table that is NOT identity-partitioned cannot
 * be written by StarRocks (static insert is rejected), so this offline unit test is how transform partition
 * handling is covered -- rebuilding the spec faithfully is what lets native {@code planFiles} prune on it.
 */
public class ReplayIcebergCatalogMetadataTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "dt", Types.IntegerType.get()),
            Types.NestedField.optional(2, "city", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "k", Types.IntegerType.get()),
            Types.NestedField.optional(5, "s", Types.StringType.get()));

    private static String sourceColumn(PartitionSpec spec, int fieldIndex) {
        return SCHEMA.findColumnName(spec.fields().get(fieldIndex).sourceId());
    }

    @Test
    public void testIdentitySingleColumn() {
        PartitionSpec spec = ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of("`dt`"));
        Assertions.assertEquals(1, spec.fields().size());
        Assertions.assertTrue(spec.fields().get(0).transform().isIdentity());
        Assertions.assertEquals("dt", sourceColumn(spec, 0));
    }

    @Test
    public void testIdentityMultiColumn() {
        PartitionSpec spec = ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of("`dt`", "`city`"));
        Assertions.assertEquals(2, spec.fields().size());
        Assertions.assertTrue(spec.fields().get(0).transform().isIdentity());
        Assertions.assertTrue(spec.fields().get(1).transform().isIdentity());
        Assertions.assertEquals("dt", sourceColumn(spec, 0));
        Assertions.assertEquals("city", sourceColumn(spec, 1));
    }

    @Test
    public void testTemporalTransforms() {
        for (String fn : new String[] {"year", "month", "day", "hour"}) {
            PartitionSpec spec = ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of(fn + "(`ts`)"));
            Assertions.assertEquals(1, spec.fields().size(), fn);
            Assertions.assertFalse(spec.isUnpartitioned(), fn + " must not fall back to unpartitioned");
            Assertions.assertFalse(spec.fields().get(0).transform().isIdentity(), fn);
            Assertions.assertEquals(fn, spec.fields().get(0).transform().toString(), fn);
            Assertions.assertEquals("ts", sourceColumn(spec, 0), fn);
        }
    }

    @Test
    public void testBucketTransform() {
        PartitionSpec spec = ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of("bucket(`k`, 8)"));
        Assertions.assertEquals(1, spec.fields().size());
        Assertions.assertEquals("bucket[8]", spec.fields().get(0).transform().toString());
        Assertions.assertEquals("k", sourceColumn(spec, 0));
    }

    @Test
    public void testTruncateTransform() {
        PartitionSpec spec = ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of("truncate(`s`, 4)"));
        Assertions.assertEquals(1, spec.fields().size());
        Assertions.assertEquals("truncate[4]", spec.fields().get(0).transform().toString());
        Assertions.assertEquals("s", sourceColumn(spec, 0));
    }

    @Test
    public void testUnknownTransformAndNullFallBackToUnpartitioned() {
        // An unrecognized transform degrades to unpartitioned (faithful, never a wrong plan) rather than
        // throwing and failing the whole replay.
        Assertions.assertTrue(
                ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, List.of("weird(`dt`)")).isUnpartitioned());
        Assertions.assertTrue(ReplayIcebergCatalogMetadata.buildPartitionSpec(SCHEMA, null).isUnpartitioned());
    }
}
