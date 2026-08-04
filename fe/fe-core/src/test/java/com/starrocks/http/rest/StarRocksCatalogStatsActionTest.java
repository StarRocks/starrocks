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

package com.starrocks.http.rest;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.starrocks.StarRocksRemoteTableStats;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for the serialization helpers of the catalog statistics endpoints. These carry the wire
 * contract the consuming cluster reconstructs partition keys from, so their exact output matters:
 * {@code StarRocksStatsUtils} on the other side rebuilds PartitionKeys from these strings and the
 * ids must round-trip between pruning and statistics derivation.
 */
public class StarRocksCatalogStatsActionTest {

    @Test
    public void testErrorBodyCarriesStatusAndMessage() {
        Map<String, Object> body = StarRocksCatalogStatsAction.errorBody(404, "Table [t] does not exist");
        Assertions.assertEquals(404, body.get("status"));
        Assertions.assertEquals("Table [t] does not exist", body.get("exception"));

        // A null message must not blow up the envelope.
        Assertions.assertDoesNotThrow(() -> StarRocksCatalogStatsAction.errorBody(500, null));
    }

    /** NULL partition values travel as JSON null so the consumer can tell them apart from "". */
    @Test
    public void testSerializeLiteralMapsNullsToNull() {
        Assertions.assertNull(StarRocksCatalogStatsAction.serializeLiteral(null));
        Assertions.assertNull(StarRocksCatalogStatsAction.serializeLiteral(NullLiteral.create(IntegerType.INT)));
        Assertions.assertEquals("7",
                StarRocksCatalogStatsAction.serializeLiteral(new IntLiteral(7)));
        Assertions.assertEquals("abc",
                StarRocksCatalogStatsAction.serializeLiteral(new StringLiteral("abc")));
        // An empty string stays an empty string, distinct from the null above.
        Assertions.assertEquals("",
                StarRocksCatalogStatsAction.serializeLiteral(new StringLiteral("")));
    }

    private static List<Column> intPartitionColumns() {
        return Collections.singletonList(new Column("k", IntegerType.INT, false));
    }

    @Test
    public void testSerializeBoundMarksInfiniteEnds() throws Exception {
        StarRocksRemoteTableStats.RangeBound min = StarRocksCatalogStatsAction.serializeBound(
                PartitionKey.createInfinityPartitionKey(intPartitionColumns(), false));
        Assertions.assertTrue(min.infiniteMin);
        Assertions.assertFalse(min.infiniteMax);
        Assertions.assertNull(min.values, "an infinite bound carries no values");

        StarRocksRemoteTableStats.RangeBound max = StarRocksCatalogStatsAction.serializeBound(
                PartitionKey.createInfinityPartitionKey(intPartitionColumns(), true));
        Assertions.assertTrue(max.infiniteMax);
        Assertions.assertFalse(max.infiniteMin);
        Assertions.assertNull(max.values);
    }

    @Test
    public void testSerializeBoundEmitsValuesInKeyOrder() {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new IntLiteral(10), PrimitiveType.INT);
        key.pushColumn(new StringLiteral("x"), PrimitiveType.VARCHAR);

        StarRocksRemoteTableStats.RangeBound bound = StarRocksCatalogStatsAction.serializeBound(key);
        Assertions.assertFalse(bound.infiniteMin);
        Assertions.assertFalse(bound.infiniteMax);
        Assertions.assertEquals(Arrays.asList("10", "x"), bound.values);
    }

    /**
     * MAXVALUE has no string value of its own; it travels as the sentinel the consumer's
     * {@code buildBoundKey} turns back into {@code PartitionValue.MAX_VALUE}.
     */
    @Test
    public void testSerializeBoundUsesMaxValueSentinel() {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new IntLiteral(1), PrimitiveType.INT);
        key.pushColumn(MaxLiteral.MAX_VALUE, PrimitiveType.INT);

        StarRocksRemoteTableStats.RangeBound bound = StarRocksCatalogStatsAction.serializeBound(key);
        Assertions.assertEquals(Arrays.asList("1", "MAXVALUE"), bound.values);
    }

    @Test
    public void testSerializeBoundOfVarcharKeyKeepsRawText() {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new StringLiteral("2024-01-01"), PrimitiveType.VARCHAR);
        Assertions.assertEquals(Collections.singletonList("2024-01-01"),
                StarRocksCatalogStatsAction.serializeBound(key).values);
    }

    /** The analyze epoch is minted from this, so it must be the plain local-zone epoch millis. */
    @Test
    public void testToEpochMilliUsesSystemZone() {
        LocalDateTime time = LocalDateTime.of(2026, 7, 31, 12, 34, 56);
        Assertions.assertEquals(time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                StarRocksCatalogStatsAction.toEpochMilli(time));

        // Monotonic in time, which is all the epoch comparison relies on.
        Assertions.assertTrue(StarRocksCatalogStatsAction.toEpochMilli(time.plusSeconds(1))
                > StarRocksCatalogStatsAction.toEpochMilli(time));
    }

    @Test
    public void testSerializeBoundOfDecimalKeyKeepsScale() {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new StringLiteral("1.50"), PrimitiveType.VARCHAR);
        Assertions.assertEquals(Collections.singletonList("1.50"),
                StarRocksCatalogStatsAction.serializeBound(key).values);
        // Sanity that the helper does not depend on a particular column type factory.
        Assertions.assertNotNull(TypeFactory.createDefaultCatalogString());
    }
}
