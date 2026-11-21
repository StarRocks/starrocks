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

import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.ColocateGroupSchemaPB;
import com.starrocks.persist.proto.ColocatePersistInfoPB;
import com.starrocks.persist.proto.ColocateRangePB;
import com.starrocks.persist.proto.GroupIdPB;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColocateTableIndexProtoTest {
    static Tuple tuple(long value) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(value))));
    }

    @Test
    public void testHashAndRangeGroupSchema() throws Exception {
        List<Column> columns = List.of(new Column("k", IntegerType.BIGINT),
                new Column("str", VarcharType.VARCHAR),
                new Column("money", new DecimalType(com.starrocks.type.PrimitiveType.DECIMAL64, 18, 4)));
        for (DistributionInfoType distribution : List.of(DistributionInfoType.HASH, DistributionInfoType.RANGE)) {
            ColocateGroupSchema source = new ColocateGroupSchema(new GroupId(10, 30), columns, 17, (short) 3, distribution);
            ColocateGroupSchema loaded = ColocateGroupSchema.fromProto(
                    ColocateGroupSchemaPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
            ColocateGroupSchema v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), ColocateGroupSchema.class);
            GsonEquivalence.assertEquivalent(v2, loaded);
            assertEquals(new GroupId(10, 30), loaded.getGroupId());
            assertEquals(17, loaded.getBucketsNum());
            assertEquals(3, loaded.getReplicationNum());
            assertEquals(distribution == DistributionInfoType.RANGE, loaded.isRangeColocate());
            assertEquals(columns.stream().map(Column::getType).toList(), loaded.getDistributionColTypes());
            assertEquals(3, loaded.getColocateColumnCount());
        }
    }

    @Test
    public void testAbsentDistributionTypeRetainsHashDefault() {
        ColocateGroupSchemaPB old = ColocateGroupSchemaPB.newBuilder()
                .setGroupId(new GroupId(1, 2).toProto()).build();
        ColocateGroupSchema schema = ColocateGroupSchema.fromProto(old);
        assertEquals(DistributionInfoType.HASH, schema.getDistributionType());
        assertEquals(0, schema.getBucketsNum());
        assertEquals(0, schema.getReplicationNum());
        assertTrue(schema.getDistributionColTypes().isEmpty());
    }

    @Test
    public void testInvalidSchemaCannotBecomeAValidGroup() {
        assertThrows(IllegalArgumentException.class,
                () -> ColocateGroupSchema.fromProto(ColocateGroupSchemaPB.getDefaultInstance()));
        ColocateGroupSchemaPB.Builder builder = ColocateGroupSchemaPB.newBuilder()
                .setGroupId(GroupIdPB.newBuilder().setDbId(1));
        assertThrows(IllegalArgumentException.class, () -> ColocateGroupSchema.fromProto(builder.build()));
        builder.setGroupId(new GroupId(1, 2).toProto()).setReplicationNum(Short.MAX_VALUE + 1);
        assertThrows(IllegalArgumentException.class, () -> ColocateGroupSchema.fromProto(builder.build()));
        builder.setReplicationNum(3).setDistributionTypeValue(999);
        assertThrows(IllegalArgumentException.class, () -> ColocateGroupSchema.fromProto(builder.build()));
        builder.setDistributionType(ColocateGroupSchemaPB.DistributionType.UNSPECIFIED);
        assertThrows(IllegalArgumentException.class, () -> ColocateGroupSchema.fromProto(builder.build()));
    }

    @Test
    public void testRangeBoundsAndTupleTypes() throws Exception {
        Tuple lower = new Tuple(List.of(Variant.of(IntegerType.BIGINT, "12"), Variant.of(VarcharType.VARCHAR, "a")));
        Tuple upper = new Tuple(List.of(Variant.of(IntegerType.BIGINT, "99"), Variant.of(VarcharType.VARCHAR, "z")));
        List<Range<Tuple>> cases = new ArrayList<>();
        cases.add(Range.all());
        cases.add(Range.of(null, upper, false, false));
        cases.add(Range.of(lower, null, true, false));
        for (boolean includeLower : List.of(false, true)) {
            for (boolean includeUpper : List.of(false, true)) {
                cases.add(Range.of(lower, upper, includeLower, includeUpper));
            }
        }
        for (Range<Tuple> range : cases) {
            ColocateRange source = new ColocateRange(range, 123456789L);
            ColocateRange loaded = ColocateRange.fromProto(ColocateRangePB.parseFrom(
                    assertInstances(source.toProto()).toByteArray()));
            assertEquals(source, loaded);
            assertEquals(123456789L, loaded.getShardGroupId());
            assertEquals(range.isLowerBoundIncluded(), loaded.getRange().isLowerBoundIncluded());
            assertEquals(range.isUpperBoundIncluded(), loaded.getRange().isUpperBoundIncluded());
            assertEquals(range.isMinimum(), loaded.getRange().isMinimum());
            assertEquals(range.isMaximum(), loaded.getRange().isMaximum());
        }
    }

    @Test
    public void testDecodedRangesKeepBucketLookupAndPackIds() {
        List<ColocateRange> ranges = List.of(
                new ColocateRange(Range.of(null, tuple(10), false, false), 81),
                new ColocateRange(Range.of(tuple(10), tuple(20), true, false), 82),
                new ColocateRange(Range.of(tuple(20), null, true, false), 83));
        ColocateRangeMgr manager = new ColocateRangeMgr();
        manager.setColocateRanges(500, ranges.stream().map(r -> ColocateRange.fromProto(r.toProto())).toList());
        assertEquals(0, manager.getColocateRangeIndex(500, null));
        assertEquals(0, manager.getColocateRangeIndex(500, tuple(9)));
        assertEquals(1, manager.getColocateRangeIndex(500, tuple(10)));
        assertEquals(1, manager.getColocateRangeIndex(500, tuple(19)));
        assertEquals(2, manager.getColocateRangeIndex(500, tuple(20)));
        assertEquals(2, manager.getColocateRangeIndex(500, tuple(Long.MAX_VALUE)));
        assertEquals(82, manager.getColocateRange(500, tuple(15)).getShardGroupId());
        assertTrue(ColocateRangeMgr.hasBoundaryAt(manager.getColocateRanges(500), tuple(10)));
        assertFalse(ColocateRangeMgr.hasBoundaryAt(manager.getColocateRanges(500), tuple(11)));
        assertEquals(java.util.Set.of(81L, 82L, 83L), manager.getAllPackShardGroupIds());
        assertEquals(-1, manager.getColocateRangeIndex(501, tuple(1)));
    }

    @Test
    public void testIncompleteRangeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ColocateRange.fromProto(ColocateRangePB.getDefaultInstance()));
        ColocateRangePB valid = new ColocateRange(Range.all(), 1).toProto();
        assertThrows(IllegalArgumentException.class, () -> ColocateRange.fromProto(valid.toBuilder().clearRange().build()));
        assertThrows(IllegalArgumentException.class,
                () -> ColocateRange.fromProto(valid.toBuilder().clearShardGroupId().build()));
        assertNotNull(ColocateRange.fromProto(valid));
    }
    @Test
    public void testAddTableRangesMatchSourceAndV2() throws Exception {
        List<ColocateRange> ranges = List.of(new ColocateRange(Range.of(tuple(10), tuple(20), true, false), 82));
        ColocatePersistInfo source = ColocatePersistInfo.createForAddTable(new GroupId(100, 500), 200,
                List.of(List.of(1L, 2L)), ranges);
        ColocatePersistInfo v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), ColocatePersistInfo.class);
        ColocatePersistInfo v3 = ColocatePersistInfo.fromProto(ColocatePersistInfoPB.parseFrom(source.toProto().toByteArray()));
        for (ColocatePersistInfo restored : List.of(v2, v3)) {
            assertEquals(200, restored.getTableId());
            assertEquals(new GroupId(100, 500), restored.getGroupId());
            assertEquals(List.of(List.of(1L, 2L)), restored.getBackendsPerBucketSeq());
            assertEquals(1, restored.getColocateRanges().size());
            assertEquals(ranges.get(0).getRange(), restored.getColocateRanges().get(0).getRange());
            assertEquals(82, restored.getColocateRanges().get(0).getShardGroupId());
        }
        ColocatePersistInfo absent = ColocatePersistInfo.fromProto(source.toProto().toBuilder().clearColocateRanges().build());
        assertEquals(List.of(), absent.getColocateRanges());
    }

}
