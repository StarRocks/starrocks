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

import com.starrocks.catalog.StringVariant;
import com.starrocks.catalog.TabletRangeProtoConverter;
import com.starrocks.catalog.Variant;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.proto.VariantPB;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TabletRangeProtoConverterTest {
    static Stream<Arguments> types() {
        return Stream.of(
                Arguments.of(BooleanType.BOOLEAN, "true"),
                Arguments.of(IntegerType.INT, "10"),
                Arguments.of(IntegerType.LARGEINT, "170141183460469231731687303715884105727"),
                Arguments.of(TypeFactory.createVarcharType(32), "boundary"),
                Arguments.of(DateType.DATETIME, "2024-01-02 03:04:05"),
                Arguments.of(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4), "123.4567"));
    }

    @ParameterizedTest(name = "{0}: preserve sentinel types and ordering")
    @MethodSource("types")
    public void testSentinelsMatchV2(Type type, String normalValue) throws Exception {
        Variant ordinary = Variant.of(type, normalValue);
        List<Variant> source = List.of(Variant.minVariant(type), Variant.nullVariant(type),
                ordinary, Variant.maxVariant(type));
        List<Variant> restored = new ArrayList<>();
        for (Variant value : source) {
            Variant v2 = throughV2(value, Variant.class);
            VariantPB pb = assertInstances(TabletRangeProtoConverter.toProto(value));
            Variant v3 = TabletRangeProtoConverter.fromProto(VariantPB.parseFrom(pb.toByteArray()));
            assertEquals(value.getClass(), v2.getClass());
            assertEquals(value.getClass(), v3.getClass());
            assertEquals(type, v3.getType());
            assertEquals(value, v2);
            assertEquals(value, v3);
            if (value != ordinary) {
                assertEquals(VariantPB.InstanceCase.INSTANCE_NOT_SET, pb.getInstanceCase());
            }
            restored.add(v3);
        }
        // MIN < NULL < ordinary values < MAX, including comparisons across concrete variant classes.
        for (int i = 0; i < restored.size(); i++) {
            for (int j = 0; j < restored.size(); j++) {
                assertEquals(Integer.compare(i, j), Integer.signum(restored.get(i).compareTo(restored.get(j))));
                assertEquals(Integer.compare(i, j),
                        Integer.signum(Variant.compatibleCompare(restored.get(i), restored.get(j))));
            }
        }
    }

    @Test
    public void testAbsentStringPayloadIsDistinctFromNullSentinel() throws Exception {
        VariantPB string = assertInstances(TabletRangeProtoConverter.toProto(new StringVariant(VarcharType.VARCHAR, null)));
        VariantPB sentinel = assertInstances(TabletRangeProtoConverter.toProto(Variant.nullVariant(VarcharType.VARCHAR)));
        assertEquals(VariantPB.InstanceCase.INSTANCE_NOT_SET, string.getInstanceCase());
        assertEquals(VariantPB.InstanceCase.INSTANCE_NOT_SET, sentinel.getInstanceCase());
        assertNotEquals(string.getKind(), sentinel.getKind());
        Variant restoredString = TabletRangeProtoConverter.fromProto(VariantPB.parseFrom(string.toByteArray()));
        Variant restoredSentinel = TabletRangeProtoConverter.fromProto(VariantPB.parseFrom(sentinel.toByteArray()));
        assertEquals(StringVariant.class, restoredString.getClass());
        assertNull(restoredString.getStringValue());
        assertEquals(Variant.nullVariant(VarcharType.VARCHAR), restoredSentinel);
    }
    private static <T> T throughV2(Object source, Class<T> type) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        SRMetaBlockWriter writer = image.getImageWriter().getBlockWriter(SRMetaBlockID.NODE_MGR, 1);
        writer.writeJson(source);
        writer.close();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        T restored = reader.readJson(type);
        reader.close();
        return restored;
    }

}
