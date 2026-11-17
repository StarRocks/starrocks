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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeObjectProtoConverter;
import com.starrocks.catalog.LargeIntVariant;
import com.starrocks.catalog.StringVariant;
import com.starrocks.catalog.TabletRangeProtoConverter;
import com.starrocks.persist.proto.ScalarTypePB;
import com.starrocks.persist.proto.TypePB;
import com.starrocks.persist.proto.VariantPB;
import com.starrocks.type.Type;
import com.starrocks.type.TypeProtoConverter;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtoInstanceTest {
    static Stream<ProtoKindsTest.Fixture> fixtures() {
        return ProtoKindsTest.fixtures();
    }

    @ParameterizedTest(name = "{0}: subtype fields use oneof instance")
    @MethodSource("fixtures")
    public void testSchemaContract(ProtoKindsTest.Fixture fixture) {
        Descriptors.Descriptor descriptor = fixture.prototype().getDescriptorForType();
        Descriptors.FieldDescriptor kind = descriptor.findFieldByName("kind");
        assertNotNull(kind);
        assertEquals(1, kind.getNumber());
        assertEquals("Kind", kind.getEnumType().getName());
        Map<Integer, String> expected = ProtoInstanceAssertions.instancesByKind(descriptor.getName());
        assertEquals(expected.size() + 1, kind.getEnumType().getValues().size());
        for (Descriptors.EnumValueDescriptor value : kind.getEnumType().getValues()) {
            if (value.getNumber() != 0) {
                String name = expected.get(value.getNumber());
                assertNotNull(name, value.getFullName());
                if (!name.isEmpty()) {
                    Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
                    assertNotNull(field, value.getFullName());
                    assertEquals("instance", field.getContainingOneof().getName());
                }
            }
        }
        assertEquals(expected.values().stream().allMatch(String::isEmpty) ? List.of() : List.of("instance"),
                descriptor.getRealOneofs().stream().map(Descriptors.OneofDescriptor::getName).toList());
    }

    @ParameterizedTest(name = "{0}: read a legacy wire payload")
    @MethodSource("fixtures")
    public void testReadAndWriteLegacyPayload(ProtoKindsTest.Fixture fixture) throws Exception {
        String instanceName = switch (fixture.toString()) {
            case "AbstractJobPB" -> "backup_job";
            case "AlterJobPB" -> "rollup_job_v2";
            case "AnalyzeJobPB", "AnalyzeStatusPB" -> "native";
            case "ComputeResourcePB" -> "warehouse";
            case "DistributionInfoPB" -> "hash";
            case "FunctionPB", "TypePB" -> "scalar";
            case "LoadJobPB" -> "broker";
            case "PartitionInfoPB", "RecyclePartitionInfoPB" -> "range";
            case "PrivilegeObjectPB" -> "database";
            case "ResourcePB" -> "spark_resource";
            case "RoutineLoadJobPB" -> "kafka";
            case "RoutineLoadProgressPB" -> "kafka_progress";
            case "SnapshotInfoPB" -> "snapshot_info";
            case "TablePB" -> "olap";
            case "TabletPB" -> "local";
            case "TxnCommitAttachmentPB" -> "insert";
            case "VariantPB" -> "bool_value";
            case "GroupProviderPB", "SecurityIntegrationPB", "TvrVersionRangePB" -> "";
            default -> throw new AssertionError("missing legacy payload for " + fixture);
        };
        int kind = fixture.toString().equals("PartitionInfoPB") || fixture.toString().equals("TablePB") ? 2 : 1;
        // Fixed wire tags from the old layout: kind is field 1, the selected payload is field 11.
        // This checks protobuf parsing; family-specific tests cover complete business objects.
        byte[] legacy;
        if (instanceName.isEmpty()) {
            legacy = new byte[] {8, (byte) kind};
        } else if (instanceName.equals("bool_value")) {
            legacy = new byte[] {8, (byte) kind, 88, 1};
        } else {
            legacy = new byte[] {8, (byte) kind, 90, 0};
        }
        Message parsed = fixture.prototype().getParserForType().parseFrom(legacy);
        Descriptors.Descriptor descriptor = parsed.getDescriptorForType();
        assertEquals(kind, ((Descriptors.EnumValueDescriptor) parsed.getField(descriptor.findFieldByName("kind"))).getNumber());
        Descriptors.FieldDescriptor instance = descriptor.getRealOneofs().isEmpty() ? null
                : parsed.getOneofFieldDescriptor(descriptor.getRealOneofs().get(0));
        assertEquals(instanceName, instance == null ? "" : instance.getName());
        assertArrayEquals(legacy, parsed.toByteArray());
    }

    @Test
    public void testFieldlessSubtypesRoundTripWithoutInstances() throws Exception {
        Map<String, List<Integer>> kinds = Map.of(
                "TypePB", List.of(5, 6, 7), "GroupProviderPB", List.of(1, 2, 3),
                "SecurityIntegrationPB", List.of(1, 2, 3), "DistributionInfoPB", List.of(3),
                "PartitionInfoPB", List.of(1), "RecyclePartitionInfoPB", List.of(3, 4, 5, 6),
                "TvrVersionRangePB", List.of(1, 2), "PrivilegeObjectPB", List.of(14));
        for (ProtoKindsTest.Fixture fixture : fixtures().toList()) {
            for (int kind : kinds.getOrDefault(fixture.toString(), List.of())) {
                Object object = fixture.converter().convert(fixture.withKind(kind));
                Message written;
                if (object instanceof Type) {
                    written = TypeProtoConverter.toProto((Type) object);
                } else if (object instanceof PEntryObject) {
                    written = PrivilegeObjectProtoConverter.toProto((PEntryObject) object);
                } else {
                    written = (Message) object.getClass().getMethod("toProto").invoke(object);
                }
                assertInstances(written);
                Descriptors.Descriptor descriptor = written.getDescriptorForType();
                assertEquals(kind, ((Descriptors.EnumValueDescriptor) written.getField(
                        descriptor.findFieldByName("kind"))).getNumber(), fixture + " kind " + kind);
                Message parsed = fixture.prototype().getParserForType().parseFrom(written.toByteArray());
                assertEquals(object.getClass(), fixture.converter().convert(parsed).getClass());
            }
        }
    }

    @Test
    public void testZeroAndNullVariantValues() throws Exception {
        for (VariantPB.Kind kind : List.of(VariantPB.Kind.BOOL, VariantPB.Kind.INT, VariantPB.Kind.LARGE_INT,
                VariantPB.Kind.STRING, VariantPB.Kind.DECIMAL)) {
            ScalarTypePB.PrimitiveType type = switch (kind) {
                case BOOL -> ScalarTypePB.PrimitiveType.BOOLEAN;
                case INT -> ScalarTypePB.PrimitiveType.INT;
                case LARGE_INT -> ScalarTypePB.PrimitiveType.LARGEINT;
                case STRING -> ScalarTypePB.PrimitiveType.VARCHAR;
                case DECIMAL -> ScalarTypePB.PrimitiveType.DECIMAL64;
                default -> throw new AssertionError(kind);
            };
            VariantPB.Builder legacy = VariantPB.newBuilder().setKind(kind).setType(TypePB.newBuilder()
                    .setKind(TypePB.Kind.SCALAR).setScalar(ScalarTypePB.newBuilder()
                            .setType(type).setPrecision(18).setScale(2)));
            // DecimalVariant requires a non-null value; LARGE_INT and STRING retain nullable legacy values.
            if (kind == VariantPB.Kind.DECIMAL) {
                legacy.setDecimalValue("0");
            }
            VariantPB old = legacy.build();
            VariantPB written = VariantPB.parseFrom(
                    TabletRangeProtoConverter.toProto(TabletRangeProtoConverter.fromProto(old)).toByteArray());
            assertInstances(written);
            VariantPB.InstanceCase expected = switch (kind) {
                case BOOL -> VariantPB.InstanceCase.BOOL_VALUE;
                case INT -> VariantPB.InstanceCase.INT_VALUE;
                case DECIMAL -> VariantPB.InstanceCase.DECIMAL_VALUE;
                default -> VariantPB.InstanceCase.INSTANCE_NOT_SET;
            };
            assertEquals(expected, written.getInstanceCase());
            // Nullable variants cannot use their business equals(), which dereferences the value.
            assertEquals(written, TabletRangeProtoConverter.toProto(TabletRangeProtoConverter.fromProto(written)));
        }
    }

    @Test
    public void testEmptyStringAndZeroLargeIntHaveValues() throws Exception {
        StringVariant emptyString = new StringVariant(VarcharType.VARCHAR, "");
        VariantPB string = VariantPB.parseFrom(TabletRangeProtoConverter.toProto(emptyString).toByteArray());
        assertInstances(string);
        assertTrue(string.hasStringValue());
        assertEquals("", string.getStringValue());
        assertEquals(emptyString, TabletRangeProtoConverter.fromProto(string));

        LargeIntVariant zeroLargeInt = new LargeIntVariant(0);
        VariantPB largeInt = VariantPB.parseFrom(TabletRangeProtoConverter.toProto(zeroLargeInt).toByteArray());
        assertInstances(largeInt);
        assertTrue(largeInt.hasLargeIntValue());
        assertEquals(zeroLargeInt, TabletRangeProtoConverter.fromProto(largeInt));
    }

    @Test
    public void testDatesReadBothLayoutsAndRemainReadableByOldCode() throws Exception {
        for (long seconds : new long[] {-1, 0, 123456789}) {
            for (long nanos : new long[] {0, 999999999}) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                CodedOutputStream oldWriter = CodedOutputStream.newInstance(bytes);
                oldWriter.writeEnum(1, 5); // DATE kind.
                oldWriter.writeInt64(15, seconds);
                oldWriter.writeInt64(16, nanos);
                oldWriter.flush();
                VariantPB legacy = VariantPB.parseFrom(bytes.toByteArray());
                VariantPB written = TabletRangeProtoConverter.toProto(TabletRangeProtoConverter.fromProto(legacy));
                assertInstances(written);
                assertEquals(VariantPB.InstanceCase.DATE, written.getInstanceCase());
                assertEquals(seconds, written.getDate().getSeconds());
                assertEquals(nanos, written.getDate().getNanos());
                // Read only the old date tags, skipping the new instance as an old reader would.
                CodedInputStream oldReader = CodedInputStream.newInstance(written.toByteArray());
                long oldSeconds = 0;
                long oldNanos = 0;
                for (int tag = oldReader.readTag(); tag != 0; tag = oldReader.readTag()) {
                    if (tag == (15 << 3)) {
                        oldSeconds = oldReader.readInt64();
                    } else if (tag == (16 << 3)) {
                        oldNanos = oldReader.readInt64();
                    } else {
                        oldReader.skipField(tag);
                    }
                }
                assertEquals(seconds, oldSeconds);
                assertEquals(nanos, oldNanos);
                assertEquals(TabletRangeProtoConverter.fromProto(legacy), TabletRangeProtoConverter.fromProto(written));
                VariantPB instanceOnly = written.toBuilder().clearDateSeconds().clearDateNanos().build();
                assertEquals(TabletRangeProtoConverter.fromProto(legacy), TabletRangeProtoConverter.fromProto(instanceOnly));
            }
        }
    }
}
