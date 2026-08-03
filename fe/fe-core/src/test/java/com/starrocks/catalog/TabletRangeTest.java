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

package com.starrocks.catalog;

import com.starrocks.common.Range;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TuplePB;
import com.starrocks.proto.VariantPB;
import com.starrocks.proto.VariantTypePB;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTabletRange;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.thrift.TVariant;
import com.starrocks.thrift.TVariantType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

public class TabletRangeTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("encodedRoundTripRanges")
    public void testEncodedRoundTrip(String description, TabletRange original) {
        String encoded = original.toEncodedString();

        Assertions.assertTrue(encoded.startsWith("v1:"));
        String payload = encoded.substring("v1:".length());
        Assertions.assertEquals(0, payload.length() % 4);
        Assertions.assertEquals(payload, Base64.getEncoder().encodeToString(Base64.getDecoder().decode(payload)));
        Assertions.assertEquals(original, TabletRange.fromEncodedString(encoded));
    }

    private static Stream<Arguments> encodedRoundTripRanges() {
        Tuple lower = tuple(new IntVariant(IntegerType.INT, 1));
        Tuple upper = tuple(new IntVariant(IntegerType.INT, 9));
        Tuple complexLower = tuple(
                new StringVariant(VarcharType.VARCHAR, "a,[b],\"c\""),
                Variant.nullVariant(VarcharType.VARCHAR),
                new StringVariant(VarcharType.VARCHAR, "NULL"));
        Tuple complexUpper = tuple(
                new StringVariant(VarcharType.VARCHAR, "z,[y],\"x\""),
                new StringVariant(VarcharType.VARCHAR, "value"),
                new StringVariant(VarcharType.VARCHAR, "NULL!"));

        return Stream.of(
                Arguments.of("all range", new TabletRange(Range.all())),
                Arguments.of("lower open", new TabletRange(Range.gt(lower))),
                Arguments.of("lower closed", new TabletRange(Range.ge(lower))),
                Arguments.of("upper open", new TabletRange(Range.lt(upper))),
                Arguments.of("upper closed", new TabletRange(Range.le(upper))),
                Arguments.of("both open", new TabletRange(Range.gtlt(lower, upper))),
                Arguments.of("lower closed upper open", new TabletRange(Range.gelt(lower, upper))),
                Arguments.of("lower open upper closed", new TabletRange(Range.gtle(lower, upper))),
                Arguments.of("both closed", new TabletRange(Range.gele(lower, upper))),
                Arguments.of("multi-column punctuation and typed null",
                        new TabletRange(Range.gelt(complexLower, complexUpper))));
    }

    @Test
    public void testEncodedStringRejectsInvalidEnvelopeAndThrift() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString("v2:AAAA"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString("v1:not-base64!"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString("v1:" + Base64.getEncoder().encodeToString(new byte[] {1, 2, 3})));
    }

    @Test
    public void testEncodedStringRejectsMissingInclusionFlags() throws Exception {
        TTabletRange missingLowerFlag = new TTabletRange();
        missingLowerFlag.setUpper_bound_included(false);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encodeThrift(missingLowerFlag)));

        TTabletRange missingUpperFlag = new TTabletRange();
        missingUpperFlag.setLower_bound_included(false);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encodeThrift(missingUpperFlag)));
    }

    @Test
    public void testEncodedStringRejectsNullTupleMembers() throws Exception {
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(true);
        tRange.setUpper_bound_included(false);
        tRange.setLower_bound(new TTuple());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encodeThrift(tRange)));
    }

    @Test
    public void testEncodedStringRejectsUnsupportedVariantType() throws Exception {
        TVariant unsupported = new TVariant();
        unsupported.setType(TypeSerializer.toThrift(new ArrayType(IntegerType.INT)));
        unsupported.setValue("[1]");
        unsupported.setVariant_type(TVariantType.NORMAL_VALUE);

        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(true);
        tRange.setUpper_bound_included(false);
        TTuple lowerBound = new TTuple();
        lowerBound.setValues(List.of(unsupported));
        tRange.setLower_bound(lowerBound);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encodeThrift(tRange)));
    }

    @ParameterizedTest(name = "{0} with value")
    @MethodSource("sentinelVariantTypes")
    public void testEncodedStringRejectsValueForSentinelVariant(TVariantType variantType) throws Exception {
        String encoded = encodeVariantWithValue(
                typeDesc(scalarType(TPrimitiveType.INT)), variantType, "unexpected");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encoded));
    }

    @Test
    public void testEncodedStringRejectsStructuralMetadataOnScalarNode() throws Exception {
        TTypeNode node = scalarNode(TPrimitiveType.INT);
        TStructField field = new TStructField();
        field.setName("unexpected");
        node.setStruct_fields(List.of(field));
        node.setIs_named(true);

        String encoded = encodeVariant(typeDesc(node), TVariantType.NORMAL_VALUE);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encoded));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonCanonicalNormalValues")
    public void testEncodedStringRejectsNonCanonicalNormalValue(
            String description, TScalarType scalarType, String value) throws Exception {
        String encoded = encodeVariant(typeDesc(scalarType), TVariantType.NORMAL_VALUE, value);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encoded));
    }

    private static Stream<Arguments> nonCanonicalNormalValues() {
        return Stream.of(
                Arguments.of("BOOLEAN numeric spelling", scalarType(TPrimitiveType.BOOLEAN), "1"),
                Arguments.of("INT signed positive spelling", scalarType(TPrimitiveType.INT), "+1"));
    }

    @Test
    public void testEncodedStringRejectsComplexTypesForSentinelVariants() throws Exception {
        TTypeDesc arrayType = TypeSerializer.toThrift(new ArrayType(IntegerType.INT));

        for (TVariantType variantType : sentinelVariantTypes()) {
            String encoded = encodeVariant(arrayType, variantType);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> TabletRange.fromEncodedString(encoded), variantType.name());
        }
    }

    @Test
    public void testEncodedStringRejectsInvalidTypesForSentinelVariants() throws Exception {
        List<TTypeDesc> invalidTypes = List.of(
                new TTypeDesc(),
                typeDesc(scalarNode(null)),
                typeDesc(scalarNode(TPrimitiveType.INVALID_TYPE)));

        for (TVariantType variantType : sentinelVariantTypes()) {
            for (TTypeDesc invalidType : invalidTypes) {
                String encoded = encodeVariant(invalidType, variantType);
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> TabletRange.fromEncodedString(encoded), variantType.name());
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidScalarParameters")
    public void testEncodedStringRejectsInvalidScalarParameters(
            String description, TScalarType scalarType, String normalValue) throws Exception {
        TTypeDesc type = typeDesc(scalarType);

        for (TVariantType variantType : allVariantTypes()) {
            String encoded = encodeVariant(type, variantType, normalValue);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> TabletRange.fromEncodedString(encoded), variantType.name());
        }
    }

    private static Stream<Arguments> invalidScalarParameters() {
        int varcharMaxLength = TypeFactory.getOlapMaxVarcharLength();
        int decimalV2MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMALV2);
        int decimal32MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32);
        int decimal64MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64);
        int decimal128MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
        int decimal256MaxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256);

        TScalarType intWithLength = scalarType(TPrimitiveType.INT);
        intWithLength.setLen(1);
        TScalarType intWithPrecision = scalarType(TPrimitiveType.INT);
        intWithPrecision.setPrecision(1);
        TScalarType intWithScale = scalarType(TPrimitiveType.INT);
        intWithScale.setScale(0);
        TScalarType intWithDatetimeNtz = scalarType(TPrimitiveType.INT);
        intWithDatetimeNtz.setDatetime_is_ntz(true);
        TScalarType varcharWithPrecision = lengthScalarType(TPrimitiveType.VARCHAR, 1);
        varcharWithPrecision.setPrecision(1);
        TScalarType decimalWithLength = decimalScalarType(TPrimitiveType.DECIMAL32, 9, 0);
        decimalWithLength.setLen(4);
        TScalarType datetimeWithExplicitFalseNtz = scalarType(TPrimitiveType.DATETIME);
        datetimeWithExplicitFalseNtz.setDatetime_is_ntz(false);
        TScalarType datetimeWithTrueNtz = scalarType(TPrimitiveType.DATETIME);
        datetimeWithTrueNtz.setDatetime_is_ntz(true);

        return Stream.of(
                Arguments.of("CHAR negative length", lengthScalarType(TPrimitiveType.CHAR, -2), "x"),
                Arguments.of("CHAR zero length", lengthScalarType(TPrimitiveType.CHAR, 0), "x"),
                Arguments.of("CHAR oversized length",
                        lengthScalarType(TPrimitiveType.CHAR, ScalarType.MAX_CHAR_LENGTH + 1), "x"),
                Arguments.of("VARCHAR negative length", lengthScalarType(TPrimitiveType.VARCHAR, -2), "x"),
                Arguments.of("VARCHAR zero length", lengthScalarType(TPrimitiveType.VARCHAR, 0), "x"),
                Arguments.of("VARCHAR oversized length",
                        lengthScalarType(TPrimitiveType.VARCHAR, varcharMaxLength + 1), "x"),
                Arguments.of("VARBINARY negative length", lengthScalarType(TPrimitiveType.VARBINARY, -2), "x"),
                Arguments.of("VARBINARY zero length", lengthScalarType(TPrimitiveType.VARBINARY, 0), "x"),
                Arguments.of("VARBINARY oversized length",
                        lengthScalarType(TPrimitiveType.VARBINARY, varcharMaxLength + 1), "x"),
                Arguments.of("HLL negative length", lengthScalarType(TPrimitiveType.HLL, -1), "x"),
                Arguments.of("HLL zero length", lengthScalarType(TPrimitiveType.HLL, 0), "x"),
                Arguments.of("HLL oversized length",
                        lengthScalarType(TPrimitiveType.HLL, HLLType.MAX_HLL_LENGTH + 1), "x"),
                Arguments.of("DECIMALV2 zero precision",
                        decimalScalarType(TPrimitiveType.DECIMALV2, 0, 0), "1"),
                Arguments.of("DECIMALV2 oversized precision",
                        decimalScalarType(TPrimitiveType.DECIMALV2, decimalV2MaxPrecision + 1, 0), "1"),
                Arguments.of("DECIMALV2 scale above nine",
                        decimalScalarType(TPrimitiveType.DECIMALV2, decimalV2MaxPrecision, 10), "1"),
                Arguments.of("DECIMAL32 negative precision",
                        decimalScalarType(TPrimitiveType.DECIMAL32, -1, 0), "1"),
                Arguments.of("DECIMAL32 zero precision",
                        decimalScalarType(TPrimitiveType.DECIMAL32, 0, 0), "1"),
                Arguments.of("DECIMAL32 oversized precision",
                        decimalScalarType(TPrimitiveType.DECIMAL32, decimal32MaxPrecision + 1, 0), "1"),
                Arguments.of("DECIMAL64 oversized precision",
                        decimalScalarType(TPrimitiveType.DECIMAL64, decimal64MaxPrecision + 1, 0), "1"),
                Arguments.of("DECIMAL128 oversized precision",
                        decimalScalarType(TPrimitiveType.DECIMAL128, decimal128MaxPrecision + 1, 0), "1"),
                Arguments.of("DECIMAL256 oversized precision",
                        decimalScalarType(TPrimitiveType.DECIMAL256, decimal256MaxPrecision + 1, 0), "1"),
                Arguments.of("DECIMAL64 negative scale",
                        decimalScalarType(TPrimitiveType.DECIMAL64, 10, -1), "1"),
                Arguments.of("DECIMAL64 scale above precision",
                        decimalScalarType(TPrimitiveType.DECIMAL64, 5, 6), "1"),
                Arguments.of("INT with length", intWithLength, "1"),
                Arguments.of("INT with precision", intWithPrecision, "1"),
                Arguments.of("INT with scale", intWithScale, "1"),
                Arguments.of("INT with datetime metadata", intWithDatetimeNtz, "1"),
                Arguments.of("VARCHAR with precision", varcharWithPrecision, "x"),
                Arguments.of("DECIMAL32 with length", decimalWithLength, "1"),
                Arguments.of("DATETIME with explicit false NTZ", datetimeWithExplicitFalseNtz,
                        "2024-01-01 00:00:00"),
                Arguments.of("DATETIME with true NTZ", datetimeWithTrueNtz,
                        "2024-01-01 00:00:00"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validScalarParameters")
    public void testEncodedStringAcceptsValidScalarParameters(
            String description, TScalarType scalarType, String normalValue) throws Exception {
        TTypeDesc type = typeDesc(scalarType);

        for (TVariantType variantType : allVariantTypes()) {
            String encoded = encodeVariant(type, variantType, normalValue);
            TabletRange decoded = Assertions.assertDoesNotThrow(
                    () -> TabletRange.fromEncodedString(encoded), variantType.name());
            Assertions.assertEquals(encoded, decoded.toEncodedString(), variantType.name());
        }
    }

    private static Stream<Arguments> validScalarParameters() {
        int varcharMaxLength = TypeFactory.getOlapMaxVarcharLength();

        return Stream.of(
                Arguments.of("CHAR wildcard", lengthScalarType(TPrimitiveType.CHAR, -1), "x"),
                Arguments.of("CHAR minimum length", lengthScalarType(TPrimitiveType.CHAR, 1), "x"),
                Arguments.of("CHAR maximum length",
                        lengthScalarType(TPrimitiveType.CHAR, ScalarType.MAX_CHAR_LENGTH), "x"),
                Arguments.of("VARCHAR wildcard", lengthScalarType(TPrimitiveType.VARCHAR, -1), "x"),
                Arguments.of("VARCHAR minimum length", lengthScalarType(TPrimitiveType.VARCHAR, 1), "x"),
                Arguments.of("VARCHAR maximum length",
                        lengthScalarType(TPrimitiveType.VARCHAR, varcharMaxLength), "x"),
                Arguments.of("VARBINARY wildcard", lengthScalarType(TPrimitiveType.VARBINARY, -1), "x"),
                Arguments.of("VARBINARY minimum length", lengthScalarType(TPrimitiveType.VARBINARY, 1), "x"),
                Arguments.of("VARBINARY maximum length",
                        lengthScalarType(TPrimitiveType.VARBINARY, varcharMaxLength), "x"),
                Arguments.of("HLL canonical length",
                        lengthScalarType(TPrimitiveType.HLL, HLLType.MAX_HLL_LENGTH), "x"),
                Arguments.of("DECIMALV2 maximum parameters",
                        decimalScalarType(TPrimitiveType.DECIMALV2,
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMALV2), 9), "1"),
                Arguments.of("DECIMAL32 maximum parameters",
                        decimalScalarType(TPrimitiveType.DECIMAL32,
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32),
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32)), "1"),
                Arguments.of("DECIMAL64 maximum parameters",
                        decimalScalarType(TPrimitiveType.DECIMAL64,
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64),
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64)), "1"),
                Arguments.of("DECIMAL128 maximum parameters",
                        decimalScalarType(TPrimitiveType.DECIMAL128,
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128),
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128)), "1"),
                Arguments.of("DECIMAL256 maximum parameters",
                        decimalScalarType(TPrimitiveType.DECIMAL256,
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256),
                                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256)), "1"),
                Arguments.of("DATETIME without metadata", scalarType(TPrimitiveType.DATETIME),
                        "2024-01-01 00:00:00"));
    }

    @Test
    public void testEncodedStringRejectsDeeplyNestedComplexType() throws Exception {
        List<TTypeNode> nodes = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            TTypeNode arrayNode = new TTypeNode();
            arrayNode.setType(TTypeNodeType.ARRAY);
            nodes.add(arrayNode);
        }
        nodes.add(scalarNode(TPrimitiveType.INT));
        TTypeDesc deeplyNestedArray = new TTypeDesc();
        deeplyNestedArray.setTypes(nodes);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString(encodeVariant(deeplyNestedArray, TVariantType.MINIMUM)));
    }

    @Test
    public void testEncodedStringRejectsTrailingBytes() throws Exception {
        String encoded = new TabletRange(Range.ge(tuple(new IntVariant(IntegerType.INT, 1)))).toEncodedString();
        byte[] serialized = Base64.getDecoder().decode(encoded.substring("v1:".length()));
        byte[] withSuffix = Arrays.copyOf(serialized, serialized.length + 3);
        withSuffix[serialized.length] = 1;
        withSuffix[serialized.length + 1] = 2;
        withSuffix[serialized.length + 2] = 3;

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletRange.fromEncodedString("v1:" + Base64.getEncoder().encodeToString(withSuffix)));
    }

    private static Tuple tuple(Variant... values) {
        return new Tuple(Arrays.asList(values));
    }

    private static String encodeThrift(TTabletRange tRange) throws Exception {
        byte[] serialized = ConfigurableSerDesFactory.getTSerializer(
                ConfigurableSerDesFactory.Protocol.COMPACT.name()).serialize(tRange);
        return "v1:" + Base64.getEncoder().encodeToString(serialized);
    }

    private static String encodeVariant(TTypeDesc type, TVariantType variantType) throws Exception {
        return encodeVariant(type, variantType, "1");
    }

    private static String encodeVariant(TTypeDesc type, TVariantType variantType, String normalValue) throws Exception {
        return encodeVariant(type, variantType, normalValue, variantType == TVariantType.NORMAL_VALUE);
    }

    private static String encodeVariantWithValue(
            TTypeDesc type, TVariantType variantType, String value) throws Exception {
        return encodeVariant(type, variantType, value, true);
    }

    private static String encodeVariant(
            TTypeDesc type, TVariantType variantType, String value, boolean setValue) throws Exception {
        TVariant variant = new TVariant();
        variant.setType(type);
        variant.setVariant_type(variantType);
        if (setValue) {
            variant.setValue(value);
        }

        TTuple tuple = new TTuple();
        tuple.setValues(List.of(variant));
        TTabletRange range = new TTabletRange();
        range.setLower_bound(tuple);
        range.setLower_bound_included(true);
        range.setUpper_bound_included(false);
        return encodeThrift(range);
    }

    private static List<TVariantType> sentinelVariantTypes() {
        return List.of(TVariantType.NULL_VALUE, TVariantType.MINIMUM, TVariantType.MAXIMUM);
    }

    private static List<TVariantType> allVariantTypes() {
        return List.of(TVariantType.NORMAL_VALUE, TVariantType.NULL_VALUE,
                TVariantType.MINIMUM, TVariantType.MAXIMUM);
    }

    private static TTypeNode scalarNode(TPrimitiveType primitiveType) {
        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);
        if (primitiveType != null) {
            TScalarType scalarType = new TScalarType();
            scalarType.setType(primitiveType);
            node.setScalar_type(scalarType);
        }
        return node;
    }

    private static TTypeDesc typeDesc(TTypeNode node) {
        TTypeDesc type = new TTypeDesc();
        type.setTypes(List.of(node));
        return type;
    }

    private static TTypeDesc typeDesc(TScalarType scalarType) {
        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);
        node.setScalar_type(scalarType);
        return typeDesc(node);
    }

    private static TScalarType scalarType(TPrimitiveType primitiveType) {
        TScalarType scalarType = new TScalarType();
        scalarType.setType(primitiveType);
        return scalarType;
    }

    private static TScalarType lengthScalarType(TPrimitiveType primitiveType, int length) {
        TScalarType scalarType = scalarType(primitiveType);
        scalarType.setLen(length);
        return scalarType;
    }

    private static TScalarType decimalScalarType(TPrimitiveType primitiveType, int precision, int scale) {
        TScalarType scalarType = scalarType(primitiveType);
        scalarType.setPrecision(precision);
        scalarType.setScale(scale);
        return scalarType;
    }

    @Test
    public void testAllRangeToThrift() {
        Range<Tuple> all = Range.all();
        TabletRange tabletRange = new TabletRange(all);

        TTabletRange tRange = tabletRange.toThrift();

        // For all range, no concrete bounds should be set.
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        // Inclusiveness flags should reflect the underlying Range semantics.
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());
    }

    @Test
    public void testAllRangeToProto() {
        Range<Tuple> all = Range.all();
        TabletRange tabletRange = new TabletRange(all);

        TabletRangePB rangePB = tabletRange.toProto();
        Assertions.assertNull(rangePB.lowerBound);
        Assertions.assertNull(rangePB.upperBound);
        Assertions.assertFalse(rangePB.lowerBoundIncluded);
        Assertions.assertFalse(rangePB.upperBoundIncluded);
    }

    @Test
    public void testClosedRangeToThrift() {
        // [ (1, "a"), (5, "z") ]
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "a")));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5),
                new StringVariant(VarcharType.VARCHAR, "z")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertTrue(tRange.isUpper_bound_included());

        TTuple tLower = tRange.getLower_bound();
        TTuple tUpper = tRange.getUpper_bound();
        Assertions.assertEquals(2, tLower.getValues().size());
        Assertions.assertEquals(2, tUpper.getValues().size());

        TVariant lowerInt = tLower.getValues().get(0);
        TVariant lowerStr = tLower.getValues().get(1);
        TVariant upperInt = tUpper.getValues().get(0);
        TVariant upperStr = tUpper.getValues().get(1);

        // All Variant values are encoded via the `value` field.
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", lowerInt.getValue());
        Assertions.assertTrue(lowerStr.isSetValue());
        Assertions.assertEquals("a", lowerStr.getValue());

        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", upperInt.getValue());
        Assertions.assertTrue(upperStr.isSetValue());
        Assertions.assertEquals("z", upperStr.getValue());
    }

    @Test
    public void testClosedRangeToProto() {
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "a")));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5),
                new StringVariant(VarcharType.VARCHAR, "z")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TabletRangePB rangePB = tabletRange.toProto();
        Assertions.assertNotNull(rangePB.lowerBound);
        Assertions.assertNotNull(rangePB.upperBound);
        Assertions.assertTrue(rangePB.lowerBoundIncluded);
        Assertions.assertTrue(rangePB.upperBoundIncluded);

        TuplePB lowerPB = rangePB.lowerBound;
        TuplePB upperPB = rangePB.upperBound;
        Assertions.assertEquals(2, lowerPB.values.size());
        Assertions.assertEquals(2, upperPB.values.size());

        VariantPB lowerInt = lowerPB.values.get(0);
        VariantPB lowerStr = lowerPB.values.get(1);
        VariantPB upperInt = upperPB.values.get(0);
        VariantPB upperStr = upperPB.values.get(1);

        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, lowerInt.variantType);
        Assertions.assertEquals("1", lowerInt.value);
        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, lowerStr.variantType);
        Assertions.assertEquals("a", lowerStr.value);

        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, upperInt.variantType);
        Assertions.assertEquals("5", upperInt.value);
        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, upperStr.variantType);
        Assertions.assertEquals("z", upperStr.value);
    }

    @Test
    public void testLowerOnlyRangeToThrift() {
        // [ (10), +inf )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.BIGINT, 10L)));
        Range<Tuple> range = Range.ge(lower);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant tv = tRange.getLower_bound().getValues().get(0);
        Assertions.assertTrue(tv.isSetValue());
        Assertions.assertEquals("10", tv.getValue());
    }

    @Test
    public void testUpperOnlyRangeToThrift() {
        // (-inf, 100 )
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.BIGINT, 100L)));
        Range<Tuple> range = Range.lt(upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant tv = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(tv.isSetValue());
        Assertions.assertEquals("100", tv.getValue());
    }

    @Test
    public void testHalfOpenRangeToThrift() {
        // [ (1),  (5) )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1)));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5)));

        Range<Tuple> range = Range.gelt(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant lowerInt = tRange.getLower_bound().getValues().get(0);
        TVariant upperInt = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", lowerInt.getValue());
        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", upperInt.getValue());
    }

    @Test
    public void testOpenRangeToThrift() {
        // ( (1),  (5) )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1)));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5)));

        Range<Tuple> range = Range.gtlt(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant lowerInt = tRange.getLower_bound().getValues().get(0);
        TVariant upperInt = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", lowerInt.getValue());
        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", upperInt.getValue());
    }

    @Test
    public void testDateRangeToThrift() {
        // [ date '2024-01-01', date '2024-01-31' ]
        Tuple lower = new Tuple(Arrays.asList(
                new DateVariant(DateType.DATE, "2024-01-01")));
        Tuple upper = new Tuple(Arrays.asList(
                new DateVariant(DateType.DATE, "2024-01-31")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());

        TVariant lowerDate = tRange.getLower_bound().getValues().get(0);
        TVariant upperDate = tRange.getUpper_bound().getValues().get(0);

        // Dates should be encoded via the `value` field.
        Assertions.assertTrue(lowerDate.isSetValue());
        Assertions.assertFalse(lowerDate.getValue().isEmpty());

        Assertions.assertTrue(upperDate.isSetValue());
        Assertions.assertFalse(upperDate.getValue().isEmpty());
    }
}
