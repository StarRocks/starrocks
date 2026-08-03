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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Range;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TTabletRange;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.thrift.TVariant;
import com.starrocks.thrift.TVariantType;
import com.starrocks.type.HLLType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public class TabletRange {
    private static final String ENCODING_PREFIX = "v1:";

    @SerializedName(value = "range")
    private final Range<Tuple> range;

    public TabletRange() {
        this.range = Range.all();
    }

    public TabletRange(Range<Tuple> range) {
        this.range = range;
    }

    public Range<Tuple> getRange() {
        return this.range;
    }

    @Override
    public String toString() {
        return range.toString();
    }

    public TTabletRange toThrift() {
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(range.isLowerBoundIncluded());
        tRange.setUpper_bound_included(range.isUpperBoundIncluded());

        if (!range.isMinimum()) {
            tRange.setLower_bound(range.getLowerBound().toThrift());
        }
        if (!range.isMaximum()) {
            tRange.setUpper_bound(range.getUpperBound().toThrift());
        }
        return tRange;
    }

    public String toEncodedString() {
        try {
            byte[] serialized = ConfigurableSerDesFactory.getTSerializer(
                    ConfigurableSerDesFactory.Protocol.COMPACT.name()).serialize(toThrift());
            return ENCODING_PREFIX + Base64.getEncoder().encodeToString(serialized);
        } catch (TException | RuntimeException e) {
            throw new IllegalStateException("Failed to encode tablet range", e);
        }
    }

    public static TabletRange fromEncodedString(String encoded) {
        if (encoded == null || !encoded.startsWith(ENCODING_PREFIX)) {
            throw new IllegalArgumentException("Unsupported tablet range encoding version");
        }

        String payload = encoded.substring(ENCODING_PREFIX.length());
        if (payload.isEmpty() || payload.length() % 4 != 0) {
            throw new IllegalArgumentException("Invalid tablet range Base64 payload");
        }

        byte[] serialized;
        try {
            serialized = Base64.getDecoder().decode(payload);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid tablet range Base64 payload", e);
        }
        if (!Base64.getEncoder().encodeToString(serialized).equals(payload)) {
            throw new IllegalArgumentException("Invalid tablet range Base64 payload");
        }

        TTabletRange tRange = new TTabletRange();
        try {
            ConfigurableSerDesFactory.getTDeserializer(
                    ConfigurableSerDesFactory.Protocol.COMPACT.name()).deserialize(tRange, serialized);
            validateEncodedRange(tRange);
            TabletRange decoded = fromThrift(tRange);
            byte[] canonical = ConfigurableSerDesFactory.getTSerializer(
                    ConfigurableSerDesFactory.Protocol.COMPACT.name()).serialize(decoded.toThrift());
            if (!Arrays.equals(serialized, canonical)) {
                throw new IllegalArgumentException("Tablet range payload is not semantically canonical compact Thrift");
            }
            return decoded;
        } catch (TException | RuntimeException e) {
            throw new IllegalArgumentException("Invalid encoded tablet range", e);
        }
    }

    private static void validateEncodedRange(TTabletRange tRange) {
        if (!tRange.isSetLower_bound_included() || !tRange.isSetUpper_bound_included()) {
            throw new IllegalArgumentException("Tablet range inclusion flags are required");
        }
        if (tRange.isSetLower_bound()) {
            validateTuple(tRange.getLower_bound());
        }
        if (tRange.isSetUpper_bound()) {
            validateTuple(tRange.getUpper_bound());
        }
    }

    private static void validateTuple(TTuple tuple) {
        if (tuple == null || !tuple.isSetValues()) {
            throw new IllegalArgumentException("Tablet range tuple values are required");
        }
        for (TVariant variant : tuple.getValues()) {
            validateVariant(variant);
        }
    }

    private static void validateVariant(TVariant variant) {
        if (variant == null || !variant.isSetType() || !variant.isSetVariant_type()) {
            throw new IllegalArgumentException("Tablet range tuple contains an invalid variant");
        }
        validateVariantType(variant.getType());

        TVariantType variantType = variant.getVariant_type();
        if (variantType == TVariantType.NORMAL_VALUE && !variant.isSetValue()) {
            throw new IllegalArgumentException("Normal tablet range variant value is required");
        }
        switch (variantType) {
            case NORMAL_VALUE:
            case NULL_VALUE:
            case MINIMUM:
            case MAXIMUM:
                Variant.fromThrift(variant);
                break;
            default:
                throw new IllegalArgumentException("Unsupported tablet range variant type");
        }
    }

    private static void validateVariantType(TTypeDesc type) {
        if (type == null || !type.isSetTypes() || type.getTypesSize() != 1) {
            throw new IllegalArgumentException("Tablet range variant type must contain one scalar node");
        }

        TTypeNode node = type.getTypes().get(0);
        if (node == null || !node.isSetType() || node.getType() != TTypeNodeType.SCALAR
                || !node.isSetScalar_type()) {
            throw new IllegalArgumentException("Tablet range variant type must be scalar");
        }

        TScalarType scalarType = node.getScalar_type();
        if (scalarType == null || !scalarType.isSetType()) {
            throw new IllegalArgumentException("Tablet range scalar type is required");
        }
        validateSupportedScalarType(scalarType);
    }

    private static void validateSupportedScalarType(TScalarType scalarType) {
        TPrimitiveType primitiveType = scalarType.getType();
        switch (primitiveType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case LARGEINT:
            case DATE:
            case TIME:
            case DATETIME:
                validateNoScalarParameters(scalarType);
                return;
            case CHAR:
                if (hasOnlyLength(scalarType)
                        && isWildcardOrBoundedLength(scalarType.getLen(), ScalarType.MAX_CHAR_LENGTH)) {
                    return;
                }
                break;
            case VARCHAR:
            case VARBINARY:
                if (hasOnlyLength(scalarType)
                        && isWildcardOrBoundedLength(scalarType.getLen(), TypeFactory.getOlapMaxVarcharLength())) {
                    return;
                }
                break;
            case HLL:
                if (hasOnlyLength(scalarType) && scalarType.getLen() == HLLType.MAX_HLL_LENGTH) {
                    return;
                }
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                if (hasOnlyDecimalParameters(scalarType) && hasValidDecimalParameters(scalarType)) {
                    return;
                }
                break;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported or invalid tablet range scalar type");
    }

    private static void validateNoScalarParameters(TScalarType scalarType) {
        if (scalarType.isSetLen() || scalarType.isSetPrecision() || scalarType.isSetScale()
                || scalarType.isSetDatetime_is_ntz()) {
            throw new IllegalArgumentException("Unsupported or invalid tablet range scalar type");
        }
    }

    private static boolean hasOnlyLength(TScalarType scalarType) {
        return scalarType.isSetLen() && !scalarType.isSetPrecision() && !scalarType.isSetScale()
                && !scalarType.isSetDatetime_is_ntz();
    }

    private static boolean isWildcardOrBoundedLength(int length, int maxLength) {
        return length == -1 || length >= 1 && length <= maxLength;
    }

    private static boolean hasOnlyDecimalParameters(TScalarType scalarType) {
        return !scalarType.isSetLen() && scalarType.isSetPrecision() && scalarType.isSetScale()
                && !scalarType.isSetDatetime_is_ntz();
    }

    private static boolean hasValidDecimalParameters(TScalarType scalarType) {
        int precision = scalarType.getPrecision();
        int scale = scalarType.getScale();
        int maxPrecision;
        int maxScale;
        switch (scalarType.getType()) {
            case DECIMALV2:
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMALV2);
                maxScale = Math.min(9, precision);
                break;
            case DECIMAL32:
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL32);
                maxScale = precision;
                break;
            case DECIMAL64:
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL64);
                maxScale = precision;
                break;
            case DECIMAL128:
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                maxScale = precision;
                break;
            case DECIMAL256:
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL256);
                maxScale = precision;
                break;
            default:
                return false;
        }
        return precision >= 1 && precision <= maxPrecision && scale >= 0 && scale <= maxScale;
    }

    public TabletRangePB toProto() {
        TabletRangePB rangePB = new TabletRangePB();
        rangePB.lowerBoundIncluded = range.isLowerBoundIncluded();
        rangePB.upperBoundIncluded = range.isUpperBoundIncluded();

        if (!range.isMinimum()) {
            rangePB.lowerBound = range.getLowerBound().toProto();
        }
        if (!range.isMaximum()) {
            rangePB.upperBound = range.getUpperBound().toProto();
        }
        return rangePB;
    }

    public static TabletRange fromThrift(TTabletRange tTabletRange) {
        Tuple lowerBound = tTabletRange.lower_bound != null ? Tuple.fromThrift(tTabletRange.lower_bound) : null;
        Tuple upperBound = tTabletRange.upper_bound != null ? Tuple.fromThrift(tTabletRange.upper_bound) : null;
        return new TabletRange(
                Range.of(lowerBound, upperBound,
                        tTabletRange.lower_bound_included, tTabletRange.upper_bound_included));
    }

    public static TabletRange fromProto(TabletRangePB tabletRangePB) {
        Tuple lowerBound = tabletRangePB.lowerBound != null ? Tuple.fromProto(tabletRangePB.lowerBound) : null;
        Tuple upperBound = tabletRangePB.upperBound != null ? Tuple.fromProto(tabletRangePB.upperBound) : null;
        boolean lowerIncluded = tabletRangePB.lowerBoundIncluded != null ? tabletRangePB.lowerBoundIncluded : false;
        boolean upperIncluded = tabletRangePB.upperBoundIncluded != null ? tabletRangePB.upperBoundIncluded : false;
        return new TabletRange(Range.of(lowerBound, upperBound, lowerIncluded, upperIncluded));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TabletRange other = (TabletRange) o;
        return Objects.equals(range, other.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range);
    }

}
