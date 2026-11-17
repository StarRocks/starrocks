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

import com.starrocks.common.Int128;
import com.starrocks.common.Range;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.TabletRangePB;
import com.starrocks.persist.proto.TupleListPB;
import com.starrocks.persist.proto.TuplePB;
import com.starrocks.persist.proto.VariantPB;
import com.starrocks.type.Type;
import com.starrocks.type.TypeProtoConverter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a tablet range (range of tuples of variants) to and from its image v3 messages.
 *
 * <p>TabletRange, Tuple and Variant already own {@code toProto} / {@code fromProto} methods for the BE-facing
 * messages in {@code com.starrocks.proto}, so the image conversions live here instead.
 */
public final class TabletRangeProtoConverter {
    private TabletRangeProtoConverter() {
    }

    public static TabletRangePB toProto(TabletRange tabletRange) {
        Range<Tuple> range = tabletRange.getRange();
        TabletRangePB.Builder builder = TabletRangePB.newBuilder()
                .setLowerBoundIncluded(range.isLowerBoundIncluded())
                .setUpperBoundIncluded(range.isUpperBoundIncluded());
        if (range.getLowerBound() != null) {
            builder.setLowerBound(toProto(range.getLowerBound()));
        }
        if (range.getUpperBound() != null) {
            builder.setUpperBound(toProto(range.getUpperBound()));
        }
        return builder.build();
    }

    public static TabletRange fromProto(TabletRangePB pb) {
        // An absent bound is an unbounded side, as in the GSON adapter.
        return new TabletRange(Range.of(pb.hasLowerBound() ? fromProto(pb.getLowerBound()) : null,
                pb.hasUpperBound() ? fromProto(pb.getUpperBound()) : null,
                pb.getLowerBoundIncluded(), pb.getUpperBoundIncluded()));
    }

    public static TuplePB toProto(Tuple tuple) {
        TuplePB.Builder builder = TuplePB.newBuilder();
        for (Variant value : tuple.getValues()) {
            builder.addValues(toProto(value));
        }
        return builder.build();
    }

    public static Tuple fromProto(TuplePB pb) {
        List<Variant> values = new ArrayList<>(pb.getValuesCount());
        for (VariantPB value : pb.getValuesList()) {
            values.add(fromProto(value));
        }
        return new Tuple(values);
    }

    public static VariantPB toProto(Variant variant) {
        VariantPB.Builder builder = VariantPB.newBuilder();
        if (variant.type != null) {
            builder.setType(TypeProtoConverter.toProto(variant.type));
        }
        if (variant instanceof BoolVariant) {
            builder.setKind(VariantPB.Kind.BOOL).setBoolValue(((BoolVariant) variant).value);
        } else if (variant instanceof IntVariant) {
            builder.setKind(VariantPB.Kind.INT).setIntValue(((IntVariant) variant).value);
        } else if (variant instanceof LargeIntVariant) {
            builder.setKind(VariantPB.Kind.LARGE_INT);
            Int128 value = ((LargeIntVariant) variant).value;
            if (value != null) {
                builder.setLargeIntValue(value.toProto());
            }
        } else if (variant instanceof StringVariant) {
            builder.setKind(VariantPB.Kind.STRING);
            String value = ((StringVariant) variant).value;
            if (value != null) {
                builder.setStringValue(value);
            }
        } else if (variant instanceof DateVariant) {
            DateVariant dateVariant = (DateVariant) variant;
            builder.setKind(VariantPB.Kind.DATE).setDate(VariantPB.DatePB.newBuilder()
                    .setSeconds(dateVariant.seconds).setNanos(dateVariant.nanos));
            // Keep older image readers able to recover both components of a date.
            builder.setDateSeconds(dateVariant.seconds).setDateNanos(dateVariant.nanos);
        } else if (variant instanceof DecimalVariant) {
            builder.setKind(VariantPB.Kind.DECIMAL);
            BigDecimal value = ((DecimalVariant) variant).value;
            if (value != null) {
                builder.setDecimalValue(value.toString());
            }
        } else if (variant instanceof NullVariant) {
            builder.setKind(VariantPB.Kind.NULL_VALUE);
        } else if (variant instanceof MinVariant) {
            builder.setKind(VariantPB.Kind.MINIMUM);
        } else if (variant instanceof MaxVariant) {
            builder.setKind(VariantPB.Kind.MAXIMUM);
        } else {
            throw new IllegalArgumentException("variant class " + variant.getClass().getName() + " cannot be persisted");
        }
        return builder.build();
    }

    public static Variant fromProto(VariantPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        Type type = pb.hasType() ? TypeProtoConverter.fromProto(pb.getType()) : null;
        switch (pb.getKind()) {
            case BOOL:
                return new BoolVariant(type, pb.getBoolValue());
            case INT:
                return new IntVariant(type, pb.getIntValue());
            case LARGE_INT:
                return new LargeIntVariant(type, pb.hasLargeIntValue() ? Int128.fromProto(pb.getLargeIntValue()) : null);
            case STRING:
                return new StringVariant(type, pb.hasStringValue() ? pb.getStringValue() : null);
            case DATE:
                return new DateVariant(type, pb.hasDate()
                        ? Instant.ofEpochSecond(pb.getDate().getSeconds(), pb.getDate().getNanos())
                        : Instant.ofEpochSecond(pb.getDateSeconds(), pb.getDateNanos()));
            case DECIMAL:
                return new DecimalVariant(type, pb.hasDecimalValue() ? new BigDecimal(pb.getDecimalValue()) : null);
            case NULL_VALUE:
                return new NullVariant(type);
            case MINIMUM:
                return new MinVariant(type);
            case MAXIMUM:
                return new MaxVariant(type);
            default:
                throw new IllegalArgumentException("unknown variant kind " + pb.getKind());
        }
    }

    /** A list of tuples that can be null travels in a wrapper; the caller handles the null case. */
    public static TupleListPB toListProto(List<Tuple> tuples) {
        TupleListPB.Builder builder = TupleListPB.newBuilder();
        for (Tuple tuple : tuples) {
            builder.addValues(toProto(tuple));
        }
        return builder.build();
    }

    public static List<Tuple> fromListProto(TupleListPB pb) {
        List<Tuple> tuples = new ArrayList<>(pb.getValuesCount());
        for (TuplePB tuple : pb.getValuesList()) {
            tuples.add(fromProto(tuple));
        }
        return tuples;
    }
}
