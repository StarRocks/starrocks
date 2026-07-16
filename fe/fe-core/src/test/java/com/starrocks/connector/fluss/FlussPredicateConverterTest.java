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

package com.starrocks.connector.fluss;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.IsNotNull;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessOrEqual;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.NotEqual;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.predicate.StartsWith;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlussPredicateConverterTest {
    private static final RowType ROW_TYPE = new RowType(Arrays.asList(
            new DataField("id", DataTypes.INT()),
            new DataField("name", DataTypes.STRING()),
            new DataField("flag", DataTypes.BOOLEAN()),
            new DataField("dt", DataTypes.DATE()),
            new DataField("ts", DataTypes.TIMESTAMP()),
            new DataField("ts_ltz", DataTypes.TIMESTAMP_LTZ()),
            new DataField("price", DataTypes.DECIMAL(10, 2))));

    private static final ColumnRefOperator ID =
            new ColumnRefOperator(0, com.starrocks.type.IntegerType.INT, "id", true, false);
    private static final ColumnRefOperator NAME =
            new ColumnRefOperator(1, com.starrocks.type.VarcharType.VARCHAR, "name", true, false);
    private static final ColumnRefOperator FLAG =
            new ColumnRefOperator(2, com.starrocks.type.BooleanType.BOOLEAN, "flag", true, false);
    private static final ColumnRefOperator DT =
            new ColumnRefOperator(3, com.starrocks.type.DateType.DATE, "dt", true, false);
    private static final ColumnRefOperator TS =
            new ColumnRefOperator(4, com.starrocks.type.DateType.DATETIME, "ts", true, false);
    private static final ColumnRefOperator TS_LTZ =
            new ColumnRefOperator(5, com.starrocks.type.DateType.DATETIME, "ts_ltz", true, false);
    private static final ColumnRefOperator PRICE =
            new ColumnRefOperator(6, TypeFactory.createUnifiedDecimalType(10, 2), "price", true, false);

    private static final FlussPredicateConverter CONVERTER =
            new FlussPredicateConverter(ROW_TYPE, ZoneOffset.UTC);

    @Test
    public void testNullAndUnsupportedPredicate() {
        Assertions.assertNull(CONVERTER.convert(null));
        Assertions.assertNull(CONVERTER.convert(ConstantOperator.createInt(1)));

        ScalarOperator noColumnPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        Assertions.assertNull(CONVERTER.convert(noColumnPredicate));
    }

    @Test
    public void testBinaryPredicates() {
        assertBinaryPredicate(BinaryType.EQ, Equal.class, 7);
        assertBinaryPredicate(BinaryType.NE, NotEqual.class, 7);
        assertBinaryPredicate(BinaryType.LT, LessThan.class, 7);
        assertBinaryPredicate(BinaryType.LE, LessOrEqual.class, 7);
        assertBinaryPredicate(BinaryType.GT, GreaterThan.class, 7);
        assertBinaryPredicate(BinaryType.GE, GreaterOrEqual.class, 7);
    }

    @Test
    public void testLiteralTypeConversion() {
        // SR stores DATE constants as LocalDateTime, while Fluss predicates expect epoch-day integers.
        LeafPredicate datePredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.EQ, DT, ConstantOperator.createDate(LocalDate.of(2026, 7, 8).atStartOfDay()))),
                Equal.class, 3, "dt");
        Assertions.assertEquals((int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.of(2026, 7, 8)),
                datePredicate.literals().get(0));

        // Plain TIMESTAMP keeps the local date-time value without applying the session time zone.
        LocalDateTime timestamp = LocalDateTime.of(2026, 7, 8, 12, 13, 14);
        LeafPredicate timestampPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.EQ, TS, ConstantOperator.createDatetime(timestamp))), Equal.class, 4, "ts");
        Assertions.assertEquals(timestamp,
                ((TimestampNtz) timestampPredicate.literals().get(0)).toLocalDateTime());

        // DECIMAL literals must use the Fluss column precision and scale when building Fluss Decimal.
        BigDecimal price = new BigDecimal("12.34");
        LeafPredicate decimalPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.EQ, PRICE, ConstantOperator.createDecimal(price,
                TypeFactory.createUnifiedDecimalType(10, 2)))), Equal.class, 6, "price");
        Assertions.assertEquals(0, price.compareTo(((Decimal) decimalPredicate.literals().get(0)).toBigDecimal()));
    }

    @Test
    public void testInAndBooleanPredicate() {
        List<ScalarOperator> inChildren = new ArrayList<>();
        inChildren.add(NAME);
        inChildren.add(ConstantOperator.createVarchar("alice"));
        inChildren.add(ConstantOperator.createVarchar("bob"));
        Predicate inPredicate = CONVERTER.convert(new InPredicateOperator(false, inChildren));
        List<Predicate> disjuncts = PredicateBuilder.splitOr(inPredicate);
        Assertions.assertEquals(2, disjuncts.size());
        LeafPredicate alicePredicate = assertLeaf(disjuncts.get(0), Equal.class, 1, "name");
        LeafPredicate bobPredicate = assertLeaf(disjuncts.get(1), Equal.class, 1, "name");
        Assertions.assertEquals(BinaryString.fromString("alice"), alicePredicate.literals().get(0));
        Assertions.assertEquals(BinaryString.fromString("bob"), bobPredicate.literals().get(0));

        LeafPredicate booleanPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.EQ, FLAG, ConstantOperator.createBoolean(true))), Equal.class, 2, "flag");
        Assertions.assertEquals(true, booleanPredicate.literals().get(0));
    }

    @Test
    public void testIsNullPredicate() {
        assertLeaf(CONVERTER.convert(new IsNullPredicateOperator(false, NAME)), IsNull.class, 1, "name");
        assertLeaf(CONVERTER.convert(new IsNullPredicateOperator(true, NAME)), IsNotNull.class, 1, "name");
    }

    @Test
    public void testLikePrefixPredicate() {
        LeafPredicate prefixPredicate = assertLeaf(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("abc%"))),
                StartsWith.class, 1, "name");
        Assertions.assertEquals(BinaryString.fromString("abc"), prefixPredicate.literals().get(0));

        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("%abc"))));
        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("a%c%"))));
        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("ab_%"))));
        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("abc\\%"))));
        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, NAME, ConstantOperator.createVarchar("ab\\_%"))));
    }

    @Test
    public void testCompoundPartialPushdown() {
        ScalarOperator pushable = new BinaryPredicateOperator(BinaryType.EQ, ID, ConstantOperator.createInt(11));
        ScalarOperator unsupported = new BinaryPredicateOperator(
                BinaryType.EQ, ConstantOperator.createInt(1), ConstantOperator.createInt(1));

        // Partial AND pushdown is safe: rows matching the full predicate must also match the pushed side.
        Predicate andPredicate = CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pushable, unsupported));
        LeafPredicate pushedLeaf = assertLeaf(andPredicate, Equal.class, 0, "id");
        Assertions.assertEquals(11, pushedLeaf.literals().get(0));

        Predicate rightOnly = CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, unsupported, pushable));
        Assertions.assertEquals(11,
                assertLeaf(rightOnly, Equal.class, 0, "id").literals().get(0));

        // Partial OR pushdown is unsafe because the unsupported side may still match rows rejected by the pushed side.
        Predicate orPredicate = CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, pushable, unsupported));
        Assertions.assertNull(orPredicate);

        // When both sides are pushable, keep the original AND structure as a Fluss compound predicate.
        Predicate bothPushable = CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryType.GE, ID, ConstantOperator.createInt(10)),
                new BinaryPredicateOperator(BinaryType.LE, ID, ConstantOperator.createInt(20))));
        Assertions.assertTrue(bothPushable instanceof CompoundPredicate);
        Assertions.assertEquals(2, ((CompoundPredicate) bothPushable).children().size());
    }

    @Test
    public void testTimestampLtzUsesSessionTimezone() {
        ZoneId sessionZoneId = ZoneId.of("Asia/Shanghai");
        FlussPredicateConverter converter = new FlussPredicateConverter(ROW_TYPE, sessionZoneId);
        LocalDateTime literal = LocalDateTime.of(2026, 7, 8, 12, 0, 0);

        LeafPredicate leafPredicate = assertLeaf(converter.convert(new BinaryPredicateOperator(
                BinaryType.EQ, TS_LTZ, ConstantOperator.createDatetime(literal))), Equal.class, 5, "ts_ltz");
        TimestampLtz timestampLtz = (TimestampLtz) leafPredicate.literals().get(0);
        Instant expectedInstant = literal.atZone(sessionZoneId).toInstant();
        Assertions.assertEquals(expectedInstant, timestampLtz.toInstant());

        LeafPredicate negatedLeafPredicate = assertLeaf(converter.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT,
                new BinaryPredicateOperator(BinaryType.EQ, TS_LTZ, ConstantOperator.createDatetime(literal)))),
                NotEqual.class, 5, "ts_ltz");
        Assertions.assertEquals(expectedInstant,
                ((TimestampLtz) negatedLeafPredicate.literals().get(0)).toInstant());

        FlussPredicateConverter utcConverter = new FlussPredicateConverter(ROW_TYPE, null);
        LeafPredicate utcLeafPredicate = assertLeaf(utcConverter.convert(new BinaryPredicateOperator(
                BinaryType.EQ, TS_LTZ, ConstantOperator.createDatetime(literal))), Equal.class, 5, "ts_ltz");
        Assertions.assertEquals(literal.toInstant(ZoneOffset.UTC),
                ((TimestampLtz) utcLeafPredicate.literals().get(0)).toInstant());
    }

    @Test
    public void testNotPredicate() {
        ScalarOperator pushable = new BinaryPredicateOperator(BinaryType.EQ, ID, ConstantOperator.createInt(11));
        ScalarOperator unsupported = new BinaryPredicateOperator(
                BinaryType.EQ, ConstantOperator.createInt(1), ConstantOperator.createInt(1));

        LeafPredicate negated = assertLeaf(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, pushable)), NotEqual.class, 0, "id");
        Assertions.assertEquals(11, negated.literals().get(0));
        Assertions.assertNull(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT,
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                        NAME, ConstantOperator.createVarchar("abc%")))));
        Assertions.assertNull(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, unsupported)));

        ScalarOperator partiallyConvertedAnd = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pushable, unsupported);
        Assertions.assertNull(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, partiallyConvertedAnd)));
        Assertions.assertNull(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT,
                new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, unsupported, pushable))));

        ScalarOperator nestedPartialAnd = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryType.GT, ID, ConstantOperator.createInt(0)),
                partiallyConvertedAnd);
        Assertions.assertNull(CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, nestedPartialAnd)));

        Predicate fullyConvertedNot = CONVERTER.convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT,
                new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryType.GE, ID, ConstantOperator.createInt(10)),
                        new BinaryPredicateOperator(BinaryType.LE, ID, ConstantOperator.createInt(20)))));
        Assertions.assertTrue(fullyConvertedNot instanceof CompoundPredicate);
        Assertions.assertEquals(2, ((CompoundPredicate) fullyConvertedNot).children().size());
    }

    @Test
    public void testUnsupportedOperands() {
        Assertions.assertNull(CONVERTER.convert(new IsNullPredicateOperator(
                false, ConstantOperator.createVarchar("not-a-column"))));
        Assertions.assertNull(CONVERTER.convert(new BinaryPredicateOperator(BinaryType.EQ, ID, NAME)));
        Assertions.assertNull(CONVERTER.convert(new InPredicateOperator(
                false, Arrays.asList(NAME, ConstantOperator.createVarchar("alice"), ID))));
        Assertions.assertNull(CONVERTER.convert(new LikePredicateOperator(
                LikePredicateOperator.LikeType.REGEXP, NAME, ConstantOperator.createVarchar("abc.*"))));
    }

    @Test
    public void testNotInPredicate() {
        Predicate notInPredicate = CONVERTER.convert(new InPredicateOperator(
                true, Arrays.asList(FLAG, ConstantOperator.createBoolean(true),
                        ConstantOperator.createBoolean(false))));
        List<Predicate> conjuncts = PredicateBuilder.splitAnd(notInPredicate);
        Assertions.assertEquals(2, conjuncts.size());
        Assertions.assertEquals(true,
                assertLeaf(conjuncts.get(0), NotEqual.class, 2, "flag").literals().get(0));
        Assertions.assertEquals(false,
                assertLeaf(conjuncts.get(1), NotEqual.class, 2, "flag").literals().get(0));
    }

    @Test
    public void testCastOperands() {
        CastOperator castId = new CastOperator(com.starrocks.type.IntegerType.BIGINT, ID);
        LeafPredicate castColumnPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.GE, castId, ConstantOperator.createInt(10))), GreaterOrEqual.class, 0, "id");
        Assertions.assertEquals(10, castColumnPredicate.literals().get(0));

        CastOperator castLiteral = new CastOperator(
                com.starrocks.type.IntegerType.BIGINT, ConstantOperator.createInt(12));
        LeafPredicate castLiteralPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                BinaryType.EQ, ID, castLiteral)), Equal.class, 0, "id");
        Assertions.assertEquals(12, castLiteralPredicate.literals().get(0));
    }

    @Test
    public void testNumericAndCharacterLiteralTypes() {
        assertSingleColumnLiteral(DataTypes.TINYINT(), com.starrocks.type.IntegerType.TINYINT,
                ConstantOperator.createTinyInt((byte) 1), (byte) 1);
        assertSingleColumnLiteral(DataTypes.SMALLINT(), com.starrocks.type.IntegerType.SMALLINT,
                ConstantOperator.createSmallInt((short) 2), (short) 2);
        assertSingleColumnLiteral(DataTypes.BIGINT(), com.starrocks.type.IntegerType.BIGINT,
                ConstantOperator.createBigint(3L), 3L);
        assertSingleColumnLiteral(DataTypes.FLOAT(), com.starrocks.type.FloatType.FLOAT,
                ConstantOperator.createFloat(4.5), 4.5F);
        assertSingleColumnLiteral(DataTypes.DOUBLE(), com.starrocks.type.FloatType.DOUBLE,
                ConstantOperator.createDouble(5.5), 5.5D);
        assertSingleColumnLiteral(DataTypes.CHAR(8), TypeFactory.createCharType(8),
                ConstantOperator.createChar("char"), BinaryString.fromString("char"));
    }

    @Test
    public void testLiteralCastsToFlussFieldTypes() {
        Assertions.assertEquals(true, literal(CONVERTER, FLAG, ConstantOperator.createVarchar("true")));
        Assertions.assertEquals((int) LocalDate.of(2026, 7, 8).toEpochDay(),
                literal(CONVERTER, DT, ConstantOperator.createVarchar("2026-07-08")));
        Assertions.assertEquals(LocalDateTime.of(2026, 7, 8, 12, 13, 14),
                ((TimestampNtz) literal(CONVERTER, TS,
                        ConstantOperator.createVarchar("2026-07-08 12:13:14"))).toLocalDateTime());
        Assertions.assertEquals(BinaryString.fromString("7"),
                literal(CONVERTER, NAME, ConstantOperator.createInt(7)));
        Decimal decimal = (Decimal) literal(CONVERTER, PRICE, ConstantOperator.createVarchar("12.34"));
        Assertions.assertEquals(0, new BigDecimal("12.34").compareTo(decimal.toBigDecimal()));

        Assertions.assertNull(convertSingleColumnLiteral(DataTypes.BINARY(8),
                com.starrocks.type.VarbinaryType.VARBINARY, ConstantOperator.createVarchar("binary")));
    }

    private static void assertSingleColumnLiteral(DataType dataType, Type columnType,
                                                  ConstantOperator literal, Object expected) {
        LeafPredicate predicate = assertLeaf(
                convertSingleColumnLiteral(dataType, columnType, literal), Equal.class, 0, "value");
        Assertions.assertEquals(expected, predicate.literals().get(0));
    }

    private static Predicate convertSingleColumnLiteral(DataType dataType, Type columnType,
                                                        ConstantOperator literal) {
        RowType rowType = new RowType(List.of(new DataField("value", dataType)));
        ColumnRefOperator column = new ColumnRefOperator(0, columnType, "value", true, false);
        FlussPredicateConverter converter = new FlussPredicateConverter(rowType, ZoneOffset.UTC);
        return converter.convert(new BinaryPredicateOperator(BinaryType.EQ, column, literal));
    }

    private static Object literal(FlussPredicateConverter converter, ColumnRefOperator column,
                                  ConstantOperator literal) {
        return assertLeaf(converter.convert(new BinaryPredicateOperator(BinaryType.EQ, column, literal)),
                Equal.class, column.getId(), column.getName()).literals().get(0);
    }

    private static void assertBinaryPredicate(BinaryType binaryType, Class<?> functionClass, Object expectedLiteral) {
        LeafPredicate leafPredicate = assertLeaf(CONVERTER.convert(new BinaryPredicateOperator(
                binaryType, ID, ConstantOperator.createInt((Integer) expectedLiteral))), functionClass, 0, "id");
        Assertions.assertEquals(expectedLiteral, leafPredicate.literals().get(0));
    }

    private static LeafPredicate assertLeaf(Predicate predicate, Class<?> functionClass, int index, String fieldName) {
        Assertions.assertTrue(predicate instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) predicate;
        Assertions.assertTrue(functionClass.isInstance(leafPredicate.function()));
        Assertions.assertEquals(index, leafPredicate.index());
        Assertions.assertEquals(fieldName, leafPredicate.fieldName());
        return leafPredicate;
    }
}
