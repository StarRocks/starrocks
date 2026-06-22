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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FoldConstantsRuleTest {
    private final FoldConstantsRule rule = new FoldConstantsRule();

    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);
    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_NULL = ConstantOperator.createNull(BooleanType.BOOLEAN);

    @Test
    public void applyCall() {

        CallOperator root = new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3")
        ));

        Function fn = new Function(new FunctionName(FunctionSet.CONCAT),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(root) {
            {
                root.getFunction();
                result = fn;
            }
        };

        ScalarOperator operator = rule.apply(root, new ScalarOperatorRewriteContext());

        assertEquals(OperatorType.CONSTANT, operator.getOpType());
        assertEquals("123", ((ConstantOperator) operator).getVarchar());
    }

    @Test
    public void applyIn() {
        InPredicateOperator ipo1 = new InPredicateOperator(ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_NULL, rule.apply(ipo1, null));

        InPredicateOperator ipo2 = new InPredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("test"));
        assertEquals(ipo2, rule.apply(ipo2, null));

        InPredicateOperator ipo3 = new InPredicateOperator(ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createVarchar("test1"));
        assertEquals(OB_TRUE, rule.apply(ipo3, null));

        InPredicateOperator ipo4 = new InPredicateOperator(true, ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createVarchar("test1"));
        assertEquals(OB_FALSE, rule.apply(ipo4, null));

        InPredicateOperator ipo5 = new InPredicateOperator(true, ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createNull(BooleanType.BOOLEAN));

        assertEquals(OB_NULL, rule.apply(ipo5, null));

        InPredicateOperator ipo6 = new InPredicateOperator(true, ConstantOperator.createTime(-1077590.0),
                ConstantOperator.createNull(DateType.TIME));
        assertEquals(OB_NULL, rule.apply(ipo6, null));
    }

    @Test
    public void applyInNull() {
        InPredicateOperator ipo2 = new InPredicateOperator(ConstantOperator.createInt(0),
                ConstantOperator.createNull(IntegerType.INT));
        assertEquals(ConstantOperator.createNull(BooleanType.BOOLEAN), rule.apply(ipo2, null));
    }

    @Test
    public void applyIsNull() {
        IsNullPredicateOperator inpo1 =
                new IsNullPredicateOperator(new ColumnRefOperator(1, BooleanType.BOOLEAN, "name", true));
        assertEquals(inpo1, rule.apply(inpo1, null));

        IsNullPredicateOperator inpo2 = new IsNullPredicateOperator(ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(inpo2, null));

        IsNullPredicateOperator inpo3 = new IsNullPredicateOperator(true, ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_FALSE, rule.apply(inpo3, null));
    }

    @Test
    public void applyCast() {
        CastOperator cast1 = new CastOperator(BooleanType.BOOLEAN, ConstantOperator.createNull(NullType.NULL));
        assertEquals(OB_NULL, rule.apply(cast1, null));

        CastOperator cast2 = new CastOperator(BooleanType.BOOLEAN,
                new ColumnRefOperator(1, VarcharType.VARCHAR, "test", true));
        assertEquals(cast2, rule.apply(cast2, null));

        CastOperator cast3 = new CastOperator(BooleanType.BOOLEAN, ConstantOperator.createChar("true"));
        assertEquals(ConstantOperator.createBoolean(true), rule.apply(cast3, null));

        CastOperator cast4 = new CastOperator(VarcharType.VARCHAR, ConstantOperator.createBigint(123));
        assertEquals(ConstantOperator.createVarchar("123"), rule.apply(cast4, null));

        CastOperator cast5 = new CastOperator(DateType.DATE, ConstantOperator.createVarchar("2020-02-12 12:23:00"));
        assertEquals(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 12, 0, 0, 0)), rule.apply(cast5, null));

        CastOperator cast6 = new CastOperator(
                IntegerType.BIGINT, ConstantOperator.createDate(LocalDateTime.of(2020, 2, 12, 0, 0, 0)));
        assertEquals(ConstantOperator.createBigint(20200212), rule.apply(cast6, null));

        CastOperator cast7 = new CastOperator(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 1, 1),
                ConstantOperator.createDecimal(BigDecimal.valueOf(0.00008),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 6, 6))
        );
        assertEquals("0.0", rule.apply(cast7, null).toString());

        // cast(96.1 as int)-> 96
        CastOperator cast8 = new CastOperator(IntegerType.INT, ConstantOperator.createDouble(96.1));
        assertEquals(ConstantOperator.createInt(96), rule.apply(cast8, null));
    }

    @Test
    public void applyBinary() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createNull(IntegerType.INT), ConstantOperator.createInt(1));
        assertEquals(OB_NULL, rule.apply(bpo1, null));

        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "name", true), ConstantOperator.createInt(1));
        assertEquals(bpo2, rule.apply(bpo2, null));

        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo3, null));

        BinaryPredicateOperator bpo4 = new BinaryPredicateOperator(BinaryType.NE,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)));
        assertEquals(OB_FALSE, rule.apply(bpo4, null));

        BinaryPredicateOperator bpo5 = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(BooleanType.BOOLEAN), ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(bpo5, null));

        BinaryPredicateOperator bpo6 = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(BooleanType.BOOLEAN), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo6, null));

        BinaryPredicateOperator bpo7 = new BinaryPredicateOperator(BinaryType.GE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo7, null));

        BinaryPredicateOperator bpo8 = new BinaryPredicateOperator(BinaryType.GT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo8, null));

        BinaryPredicateOperator bpo9 = new BinaryPredicateOperator(BinaryType.LE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo9, null));

        BinaryPredicateOperator bpo10 = new BinaryPredicateOperator(BinaryType.LT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo10, null));

    }

    @Test
    public void applyLikeFolding() {
        // Values below are what the FE Parser produces after the first layer of string-literal
        // escaping (AstBuilder.escapeBackSlash). Java string mapping:
        //   SQL 'a\\b'   → Java "a\\b"  (3 chars: a, \, b)
        //   SQL 'a\\\\b' → Java "a\\\\b" (4 chars: a, \, \, b)
        //   SQL '100\\%' → Java "100\\%" (5 chars: 1,0,0,\,%) — Parser keeps \% intact
        //   SQL 'a\\_b'  → Java "a\\_b"  (4 chars: a,\,_,b)   — Parser keeps \_ intact

        // Bug regression: 'a\\b' LIKE 'a\\\\b' must return 1 (MySQL), was 0 before fix
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a\\b"),
                ConstantOperator.createVarchar("a\\\\b")), null));

        // Bug regression: 'a\\b' LIKE 'a\\b' must return 0 (MySQL), was 1 before fix
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a\\b"),
                ConstantOperator.createVarchar("a\\b")), null));

        // Escaped % as literal: '100%' LIKE '100\%' → 1 (\% matches the literal % in text)
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("100%"),
                ConstantOperator.createVarchar("100\\%")), null));

        // Escaped % must NOT act as wildcard: '100abc' LIKE '100\%' → 0
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("100abc"),
                ConstantOperator.createVarchar("100\\%")), null));

        // Plain equals (no backslash) — existing behavior preserved
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("hello"),
                ConstantOperator.createVarchar("hello")), null));

        // Plain startsWith — existing behavior preserved
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("abcdef"),
                ConstantOperator.createVarchar("abc%")), null));

        // Pattern with _ wildcard: not a fast-path pattern, returned to BE as-is
        LikePredicateOperator withUnderscore = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("abc"),
                ConstantOperator.createVarchar("a_c"));
        assertEquals(withUnderscore, rule.apply(withUnderscore, null));

        // Non-constant text: not folded, returned as-is
        LikePredicateOperator nonConst = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                new ColumnRefOperator(1, VarcharType.VARCHAR, "col", true),
                ConstantOperator.createVarchar("a\\\\b"));
        assertEquals(nonConst, rule.apply(nonConst, null));

        // endsWith path: 'xyzabc' LIKE '%abc' → 1
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xyzabc"),
                ConstantOperator.createVarchar("%abc")), null));

        // contains path: 'xyzabcdef' LIKE '%abc%' → 1
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xyzabcdef"),
                ConstantOperator.createVarchar("%abc%")), null));

        // Trailing lone backslash — invalid LIKE escape, not folded → return predicate
        LikePredicateOperator trailingSlash = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a\\"),
                ConstantOperator.createVarchar("a\\"));
        assertEquals(trailingSlash, rule.apply(trailingSlash, null));

        // startsWith with escaped % in body: text must start with literal "a%xy"
        // SQL pattern 'a\%xy%': Parser keeps \% → Java "a\\%xy%"; trailing % is the real wildcard
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a%xyz"),
                ConstantOperator.createVarchar("a\\%xy%")), null));

        // \% in body is NOT a wildcard: "aXxyz" does not start with "a%xy"
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("aXxyz"),
                ConstantOperator.createVarchar("a\\%xy%")), null));

        // --- Escaped wildcard directly adjacent to a real wildcard (regression for the
        //     stripStart/stripEnd over-strip bug; only passes with the capture-group fix) ---

        // startsWith: SQL 'abc\%%' → Java "abc\\%%": literal "abc%" + trailing wildcard %.
        // Must match text starting with "abc%".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("abc%xyz"),
                ConstantOperator.createVarchar("abc\\%%")), null));
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("abcXyz"),
                ConstantOperator.createVarchar("abc\\%%")), null));

        // endsWith: SQL '%\%abc' → Java "%\\%abc": leading wildcard % + literal "%abc".
        // Must match text ending with "%abc".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xyz%abc"),
                ConstantOperator.createVarchar("%\\%abc")), null));
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xyzabc"),
                ConstantOperator.createVarchar("%\\%abc")), null));

        // contains: SQL '%a\%b%' → Java "%a\\%b%": wildcards on both ends, literal "a%b" inside.
        // Must match text containing "a%b".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xa%by"),
                ConstantOperator.createVarchar("%a\\%b%")), null));
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("xaXby"),
                ConstantOperator.createVarchar("%a\\%b%")), null));

        // --- Other escape forms ---

        // Double backslash + wildcard: SQL 'a\\%' → Java "a\\\\%": literal "a\" + wildcard %.
        // Must match text starting with "a\".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a\\xyz"),
                ConstantOperator.createVarchar("a\\\\%")), null));

        // Trailing escaped backslash (EQUALS): SQL 'abc\\' → Java "abc\\\\": literal "abc\".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("abc\\"),
                ConstantOperator.createVarchar("abc\\\\")), null));

        // Escaped underscore as a literal: SQL 'a\_b' → Java "a\\_b": literal "a_b".
        assertEquals(OB_TRUE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("a_b"),
                ConstantOperator.createVarchar("a\\_b")), null));
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("aXb"),
                ConstantOperator.createVarchar("a\\_b")), null));

        // --- Cases that are intentionally NOT folded (returned to BE) ---

        // Pure wildcards have no literal body → not folded
        LikePredicateOperator pureWildcard = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("anything"),
                ConstantOperator.createVarchar("%"));
        assertEquals(pureWildcard, rule.apply(pureWildcard, null));

        // Empty pattern → not folded
        LikePredicateOperator emptyPattern = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("x"),
                ConstantOperator.createVarchar(""));
        assertEquals(emptyPattern, rule.apply(emptyPattern, null));

        // REGEXP (not LIKE) type → not folded by this rule
        LikePredicateOperator regexp = new LikePredicateOperator(
                LikePredicateOperator.LikeType.REGEXP,
                ConstantOperator.createVarchar("abc"),
                ConstantOperator.createVarchar("abc"));
        assertEquals(regexp, rule.apply(regexp, null));

        // --- Other correctness checks ---

        // NULL operand → NULL
        assertEquals(OB_NULL, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createNull(VarcharType.VARCHAR),
                ConstantOperator.createVarchar("abc")), null));

        // LIKE folding is case-sensitive (binary collation): 'ABC' LIKE 'abc' → 0
        assertEquals(OB_FALSE, rule.apply(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE,
                ConstantOperator.createVarchar("ABC"),
                ConstantOperator.createVarchar("abc")), null));
    }
}