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

package com.starrocks.connector.kudu;

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static com.starrocks.catalog.KuduTableTest.genColumnSchema;

public class KuduPredicateConverterTest {

    private static final Schema SCHEMA = new Schema(Arrays.asList(
            genColumnSchema("f0", org.apache.kudu.Type.INT32),
            genColumnSchema("f1", org.apache.kudu.Type.STRING),
            genColumnSchema("f2", org.apache.kudu.Type.INT32),
            genColumnSchema("f3", org.apache.kudu.Type.DATE),
            genColumnSchema("f4", org.apache.kudu.Type.UNIXTIME_MICROS)
    ));

    private static final ColumnRefOperator F0 = new ColumnRefOperator(0, Type.INT, "f0", true, false);
    private static final ColumnRefOperator F1 = new ColumnRefOperator(0, Type.VARCHAR, "f1", true, false);
    private static final ColumnRefOperator F3 = new ColumnRefOperator(0, Type.DATE, "f3", true, false);
    private static final ColumnRefOperator F4 = new ColumnRefOperator(0, Type.DATETIME, "f4", true, false);
    private static final CastOperator F0_CAST = new CastOperator(
            Type.VARCHAR, new ColumnRefOperator(0, Type.INT, "f0", true, false));

    private static final KuduPredicateConverter CONVERTER = new KuduPredicateConverter(SCHEMA);

    @Test
    public void testNull() {
        List<KuduPredicate> result = CONVERTER.convert(null);
        Assert.assertNull(result);
    }

    @Test
    public void testIsNull() {
        ScalarOperator op = new IsNullPredicateOperator(false, F0);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` NONE");
    }

    @Test
    public void testEq() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` = 5");
    }

    @Test
    public void testEqCast() {
        ConstantOperator value = ConstantOperator.createVarchar("5");
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F0_CAST, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` = 5");
    }

    @Test
    public void testEqDate() {
        ConstantOperator value = ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0));
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F3, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f3` = 2024-01-01");
    }

    @Test
    public void testEqDateTime() {
        ConstantOperator value = ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0));
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.EQ, F4, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f4` = 2024-01-01T00:00:00.000000Z");
    }

    @Test
    public void testLt() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LT, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` < 5");
    }

    @Test
    public void testLe() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.LE, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` < 6");
    }

    @Test
    public void testGt() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GT, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` >= 6");
    }

    @Test
    public void testGe() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new BinaryPredicateOperator(BinaryType.GE, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` >= 5");
    }

    @Test
    public void testIn() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ConstantOperator value1 = ConstantOperator.createInt(6);
        ConstantOperator value2 = ConstantOperator.createInt(7);
        ScalarOperator op = new InPredicateOperator(false, F0, value, value1, value2);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` IN (5, 6, 7)");
    }

    @Test
    public void testLike() {
        ConstantOperator value = ConstantOperator.createInt(5);
        ScalarOperator op = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, F0, value);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void testNot() {
        ConstantOperator intOp = ConstantOperator.createInt(5);
        ConstantOperator varcharOp = ConstantOperator.createVarchar("abc");
        ScalarOperator ge1 = new BinaryPredicateOperator(BinaryType.GT, F0, intOp);
        ScalarOperator ge2 = new BinaryPredicateOperator(BinaryType.GT, F1, varcharOp);
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, ge1, ge2);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void testAnd() {
        ConstantOperator intOp = ConstantOperator.createInt(5);
        ConstantOperator varcharOp = ConstantOperator.createVarchar("abc");
        ScalarOperator gt = new BinaryPredicateOperator(BinaryType.GT, F0, intOp);
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, F1, varcharOp);
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, gt, eq);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.get(0).toString(), "`f0` >= 6");
        Assert.assertEquals(result.get(1).toString(), "`f1` = \"abc\"");
    }

    @Test
    public void testOr() {
        ConstantOperator intOp = ConstantOperator.createInt(5);
        ConstantOperator varcharOp = ConstantOperator.createVarchar("abc");
        ScalarOperator gt = new BinaryPredicateOperator(BinaryType.GT, F0, intOp);
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, F1, varcharOp);
        ScalarOperator op = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, gt, eq);
        List<KuduPredicate> result = CONVERTER.convert(op);
        Assert.assertEquals(result.size(), 0);
    }
}
