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
package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.GreaterThan;
import io.delta.standalone.expressions.GreaterThanOrEqual;
import io.delta.standalone.expressions.In;
import io.delta.standalone.expressions.IsNotNull;
import io.delta.standalone.expressions.IsNull;
import io.delta.standalone.expressions.LessThan;
import io.delta.standalone.expressions.LessThanOrEqual;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.expressions.Not;
import io.delta.standalone.expressions.Or;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

public class DeltaLakeExprVisitorTest {

    private static final StructType SCHEMA = new StructType(
            new StructField[] {
                    new StructField("c1", new IntegerType(), true),
                    new StructField("c2", new FloatType(), true),
                    new StructField("c3", new DateType(), true),
                    new StructField("c4", new TimestampType(), true),
                    new StructField("c5", new BooleanType(), true),
                    new StructField("c6", new StringType(), true)
            });

    private SlotRef s1;
    private SlotRef s2;
    private SlotRef s3;
    private SlotRef s4;
    private SlotRef s5;
    private SlotRef s6;

    private void initialSlotRef() {
        SlotDescriptor s1 = new SlotDescriptor(new SlotId(1), "c1", Type.INT, true);
        s1.setColumn(new Column("c1", Type.INT));
        this.s1 = new SlotRef(s1);
        SlotDescriptor s2 = new SlotDescriptor(new SlotId(2), "c2", Type.FLOAT, true);
        s2.setColumn(new Column("c2", Type.FLOAT));
        this.s2 = new SlotRef(s2);
        SlotDescriptor s3 = new SlotDescriptor(new SlotId(3), "c3", Type.DATE, true);
        s3.setColumn(new Column("c3", Type.DATE));
        this.s3 = new SlotRef(s3);
        SlotDescriptor s4 = new SlotDescriptor(new SlotId(4), "c4", Type.DATETIME, true);
        s4.setColumn(new Column("c4", Type.DATETIME));
        this.s4 = new SlotRef(s4);
        SlotDescriptor s5 = new SlotDescriptor(new SlotId(5), "c5", Type.BOOLEAN, true);
        s5.setColumn(new Column("c5", Type.BOOLEAN));
        this.s5 = new SlotRef(s5);
        SlotDescriptor s6 = new SlotDescriptor(new SlotId(6), "c6", Type.STRING, true);
        s6.setColumn(new Column("c6", Type.STRING));
        this.s6 = new SlotRef(s6);
    }


    @Test
    public void testToDeltalakeExpression() throws AnalysisException, ParseException {
        initialSlotRef();
        ExpressionConverter converter = new ExpressionConverter(SCHEMA);
        Expression convertedExpr;
        Expression expectedExpr;
        //  isNull
        convertedExpr = converter.convert(new IsNullPredicate(s1, false));
        expectedExpr = new IsNull(new io.delta.standalone.expressions.Column("c1", new IntegerType()));
        Assert.assertEquals("Generated isNull expression should be correct",
                convertedExpr.toString(),
                expectedExpr.toString());

        //notNull
        convertedExpr = converter.convert(new IsNullPredicate(s1, true));
        expectedExpr = new IsNotNull(new io.delta.standalone.expressions.Column("c1", new IntegerType()));
        Assert.assertEquals("Generated notNull expression should be correct",
                convertedExpr.toString(),
                expectedExpr.toString());

        // equal date
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2023-01-01");
        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        LiteralExpr literalExpr = new DateLiteral(ldt, Type.DATE);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.EQ, s3, literalExpr));
        expectedExpr = new EqualTo(new io.delta.standalone.expressions.Column("c3", new DateType()),
                Literal.of(new java.sql.Date(date.getTime())));
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        //equal datetime
        literalExpr = new DateLiteral(ldt, Type.DATETIME);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.EQ, s4, literalExpr));
        expectedExpr = new EqualTo(new io.delta.standalone.expressions.Column("c4", new TimestampType()),
                Literal.of(new java.sql.Timestamp(date.getTime())));
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // notEqual
        literalExpr = new DateLiteral(ldt, Type.DATETIME);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.NE, s4, literalExpr));
        expectedExpr = new Not(new EqualTo(new io.delta.standalone.expressions.Column("c4", new TimestampType()),
                Literal.of(new java.sql.Timestamp(date.getTime()))));
        Assert.assertEquals("Generated notEqual expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // greaterThan
        literalExpr = new FloatLiteral(1.1d, Type.FLOAT);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.GT, s2, literalExpr));
        expectedExpr = new GreaterThan(new io.delta.standalone.expressions.Column("c2", new FloatType()),
                Literal.of(1.1f));
        Assert.assertEquals("Generated greaterThan expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());

        //greaterThanOrEqual
        literalExpr = new FloatLiteral(1.1d, Type.FLOAT);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.GE, s2, literalExpr));
        expectedExpr = new GreaterThanOrEqual(new io.delta.standalone.expressions.Column("c2", new FloatType()),
                Literal.of(1.1f));
        Assert.assertEquals("Generated greaterThanOrEqual expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());

        // lessThan
        literalExpr = new FloatLiteral(1.1d, Type.FLOAT);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.LT, s2, literalExpr));
        expectedExpr = new LessThan(new io.delta.standalone.expressions.Column("c2", new FloatType()),
                Literal.of(1.1f));
        Assert.assertEquals("Generated lessThan expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());

        //greaterThanOrEqual
        literalExpr = new FloatLiteral(1.1d, Type.FLOAT);
        convertedExpr = converter.convert(new BinaryPredicate(BinaryType.LE, s2, literalExpr));
        expectedExpr = new LessThanOrEqual(new io.delta.standalone.expressions.Column("c2", new FloatType()),
                Literal.of(1.1f));
        Assert.assertEquals("Generated lessThanOrEqual expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());


        //not in
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("aa"));
        inList.add(new StringLiteral("bb"));
        inList.add(new StringLiteral("cc"));
        Predicate inExpr = new InPredicate(s6, inList, true);
        convertedExpr = converter.convert(inExpr);
        List<Expression> inExpression = Lists.newArrayList();
        inExpression.add(Literal.of("aa"));
        inExpression.add(Literal.of("bb"));
        inExpression.add(Literal.of("cc"));
        expectedExpr = new Not(new In(new io.delta.standalone.expressions.Column("c6", new StringType()),
                inExpression));
        Assert.assertEquals("Generated not in expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());

        //in
        inExpr = new InPredicate(s6, inList, false);
        convertedExpr = converter.convert(inExpr);
        expectedExpr = new In(new io.delta.standalone.expressions.Column("c6", new StringType()), inExpression);
        Assert.assertEquals("Generated in expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());

        //or
        literalExpr = new IntLiteral(3);
        BinaryPredicate expr1 = new BinaryPredicate(BinaryType.GE, s1, literalExpr);
        literalExpr = new StringLiteral("aa");
        BinaryPredicate expr2 = new BinaryPredicate(BinaryType.LT, s6, literalExpr);
        convertedExpr = converter.convert(new CompoundPredicate(CompoundPredicate.Operator.OR, expr1, expr2));
        expectedExpr = new Or(
                new GreaterThanOrEqual(
                        new io.delta.standalone.expressions.Column("c1", new IntegerType()), Literal.of(3)),
                new LessThan(
                        new io.delta.standalone.expressions.Column("c6", new StringType()), Literal.of("aa"))
        );

        Assert.assertEquals("Generated or expression should be correct",
                convertedExpr.toString(), expectedExpr.toString());
    }

    @Test
    public void testToDateToStringExpression() throws AnalysisException, ParseException {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), "dt", Type.DATE, true);
        slotDescriptor.setColumn(new Column("dt", Type.DATE));
        SlotRef slotRef = new SlotRef(slotDescriptor);
        // create view c3 = "2023-01-01"(date)
        // delta lake type is string : "2023-01-01"

        StructType structType = new StructType(new StructField[] {
                new StructField("dt", new StringType(), true),
                new StructField("c1", new FloatType(), true)
        });
        ExpressionConverter converter = new ExpressionConverter(structType);
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2023-01-01");
        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        LiteralExpr literalExpr = new DateLiteral(ldt, Type.DATE);
        Expression convertedExpr = converter.convert(new BinaryPredicate(BinaryType.EQ,
                slotRef, literalExpr));
        Expression expectedExpr = new EqualTo(new io.delta.standalone.expressions.Column("dt", new StringType()),
                Literal.of("2023-01-01"));
        Assert.assertEquals("Generated date type convert string type expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());
    }

}
