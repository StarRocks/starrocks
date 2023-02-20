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


package com.starrocks.external.elasticsearch;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.IdGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QueryConverterTest {

    static IdGenerator<SlotId> idGenerator = SlotId.createGenerator();

    QueryConverter queryConverter = new QueryConverter();

    @Test
    public void testTranslateIsNullPredicate() {
        SlotRef kSlotRef = mockSlotRef("k", Type.BOOLEAN);
        IsNullPredicate isNullPredicate = new IsNullPredicate(kSlotRef, false);
        IsNullPredicate isNotNullPredicate = new IsNullPredicate(kSlotRef, true);
        Assert.assertEquals("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"k\"}}}}",
                queryConverter.convert(isNullPredicate).toString());
        Assert.assertEquals("{\"exists\":{\"field\":\"k\"}}",
                queryConverter.convert(isNotNullPredicate).toString());
    }

    @Test
    public void testTranslateInPredicate() {
        SlotRef codeSlotRef = mockSlotRef("code", Type.INT);
        List<Expr> codeLiterals = new ArrayList<>();
        IntLiteral codeLiteral1 = new IntLiteral(1);
        IntLiteral codeLiteral2 = new IntLiteral(2);
        IntLiteral codeLiteral3 = new IntLiteral(3);
        codeLiterals.add(codeLiteral1);
        codeLiterals.add(codeLiteral2);
        codeLiterals.add(codeLiteral3);
        InPredicate inPredicate = new InPredicate(codeSlotRef, codeLiterals, false);
        InPredicate notInPredicate = new InPredicate(codeSlotRef, codeLiterals, true);
        Assert.assertEquals("{\"terms\":{\"code\":[1,2,3]}}", queryConverter.convert(inPredicate).toString());
        Assert.assertEquals("{\"bool\":{\"must_not\":{\"terms\":{\"code\":[1,2,3]}}}}",
                queryConverter.convert(notInPredicate).toString());
    }

    @Test
    public void testTranslateRawQuery() {
        SlotRef serviceSlotRef = mockSlotRef("service", Type.STRING);
        // normal test
        String normalValue = "{\"term\":{\"service\":{\"value\":\"starrocks\"}}}";
        StringLiteral normalValueLiteral = new StringLiteral(normalValue);
        List<Expr> params = new ArrayList<>();
        params.add(serviceSlotRef);
        params.add(normalValueLiteral);
        FunctionCallExpr normalESQueryExpr = new FunctionCallExpr("esquery", params);
        Assert.assertEquals(normalValue, queryConverter.convert(normalESQueryExpr).toString());
        // illegal test
        String illegalValue = "{\"term\":{\"service\":{\"value\":\"starrocks\"}},\"k\":3}";
        StringLiteral illegalValueLiteral = new StringLiteral(illegalValue);
        List<Expr> illegalParams = new ArrayList<>();
        illegalParams.add(serviceSlotRef);
        illegalParams.add(illegalValueLiteral);
        FunctionCallExpr illegalESQueryExpr = new FunctionCallExpr("esquery", illegalParams);
        ExceptionChecker.expectThrows(StarRocksESException.class, () -> queryConverter.convert(illegalESQueryExpr));
    }

    @Test
    public void testTranslateLikePredicate() {
        SlotRef name = mockSlotRef("name", Type.STRING);
        StringLiteral nameLiteral1 = new StringLiteral("%1%");
        StringLiteral nameLiteral2 = new StringLiteral("*1*");
        StringLiteral nameLiteral3 = new StringLiteral("1_2");
        LikePredicate likePredicate1 = new LikePredicate(LikePredicate.Operator.LIKE, name, nameLiteral1);
        LikePredicate regexPredicate = new LikePredicate(LikePredicate.Operator.REGEXP, name, nameLiteral2);
        LikePredicate likePredicate2 = new LikePredicate(LikePredicate.Operator.LIKE, name, nameLiteral3);

        Assert.assertEquals("{\"wildcard\":{\"name\":\"*1*\"}}", queryConverter.convert(likePredicate1).toString());
        Assert.assertEquals("{\"wildcard\":{\"name\":\"*1*\"}}", queryConverter.convert(regexPredicate).toString());
        Assert.assertEquals("{\"wildcard\":{\"name\":\"1?2\"}}", queryConverter.convert(likePredicate2).toString());
    }

    @Test
    public void testTranslateRangePredicate() {
        SlotRef valueSlotRef = mockSlotRef("value", Type.INT);
        IntLiteral intLiteral = new IntLiteral(1000);
        Expr leExpr = new BinaryPredicate(BinaryPredicate.Operator.LE, valueSlotRef, intLiteral);
        Expr ltExpr = new BinaryPredicate(BinaryPredicate.Operator.LT, valueSlotRef, intLiteral);
        Expr geExpr = new BinaryPredicate(BinaryPredicate.Operator.GE, valueSlotRef, intLiteral);
        Expr gtExpr = new BinaryPredicate(BinaryPredicate.Operator.GT, valueSlotRef, intLiteral);

        Expr eqExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, valueSlotRef, intLiteral);
        Expr neExpr = new BinaryPredicate(BinaryPredicate.Operator.NE, valueSlotRef, intLiteral);
        Assert.assertEquals("{\"range\":{\"value\":{\"lt\":1000}}}",
                queryConverter.convert(ltExpr).toString());
        Assert.assertEquals("{\"range\":{\"value\":{\"lte\":1000}}}",
                queryConverter.convert(leExpr).toString());
        Assert.assertEquals("{\"range\":{\"value\":{\"gt\":1000}}}",
                queryConverter.convert(gtExpr).toString());
        Assert.assertEquals("{\"range\":{\"value\":{\"gte\":1000}}}",
                queryConverter.convert(geExpr).toString());
        Assert.assertEquals("{\"term\":{\"value\":1000}}", queryConverter.convert(eqExpr).toString());
        Assert.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"value\":1000}}}}",
                queryConverter.convert(neExpr).toString());
    }

    @Test
    public void testTranslateCompoundPredicate() {
        SlotRef col1SlotRef = mockSlotRef("col1", Type.INT);
        IntLiteral intLiteral1 = new IntLiteral(100);
        SlotRef col2SlotRef = mockSlotRef("col2", Type.INT);

        IntLiteral intLiteral2 = new IntLiteral(200);
        BinaryPredicate bp1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, col1SlotRef, intLiteral1);
        BinaryPredicate bp2 = new BinaryPredicate(BinaryPredicate.Operator.GT, col2SlotRef, intLiteral2);
        CompoundPredicate andPredicate =
                new CompoundPredicate(CompoundPredicate.Operator.AND, bp1, bp2);
        Assert.assertEquals("{\"bool\":{\"must\":[{\"term\":{\"col1\":100}},{\"range\":{\"col2\":{\"gt\":200}}}]}}",
                queryConverter.convert(andPredicate).toString());

        CompoundPredicate orPredicate =
                new CompoundPredicate(CompoundPredicate.Operator.OR, bp1, bp2);
        Assert.assertEquals("{\"bool\":{\"should\":[{\"term\":{\"col1\":100}},{\"range\":{\"col2\":{\"gt\":200}}}]}}",
                queryConverter.convert(orPredicate).toString());

        CompoundPredicate notPredicate = new CompoundPredicate(CompoundPredicate.Operator.NOT, bp2, null);
        Assert.assertEquals("{\"bool\":{\"must_not\":{\"range\":{\"col2\":{\"gt\":200}}}}}",
                queryConverter.convert(notPredicate).toString());
    }

    SlotRef mockSlotRef(String colName, Type type) {
        SlotDescriptor slotDesc = new SlotDescriptor(idGenerator.getNextId(), "", type, true);
        slotDesc.setColumn(new Column(colName, type));
        SlotRef slotRef = new SlotRef(randomLabel(), slotDesc);
        return slotRef;
    }

    String randomLabel() {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            int number = random.nextInt(str.length());
            stringBuffer.append(str.charAt(number));
        }
        return stringBuffer.toString();

    }
}
