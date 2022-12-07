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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class ExpressionGsonSerializationTest {

    public static class ExpressionList implements Writable {
        @SerializedName(value = "expressions")
        public List<Expr> expressions = Lists.newArrayList();

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static ExpressionList read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, ExpressionList.class);
        }
    }

    @Test
    public void testSerializeSlotRef() {
        TableName tableName = new TableName("test", "tbl1");
        IdGenerator<SlotId> slotIdIdGenerator = SlotId.createGenerator();
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotDescriptor slotDescriptor = new SlotDescriptor(slotIdIdGenerator.getNextId(), null);
        slotDescriptor.setColumn(k1);
        SlotRef slotRef = new SlotRef(tableName, "k1");
        slotRef.setDesc(slotDescriptor);
        // because expr serialized with function tosql
        String sql = slotRef.toSql();
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        Assert.assertTrue(expr instanceof SlotRef);
        // serialize
        String json = GsonUtils.GSON.toJson(slotRef);
        // deserialize
        Expr readExpr = GsonUtils.GSON.fromJson(json, SlotRef.class);
        Assert.assertEquals(expr, readExpr);
    }

    @Test
    public void testSerializeFunctionCallExpr() throws IOException {

        TableName tableName = new TableName("test", "tbl1");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotDescriptor slotDescriptor2 = new SlotDescriptor(SlotId.createGenerator().getNextId(), null);
        slotDescriptor2.setColumn(k2);
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        slotRef2.setDesc(slotDescriptor2);
        List<Expr> fnChildren = Lists.newArrayList();
        fnChildren.add(new StringLiteral("month"));
        fnChildren.add(slotRef2);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", fnChildren);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                "date_trunc", new Type[] {Type.VARCHAR, Type.DATETIME}, Function.CompareMode.IS_IDENTICAL));
        // because expr serialized with function tosql
        String sql = functionCallExpr.toSql();
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        Assert.assertTrue(expr instanceof FunctionCallExpr);
        // serialize
        String json = GsonUtils.GSON.toJson(functionCallExpr);
        // deserialize
        Expr readExpr = GsonUtils.GSON.fromJson(json, FunctionCallExpr.class);
        Assert.assertEquals(expr, readExpr);
    }

    @Test
    public void testSerializeColumnList() {
        TableName tableName = new TableName("test", "tbl1");
        IdGenerator<SlotId> slotIdIdGenerator = SlotId.createGenerator();
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotDescriptor slotDescriptor = new SlotDescriptor(slotIdIdGenerator.getNextId(), null);
        slotDescriptor.setColumn(k1);
        SlotRef slotRef = new SlotRef(tableName, "k1");
        slotRef.setDesc(slotDescriptor);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotDescriptor slotDescriptor2 = new SlotDescriptor(slotIdIdGenerator.getNextId(), null);
        slotDescriptor2.setColumn(k2);
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        slotRef2.setDesc(slotDescriptor2);
        List<Expr> fnChildren = Lists.newArrayList();
        fnChildren.add(new StringLiteral("month"));
        fnChildren.add(slotRef2);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", fnChildren);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                "date_trunc", new Type[] {Type.VARCHAR, Type.DATETIME}, Function.CompareMode.IS_IDENTICAL));
        ExpressionList expressionList = new ExpressionList();
        expressionList.expressions.add(slotRef);
        expressionList.expressions.add(functionCallExpr);
        // format expressions
        List<Expr> formattedExpressions = Lists.newArrayList();
        for (Expr expression : expressionList.expressions) {
            formattedExpressions.add(SqlParser.parseSqlToExpr(expression.toSql(), SqlModeHelper.MODE_DEFAULT));
        }
        // serialize
        String json = GsonUtils.GSON.toJson(expressionList);
        // deserialize
        ExpressionList readExpressionList = GsonUtils.GSON.fromJson(json, ExpressionList.class);
        Assert.assertEquals(2, readExpressionList.expressions.size());
        Assert.assertEquals(formattedExpressions.get(0), readExpressionList.expressions.get(0));
        Assert.assertEquals(formattedExpressions.get(1), readExpressionList.expressions.get(1));
    }

}
