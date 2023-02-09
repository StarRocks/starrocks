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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SetStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserVariable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class SetStmtTest {

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException {
        List<SetListItem> vars = Lists.newArrayList(new UserVariable("times", new IntLiteral(100L)),
                new SetNamesVar("utf8"));
        SetStmt stmt = new SetStmt(vars);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("times", ((UserVariable) stmt.getSetListItems().get(0)).getVariable());
        Assert.assertEquals("100", ((UserVariable) stmt.getSetListItems().get(0)).getEvaluatedExpression().getStringValue());
        Assert.assertTrue(stmt.getSetListItems().get(1) instanceof SetNamesVar);
        Assert.assertEquals("utf8", ((SetNamesVar) stmt.getSetListItems().get(1)).getCharset());
    }

    @Test(expected = SemanticException.class)
    public void testNoVariable() {
        SystemVariable var = new SystemVariable(SetType.SESSION, "", new StringLiteral("utf-8"));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
        Assert.fail("No exception throws.");
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testNonConstantExpr() throws UserException {
        SlotDescriptor descriptor = new SlotDescriptor(new SlotId(1), "x",
                Type.INT, false);
        Expr lhsExpr = new SlotRef(descriptor);
        Expr rhsExpr = new IntLiteral(100L);
        ArithmeticExpr addExpr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, lhsExpr, rhsExpr);
        SystemVariable var = new SystemVariable(SetType.SESSION, SessionVariable.SQL_SELECT_LIMIT, addExpr);
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Set statement only support constant expr.");
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
    }

    @Test
    public void setResourceGroup() throws UserException {
        SystemVariable setEmpty = new SystemVariable(SetType.SESSION, SessionVariable.RESOURCE_GROUP, new StringLiteral(""));
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setEmpty)), ctx);

        SystemVariable setVar = new SystemVariable(SetType.SESSION, SessionVariable.RESOURCE_GROUP, new StringLiteral("not_exists"));
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            Assert.fail("should fail");
        } catch (SemanticException e) {
            Assert.assertEquals("resource group not exists: not_exists", e.getMessage());
        }
    }

    @Test
    public void testSetNonNegativeLongVariable() throws UserException {
        List<String> fields = Lists.newArrayList(
                SessionVariable.LOAD_MEM_LIMIT,
                SessionVariable.QUERY_MEM_LIMIT,
                SessionVariable.SQL_SELECT_LIMIT);

        for (String field : fields) {
            Assert.assertThrows("is not a number", SemanticException.class, () -> {
                SystemVariable setVar = new SystemVariable(SetType.SESSION, field, new StringLiteral("non_number"));
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            });

            Assert.assertThrows("must be equal or greater than 0", SemanticException.class, () -> {
                SystemVariable setVar = new SystemVariable(SetType.SESSION, field, new StringLiteral("-1"));
                SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setVar)), ctx);
            });

            SystemVariable var = new SystemVariable(SetType.SESSION, field, new StringLiteral("0"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assert.assertEquals(field, var.getVariable());
            Assert.assertEquals("0", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new StringLiteral("10"));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assert.assertEquals(field, var.getVariable());
            Assert.assertEquals("10", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new IntLiteral(0));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assert.assertEquals(field, var.getVariable());
            Assert.assertEquals("0", var.getResolvedExpression().getStringValue());

            var = new SystemVariable(SetType.SESSION, field, new IntLiteral(10));
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), ctx);
            Assert.assertEquals(field, var.getVariable());
            Assert.assertEquals("10", var.getResolvedExpression().getStringValue());
        }
    }

}
