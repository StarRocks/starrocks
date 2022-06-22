// This file is made available under Elastic License 2.0.
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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class SetStmtTest {
    private Analyzer analyzer;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException {
        List<SetVar> vars = Lists.newArrayList(new SetVar("times", new IntLiteral(100L)),
                new SetVar(SetType.GLOBAL, "names", new StringLiteral("utf-8")));
        SetStmt stmt = new SetStmt(vars);

        stmt.analyze(analyzer);

        Assert.assertEquals("SET DEFAULT times = 100, GLOBAL names = 'utf-8'", stmt.toString());
        Assert.assertEquals(vars, stmt.getSetVars());
    }

    @Test
    public void testNormal2() throws UserException {
        SetVar var = new SetVar(SetType.DEFAULT, "names", new StringLiteral("utf-8"));
        var.analyze(analyzer);

        Assert.assertEquals(SetType.DEFAULT, var.getType());
        var.setType(SetType.GLOBAL);
        Assert.assertEquals(SetType.GLOBAL, var.getType());
        Assert.assertEquals("names", var.getVariable());
        Assert.assertEquals("utf-8", var.getValue().getStringValue());

        Assert.assertEquals("GLOBAL names = 'utf-8'", var.toString());

        var = new SetVar("times", new IntLiteral(100L));
        var.analyze(analyzer);
        Assert.assertEquals("DEFAULT times = 100", var.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoVar() throws UserException {
        SetStmt stmt = new SetStmt(Lists.<SetVar>newArrayList());

        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNullVar() throws UserException {
        SetStmt stmt = new SetStmt(null);

        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoVariable() throws UserException {
        SetVar var = new SetVar(SetType.DEFAULT, "", new StringLiteral("utf-8"));
        var.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testNonConstantExpr() throws UserException {
        Expr lhsExpr = new SlotRef(new TableName("xx", "xx"), "xx");
        Expr rhsExpr = new IntLiteral(100L);
        ArithmeticExpr addExpr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, lhsExpr, rhsExpr);
        SetVar var = new SetVar(SetType.DEFAULT, SessionVariable.SQL_SELECT_LIMIT, addExpr);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Set statement only support constant expr.");
        var.analyze(analyzer);
    }

    @Test
    public void setResourceGroup() throws UserException {
        SetVar setEmpty = new SetVar(SetType.DEFAULT, SessionVariable.RESOURCE_GROUP, new StringLiteral(""));
        setEmpty.analyze(analyzer);

        SetVar setVar = new SetVar(SetType.DEFAULT, SessionVariable.RESOURCE_GROUP, new StringLiteral("not_exists"));
        try {
            setVar.analyze(analyzer);
            Assert.fail("should fail");
        } catch (UserException e) {
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
            Assert.assertThrows("is not a number", AnalysisException.class, () -> {
                SetVar setVar = new SetVar(SetType.DEFAULT, field, new StringLiteral("non_number"));
                setVar.analyze(analyzer);
            });

            Assert.assertThrows("must be equal or greater than 0", AnalysisException.class, () -> {
                SetVar setVar = new SetVar(SetType.DEFAULT, field, new StringLiteral("-1"));
                setVar.analyze(analyzer);
            });

            SetVar var = new SetVar(SetType.DEFAULT, field, new StringLiteral("0"));
            var.analyze(analyzer);
            Assert.assertEquals(String.format("DEFAULT %s = '0'", field), var.toString());

            var = new SetVar(SetType.DEFAULT, field, new StringLiteral("10"));
            var.analyze(analyzer);
            Assert.assertEquals(String.format("DEFAULT %s = '10'", field), var.toString());

            var = new SetVar(SetType.DEFAULT, field, new IntLiteral(0));
            var.analyze(analyzer);
            Assert.assertEquals(String.format("DEFAULT %s = 0", field), var.toString());

            var = new SetVar(SetType.DEFAULT, field, new IntLiteral(10));
            var.analyze(analyzer);
            Assert.assertEquals(String.format("DEFAULT %s = 10", field), var.toString());
        }
    }

}
