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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class VariableExprTest {

    @Test
    public void testNormal() throws Exception {
        VariableExpr desc = new VariableExpr("version_comment");
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(desc, UtFrameUtils.createDefaultCtx());
        Assertions.assertEquals(SetType.SESSION, desc.getSetType());
        Assertions.assertEquals("version_comment", desc.getName());
        Assertions.assertEquals(ScalarType.createType(PrimitiveType.VARCHAR), desc.getType());
        Assertions.assertEquals(SetType.SESSION, desc.getSetType());
    }

    @Test
    public void testNoVar() {
        assertThrows(SemanticException.class, () -> {
            VariableExpr desc = new VariableExpr("zcPrivate");
            ExpressionAnalyzer.analyzeExpressionIgnoreSlot(desc, UtFrameUtils.createDefaultCtx());
            Assertions.fail("No exception throws.");
        });
    }

}