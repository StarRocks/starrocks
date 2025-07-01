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
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetUserPropertyVar;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetUserPropertyVarTest {
    @Test
    public void testNormal() throws StarRocksException {
        SetUserPropertyVar var = new SetUserPropertyVar("quota.normal", "1000");
        Assertions.assertEquals("quota.normal", var.getPropertyKey());
        Assertions.assertEquals("1000", var.getPropertyValue());
    }

    @Test
    public void testUnknownProperty() {
        assertThrows(SemanticException.class, () -> {
            SetUserPropertyVar var = new SetUserPropertyVar("unknown_property", "1000");
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), null);
            Assertions.fail("No exception throws.");
        });
    }
}