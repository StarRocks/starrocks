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

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetNamesVar;
import org.junit.Assert;
import org.junit.Test;

public class SetNamesVarTest {

    @Test
    public void testNormal() throws AnalysisException {
        SetNamesVar var = new SetNamesVar(null, null);
        var.analyze();
        Assert.assertEquals("utf8", var.getCharset().toLowerCase());
        Assert.assertEquals("DEFAULT", var.getCollate());

        var = new SetNamesVar("UTf8", null);
        var.analyze();
        Assert.assertEquals("utf8", var.getCharset().toLowerCase());
        Assert.assertEquals("DEFAULT", var.getCollate());

        var = new SetNamesVar("UTf8", "aBc");
        var.analyze();
        Assert.assertEquals("utf8", var.getCharset().toLowerCase());
        Assert.assertEquals("aBc", var.getCollate());
    }

    @Test(expected = SemanticException.class)
    public void testUnsupported()  {
        SetNamesVar var = new SetNamesVar("gbk");
        var.analyze();
        Assert.fail("No exception throws.");
    }
}