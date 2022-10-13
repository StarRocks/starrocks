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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LabelNameTest {
    @Mocked
    private Analyzer analyzer;

    @Before
    public void setUp() {
    }

    @Test
    public void testNormal() throws AnalysisException {
        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";
            }
        };

        LabelName label = new LabelName("testDb", "testLabel");
        label.analyze(new ConnectContext());
        Assert.assertEquals("testDb", label.getDbName());
        Assert.assertEquals("testLabel", label.getLabelName());

        label = new LabelName("", "testLabel");
        ConnectContext context = new ConnectContext();
        context.setDatabase("testDb");
        label.analyze(context);
        Assert.assertEquals("testDb", label.getDbName());
        Assert.assertEquals("testLabel", label.getLabelName());
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws SemanticException {
        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = null;
            }
        };

        LabelName label = new LabelName("", "testLabel");
        label.analyze(new ConnectContext());
        Assert.fail("No exception throws");
    }

    @Test(expected = SemanticException.class)
    public void testNoLabel() throws SemanticException {
        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";
            }
        };

        LabelName label = new LabelName("", "");
        label.analyze(new ConnectContext());
        Assert.fail("No exception throws");
    }

}