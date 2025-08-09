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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LabelNameTest {

    @BeforeEach
    public void setUp() {
    }

    @Test
    public void testNormal() throws AnalysisException {
        LabelName label = new LabelName("testDb", "testLabel");
        label.analyze(new ConnectContext());
        Assertions.assertEquals("testDb", label.getDbName());
        Assertions.assertEquals("testLabel", label.getLabelName());

        label = new LabelName("", "testLabel");
        ConnectContext context = new ConnectContext();
        context.setDatabase("testDb");
        label.analyze(context);
        Assertions.assertEquals("testDb", label.getDbName());
        Assertions.assertEquals("testLabel", label.getLabelName());
    }

    @Test
    public void testNoDb() {
        assertThrows(SemanticException.class, () -> {
            LabelName label = new LabelName("", "testLabel");
            label.analyze(new ConnectContext());
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testNoLabel() {
        assertThrows(SemanticException.class, () -> {
            LabelName label = new LabelName("", "");
            label.analyze(new ConnectContext());
            Assertions.fail("No exception throws");
        });
    }

}