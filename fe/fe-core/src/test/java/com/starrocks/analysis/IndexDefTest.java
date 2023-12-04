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
import com.starrocks.analysis.IndexDef.IndexType;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

public class IndexDefTest {
    private IndexDef def;

    @Before
    public void setUp() throws Exception {
        def = new IndexDef("index1", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "balabala");
    }

    @Test
    public void testAnalyzeNormal() {
        def.analyze();
    }

    @Test
    public void testAnalyzeExpection() {
        try {
            def = new IndexDef(
                    "index1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxx"
                            + "xxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxinde"
                            + "x1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxx"
                            + "xxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxx"
                            + "xxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxx",
                    Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP,
                    "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (SemanticException e) {
            Assert.assertTrue(e instanceof SemanticException);
        }
        try {
            def = new IndexDef("", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (SemanticException e) {
            Assert.assertTrue(e instanceof SemanticException);
        }

        def = new IndexDef("", null, IndexType.GIN, "");
        Assertions.assertThrows(SemanticException.class, () -> def.analyze(), "Index can not accept null column.");

        def = new IndexDef("", Lists.newArrayList("k1", "k2"), IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> def.analyze(), "bitmap index can only apply to a single column.");

        def = new IndexDef("", Lists.newArrayList("k1", "k2"), IndexType.GIN, "");
        Assertions.assertThrows(SemanticException.class,
                () -> def.analyze(), "bitmap index can only apply to a single column.");


    }

    @Test
    public void toSql() {
        Assert.assertEquals("INDEX index1 (`col1`) USING BITMAP COMMENT 'balabala'", def.toSql());
        Assert.assertEquals("INDEX index1 ON table1 (`col1`) USING BITMAP COMMENT 'balabala'",
                def.toSql("table1"));

        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "k1");
        def = new IndexDef("index1", Lists.newArrayList("k1", "k2"), IndexType.GIN, "", properties);
        Assert.assertEquals("INDEX index1 ON table1 (`k1`,`k2`) USING GIN (\"k1\"=\"k1\") COMMENT ''",
                def.toSql("table1"));
    }
}
