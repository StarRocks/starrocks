// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/RowDelimiterTest.java

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
import org.junit.Assert;
import org.junit.Test;

public class RowDelimiterTest {
    @Test
    public void testNormal() throws AnalysisException {
        // \n
        RowDelimiter delimiter = new RowDelimiter("\n");
        delimiter.analyze();
        Assert.assertEquals("'\n'", delimiter.toSql());
        Assert.assertEquals("\n", delimiter.getRowDelimiter());

        // \x01
        delimiter = new RowDelimiter("\\x01");
        delimiter.analyze();
        Assert.assertEquals("'\\x01'", delimiter.toSql());
        Assert.assertEquals("\1", delimiter.getRowDelimiter());

        // \x00 \x01
        delimiter = new RowDelimiter("\\x0001");
        delimiter.analyze();
        Assert.assertEquals("'\\x0001'", delimiter.toSql());
        Assert.assertEquals("\0\1", delimiter.getRowDelimiter());

        delimiter = new RowDelimiter("|");
        delimiter.analyze();
        Assert.assertEquals("'|'", delimiter.toSql());
        Assert.assertEquals("|", delimiter.getRowDelimiter());

        delimiter = new RowDelimiter("\\|");
        delimiter.analyze();
        Assert.assertEquals("'\\|'", delimiter.toSql());
        Assert.assertEquals("\\|", delimiter.getRowDelimiter());
    }

    @Test(expected = AnalysisException.class)
    public void testHexFormatError() throws AnalysisException {
        RowDelimiter delimiter = new RowDelimiter("\\x0g");
        delimiter.analyze();
    }

    @Test(expected = AnalysisException.class)
    public void testHexLengthError() throws AnalysisException {
        RowDelimiter delimiter = new RowDelimiter("\\x011");
        delimiter.analyze();
    }
}
