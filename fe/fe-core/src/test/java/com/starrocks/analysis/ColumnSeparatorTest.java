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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ColumnSeparatorTest.java

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
import com.starrocks.sql.ast.ColumnSeparator;
import org.junit.Assert;
import org.junit.Test;

public class ColumnSeparatorTest {
    @Test
    public void testNormal() throws AnalysisException {
        // \t
        ColumnSeparator separator = new ColumnSeparator("\t");
        Assert.assertEquals("'\t'", separator.toSql());
        Assert.assertEquals("\t", separator.getColumnSeparator());

        // \x01
        separator = new ColumnSeparator("\\x01");
        Assert.assertEquals("'\\x01'", separator.toSql());
        Assert.assertEquals("\1", separator.getColumnSeparator());

        // \x00 \x01
        separator = new ColumnSeparator("\\x0001");
        Assert.assertEquals("'\\x0001'", separator.toSql());
        Assert.assertEquals("\0\1", separator.getColumnSeparator());

        separator = new ColumnSeparator("|");
        Assert.assertEquals("'|'", separator.toSql());
        Assert.assertEquals("|", separator.getColumnSeparator());

        separator = new ColumnSeparator("\\|");
        Assert.assertEquals("'\\|'", separator.toSql());
        Assert.assertEquals("\\|", separator.getColumnSeparator());
    }

    @Test(expected = SemanticException.class)
    public void testHexFormatError() throws SemanticException {
        ColumnSeparator separator = new ColumnSeparator("\\x0g");
    }

    @Test(expected = SemanticException.class)
    public void testHexLengthError() throws SemanticException {
        ColumnSeparator separator = new ColumnSeparator("\\x011");
    }
}