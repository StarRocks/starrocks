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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnSeparatorTest {
    @Test
    public void testNormal() throws AnalysisException {
        // \t
        ColumnSeparator separator = new ColumnSeparator("\t");
        Assertions.assertEquals("'\t'", separator.toSql());
        Assertions.assertEquals("\t", separator.getColumnSeparator());
        Assertions.assertEquals("\t", separator.getOriSeparator());

        // \\t
        separator = new ColumnSeparator("\\t");
        Assertions.assertEquals("'\\t'", separator.toSql());
        Assertions.assertEquals("\t", separator.getColumnSeparator());
        Assertions.assertEquals("\\t", separator.getOriSeparator());

        // \\t\\t
        separator = new ColumnSeparator("\\t\\t");
        Assertions.assertEquals("'\\t\\t'", separator.toSql());
        Assertions.assertEquals("\t\t", separator.getColumnSeparator());
        Assertions.assertEquals("\\t\\t", separator.getOriSeparator());

        // a\\ta\\t
        separator = new ColumnSeparator("a\\ta\\t");
        Assertions.assertEquals("'a\\ta\\t'", separator.toSql());
        Assertions.assertEquals("a\ta\t", separator.getColumnSeparator());
        Assertions.assertEquals("a\\ta\\t", separator.getOriSeparator());

        // \\t\\n
        separator = new ColumnSeparator("\\t\\n");
        Assertions.assertEquals("'\\t\\n'", separator.toSql());
        Assertions.assertEquals("\t\n", separator.getColumnSeparator());
        Assertions.assertEquals("\\t\\n", separator.getOriSeparator());

        // \x01
        separator = new ColumnSeparator("\\x01");
        Assertions.assertEquals("'\\x01'", separator.toSql());
        Assertions.assertEquals("\1", separator.getColumnSeparator());
        Assertions.assertEquals("\\x01", separator.getOriSeparator());

        // \x00 \x01
        separator = new ColumnSeparator("\\x0001");
        Assertions.assertEquals("'\\x0001'", separator.toSql());
        Assertions.assertEquals("\0\1", separator.getColumnSeparator());
        Assertions.assertEquals("\\x0001", separator.getOriSeparator());

        separator = new ColumnSeparator("|");
        Assertions.assertEquals("'|'", separator.toSql());
        Assertions.assertEquals("|", separator.getColumnSeparator());
        Assertions.assertEquals("|", separator.getOriSeparator());

        separator = new ColumnSeparator("\\|");
        Assertions.assertEquals("'\\|'", separator.toSql());
        Assertions.assertEquals("\\|", separator.getColumnSeparator());
        Assertions.assertEquals("\\|", separator.getOriSeparator());
    }

    @Test
    public void testHexFormatError() {
        assertThrows(SemanticException.class, () -> {
            ColumnSeparator separator = new ColumnSeparator("\\x0g");
        });
    }

    @Test
    public void testHexLengthError() {
        assertThrows(SemanticException.class, () -> {
            ColumnSeparator separator = new ColumnSeparator("\\x011");
        });
    }
}