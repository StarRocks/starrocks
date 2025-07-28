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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.RowDelimiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RowDelimiterTest {
    @Test
    public void testNormal() throws AnalysisException {
        // \n
        RowDelimiter delimiter = new RowDelimiter("\n");
        Assertions.assertEquals("'\n'", delimiter.toSql());
        Assertions.assertEquals("\n", delimiter.getRowDelimiter());
        Assertions.assertEquals("\n", delimiter.getOriDelimiter());

        // \\n
        delimiter = new RowDelimiter("\\n");
        Assertions.assertEquals("'\\n'", delimiter.toSql());
        Assertions.assertEquals("\n", delimiter.getRowDelimiter());
        Assertions.assertEquals("\\n", delimiter.getOriDelimiter());

        // \\n\\n
        delimiter = new RowDelimiter("\\n\\n");
        Assertions.assertEquals("'\\n\\n'", delimiter.toSql());
        Assertions.assertEquals("\n\n", delimiter.getRowDelimiter());
        Assertions.assertEquals("\\n\\n", delimiter.getOriDelimiter());

        // a\\na\\n
        delimiter = new RowDelimiter("a\\na\\n");
        Assertions.assertEquals("'a\\na\\n'", delimiter.toSql());
        Assertions.assertEquals("a\na\n", delimiter.getRowDelimiter());
        Assertions.assertEquals("a\\na\\n", delimiter.getOriDelimiter());

        // \x01
        delimiter = new RowDelimiter("\\x01");
        Assertions.assertEquals("'\\x01'", delimiter.toSql());
        Assertions.assertEquals("\1", delimiter.getRowDelimiter());
        Assertions.assertEquals("\\x01", delimiter.getOriDelimiter());

        // \x00 \x01
        delimiter = new RowDelimiter("\\x0001");
        Assertions.assertEquals("'\\x0001'", delimiter.toSql());
        Assertions.assertEquals("\0\1", delimiter.getRowDelimiter());
        Assertions.assertEquals("\\x0001", delimiter.getOriDelimiter());

        delimiter = new RowDelimiter("|");
        Assertions.assertEquals("'|'", delimiter.toSql());
        Assertions.assertEquals("|", delimiter.getRowDelimiter());
        Assertions.assertEquals("|", delimiter.getOriDelimiter());

        delimiter = new RowDelimiter("\\|");
        Assertions.assertEquals("'\\|'", delimiter.toSql());
        Assertions.assertEquals("\\|", delimiter.getRowDelimiter());
        Assertions.assertEquals("\\|", delimiter.getOriDelimiter());
    }

    @Test
    public void testHexFormatError() {
        assertThrows(SemanticException.class, () -> {
            RowDelimiter delimiter = new RowDelimiter("\\x0g");
        });
    }

    @Test
    public void testHexLengthError() {
        assertThrows(SemanticException.class, () -> {
            RowDelimiter delimiter = new RowDelimiter("\\x011");
        });
    }
}
