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

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ColumnPosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnPositionTest {
    @Test
    public void testNormal() throws AnalysisException {
        ColumnPosition pos = ColumnPosition.FIRST;
        analyze(pos);
        Assertions.assertEquals("FIRST", pos.toString());
        Assertions.assertNull(pos.getLastCol());
        Assertions.assertTrue(pos.isFirst());

        pos = new ColumnPosition("col");
        analyze(pos);
        Assertions.assertEquals("AFTER `col`", pos.toString());
        Assertions.assertEquals("col", pos.getLastCol());
        Assertions.assertFalse(pos.isFirst());
    }

    @Test
    public void testNoCol() {
        assertThrows(SemanticException.class, () -> {
            ColumnPosition pos = new ColumnPosition("");
            analyze(pos);
            Assertions.fail("No exception throws.");
        });
    }

    private void analyze(ColumnPosition pos) {
        if (pos != ColumnPosition.FIRST && Strings.isNullOrEmpty(pos.getLastCol())) {
            throw new SemanticException("Column is empty.");
        }
    }
}