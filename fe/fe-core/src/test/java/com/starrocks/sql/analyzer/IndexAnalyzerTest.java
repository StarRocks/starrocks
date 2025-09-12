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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.ast.IndexDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE;

public class IndexAnalyzerTest {

    @Test
    public void testAnalyzeNormal() {
        IndexDef indexDef = new IndexDef("index1", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "");
        IndexAnalyzer.analyze(indexDef);
    }

    @Test
    public void testAnalyzeException() {
        // Test null columns
        IndexDef indexDef1 = new IndexDef("index1", null, IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef1),
                "Index can not accept null column.");

        // Test empty index name
        IndexDef indexDef2 = new IndexDef("", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef2),
                "index name cannot be blank.");

        // Test too long index name
        String longName = "index1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxx"
                + "xxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxinde"
                + "x1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxx"
                + "xxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxx"
                + "xxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxx";
        IndexDef indexDef3 = new IndexDef(longName, Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef3),
                "index name too long, the index name length at most is 64.");

        // Test multiple columns (not supported)
        IndexDef indexDef4 = new IndexDef("index1", Lists.newArrayList("col1", "col2"), IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef4),
                "index1 index can only apply to a single column.");

        // Test duplicate columns
        IndexDef indexDef5 = new IndexDef("index1", Lists.newArrayList("col1", "COL1"), IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef5),
                "columns of index has duplicated.");
    }

    @Test
    public void testAnalyzeExceptionWithTryCatch() {
        // Test too long index name with try-catch (from original IndexDefTest)
        try {
            String longName = "index1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxx"
                    + "xxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxinde"
                    + "x1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxx"
                    + "xxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxx"
                    + "xxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxx";
            IndexDef indexDef1 = new IndexDef(longName, Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "");
            IndexAnalyzer.analyze(indexDef1);
            Assertions.fail("No exception throws.");
        } catch (SemanticException e) {
            Assertions.assertTrue(e instanceof SemanticException);
        }

        // Test empty index name with try-catch (from original IndexDefTest)
        try {
            IndexDef indexDef2 = new IndexDef("", Lists.newArrayList("col1"), IndexDef.IndexType.BITMAP, "");
            IndexAnalyzer.analyze(indexDef2);
            Assertions.fail("No exception throws.");
        } catch (SemanticException e) {
            Assertions.assertTrue(e instanceof SemanticException);
        }

        // Test null columns with try-catch (from original IndexDefTest)
        IndexDef indexDef3 = new IndexDef("", null, IndexDef.IndexType.GIN, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef3),
                "Index can not accept null column.");

        // Test multiple columns for BITMAP (from original IndexDefTest)
        IndexDef indexDef4 = new IndexDef("", Lists.newArrayList("k1", "k2"), IndexDef.IndexType.BITMAP, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef4),
                "bitmap index can only apply to a single column.");

        // Test multiple columns for GIN (from original IndexDefTest)
        IndexDef indexDef5 = new IndexDef("", Lists.newArrayList("k1", "k2"), IndexDef.IndexType.GIN, "");
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.analyze(indexDef5),
                "bitmap index can only apply to a single column.");
    }

    @Test
    public void testCheckColumn() {
        // Create a test column
        Column column =
                new Column("test_col", ScalarType.createType(PrimitiveType.INT), false, null, true, NULL_DEFAULT_VALUE, "");
        Map<String, String> properties = new HashMap<>();

        // Test BITMAP index with valid column
        IndexAnalyzer.checkColumn(column, IndexDef.IndexType.BITMAP, properties, KeysType.DUP_KEYS);

        // Test unsupported index type
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.checkColumn(column, null, properties, KeysType.DUP_KEYS),
                "Unsupported index type: null");
    }
}