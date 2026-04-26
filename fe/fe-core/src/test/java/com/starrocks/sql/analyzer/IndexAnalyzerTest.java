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
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
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
        Column column = new Column("test_col",
                IntegerType.INT, false, null, true, NULL_DEFAULT_VALUE, "");
        Map<String, String> properties = new HashMap<>();

        // Test BITMAP index with valid column
        IndexAnalyzer.checkColumn(column, IndexDef.IndexType.BITMAP, properties, KeysType.DUP_KEYS);

        // Test unsupported index type
        Assertions.assertThrows(SemanticException.class,
                () -> IndexAnalyzer.checkColumn(column, null, properties, KeysType.DUP_KEYS),
                "Unsupported index type: null");
    }
    
    @Test
    public void testCheckInvertedIndexNgram() {
        // Test case 1: Valid gram_num value
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("dict_gram_num", "5");
        properties1.put("imp_lib", "builtin");
        IndexAnalyzer.checkInvertedIndexNgram(properties1); // Should not throw exception

        // Test case 2: Invalid gram_num value (not numeric)
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("dict_gram_num", "invalid");
        properties2.put("imp_lib", "builtin");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties2);
        }, "INVERTED index dict gram num invalid is a invalid number.");

        // Test case 3: Non-positive gram_num value
        Map<String, String> properties3 = new HashMap<>();
        properties3.put("dict_gram_num", "-1");
        properties3.put("imp_lib", "builtin");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties3);
        }, "INVERTED index dict gram num -1 should be greater than zero.");

        // Test case 4: Zero gram_num value
        Map<String, String> properties4 = new HashMap<>();
        properties4.put("dict_gram_num", "0");
        properties4.put("imp_lib", "builtin");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties4);
        }, "INVERTED index dict gram num 0 should be greater than zero.");

        // Test case 5: Non-builtin implementation with gram_num (should fail)
        Map<String, String> properties5 = new HashMap<>();
        properties5.put("dict_gram_num", "5");
        properties5.put("imp_lib", "clucene");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties5);
        }, "INVERTED index with clucene implement is invalid for dict gram.");

        // Test case 6: Non-builtin implementation with invalid gram_num (should fail with gram_num error first)
        Map<String, String> properties6 = new HashMap<>();
        properties6.put("dict_gram_num", "invalid");
        properties6.put("imp_lib", "clucene");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties6);
        }, "INVERTED index dict gram num invalid is a invalid number.");

        // Test case 7: Null gram_num (should not throw exception)
        Map<String, String> properties7 = new HashMap<>();
        properties7.put("imp_lib", "builtin");
        IndexAnalyzer.checkInvertedIndexNgram(properties7); // Should not throw exception

        // Test case 8: Empty gram_num (should throw exception as it's not numeric)
        Map<String, String> properties8 = new HashMap<>();
        properties8.put("dict_gram_num", "");
        properties8.put("imp_lib", "builtin");
        Assertions.assertThrows(SemanticException.class, () -> {
            IndexAnalyzer.checkInvertedIndexNgram(properties8);
        }, "INVERTED index dict gram num  should be greater than zero.");
    }
}