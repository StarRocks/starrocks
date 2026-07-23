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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/FeNameFormatTest.java

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

package com.starrocks.common;

import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static com.starrocks.sql.analyzer.FeNameFormat.SPECIAL_CHARACTERS_IN_DB_NAME;

public class FeNameFormatTest {

    @Test
    public void testCheckColumnName() {

        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("_id"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("01"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("Space Test"));
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName("@timestamp"));

        String rndStr = "0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
        Random r = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 64; i++) {
            sb.append(rndStr.charAt(r.nextInt(rndStr.length())));
        }
        // length 64
        ExceptionChecker.expectThrowsNoException(() -> FeNameFormat.checkColumnName(sb.toString()));
        ExceptionChecker.expectThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("Column \0 Name"));
        ExceptionChecker.expectThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("\0"));
        // length 0
        ExceptionChecker.expectThrows(SemanticException.class, () -> FeNameFormat.checkColumnName(""));
    }

    @Test
    public void testCheckDbName() {
        String prefix = "a";
        String dbName = prefix;
        for (char c : SPECIAL_CHARACTERS_IN_DB_NAME) {
            dbName += c;
        }

        while (dbName.length() < 256) {
            dbName += "a";
        }
        String finalDbName = dbName;
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkDbName(finalDbName));

        dbName += "a";
        String finalDbName1 = dbName;
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkDbName(finalDbName1));

        prefix = "_a";
        dbName = prefix;
        for (char c : SPECIAL_CHARACTERS_IN_DB_NAME) {
            dbName += c;
        }

        while (dbName.length() < 256) {
            dbName += "a";
        }
        String finalDbName2 = dbName;
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkDbName(finalDbName2));


        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkDbName("!abc"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkDbName("ab.c"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkDbName("ab c"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkDbName("ab\0c"));
    }

    @Test
    public void testCheckTableName() {
        // length boundary: 1024 chars is accepted, 1025 is rejected
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkTableName("a".repeat(1024)));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkTableName("my_table"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkTableName("a".repeat(1025)));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkTableName(""));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkTableName("ab\0c"));
    }

    @Test
    public void testCheckNameTooLongMessage() {
        // over-length names report ERR_TOO_LONG_IDENT ("is too long"), not the generic wrong-name error
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkTableName("a".repeat(1025)));
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkDbName("a".repeat(257)));
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkColumnName("a".repeat(1025)));
    }

    @Test
    public void testCheckNamespace() {
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkNamespace("abc"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkNamespace("ns1.ns2"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkNamespace("ns1.ns2.ns3"));
        // leading special character is rejected
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkNamespace("!ns"));
        // over-length (> 256) reports ERR_TOO_LONG_IDENT
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkNamespace("a".repeat(257)));
    }

    @Test
    public void testCheckPartitionName() {
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkPartitionName("p1"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkPartitionName("_p1"));
        // invalid character / empty go through the generic wrong-name error
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkPartitionName("!p"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkPartitionName(""));
        // over-length (> 64) reports ERR_TOO_LONG_IDENT
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkPartitionName("a".repeat(65)));
        // forbidden placeholder prefix is rejected even when the format is otherwise valid
        Assertions.assertThrows(SemanticException.class,
                () -> FeNameFormat.checkPartitionName(FeNameFormat.FORBIDDEN_PARTITION_NAME + "x"));
    }

    @Test
    public void testCheckCommonName() {
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkResourceName("res1"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkCatalogName("cat1"));
        // invalid character / empty go through the generic wrong-name-format error
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkResourceName("!res"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkWarehouseName(""));
        // over-length (> 64) reports ERR_TOO_LONG_IDENT
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "is too long",
                () -> FeNameFormat.checkStorageVolumeName("a".repeat(65)));
    }

    @Test
    public void testCheckColNameInSharedNothing() {
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkColumnName("abc.abc"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("!abc"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("!abc"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("abc<>"));
        Assertions.assertThrows(SemanticException.class, () -> FeNameFormat.checkColumnName("$abc!abc"));

    }

    @Test
    public void testCheckColNameInSharedData() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkColumnName("abc.abc"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkColumnName("!abc"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkColumnName("abc<>"));
        Assertions.assertDoesNotThrow(() -> FeNameFormat.checkColumnName("$abc!abc"));
    }

}
