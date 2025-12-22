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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.FunctionName;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FunctionRefAnalyzerTest {
    @Test
    public void testResolveFunctionNameForGlobalFunction() {
        // Test resolving global function name
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, true);
        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(functionRef, "test_db");

        Assertions.assertTrue(functionName.isGlobalFunction());
        Assertions.assertEquals(FunctionRefAnalyzer.GLOBAL_UDF_DB, functionName.getDb());
        Assertions.assertEquals("my_func", functionName.getFunction());
    }

    @Test
    public void testResolveFunctionNameForNonGlobalFunctionWithExplicitDb() {
        // Test resolving non-global function with explicit database
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("mydb", "my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);
        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(functionRef, "default_db");

        Assertions.assertFalse(functionName.isGlobalFunction());
        Assertions.assertEquals("mydb", functionName.getDb());
        Assertions.assertEquals("my_func", functionName.getFunction());
    }

    @Test
    public void testResolveFunctionNameForNonGlobalFunctionWithoutDb() {
        // Test resolving non-global function without explicit database
        // This tests the bug fix scenario
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);
        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(functionRef, "test_db");

        Assertions.assertFalse(functionName.isGlobalFunction());
        Assertions.assertEquals("test_db", functionName.getDb());
        Assertions.assertEquals("my_func", functionName.getFunction());
    }

    @Test
    public void testResolveFunctionNameForGlobalFunctionWithInvalidDb() {
        // Test that global function should not have database prefix
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("mydb", "my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, true);

        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionRefAnalyzer.resolveFunctionName(functionRef, "test_db");
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid function name"));
    }

    @Test
    public void testAnalyzeFunctionRefWithValidName() {
        // Test analyzing function reference with valid name
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("my_valid_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);

        Assertions.assertDoesNotThrow(() -> {
            FunctionRefAnalyzer.analyzeFunctionRef(functionRef, "test_db");
        });
    }

    @Test
    public void testAnalyzeFunctionRefWithInvalidNameStartingWithDigit() {
        // Test analyzing function reference with name starting with digit
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("1invalid_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);

        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionRefAnalyzer.analyzeFunctionRef(functionRef, "test_db");
        });

        Assertions.assertTrue(exception.getMessage().contains("Function cannot start with a digit"));
    }

    @Test
    public void testAnalyzeFunctionRefWithInvalidCharacters() {
        // Test analyzing function reference with special characters
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("my-invalid-func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);

        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionRefAnalyzer.analyzeFunctionRef(functionRef, "test_db");
        });

        Assertions.assertTrue(exception.getMessage().contains(
                "Function names must be all alphanumeric or underscore"));
    }

    @Test
    public void testAnalyzeFunctionRefWithoutDatabaseContext() {
        // Test analyzing function reference without database context
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);

        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionRefAnalyzer.analyzeFunctionRef(functionRef, null);
        });

        Assertions.assertTrue(exception.getMessage().contains("No database selected"));
    }

    @Test
    public void testAnalyzeFunctionRefWithTooManyParts() {
        // Test analyzing function reference with too many parts (catalog.db.func)
        QualifiedName funcName = QualifiedName.of(ImmutableList.of("catalog", "mydb", "my_func"));
        FunctionRef functionRef = new FunctionRef(funcName, null, NodePosition.ZERO, false);

        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionRefAnalyzer.analyzeFunctionRef(functionRef, "test_db");
        });

        Assertions.assertTrue(exception.getMessage().contains(
                "Function names must be all alphanumeric or underscore"));
    }

    @Test
    public void testFunctionNameCaseInsensitive() {
        // Test that function names are case-insensitive (normalized to lowercase)
        QualifiedName funcName1 = QualifiedName.of(ImmutableList.of("MY_FUNC"));
        QualifiedName funcName2 = QualifiedName.of(ImmutableList.of("my_func"));
        FunctionRef functionRef1 = new FunctionRef(funcName1, null, NodePosition.ZERO, false);
        FunctionRef functionRef2 = new FunctionRef(funcName2, null, NodePosition.ZERO, false);

        FunctionName name1 = FunctionRefAnalyzer.resolveFunctionName(functionRef1, "test_db");
        FunctionName name2 = FunctionRefAnalyzer.resolveFunctionName(functionRef2, "test_db");

        Assertions.assertEquals(name1.getFunction(), name2.getFunction());
        Assertions.assertEquals("my_func", name1.getFunction());
    }

    @Test
    public void testGlobalUdfDbConstant() {
        // Test that GLOBAL_UDF_DB constant is correctly set
        Assertions.assertEquals(FunctionName.GLOBAL_UDF_DB, FunctionRefAnalyzer.GLOBAL_UDF_DB);
        Assertions.assertEquals("__global_udf_db__", FunctionRefAnalyzer.GLOBAL_UDF_DB);
    }
}
