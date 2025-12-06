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

import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Unit tests for FunctionAnalyzer.validateNamedArguments() method.
 * These tests verify the common Named Arguments validation logic
 * without depending on any specific function implementation.
 */
public class NamedArgumentValidatorTest {

    // ========== Helper Methods ==========

    /**
     * Creates a mock ScalarFunction with named arguments support
     *
     * @param paramNames   Array of parameter names
     * @param paramTypes   Array of parameter types
     * @param defaultExprs Array of default expressions (null for required params)
     * @return Mock ScalarFunction for testing
     */
    private ScalarFunction createMockFunction(String[] paramNames, Type[] paramTypes, Expr[] defaultExprs) {
        Assertions.assertEquals(paramNames.length, paramTypes.length,
                "Parameter names and types must have same length");
        Assertions.assertEquals(paramNames.length, defaultExprs.length,
                "Parameter names and defaults must have same length");

        // Create function with return type
        ScalarFunction fn = new ScalarFunction(
                new FunctionName("test_function"),
                Arrays.asList(paramTypes),
                IntegerType.INT,
                false
        );

        // Set named argument information - setArgNames expects List<String>
        fn.setArgNames(Arrays.asList(paramNames));

        // Set default values using Vector<Pair<String, Expr>>
        // Only add parameters that have default values (non-null)
        Vector<Pair<String, Expr>> defaultArgs = new Vector<>();
        for (int i = 0; i < paramNames.length; i++) {
            if (defaultExprs[i] != null) {
                defaultArgs.add(new Pair<>(paramNames[i], defaultExprs[i]));
            }
        }
        if (!defaultArgs.isEmpty()) {
            fn.setDefaultNamedArgs(defaultArgs);
        }

        return fn;
    }

    // ========== Test Category 1: Valid Cases ==========

    @Test
    public void testValidNamedArgumentsInOrder() {
        // Function: param_a (required), param_b (required), param_c (default=0)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b", "param_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(0)}
        );

        // All parameters provided in order
        List<String> paramNames = Arrays.asList("param_a", "param_b", "param_c");

        // Should not throw
        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    @Test
    public void testValidNamedArgumentsDifferentOrder() {
        // Function: param_a (required), param_b (required), param_c (default=0)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b", "param_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(0)}
        );

        // Parameters in different order
        List<String> paramNames = Arrays.asList("param_c", "param_b", "param_a");

        // Should not throw - order doesn't matter for named args
        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    @Test
    public void testValidNamedArgumentsOnlyRequired() {
        // Function: param_a (required), param_b (required), param_c (default=0)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b", "param_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(0)}
        );

        // Only required parameters provided
        List<String> paramNames = Arrays.asList("param_a", "param_b");

        // Should not throw - optional params can be omitted
        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    // ========== Test Category 2: Duplicate Parameter ==========

    @Test
    public void testDuplicateParameterName() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Duplicate parameter name
        List<String> paramNames = Arrays.asList("param_a", "param_a");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("duplicate parameter"),
                "Expected 'duplicate parameter' in: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("param_a"),
                "Expected parameter name 'param_a' in: " + ex.getMessage());
    }

    @Test
    public void testMultipleDuplicates() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b", "param_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, null}
        );

        // Multiple duplicates - first duplicate should be caught
        List<String> paramNames = Arrays.asList("param_a", "param_b", "param_a");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("duplicate"),
                "Expected 'duplicate' in: " + ex.getMessage());
    }

    // ========== Test Category 3: Unknown Parameter ==========

    @Test
    public void testUnknownParameterName() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Unknown parameter name
        List<String> paramNames = Arrays.asList("param_a", "unknown_param");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(
                ex.getMessage().contains("does not support parameter") ||
                        ex.getMessage().contains("unknown parameter"),
                "Expected unknown parameter error in: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("unknown_param"),
                "Expected parameter name 'unknown_param' in: " + ex.getMessage());
    }

    @Test
    public void testCaseSensitiveParameterName() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Wrong case - should suggest correct name
        List<String> paramNames = Arrays.asList("PARAM_A", "param_b");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        // Should provide suggestion for case-insensitive match
        Assertions.assertTrue(ex.getMessage().contains("Did you mean"),
                "Expected suggestion in: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("param_a"),
                "Expected correct name 'param_a' in suggestion: " + ex.getMessage());
    }

    @Test
    public void testSimilarParameterNameSuggestion() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"timeout_ms", "retry_count"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Wrong case - should suggest similar name
        List<String> paramNames = Arrays.asList("TIMEOUT_MS", "retry_count");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("Did you mean 'timeout_ms'"),
                "Expected suggestion for 'timeout_ms' in: " + ex.getMessage());
    }

    // ========== Test Category 4: Missing Required Parameter ==========

    @Test
    public void testMissingRequiredParameter() {
        // url is required, method and timeout have defaults
        ScalarFunction mockFn = createMockFunction(
                new String[] {"url", "method", "timeout"},
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, IntegerType.INT},
                new Expr[] {null, new StringLiteral("GET"), new IntLiteral(30000)}
        );

        // Missing required 'url'
        List<String> paramNames = Arrays.asList("method", "timeout");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("required parameter"),
                "Expected 'required parameter' in: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("url"),
                "Expected parameter name 'url' in: " + ex.getMessage());
    }

    @Test
    public void testMissingMultipleRequiredParameters() {
        // Both param_a and param_b are required
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b", "param_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(0)}
        );

        // Only optional param_c provided
        List<String> paramNames = Arrays.asList("param_c");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("required parameter"),
                "Expected 'required parameter' in: " + ex.getMessage());
    }

    @Test
    public void testAllRequiredParametersMissing() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"param_a", "param_b"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Empty list - all required params missing
        List<String> paramNames = new ArrayList<>();

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("required parameter"),
                "Expected 'required parameter' in: " + ex.getMessage());
    }

    // ========== Test Category 5: Function Without Named Args Support ==========

    @Test
    public void testFunctionWithoutNamedArgsSupport() {
        // Create function without named args support
        ScalarFunction fn = new ScalarFunction(
                new FunctionName("no_named_args_function"),
                Arrays.asList(new Type[] {IntegerType.INT}),
                IntegerType.INT,
                false
        );
        // Don't set argNames or namedArgs

        List<String> paramNames = Arrays.asList("param_a");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("no_named_args_function", fn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("does not support named parameters"),
                "Expected 'does not support named parameters' in: " + ex.getMessage());
    }

    @Test
    public void testNullFunction() {
        List<String> paramNames = Arrays.asList("param_a");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("null_function", null, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("does not support named parameters"),
                "Expected 'does not support named parameters' in: " + ex.getMessage());
    }

    // ========== Test Category 6: Edge Cases ==========

    @Test
    public void testSingleParameter() {
        ScalarFunction mockFn = createMockFunction(
                new String[] {"single_param"},
                new Type[] {IntegerType.INT},
                new Expr[] {null}
        );

        List<String> paramNames = Arrays.asList("single_param");

        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    @Test
    public void testAllOptionalParameters() {
        // All parameters have defaults
        ScalarFunction mockFn = createMockFunction(
                new String[] {"opt_a", "opt_b", "opt_c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {new IntLiteral(1), new IntLiteral(2), new IntLiteral(3)}
        );

        // Empty list is valid when all params are optional
        List<String> paramNames = new ArrayList<>();

        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    @Test
    public void testManyParameters() {
        // Test with many parameters (8 like http_request)
        String[] names = {"url", "method", "body", "headers", "timeout_ms", "ssl_verify", "username", "password"};
        Type[] types = {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR,
                IntegerType.INT, BooleanType.BOOLEAN, VarcharType.VARCHAR, VarcharType.VARCHAR};
        Expr[] defaults = {null, new StringLiteral("GET"), new StringLiteral(""), new StringLiteral("{}"),
                new IntLiteral(30000), new IntLiteral(1), new StringLiteral(""), new StringLiteral("")};

        ScalarFunction mockFn = createMockFunction(names, types, defaults);

        // Provide only required (url) and some optional
        List<String> paramNames = Arrays.asList("url", "method", "timeout_ms");

        Assertions.assertDoesNotThrow(() -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });
    }

    @Test
    public void testManyParametersMissingRequired() {
        // Same as above but missing required 'url'
        String[] names = {"url", "method", "body", "headers", "timeout_ms", "ssl_verify", "username", "password"};
        Type[] types = {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR,
                IntegerType.INT, BooleanType.BOOLEAN, VarcharType.VARCHAR, VarcharType.VARCHAR};
        Expr[] defaults = {null, new StringLiteral("GET"), new StringLiteral(""), new StringLiteral("{}"),
                new IntLiteral(30000), new IntLiteral(1), new StringLiteral(""), new StringLiteral("")};

        ScalarFunction mockFn = createMockFunction(names, types, defaults);

        // Missing required 'url'
        List<String> paramNames = Arrays.asList("method", "timeout_ms");

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () -> {
            FunctionAnalyzer.validateNamedArguments("test_function", mockFn, paramNames);
        });

        Assertions.assertTrue(ex.getMessage().contains("url"),
                "Expected 'url' in error message: " + ex.getMessage());
    }
}
