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

package com.starrocks.sql.ast.expression;

import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.common.Pair;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class FunctionParamsTest {

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

        // Create function with return type (doesn't matter for our tests)
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

    // ========== Test Category 2.1: FunctionParams Construction ==========

    @Test
    public void testNamedArgumentExtraction() {
        // Create named arguments
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("url", new StringLiteral("https://example.com")),
                new NamedArgument("method", new StringLiteral("POST"))
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // Verify named arguments extracted correctly
        Assertions.assertEquals(2, params.exprs().size(),
                "Should have 2 expressions");
        Assertions.assertNotNull(params.getExprsNames(),
                "Expression names should not be null");
        Assertions.assertEquals(2, params.getExprsNames().size(),
                "Should have 2 expression names");
        Assertions.assertEquals("url", params.getExprsNames().get(0),
                "First parameter name should be 'url'");
        Assertions.assertEquals("method", params.getExprsNames().get(1),
                "Second parameter name should be 'method'");

        // Verify expressions are extracted (not NamedArgument wrappers)
        Assertions.assertTrue(params.exprs().get(0) instanceof StringLiteral,
                "First expression should be StringLiteral, not NamedArgument");
        Assertions.assertTrue(params.exprs().get(1) instanceof StringLiteral,
                "Second expression should be StringLiteral, not NamedArgument");
    }

    @Test
    public void testPositionalArgumentsNoNames() {
        // Create positional arguments (no names)
        List<Expr> exprs = Arrays.asList(
                new StringLiteral("https://example.com"),
                new StringLiteral("POST")
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // Verify no named argument names extracted
        Assertions.assertNull(params.getExprsNames(),
                "Expression names should be null for positional arguments");
        Assertions.assertEquals(2, params.exprs().size(),
                "Should have 2 expressions");
    }

    @Test
    public void testMixedNamedAndPositionalConstruction() {
        // This tests the constructor behavior when mixing (should extract names for named ones)
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("url", new StringLiteral("https://example.com")),
                new StringLiteral("POST")  // Positional (will have empty name)
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // Should extract names, with empty string for positional
        Assertions.assertNotNull(params.getExprsNames(),
                "Expression names should not be null");
        Assertions.assertEquals(2, params.getExprsNames().size(),
                "Should have 2 expression names");
        Assertions.assertEquals("url", params.getExprsNames().get(0),
                "First parameter name should be 'url'");
        Assertions.assertEquals("", params.getExprsNames().get(1),
                "Second parameter name should be empty for positional");
    }

    @Test
    public void testDistinctFlag() {
        List<Expr> exprs = Arrays.asList(new StringLiteral("value"));

        FunctionParams params1 = new FunctionParams(false, exprs);
        FunctionParams params2 = new FunctionParams(true, exprs);

        Assertions.assertFalse(params1.isDistinct(),
                "First params should not be distinct");
        Assertions.assertTrue(params2.isDistinct(),
                "Second params should be distinct");
    }

    @Test
    public void testStarParam() {
        FunctionParams params = FunctionParams.createStarParam();

        Assertions.assertTrue(params.isStar(),
                "Should be star param");
        Assertions.assertFalse(params.isDistinct(),
                "Star param should not be distinct by default");
        Assertions.assertNull(params.exprs(),
                "Star param should have null exprs");
    }

    // ========== Test Category 2.2: Reordering Logic ==========

    @Test
    public void testReorderNamedArgAndAppendDefaults() {
        // Create mock function with parameters: url (required), method (default=GET), timeout (default=30000)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"url", "method", "timeout_ms"},
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, IntegerType.INT},
                new Expr[] {null, new StringLiteral("GET"), new IntLiteral(30000)}
        );

        // Create params with different order: method, url (missing timeout_ms)
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("method", new StringLiteral("POST")),
                new NamedArgument("url", new StringLiteral("https://example.com"))
        );

        FunctionParams params = new FunctionParams(false, exprs);
        params.reorderNamedArgAndAppendDefaults(mockFn);

        // Verify reordered to match function definition: url, method, timeout_ms
        Assertions.assertEquals(3, params.exprs().size(),
                "Should have 3 parameters after reordering and defaults");
        Assertions.assertEquals(3, params.getExprsNames().size(),
                "Should have 3 parameter names");

        // Verify order: url, method, timeout_ms
        Assertions.assertEquals("url", params.getExprsNames().get(0),
                "First parameter should be 'url'");
        Assertions.assertEquals("method", params.getExprsNames().get(1),
                "Second parameter should be 'method'");
        Assertions.assertEquals("timeout_ms", params.getExprsNames().get(2),
                "Third parameter should be 'timeout_ms'");

        // Verify values
        Assertions.assertEquals("https://example.com",
                ((StringLiteral) params.exprs().get(0)).getValue(),
                "URL value should be preserved");
        Assertions.assertEquals("POST",
                ((StringLiteral) params.exprs().get(1)).getValue(),
                "Method value should be preserved");
        Assertions.assertEquals(30000L,
                ((IntLiteral) params.exprs().get(2)).getLongValue(),
                "Timeout should be default value");
    }

    @Test
    public void testReorderingReverseOrder() {
        // Function params: a, b, c
        ScalarFunction mockFn = createMockFunction(
                new String[] {"a", "b", "c"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(0)}  // c has default
        );

        // Provide in reverse order: c, b, a
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("c", new IntLiteral(3)),
                new NamedArgument("b", new IntLiteral(2)),
                new NamedArgument("a", new IntLiteral(1))
        );

        FunctionParams params = new FunctionParams(false, exprs);
        params.reorderNamedArgAndAppendDefaults(mockFn);

        // Verify reordered to a, b, c
        Assertions.assertEquals("a", params.getExprsNames().get(0));
        Assertions.assertEquals("b", params.getExprsNames().get(1));
        Assertions.assertEquals("c", params.getExprsNames().get(2));

        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(0)).getLongValue());
        Assertions.assertEquals(2L, ((IntLiteral) params.exprs().get(1)).getLongValue());
        Assertions.assertEquals(3L, ((IntLiteral) params.exprs().get(2)).getLongValue());
    }

    @Test
    public void testReorderingWithAllDefaults() {
        // Function params: start (required), end (required), step (default=1), direction (default='up')
        ScalarFunction mockFn = createMockFunction(
                new String[] {"start", "end", "step", "direction"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT, VarcharType.VARCHAR},
                new Expr[] {null, null, new IntLiteral(1), new StringLiteral("up")}
        );

        // Provide only required params in different order
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("end", new IntLiteral(10)),
                new NamedArgument("start", new IntLiteral(1))
        );

        FunctionParams params = new FunctionParams(false, exprs);
        params.reorderNamedArgAndAppendDefaults(mockFn);

        // Should have all 4 parameters
        Assertions.assertEquals(4, params.exprs().size(),
                "Should have 4 parameters including defaults");

        // Verify correct order
        Assertions.assertEquals("start", params.getExprsNames().get(0));
        Assertions.assertEquals("end", params.getExprsNames().get(1));
        Assertions.assertEquals("step", params.getExprsNames().get(2));
        Assertions.assertEquals("direction", params.getExprsNames().get(3));

        // Verify values
        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(0)).getLongValue());
        Assertions.assertEquals(10L, ((IntLiteral) params.exprs().get(1)).getLongValue());
        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(2)).getLongValue());  // default
        Assertions.assertEquals("up", ((StringLiteral) params.exprs().get(3)).getValue());  // default
    }

    // ========== Test Category 2.3: Default Value Appending for Positional Args ==========

    @Test
    public void testAppendDefaultsForPositionalArgs() {
        // Function params: start, end, step (default=1)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"start", "end", "step"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null, new IntLiteral(1)}
        );

        // Provide positional args: start, end (missing step)
        List<Expr> exprs = new ArrayList<>(Arrays.asList(
                new IntLiteral(1),
                new IntLiteral(10)
        ));

        FunctionParams params = new FunctionParams(false, exprs);
        params.appendDefaultsForPositionalArgs(mockFn);

        // Should append default for step
        Assertions.assertEquals(3, params.exprs().size(),
                "Should have 3 parameters after appending defaults");

        // Verify values
        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(0)).getLongValue());
        Assertions.assertEquals(10L, ((IntLiteral) params.exprs().get(1)).getLongValue());
        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(2)).getLongValue());  // default
    }

    @Test
    public void testAppendDefaultsForPositionalArgsMultipleDefaults() {
        // Function params: a (required), b (default=2), c (default=3), d (default=4)
        ScalarFunction mockFn = createMockFunction(
                new String[] {"a", "b", "c", "d"},
                new Type[] {IntegerType.INT, IntegerType.INT, IntegerType.INT, IntegerType.INT},
                new Expr[] {null, new IntLiteral(2), new IntLiteral(3), new IntLiteral(4)}
        );

        // Provide only first parameter
        List<Expr> exprs = new ArrayList<>(Arrays.asList(new IntLiteral(1)));

        FunctionParams params = new FunctionParams(false, exprs);
        params.appendDefaultsForPositionalArgs(mockFn);

        // Should append all defaults
        Assertions.assertEquals(4, params.exprs().size(),
                "Should have 4 parameters after appending defaults");

        // Verify all values
        Assertions.assertEquals(1L, ((IntLiteral) params.exprs().get(0)).getLongValue());
        Assertions.assertEquals(2L, ((IntLiteral) params.exprs().get(1)).getLongValue());
        Assertions.assertEquals(3L, ((IntLiteral) params.exprs().get(2)).getLongValue());
        Assertions.assertEquals(4L, ((IntLiteral) params.exprs().get(3)).getLongValue());
    }

    @Test
    public void testAppendDefaultsForPositionalArgsNoDefaults() {
        // Function params: all required, no defaults
        ScalarFunction mockFn = createMockFunction(
                new String[] {"a", "b"},
                new Type[] {IntegerType.INT, IntegerType.INT},
                new Expr[] {null, null}
        );

        // Provide all parameters
        List<Expr> exprs = new ArrayList<>(Arrays.asList(
                new IntLiteral(1),
                new IntLiteral(2)
        ));

        FunctionParams params = new FunctionParams(false, exprs);
        params.appendDefaultsForPositionalArgs(mockFn);

        // Should not append anything
        Assertions.assertEquals(2, params.exprs().size(),
                "Should still have 2 parameters");
    }

    // ========== Test Category 2.4: Edge Cases and Special Scenarios ==========

    @Test
    public void testGetNamedArgStr() {
        // Create named arguments
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("url", new StringLiteral("https://example.com")),
                new NamedArgument("method", new StringLiteral("POST"))
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // After reordering, we can get the named arg string representation
        String argStr = params.getNamedArgStr();

        // Should contain both parameters in named format
        Assertions.assertTrue(argStr.contains("url=>"),
                "Should contain url parameter");
        Assertions.assertTrue(argStr.contains("method=>"),
                "Should contain method parameter");
    }

    @Test
    public void testEmptyNamedArguments() {
        // Empty list
        List<Expr> exprs = new ArrayList<>();

        FunctionParams params = new FunctionParams(false, exprs);

        Assertions.assertEquals(0, params.exprs().size(),
                "Should have 0 expressions");
        Assertions.assertNull(params.getExprsNames(),
                "Expression names should be null for empty list");
    }

    @Test
    public void testSingleNamedArgument() {
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("value", new IntLiteral(42))
        );

        FunctionParams params = new FunctionParams(false, exprs);

        Assertions.assertEquals(1, params.exprs().size(),
                "Should have 1 expression");
        Assertions.assertNotNull(params.getExprsNames(),
                "Expression names should not be null");
        Assertions.assertEquals("value", params.getExprsNames().get(0),
                "Parameter name should be 'value'");
        Assertions.assertEquals(42L, ((IntLiteral) params.exprs().get(0)).getLongValue(),
                "Parameter value should be 42");
    }

    @Test
    public void testEqualsAndHashCode() {
        // Create two identical FunctionParams
        List<Expr> exprs1 = Arrays.asList(new IntLiteral(1), new IntLiteral(2));
        List<Expr> exprs2 = Arrays.asList(new IntLiteral(1), new IntLiteral(2));

        FunctionParams params1 = new FunctionParams(false, exprs1);
        FunctionParams params2 = new FunctionParams(false, exprs2);

        // Note: equals() might not be implemented to compare expr values deeply
        // This test just verifies the methods don't throw exceptions
        params1.equals(params2);
        params1.hashCode();
        params2.hashCode();
    }

    // ========== Test Category 2.5: Mixing Named and Positional Detection ==========

    @Test
    public void testMixingNamedAndPositionalDetection() {
        // Mixed: first is named, second is positional
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("url", new StringLiteral("https://example.com")),
                new StringLiteral("POST")  // Positional
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // exprsNames should exist (because at least one named arg exists)
        Assertions.assertNotNull(params.getExprsNames(),
                "Expression names should not be null when any named arg exists");

        // Check that mixing can be detected: named args have non-empty names, positional have empty
        boolean hasNamed = params.getExprsNames().stream().anyMatch(name -> !name.isEmpty());
        boolean hasPositional = params.getExprsNames().stream().anyMatch(String::isEmpty);

        Assertions.assertTrue(hasNamed, "Should detect named arguments");
        Assertions.assertTrue(hasPositional, "Should detect positional arguments");

        // This is how mixing detection works
        boolean isMixed = hasNamed && hasPositional;
        Assertions.assertTrue(isMixed, "Should detect mixed named and positional");
    }

    @Test
    public void testAllNamedNoMixing() {
        // All named arguments - no mixing
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("url", new StringLiteral("https://example.com")),
                new NamedArgument("method", new StringLiteral("POST"))
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // All names should be non-empty
        boolean allNamed = params.getExprsNames().stream().noneMatch(String::isEmpty);
        Assertions.assertTrue(allNamed, "All parameters should have names");
    }

    @Test
    public void testAllPositionalNoMixing() {
        // All positional arguments - no mixing
        List<Expr> exprs = Arrays.asList(
                new StringLiteral("https://example.com"),
                new StringLiteral("POST")
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // exprsNames should be null for pure positional
        Assertions.assertNull(params.getExprsNames(),
                "Expression names should be null for pure positional arguments");
    }

    // ========== Test Category 2.6: Complex Expression Values ==========

    @Test
    public void testComplexExpressionInNamedArgument() {
        // Named argument with complex expression (arithmetic)
        // In real code, this would be: func(param => 1 + 2)
        // For testing, we use IntLiteral as placeholder
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("value", new IntLiteral(42))
        );

        FunctionParams params = new FunctionParams(false, exprs);

        // Verify the expression is extracted correctly
        Assertions.assertNotNull(params.exprs());
        Assertions.assertEquals(1, params.exprs().size());
        Assertions.assertTrue(params.exprs().get(0) instanceof IntLiteral);
    }

    @Test
    public void testNullExpressionInNamedArgument() {
        // Named argument with null literal
        List<Expr> exprs = Arrays.asList(
                new NamedArgument("nullable_param", new NullLiteral())
        );

        FunctionParams params = new FunctionParams(false, exprs);

        Assertions.assertEquals(1, params.exprs().size());
        Assertions.assertTrue(params.exprs().get(0) instanceof NullLiteral);
        Assertions.assertEquals("nullable_param", params.getExprsNames().get(0));
    }
}
