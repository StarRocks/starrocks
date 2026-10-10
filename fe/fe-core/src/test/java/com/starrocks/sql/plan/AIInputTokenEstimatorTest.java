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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatisticsCollectJob;
import com.starrocks.statistic.base.PrimitiveTypeColumnStats;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.ArrayType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class AIInputTokenEstimatorTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true);
    private static final ColumnRefOperator OUTPUT = new ColumnRefOperator(2, VarcharType.VARCHAR, "result", true);

    @Test
    void testUtf8NullAndCapability() {
        OptExpression values = values();
        assertTokens("26", estimate(200100, values, ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("10", estimate(200130, values, ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("26", estimate(200140, values,
                ConstantOperator.createVarchar("provider"), ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("16", estimate(200100, values, ConstantOperator.createVarchar("")));
        assertTokens("0", estimate(200100, scan(100, ColumnStatistic.unknown()),
                ConstantOperator.createNull(VarcharType.VARCHAR)));
    }

    @Test
    void testAnalyzedColumnsAliasesAndCommonExpressions() {
        OptExpression scan = scan(2.2, ColumnStatistic.builder().setAverageRowSize(0.5).build());
        assertTokens("54", estimate(200100, scan, TEXT));
        ColumnRefOperator alias = new ColumnRefOperator(3, VarcharType.VARCHAR, "alias", true);
        OptExpression project = OptExpression.create(new PhysicalProjectOperator(Map.of(alias, TEXT), Map.of()), scan);
        assertTokens("54", estimate(200100, project, alias));
        ScalarOperator concat = concat(ConstantOperator.createVarchar("x"), alias);
        assertTokens("57", estimate(200100, project, concat));
        ColumnRefOperator commonOutput = new ColumnRefOperator(4, VarcharType.VARCHAR, "common", true);
        OptExpression common = OptExpression.create(new PhysicalProjectOperator(Map.of(commonOutput, alias),
                Map.of(alias, concat(TEXT, ConstantOperator.createVarchar("xy")))), scan);
        assertTokens("60", estimate(200100, common, commonOutput));
    }

    @Test
    void testNativeStringStatisticsUseCharacterUnits() {
        String dataSizeExpression = "IFNULL(SUM(CHAR_LENGTH(`text`)), 0)";
        Assertions.assertEquals(dataSizeExpression,
                StatisticsCollectJob.fullAnalyzeGetDataSize("`text`", VarcharType.VARCHAR));
        Assertions.assertEquals(dataSizeExpression,
                new PrimitiveTypeColumnStats("text", VarcharType.VARCHAR).getFullDataSize());

        // Collected rows: "中🙂abc", NULL. CHAR_LENGTH totals 5, while UTF-8 totals 10 bytes.
        TStatisticData data = new TStatisticData().setRowCount(2).setNullCount(1).setDataSize(5);
        ColumnStatistic statistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                data, "default_catalog", "test", "t", "text", VarcharType.VARCHAR);
        Assertions.assertEquals(2.5, statistic.getAverageRowSize());
        Assertions.assertEquals(0.5, statistic.getNullsFraction());

        OptExpression scan = scan(2, statistic);
        assertTokens("20", estimate(200130, scan, TEXT));
        assertTokens("52", estimate(200100, scan, TEXT));
    }

    @Test
    void testMissingOrUntrustedStatisticsRemainUnknown() {
        for (ColumnStatistic statistic : List.of(ColumnStatistic.unknown(),
                ColumnStatistic.builder().setAverageRowSize(Double.NaN).build(),
                ColumnStatistic.builder().setAverageRowSize(Double.POSITIVE_INFINITY).build(),
                ColumnStatistic.builder().setAverageRowSize(-1).build())) {
            assertUnknown(estimate(200100, scan(10, statistic), TEXT));
        }
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        scan.setStatistics(Statistics.builder().setOutputRowCount(10).build());
        assertUnknown(estimate(200100, scan, TEXT));
        // Even a plausible CBO fallback width is not analyzed text-size evidence.
        scan.setStatistics(Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(TEXT, ColumnStatistic.builder().setAverageRowSize(2).build()).build());
        assertUnknown(estimate(200100, scan, TEXT));
    }

    @Test
    void testLocalLimitsAndBroadcastDoNotUnderestimate() {
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        scan.getOp().setLimit(1);
        assertUnknown(estimate(200100, scan, TEXT));
        scan.getOp().setLimit(Operator.DEFAULT_LIMIT);
        OptExpression broadcast = OptExpression.create(
                new PhysicalDistributionOperator(DistributionSpec.createReplicatedDistributionSpec()), scan);
        assertUnknown(estimate(200100, broadcast, TEXT));
        OptExpression gather = OptExpression.create(
                new PhysicalDistributionOperator(DistributionSpec.createGatherDistributionSpec()), scan);
        assertTokens("240", estimate(200100, gather, TEXT));
    }

    @Test
    void testOverflowAndIncompleteAggregate() {
        AIInputTokenEstimate huge = estimate(200100,
                scan(1e20, ColumnStatistic.builder().setAverageRowSize(1e10).build()), TEXT);
        assertTokens("4000000001600000000000000000000", huge);
        Assertions.assertEquals(huge.getTokens().multiply(BigInteger.TWO), huge.add(huge).getTokens());
        Assertions.assertSame(huge, AIInputTokenEstimate.none().add(huge));
        AIInputTokenEstimate unknown = AIInputTokenEstimate.unknown("missing statistics");
        assertUnknown(huge.add(unknown));
        assertUnknown(unknown.add(huge));
        Assertions.assertNull(huge.add(unknown).getTokens());
    }

    @Test
    void testUnsupportedExpressionsAndCycles() {
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        assertUnknown(estimate(200124, scan(10, ColumnStatistic.unknown()), TEXT));
        assertUnknown(estimate(200100, scan, new CallOperator("repeat", VarcharType.VARCHAR,
                List.of(TEXT, ConstantOperator.createInt(10)))));
        ColumnRefOperator other = new ColumnRefOperator(3, VarcharType.VARCHAR, "other", true);
        OptExpression cyclic = OptExpression.create(new PhysicalProjectOperator(Map.of(TEXT, other, other, TEXT), Map.of()),
                scan);
        assertUnknown(estimate(200100, cyclic, TEXT));
    }

    @Test
    void testConcatUdfIsNotTreatedAsBuiltin() {
        Function udf = Mockito.mock(Function.class);
        Mockito.when(udf.getBinaryType()).thenReturn(TFunctionBinaryType.SRJAR);
        assertUnknown(estimate(200100, values(),
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(), udf)));
    }

    @Test
    void testSingleTextTemplatesAndOverloads() {
        String text = "中🙂abc";
        assertPrompt(200110, "Analyze the overall sentiment of the following text. "
                + "Output exactly one lowercase word from this list: positive, negative, neutral, mixed, unknown. "
                + "No punctuation, no explanation.\n\nText: " + text, text);
        assertPrompt(200116, "Fix the grammar and spelling of the following text. "
                + "Preserve the original meaning and tone. Output only the corrected text, nothing else.\n\nText: "
                + text, text);
        assertPrompt(200124, "Summarize the following text concisely, capturing the key points. "
                + "Output only the summary, nothing else.\n\nText: " + text, text);
    }

    @Test
    void testTwoTextTemplatesAndRequiredNull() {
        assertPrompt(200122, "Calculate the semantic similarity between the following two texts.\n"
                + "Output only a single decimal number between 0.00 and 1.00 (0 = completely different, "
                + "1 = identical meaning). No explanation, no extra text.\n\nText 1: 中\nText 2: other", "中", "other");
        assertPrompt(200126, "Given the following text, determine if this condition is true. "
                + "You MUST respond with exactly true or false and nothing else.\nText: text\nCondition: condition",
                "text", "condition");
        // A required NULL prevents the request even when another input has no usable size statistics.
        assertTokens("0", estimate(200126, scan(10, ColumnStatistic.unknown()), TEXT,
                ConstantOperator.createNull(VarcharType.VARCHAR)));
    }

    @Test
    void testArrayTemplatesIncludeJsonEncoding() {
        ScalarOperator array = array("a\"b\\c\n", "中🙂/");
        String json = "[\"a\\\"b\\\\c\\n\", \"中🙂/\"]";
        String text = "hello";
        String[] prompts = {
                "Classify the following text into exactly one of these categories: " + json + ".\n"
                        + "Return a JSON object in this exact format: {\"labels\": [\"<chosen_category>\"]}\n"
                        + "The array must contain exactly one string that matches one of the given categories.\n"
                        + "Output only valid JSON, no markdown, no explanation.\n\nText: " + text,
                "Extract a value for each of the following keys from the text below.\nKeys: " + json
                        + "\nFor each key, extract exactly one value. If a key's value is not found, use null.\n"
                        + "Return a JSON object in this exact format: {\"response\": {\"key1\": \"value1\", \"key2\": null}}\n"
                        + "Output only valid JSON, no markdown, no explanation.\n\nText: " + text,
                "Redact personally identifiable information (PII) in the text below.\nCategories to redact: " + json
                        + "\nReplace each detected PII value with its uppercase category name in square brackets, "
                        + "e.g. [NAME], [ADDRESS], [EMAIL], [PHONE], [SSN].\n"
                        + "If no PII is found, return the original text unchanged. "
                        + "Output only the redacted text, nothing else.\n\nText: " + text
        };
        long[] ids = {200112, 200114, 200118};
        for (int i = 0; i < ids.length; i++) {
            String tokens = promptTokens(prompts[i]);
            assertTokens(tokens, estimate(ids[i], values(), ConstantOperator.createVarchar(text), array));
            assertTokens(tokens, estimate(ids[i] + 1, values(), ConstantOperator.createVarchar("model"),
                    ConstantOperator.createVarchar(text), array));
        }
        for (ScalarOperator invalid : List.of(array(), array(""), array("\u2003"),
                array("\uD800"), new ArrayOperator(new ArrayType(VarcharType.VARCHAR), true, List.of(TEXT)),
                new ArrayOperator(new ArrayType(VarcharType.VARCHAR), true,
                        List.of(ConstantOperator.createNull(VarcharType.VARCHAR))))) {
            assertUnknown(estimate(200112, values(), ConstantOperator.createVarchar(text), invalid));
        }
    }

    @Test
    void testArrayEscapingUsesBeJsonContract() {
        String value = "x\u0000\b\t\n\f\r\u001f\"\\/<>&\u2028\u2029🙂"; // Include Unicode line/paragraph separators.
        String json = "[\"x\\u0000\\b\\t\\n\\f\\r\\u001f\\\"\\\\/<>&\u2028\u2029🙂\"]"; // BE keeps these as UTF-8.
        // CLASSIFY's fixed template is 289 bytes; the text is five bytes, plus 16 framing tokens.
        assertTokens(String.valueOf(289 + 5 + 16 + json.getBytes(StandardCharsets.UTF_8).length),
                estimate(200112, values(), ConstantOperator.createVarchar("hello"), array(value)));
    }

    @Test
    void testTranslateBranchesAndNullPolicies() {
        String auto = "Translate the following text into 中文. Auto-detect the source language. "
                + "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: hello";
        assertPrompt(200120, auto, "hello", "", "中文");
        assertTokens(promptTokens(auto), estimate(200120, values(), ConstantOperator.createVarchar("hello"),
                ConstantOperator.createNull(VarcharType.VARCHAR), ConstantOperator.createVarchar("中文")));
        assertTokens(promptTokens(auto), estimate(200121, values(), ConstantOperator.createVarchar("model"),
                ConstantOperator.createVarchar("hello"), ConstantOperator.createNull(VarcharType.VARCHAR),
                ConstantOperator.createVarchar("中文")));
        assertPrompt(200120, "Translate the following text from   into 中文. "
                + "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: hello",
                "hello", " ", "中文");
        assertPrompt(200120, "Translate the following text from English into \u2003. "
                + "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: hello",
                "hello", "English", "\u2003");
        for (ConstantOperator target : List.of(ConstantOperator.createNull(VarcharType.VARCHAR),
                ConstantOperator.createVarchar(" \t\r\n\f\u000b"))) {
            assertTokens("0", estimate(200120, scan(10, ColumnStatistic.unknown()), TEXT,
                    ConstantOperator.createNull(VarcharType.VARCHAR), target));
        }
        // A source column may mix auto-detect and explicit-source rows. Reserve the larger fixed template.
        assertTokens(new BigInteger(promptTokens(auto)).add(BigInteger.valueOf(8)).multiply(BigInteger.TWO).toString(),
                estimate(200120, scan(2, ColumnStatistic.builder().setAverageRowSize(2).build()),
                        ConstantOperator.createVarchar("hello"), TEXT, ConstantOperator.createVarchar("中文")));
    }

    @Test
    void testSemanticInputsFollowProjectionAliases() {
        OptExpression project = OptExpression.create(new PhysicalProjectOperator(
                Map.of(TEXT, ConstantOperator.createVarchar("")), Map.of()), values());
        assertTokens("190", estimate(200120, project, ConstantOperator.createVarchar("hello"), TEXT,
                ConstantOperator.createVarchar("中文")));
        OptExpression scan = scan(2, ColumnStatistic.builder().setAverageRowSize(0.5).build());
        assertTokens("260", estimate(200124, scan, TEXT));
        assertTokens("266", estimate(200124, scan, concat(TEXT, ConstantOperator.createVarchar("中"))));
        ColumnRefOperator arrayAlias = new ColumnRefOperator(3, new ArrayType(VarcharType.VARCHAR), "keys", false);
        OptExpression arrayProject = OptExpression.create(new PhysicalProjectOperator(
                Map.of(arrayAlias, array("name", "age")), Map.of()), values());
        Assertions.assertEquals(estimate(200114, values(), ConstantOperator.createVarchar("hello"), array("name", "age"))
                        .getTokens(), estimate(200114, arrayProject, ConstantOperator.createVarchar("hello"), arrayAlias)
                        .getTokens());
    }

    private static ArrayOperator array(String... values) {
        return new ArrayOperator(new ArrayType(VarcharType.VARCHAR), false,
                Arrays.stream(values).map(ConstantOperator::createVarchar)
                        .map(ScalarOperator.class::cast).toList());
    }

    private static String promptTokens(String prompt) {
        return String.valueOf(prompt.getBytes(StandardCharsets.UTF_8).length + 16);
    }

    private static void assertPrompt(long id, String prompt, String... inputs) {
        ScalarOperator[] arguments = Arrays.stream(inputs).map(ConstantOperator::createVarchar)
                .toArray(ScalarOperator[]::new);
        assertTokens(promptTokens(prompt), estimate(id, values(), arguments));
        ScalarOperator[] explicitModel = new ScalarOperator[arguments.length + 1];
        explicitModel[0] = ConstantOperator.createVarchar("model");
        System.arraycopy(arguments, 0, explicitModel, 1, arguments.length);
        assertTokens(promptTokens(prompt), estimate(id + 1, values(), explicitModel));
    }

    private static CallOperator concat(ScalarOperator... arguments) {
        Function builtin = Mockito.mock(Function.class);
        Mockito.when(builtin.getBinaryType()).thenReturn(TFunctionBinaryType.BUILTIN);
        return new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(arguments), builtin);
    }

    private static OptExpression values() {
        return OptExpression.create(new PhysicalValuesOperator(List.of(), List.of(List.of()),
                Operator.DEFAULT_LIMIT, null, null));
    }

    private static OptExpression scan(double rows, ColumnStatistic statistic) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.isNativeTable()).thenReturn(true);
        Mockito.when(table.getBaseIndexMetaId()).thenReturn(1L);
        OptExpression expression = OptExpression.create(new PhysicalOlapScanOperator(table,
                Map.of(TEXT, new Column("text", VarcharType.VARCHAR)), DistributionSpec.createAnyDistributionSpec(),
                Operator.DEFAULT_LIMIT, null, 1, List.of(), List.of(), List.of(), List.of(), null, false, null));
        expression.setStatistics(Statistics.builder().setOutputRowCount(rows).setStatsSource(Statistics.StatsSource.ANALYZE)
                .addColumnStatistic(TEXT, statistic).build());
        return expression;
    }

    private static AIInputTokenEstimate estimate(long functionId, OptExpression input, ScalarOperator... arguments) {
        Function function = Mockito.mock(Function.class);
        Mockito.when(function.isAi()).thenReturn(true);
        Mockito.when(function.getFunctionId()).thenReturn(functionId);
        CallOperator call = new CallOperator("ai", VarcharType.VARCHAR, List.of(arguments), function);
        OptExpression ai = OptExpression.create(new PhysicalAIProjectOperator(Map.of(OUTPUT, call), Map.of()), input);
        return AIInputTokenEstimator.estimate(ai, false);
    }

    private static void assertTokens(String expected, AIInputTokenEstimate estimate) {
        Assertions.assertEquals(AIInputTokenEstimate.Status.ESTIMATED, estimate.getStatus(), estimate.toString());
        Assertions.assertEquals(new BigInteger(expected), estimate.getTokens());
    }

    private static void assertUnknown(AIInputTokenEstimate estimate) {
        Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, estimate.getStatus(), estimate.toString());
    }
}
