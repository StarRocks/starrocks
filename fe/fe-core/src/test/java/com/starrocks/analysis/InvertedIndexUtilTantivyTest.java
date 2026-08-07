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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.InvertedIndexParams.SearchParamsKey;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;

/**
 * Tantivy-specific validation tests for {@link InvertedIndexUtil}, separate
 * from {@link GINIndexTest} so that adding tantivy enum values and the
 * support_phrase / support_bm25 / table-model checks does not perturb the
 * existing CLucene/Builtin coverage.
 */
public class InvertedIndexUtilTantivyTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.enable_experimental_gin = true;
        PlanTestBase.beforeClass();
    }

    private static String impLibKey() {
        return IMP_LIB.name().toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> tantivyProps() {
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.TANTIVY.name().toLowerCase(Locale.ROOT));
        return props;
    }

    @Test
    public void tantivyOnDupKeysVarchar_passes() {
        Column col = new Column("txt", Type.STRING, true);
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, tantivyProps(), KeysType.DUP_KEYS));
    }

    @Test
    public void tantivyOnPrimaryKeysTable_passes() {
        Column col = new Column("txt", Type.STRING, true);
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, tantivyProps(), KeysType.PRIMARY_KEYS));
    }

    @Test
    public void unknownImpLib_isRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), "lucene9");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().toLowerCase().contains("clucene"),
                "expected supported impls listed in message: " + ex.getMessage());
    }

    @Test
    public void supportPhraseOnTantivy_passes() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT), "true");
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertEquals("true",
                props.get(SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT)));
    }

    @Test
    public void supportPhraseOnClucene_isRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.CLUCENE.name().toLowerCase(Locale.ROOT));
        props.put(SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT), "true");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("support_phrase"),
                "expected support_phrase in message: " + ex.getMessage());
    }

    @Test
    public void supportBm25OnBuiltin_isRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        props.put(SearchParamsKey.SUPPORT_BM25.name().toLowerCase(Locale.ROOT), "true");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("support_bm25"),
                "expected support_bm25 in message: " + ex.getMessage());
    }

    @Test
    public void tantivyInSharedDataMode_isAllowed() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Column col = new Column("txt", Type.STRING, true);
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, tantivyProps(), KeysType.DUP_KEYS));
    }

    @Test
    public void cjkParserOnTantivy_normalizedToChinese() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "cjk");
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertEquals("chinese",
                props.get(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY),
                "cjk should be normalized to chinese");
    }

    @Test
    public void cjkParserOnClucene_isRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.CLUCENE.name().toLowerCase(Locale.ROOT));
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "cjk");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("cjk"),
                "expected cjk in message: " + ex.getMessage());
    }

    @Test
    public void ikParserDefaultsToMaxWord() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ik");
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertEquals("ik_max_word",
                props.get(InvertedIndexUtil.INVERTED_INDEX_PARSER_MODE_KEY));
    }

    @Test
    public void ikSmartParserModePasses() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ik");
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_MODE_KEY, "ik_smart");
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
    }

    @Test
    public void invalidIkParserModeIsRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ik");
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_MODE_KEY, "search");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("ik_max_word"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("ik_smart"), ex.getMessage());
    }

    @Test
    public void ikParserOnBuiltinIsRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ik");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("tantivy"), ex.getMessage());
    }

    @Test
    public void parserModeOnNonIkParserIsRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "jieba");
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_MODE_KEY, "ik_smart");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("only supported"), ex.getMessage());
    }

    @Test
    public void ngramParserWithValidRangePasses() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ngram");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MIN_GRAM_KEY, "2");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MAX_GRAM_KEY, "3");
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
    }

    @Test
    public void ngramParserRequiresBothBounds() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ngram");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MIN_GRAM_KEY, "2");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("both min_gram and max_gram"), ex.getMessage());
    }

    @Test
    public void ngramParserRejectsInvalidBounds() {
        Column col = new Column("txt", Type.STRING, true);
        for (String[] bounds : new String[][] {
                {"0", "2"},
                {"3", "2"},
                {"two", "3"},
                {"2", "999999999999999999999"},
        }) {
            Map<String, String> props = tantivyProps();
            props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ngram");
            props.put(InvertedIndexUtil.INVERTED_INDEX_MIN_GRAM_KEY, bounds[0]);
            props.put(InvertedIndexUtil.INVERTED_INDEX_MAX_GRAM_KEY, bounds[1]);
            Assertions.assertThrows(SemanticException.class,
                    () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS),
                    "bounds should be rejected: " + bounds[0] + ", " + bounds[1]);
        }
    }

    @Test
    public void ngramParserOnBuiltinIsRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        props.put(impLibKey(), InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "ngram");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MIN_GRAM_KEY, "2");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MAX_GRAM_KEY, "3");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("tantivy"), ex.getMessage());
    }

    @Test
    public void ngramBoundsOnOtherParserAreRejected() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        props.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "jieba");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MIN_GRAM_KEY, "2");
        props.put(InvertedIndexUtil.INVERTED_INDEX_MAX_GRAM_KEY, "3");
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex.getMessage().contains("only supported"), ex.getMessage());
    }

    @Test
    public void supportPhraseDefaultFilled_forTantivy() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        String spKey = SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT);
        Assertions.assertEquals("true", props.get(spKey),
                "support_phrase should be auto-filled with 'true' for tantivy");
    }

    @Test
    public void supportBm25DefaultFilled_forTantivy() {
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = tantivyProps();
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        String bm25Key = SearchParamsKey.SUPPORT_BM25.name().toLowerCase(Locale.ROOT);
        Assertions.assertEquals("true", props.get(bm25Key),
                "support_bm25 should be auto-filled with 'true' for tantivy");
    }

    @Test
    public void sharedDataDefault_isStillBuiltin_notTantivy() {
        // Verify the default-when-unspecified behavior is unchanged.
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Column col = new Column("txt", Type.STRING, true);
        Map<String, String> props = new HashMap<>();
        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(col, props, KeysType.DUP_KEYS));
        Assertions.assertEquals(
                InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT),
                props.get(impLibKey()),
                "shared-data default should remain BUILTIN, not TANTIVY");
    }
}
