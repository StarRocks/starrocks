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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.InvertedIndexParams.IndexParamsKey;
import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.InvertedIndexParams.SearchParamsKey;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.IndexAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.StringType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;

public class GINIndexTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.enable_experimental_gin = true;
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `test_index_tbl` (\n" +
                "  `f1` int NOT NULL COMMENT \"\",\n" +
                "  `f2` string NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`f1`)\n" +
                "DISTRIBUTED BY HASH(`f1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `docs` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  INDEX idx_content (`content`) USING GIN(\"imp_lib\" = \"clucene\", \"parser\" = \"standard\", " +
                "\"support_phrase\" = \"true\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // A second table whose GIN index does NOT enable support_phrase. MATCH_PHRASE queries
        // against `docs_no_phrase.content` must be rejected by the FE analyzer.
        starRocksAssert.withTable("CREATE TABLE `docs_no_phrase` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  INDEX idx_content (`content`) USING GIN(\"imp_lib\" = \"clucene\", \"parser\" = \"english\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // A third table without any GIN index at all. MATCH_PHRASE must still be rejected.
        starRocksAssert.withTable("CREATE TABLE `docs_no_index` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // A builtin GIN index cannot serve MATCH_PHRASE because the builtin implementation
        // does not store term positions. The FE must surface a dedicated error.
        starRocksAssert.withTable("CREATE TABLE `docs_builtin` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  INDEX idx_content (`content`) USING GIN(\"imp_lib\" = \"builtin\", \"parser\" = \"english\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // A clucene GIN index with parser=none is not tokenized and therefore cannot serve
        // MATCH_PHRASE. The FE must surface a dedicated error.
        starRocksAssert.withTable("CREATE TABLE `docs_parser_none` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  INDEX idx_content (`content`) USING GIN(\"imp_lib\" = \"clucene\", \"parser\" = \"none\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // GIN index on `tag` but NOT on `content`. MATCH_PHRASE on `content` must be rejected
        // because the GIN-on-another-column case should not silently fall back to a non-GIN
        // execution; it must surface the dedicated FE error.
        starRocksAssert.withTable("CREATE TABLE `docs_gin_other_col` (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `tag` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `content` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  INDEX idx_tag (`tag`) USING GIN(\"imp_lib\" = \"clucene\", \"parser\" = \"english\", " +
                "\"support_phrase\" = \"true\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testCheckInvertedIndex() {
        Column c1 = new Column("f1", ArrayType.ARRAY_FLOAT, true);

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c1, new HashMap<>(), KeysType.UNIQUE_KEYS),
                "The inverted index can only be build on DUPLICATE/PRIMARY_KEYS table.");

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c1, new HashMap<>(), KeysType.DUP_KEYS),
                "The inverted index can only be build on column with type of CHAR/STRING/VARCHAR type.");

        Column c2 = new Column("f2", StringType.STRING, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), "???");
                }}, KeysType.DUP_KEYS),
                "Only support clucene or builtin implement for now. ");

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, "french");
                }}, KeysType.DUP_KEYS));

        Column c3 = new Column("f3", FloatType.FLOAT, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c3, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_CHINESE);
                }}, KeysType.DUP_KEYS));

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put("xxx", "yyy");
                }}, KeysType.DUP_KEYS));

        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_CHINESE);
                    put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
                    put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
                    put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
                    put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
                }}, KeysType.DUP_KEYS));

        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        // Without imp_lib in shared-data mode should default to builtin.
        HashMap<String, String> properties = new HashMap<>();
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, properties, KeysType.DUP_KEYS));
        Assertions.assertEquals(InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT),
                properties.get(IMP_LIB.name().toLowerCase(Locale.ROOT)));

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                }}, KeysType.DUP_KEYS),
                "Clucene inverted index does not support shared data mode");

        // Builtin implementation is allowed in shared-data mode.
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.BUILTIN.name());
                }}, KeysType.DUP_KEYS));
    }

    @Test
    public void testCheckInvertedIndexLowerCasesPropertyKeys() {
        Column c = new Column("f2", StringType.STRING, true);

        Map<String, String> upperCaseKey = new HashMap<>();
        upperCaseKey.put(IMP_LIB.name().toUpperCase(Locale.ROOT), InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        Assertions.assertDoesNotThrow(() -> IndexAnalyzer.checkInvertedIndexValid(c, upperCaseKey, KeysType.DUP_KEYS));
        Assertions.assertEquals(InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT),
                upperCaseKey.get(IMP_LIB.name().toLowerCase(Locale.ROOT)));
        Assertions.assertFalse(upperCaseKey.containsKey(IMP_LIB.name().toUpperCase(Locale.ROOT)));

        // Values stay untouched: only keys are case-folded.
        Map<String, String> mixedCaseKeys = new HashMap<>();
        mixedCaseKeys.put("Imp_Lib", InvertedIndexImpType.BUILTIN.name().toUpperCase(Locale.ROOT));
        mixedCaseKeys.put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY.toUpperCase(Locale.ROOT),
                IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
        Assertions.assertDoesNotThrow(() -> IndexAnalyzer.checkInvertedIndexValid(c, mixedCaseKeys, KeysType.DUP_KEYS));
        Assertions.assertEquals(InvertedIndexImpType.BUILTIN.name().toUpperCase(Locale.ROOT),
                mixedCaseKeys.get(IMP_LIB.name().toLowerCase(Locale.ROOT)));
        Assertions.assertEquals(IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH,
                mixedCaseKeys.get(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY));

        // dict_gram_num needs the folding (BE uses an exact find()); lower_case only pins the stored spelling.
        Map<String, String> upperCaseIndexParams = new HashMap<>();
        upperCaseIndexParams.put(IMP_LIB.name().toUpperCase(Locale.ROOT),
                InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        upperCaseIndexParams.put(IndexParamsKey.DICT_GRAM_NUM.name().toUpperCase(Locale.ROOT), "4");
        upperCaseIndexParams.put(IndexParamsKey.PARSER.name().toUpperCase(Locale.ROOT),
                IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
        upperCaseIndexParams.put(IndexAnalyzer.INVERTED_INDEX_LOWER_CASE_KEY.toUpperCase(Locale.ROOT), "true");
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(c, upperCaseIndexParams, KeysType.DUP_KEYS));
        Assertions.assertEquals("4", upperCaseIndexParams.get(IndexAnalyzer.INVERTED_INDEX_DICT_GRAM_NUM_KEY));
        Assertions.assertEquals("true", upperCaseIndexParams.get(IndexAnalyzer.INVERTED_INDEX_LOWER_CASE_KEY));
        Assertions.assertTrue(upperCaseIndexParams.keySet().stream()
                .allMatch(key -> key.equals(key.toLowerCase(Locale.ROOT))));

        Map<String, String> collidingKeys = new HashMap<>();
        collidingKeys.put(IMP_LIB.name().toUpperCase(Locale.ROOT), InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT));
        collidingKeys.put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name().toLowerCase(Locale.ROOT));
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Duplicated index property for GIN after lower-casing the key: " + IMP_LIB.name().toLowerCase(Locale.ROOT),
                () -> IndexAnalyzer.checkInvertedIndexValid(c, collidingKeys, KeysType.DUP_KEYS));
    }

    @Test
    public void testUpperCaseImpLibReachesBeAsLowerCaseKey() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t_upper_case_imp_lib` (\n" +
                "  `k` int NOT NULL COMMENT \"\",\n" +
                "  `v` varchar(50) NOT NULL COMMENT \"\",\n" +
                "  INDEX gin_v (`v`) USING GIN(\"IMP_LIB\" = \"builtin\", \"PARSER\" = \"english\", " +
                "\"DICT_GRAM_NUM\" = \"4\")\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "t_upper_case_imp_lib");
        TOlapTableIndex olapIndex = table.getIndexes().get(0).toThrift();
        // BE fails the load with "Can not get inverted imp type" unless the key lands here exactly as imp_lib.
        Assertions.assertEquals(InvertedIndexImpType.BUILTIN.name().toLowerCase(Locale.ROOT),
                olapIndex.getCommon_properties().get(IMP_LIB.name().toLowerCase(Locale.ROOT)));
        Assertions.assertEquals(IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH,
                olapIndex.getIndex_properties().get(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY));
        // BE reads the gram num with an exact find() as well, so an upper-case key silently disables the dictionary.
        Assertions.assertEquals("4", olapIndex.getIndex_properties().get(IndexAnalyzer.INVERTED_INDEX_DICT_GRAM_NUM_KEY));
        Assertions.assertTrue(olapIndex.getExtra_properties().isEmpty());
        starRocksAssert.dropTable("t_upper_case_imp_lib");
    }

    @Test
    public void testIndexToThrift() {
        int indexId = 0;
        String indexName = "test_index";
        List<ColumnId> columns = Collections.singletonList(ColumnId.create("f1"));

        Index index = new Index(indexId, indexName, columns, IndexType.GIN, "", new HashMap<>() {{
            put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
            put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
            put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
            put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
            put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
        }});

        index.hashCode();

        TOlapTableIndex olapIndex = index.toThrift();
        Assertions.assertEquals(indexId, olapIndex.getIndex_id());
        Assertions.assertEquals(indexName, olapIndex.getIndex_name());
        Assertions.assertEquals(TIndexType.GIN, olapIndex.getIndex_type());
        Assertions.assertEquals(Lists.newArrayList("f1"), olapIndex.getColumns());
        Assertions.assertEquals(
                Collections.singletonMap(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name()),
                olapIndex.getCommon_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
        }}, olapIndex.getIndex_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
        }}, olapIndex.getIndex_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
            put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
            put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
        }}, olapIndex.getSearch_properties());
    }

    @Test
    public void testMatchExpr() {
        SlotRef slot = new SlotRef(null, null, null);
        StringLiteral stringExpr = new StringLiteral("test");
        MatchExpr expr = new MatchExpr(slot, stringExpr);
        MatchExpr newMatch = (MatchExpr) expr.clone();
        MatchExpr phraseExpr = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, slot, stringExpr);
        Assertions.assertEquals(MatchExpr.MatchOperator.MATCH_PHRASE, phraseExpr.getMatchOperator());
    }

    @Test
    public void testMatchPhraseParseAnalyzeAndPlan() throws Exception {
        String sql = "SELECT * FROM docs WHERE content MATCH_PHRASE 'inverted index'";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertNotNull(statement);

        String plan = getFragmentPlan(sql);
        assertContains(plan, "content MATCH_PHRASE 'inverted index'");
    }

    @Test
    public void testMatchPhraseAnalyzeInvalidOperands() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "left operand of MATCH must be of type STRING with NOT NULL",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs WHERE id MATCH_PHRASE 'inverted index'", connectContext));
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "right operand of MATCH must be of type StringLiteral with NOT NULL",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs WHERE content MATCH_PHRASE 1", connectContext));
    }

    @Test
    public void testSupportPhraseDdlValidation() {
        Column col = new Column("f2", StringType.STRING, true);

        // support_phrase=true requires imp_lib=clucene (default is clucene, so omitting imp_lib is OK)
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "true");
                }}, KeysType.DUP_KEYS));

        // support_phrase=true + imp_lib=builtin must fail
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.BUILTIN.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "true");
                }}, KeysType.DUP_KEYS),
                "support_phrase is only supported when imp_lib = clucene");

        // support_phrase=true + parser=none must fail (positions on untokenized fields are meaningless)
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_NONE);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "true");
                }}, KeysType.DUP_KEYS),
                "support_phrase=true requires a tokenizing parser");

        // Illegal value (neither true nor false) must fail
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "yes");
                }}, KeysType.DUP_KEYS),
                "INVERTED index support_phrase should be true or false");

        // support_phrase=false is always OK (matches the backward-compat default for old DDLs that
        // omit the key entirely)
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "false");
                }}, KeysType.DUP_KEYS));
    }

    @Test
    public void testMatchPhraseRejectedWithoutSupportPhrase() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_phrase WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    @Test
    public void testMatchPhraseRejectedWithoutGinIndex() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "MATCH_PHRASE requires a GIN index on column 'content'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_index WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    @Test
    public void testMatchPhraseRejectedOnBuiltinImpl() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "is only supported when imp_lib = 'clucene'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_builtin WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    @Test
    public void testMatchPhraseRejectedOnParserNone() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires a tokenizing parser",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_parser_none WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    @Test
    public void testMatchPhraseAcceptedWithSupportPhrase() throws Exception {
        ExceptionChecker.expectThrowsNoException(
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    // The FE validator must still reject MATCH_PHRASE on a column whose table has a GIN index
    // on a DIFFERENT column. Without this check the query would be silently translated and
    // either return wrong results or fail at the BE with a generic "no GIN index" error.
    @Test
    public void testMatchPhraseRejectedOnOtherColumnGinIndex() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "MATCH_PHRASE requires a GIN index on column 'content'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_gin_other_col WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    // NOT MATCH_PHRASE parses into CompoundPredicate(NOT, MatchExpr(...)). Without
    // descending into the wrapped MatchExpr the analyzer would silently skip the
    // support_phrase check and let the request reach the BE.
    @Test
    public void testMatchPhraseValidationRunsUnderNotPrefix() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_phrase WHERE NOT (content MATCH_PHRASE 'inverted index')",
                        connectContext));
    }

    // AND-combined predicates: the support_phrase check has to run for the MATCH_PHRASE
    // operand even when sandwiched between other predicates. The first MATCH (no phrase
    // requirement) must not short-circuit validation of the second one.
    @Test
    public void testMatchPhraseValidationRunsInsideAndPredicate() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_phrase WHERE content MATCH 'inverted' "
                                + "AND content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    // OR-combined predicates: same as AND but the MATCH_PHRASE is the first operand. We
    // assert the analyzer descends into both branches of CompoundPredicate.
    @Test
    public void testMatchPhraseValidationRunsInsideOrPredicate() {
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_phrase WHERE content MATCH_PHRASE 'inverted index' "
                                + "OR id = 1",
                        connectContext));
    }

    // Multi-table (JOIN) resolution: the SlotRef must walk to the correct underlying OlapTable
    // even when two relations have a column of the same physical name. We use table-qualified
    // slots without aliases so that the FE validator can look the table up by its real name
    // (resolving aliases to their underlying OlapTable is currently a silent-skip path that
    // is intentionally out of scope for this PR).
    @Test
    public void testMatchPhraseWithMultipleTablesInFromClause() throws Exception {
        // Positive: docs.content has support_phrase=true → accepted.
        ExceptionChecker.expectThrowsNoException(
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT docs.id FROM docs, docs_no_phrase "
                                + "WHERE docs.id = docs_no_phrase.id "
                                + "AND docs.content MATCH_PHRASE 'inverted index'",
                        connectContext));

        // Negative: docs_no_phrase.content has support_phrase=false → rejected even though
        // another relation in the same FROM clause (docs) has a matching column with
        // support_phrase=true. This guards against the analyzer accidentally picking up
        // the wrong relation's GIN properties.
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT docs.id FROM docs, docs_no_phrase "
                                + "WHERE docs.id = docs_no_phrase.id "
                                + "AND docs_no_phrase.content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    // Pins down the case-insensitive value handling in IndexAnalyzer.checkInvertedIndexSupportPhrase.
    // FE/BE both call equalsIgnoreCase/to_lower_copy, so mixed-case "TRUE"/"True" must be
    // accepted at DDL time, otherwise older or hand-edited DDLs would silently lose phrase
    // support.
    @Test
    public void testSupportPhraseDdlAcceptsMixedCaseValue() {
        Column col = new Column("f2", StringType.STRING, true);
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "TRUE");
                }}, KeysType.DUP_KEYS));
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "True");
                }}, KeysType.DUP_KEYS));
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkInvertedIndexValid(col, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY, IndexAnalyzer.INVERTED_INDEX_PARSER_ENGLISH);
                    put(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY, "False");
                }}, KeysType.DUP_KEYS));
    }

    // Locks the public-contract default for SUPPORT_PHRASE: when the DDL does not set the
    // property, the FE behaves as if it were "false" and rejects MATCH_PHRASE. This is the
    // contract that matters end-to-end (the BE side asserts the symmetric default in
    // get_support_phrase_from_properties). Changing this default would silently flip the
    // storage format on upgrade and risk crashing the BE when reading old segments.
    @Test
    public void testSupportPhraseDefaultsToFalse() throws Exception {
        // Pin the key spelling: the FE constant and the enum name must agree so the FE/BE
        // BE round-trip stays consistent.
        Assertions.assertEquals("support_phrase",
                IndexParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT));
        Assertions.assertEquals(IndexAnalyzer.INVERTED_INDEX_SUPPORT_PHRASE_KEY,
                IndexParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT));

        // End-to-end default check: a GIN index that omits support_phrase must reject
        // MATCH_PHRASE. docs_no_phrase is created without the property in beforeClass,
        // simulating an older index that predates this feature.
        ExceptionChecker.expectThrowsWithMsg(Exception.class,
                "requires the GIN index to be created with 'support_phrase' = 'true'",
                () -> UtFrameUtils.parseStmtWithNewParser(
                        "SELECT * FROM docs_no_phrase WHERE content MATCH_PHRASE 'inverted index'",
                        connectContext));
    }

    // Smoke test for the SqlToScalarOperatorTranslator path: a MATCH_PHRASE in SQL must
    // produce a MatchExprOperator (rendered as "<col> MATCH_PHRASE 'literal'" by
    // ExpressionPrinter) in the planner output. This covers the AST -> ScalarOperator
    // translation step that has otherwise no direct UT.
    @Test
    public void testMatchPhraseTranslatesToMatchExprOperatorInPlan() throws Exception {
        String plan = getFragmentPlan(
                "SELECT * FROM docs WHERE content MATCH_PHRASE 'inverted index'");
        // The fragment plan prints the scalar predicate via ExpressionPrinter, which
        // formats MatchExprOperator as `<col> MATCH_PHRASE '<literal>'`.
        assertContains(plan, "MATCH_PHRASE 'inverted index'");
        // The keyword must NOT be downgraded to plain MATCH/MATCH_ANY/MATCH_ALL.
        Assertions.assertFalse(plan.contains("MATCH 'inverted index'"),
                "MATCH_PHRASE must not be lowered to MATCH; full plan was:\n" + plan);
        Assertions.assertFalse(plan.contains("MATCH_ALL 'inverted index'"));
        Assertions.assertFalse(plan.contains("MATCH_ANY 'inverted index'"));
    }

    @Test
    public void testGINWithAutoIncrement() throws Exception {
        // Test builtin GIN with AUTO_INCREMENT and replicated_storage = true (Should succeed)
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.withTable(
                "CREATE TABLE `t_builtin` (" +
                        "  `k` BIGINT AUTO_INCREMENT," +
                        "  `msg_all` varchar(100)," +
                        "  INDEX idx_msg_all (`msg_all`) USING GIN(\"imp_lib\" = \"builtin\", \"parser\" = \"standard\")" +
                        ") ENGINE=OLAP " +
                        "DUPLICATE KEY(`k`) " +
                        "DISTRIBUTED BY HASH(`k`) BUCKETS 1 " +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"replicated_storage\" = \"true\" );"));
        starRocksAssert.dropTable("t_builtin");

        // Test clucene GIN with AUTO_INCREMENT and replicated_storage = true (Should fail)
        // because OlapTableFactory will force replicated_storage to false for clucene GIN
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Table with AUTO_INCREMENT column must use Replicated Storage",
                () -> starRocksAssert.withTable(
                        "CREATE TABLE `t_clucene` (" +
                                "  `k` BIGINT AUTO_INCREMENT," +
                                "  `msg_all` varchar(100)," +
                                "  INDEX idx_msg_all (`msg_all`) USING GIN(\"imp_lib\" = \"clucene\", \"parser\" = \"standard\")" +
                                ") ENGINE=OLAP " +
                                "DUPLICATE KEY(`k`) " +
                                "DISTRIBUTED BY HASH(`k`) BUCKETS 1 " +
                                "PROPERTIES ( \"replication_num\" = \"1\", \"replicated_storage\" = \"true\" );"));

        // Test builtin GIN with AUTO_INCREMENT and replicated_storage = false (Should fail)
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Table with AUTO_INCREMENT column must use Replicated Storage",
                () -> starRocksAssert.withTable(
                        "CREATE TABLE `t_builtin_no_rs` (" +
                                "  `k` BIGINT AUTO_INCREMENT," +
                                "  `msg_all` varchar(100)," +
                                "  INDEX idx_msg_all (`msg_all`) USING GIN(\"imp_lib\" = \"builtin\", \"parser\" = \"standard\")" +
                                ") ENGINE=OLAP " +
                                "DUPLICATE KEY(`k`) " +
                                "DISTRIBUTED BY HASH(`k`) BUCKETS 1 " +
                                "PROPERTIES ( \"replication_num\" = \"1\", \"replicated_storage\" = \"false\" );"));
    }

    @Test
    public void testMaterializedViewGINIndexProperties() throws Exception {
        // Create a MV with GIN index on the DUP_KEYS table
        String mvSql = "create materialized view test_mv_gin_index " +
                "(f1, f2, " +
                "INDEX gin_idx1 (`f2`) USING GIN" +
                ") " +
                "DISTRIBUTED BY HASH(`f1`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "(" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select f1, f2 from test_index_tbl;";

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(mvSql, connectContext);
        Assertions.assertInstanceOf(CreateMaterializedViewStatement.class, stmt);
        CreateMaterializedViewStatement createMVStmt = (CreateMaterializedViewStatement) stmt;

        // Verify that the MV indexes have properties (not empty)
        List<Index> mvIndexes = createMVStmt.getMvIndexes();
        Assertions.assertEquals(1, mvIndexes.size());

        Index ginIndex = mvIndexes.get(0);
        Assertions.assertEquals("gin_idx1", ginIndex.getIndexName());
        Assertions.assertEquals(IndexType.GIN, ginIndex.getIndexType());

        // Key assertion: properties should NOT be empty
        java.util.Map<String, String> properties = ginIndex.getProperties();
        Assertions.assertNotNull(properties, "Index properties should not be null");
        Assertions.assertFalse(properties.isEmpty(), "Index properties should not be empty");

        // Verify imp_lib default property is present
        Assertions.assertTrue(
                properties.containsKey(IMP_LIB.name().toLowerCase(Locale.ROOT)),
                "Index properties should contain imp_lib");
        Assertions.assertEquals(
                InvertedIndexImpType.CLUCENE.toString().toLowerCase(),
                properties.get(IMP_LIB.name().toLowerCase(Locale.ROOT)),
                "imp_lib should default to clucene");

        // Verify parser default property is present
        Assertions.assertTrue(
                properties.containsKey(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY),
                "Index properties should contain parser");
        Assertions.assertEquals(
                IndexAnalyzer.INVERTED_INDEX_PARSER_NONE,
                properties.get(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY),
                "parser should default to none");

        // Verify properties are correctly passed to Thrift object
        TOlapTableIndex olapIndex = ginIndex.toThrift();
        Assertions.assertEquals(
                InvertedIndexImpType.CLUCENE.toString().toLowerCase(),
                olapIndex.getCommon_properties().get(IMP_LIB.name().toLowerCase(Locale.ROOT)),
                "Thrift common_properties should contain imp_lib with default value");
        Assertions.assertEquals(
                IndexAnalyzer.INVERTED_INDEX_PARSER_NONE,
                olapIndex.getIndex_properties().get(IndexAnalyzer.INVERTED_INDEX_PARSER_KEY),
                "Thrift index_properties should contain parser with default value");
    }

    @Test
    public void testMaterializedViewGINIndexGetsDistinctIndexId() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `mv_index_base` (\n" +
                "  `f1` int NOT NULL COMMENT \"\",\n" +
                "  `f2` varchar(200) NOT NULL COMMENT \"\",\n" +
                "  `f3` varchar(200) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`f1`)\n" +
                "DISTRIBUTED BY HASH(`f1`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_two_gin\n" +
                "(f1, f2, f3,\n" +
                " INDEX gin_idx1 (`f2`) USING GIN,\n" +
                " INDEX gin_idx2 (`f3`) USING GIN)\n" +
                "DISTRIBUTED BY HASH(f1) BUCKETS 1\n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT f1, f2, f3 FROM mv_index_base;");

        OlapTable mv = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "mv_two_gin");
        List<Index> mvIndexes = mv.getIndexes();
        Assertions.assertEquals(2, mvIndexes.size());
        // A GIN index must carry a real id: it goes into the tablet schema pushed to BE and, for the
        // CLucene backend, into the on-disk index file name.
        for (Index index : mvIndexes) {
            Assertions.assertTrue(index.getIndexId() >= 0,
                    "GIN index " + index.getIndexName() + " should get a valid index id, got "
                            + index.getIndexId());
        }
        // Two indexes sharing an id would resolve to the same .ivt directory and clobber each other.
        Assertions.assertNotEquals(mvIndexes.get(0).getIndexId(), mvIndexes.get(1).getIndexId(),
                "two GIN indexes on one materialized view must not share an index id");
    }
}
