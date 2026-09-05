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

package com.starrocks.catalog;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTextAnalyzerStmt;
import com.starrocks.sql.ast.DescTextAnalyzerStmt;
import com.starrocks.sql.ast.DropTextAnalyzerStmt;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.ShowCreateTextAnalyzerStmt;
import com.starrocks.sql.ast.ShowTextAnalyzersStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TextAnalyzerTest {
    @Test
    public void testCanonicalDefinitionAndDigest() {
        String definition = "{\"token_filter\":[{\"type\":\"lowercase\"}],"
                + "\"tokenizer\":{\"type\":\"cjk\"}}";
        TextAnalyzer.Definition first = TextAnalyzer.canonicalize(definition);
        TextAnalyzer.Definition second = TextAnalyzer.canonicalize(first.getCanonicalJson());
        Assertions.assertEquals(first.getCanonicalJson(), second.getCanonicalJson());
        Assertions.assertEquals(first.getDigest(), second.getDigest());
        Assertions.assertEquals("{\"spec_version\":1,\"runtime_abi_version\":1,"
                        + "\"builtin_model_version\":\"starrocks-tantivy-3.5-v1\",\"char_filter\":[],"
                        + "\"tokenizer\":{\"type\":\"chinese\"},\"token_filter\":[{\"type\":\"lowercase\"}],"
                        + "\"resource_refs\":[]}",
                first.getCanonicalJson());
        Assertions.assertEquals("7b054591ed8e95c775dac57c1b1a7a9e4649d420d6ee814ec269e4768aa6a8f2",
                first.getDigest());
    }

    @Test
    public void testRejectExternalResourceAndUnknownField() {
        Assertions.assertThrows(SemanticException.class, () -> TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"jieba\",\"path\":\"/tmp/dict\"}}"));
        Assertions.assertThrows(SemanticException.class, () -> TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"standard\"},"
                        + "\"resource_refs\":[{\"name\":\"dict\",\"digest\":\"x\"}]}"));
        Assertions.assertThrows(SemanticException.class, () -> TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"ngram\",\"min_gram\":1,\"max_gram\":33}}"));
        Assertions.assertThrows(SemanticException.class, () -> TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"ngram\",\"min_gram\":1.5,\"max_gram\":2}}"));
        Assertions.assertTrue(TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"ik\",\"mode\":\"ik_smart\"}}")
                .getCanonicalJson().contains("\"mode\":\"search\""));
    }

    @Test
    public void testTextAnalyzerGrammar() {
        StatementBase create = parse("CREATE TEXT ANALYZER search.product_cn PROPERTIES "
                + "(\"definition\"='{\"tokenizer\":{\"type\":\"standard\"}}')");
        Assertions.assertInstanceOf(CreateTextAnalyzerStmt.class, create);
        Assertions.assertEquals("search.product_cn", ((CreateTextAnalyzerStmt) create).getAnalyzerName());

        DropTextAnalyzerStmt drop = (DropTextAnalyzerStmt) parse(
                "DROP TEXT ANALYZER IF EXISTS search.product_cn RESTRICT");
        Assertions.assertTrue(drop.isIfExists());
        ShowTextAnalyzersStmt show = (ShowTextAnalyzersStmt) parse("SHOW TEXT ANALYZERS FROM search");
        Assertions.assertEquals(8, show.getMetaData().getColumnCount());
        Assertions.assertThrows(RuntimeException.class, () -> show.getMetaData().getColumnIdx("Version"));
        Assertions.assertInstanceOf(DescTextAnalyzerStmt.class,
                parse("DESC TEXT ANALYZER search.product_cn"));
        ShowCreateTextAnalyzerStmt showCreate = (ShowCreateTextAnalyzerStmt) parse(
                "SHOW CREATE TEXT ANALYZER search.product_cn");
        Assertions.assertEquals(2, showCreate.getMetaData().getColumnCount());

        Assertions.assertThrows(ParsingException.class, () -> parse(
                "CREATE OR REPLACE TEXT ANALYZER search.product_cn PROPERTIES "
                        + "(\"definition\"='{\"tokenizer\":{\"type\":\"standard\"}}')"));
        Assertions.assertThrows(ParsingException.class, () -> parse(
                "CREATE TEXT ANALYZER IF NOT EXISTS search.product_cn PROPERTIES "
                        + "(\"definition\"='{\"tokenizer\":{\"type\":\"standard\"}}')"));
        Assertions.assertThrows(ParsingException.class, () -> parse(
                "DROP TEXT ANALYZER search.product_cn VERSION 1 RESTRICT"));
        Assertions.assertThrows(ParsingException.class, () -> parse(
                "DESC TEXT ANALYZER search.product_cn VERSION 1"));
        Assertions.assertThrows(ParsingException.class, () -> parse(
                "SHOW CREATE TEXT ANALYZER search.product_cn VERSION 1"));
    }

    @Test
    public void testDatabaseImageRoundTrip() {
        Database database = new Database(10, "search");
        TextAnalyzer.Definition definition = TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"ik\",\"mode\":\"search\"}}");
        database.putTextAnalyzer(new TextAnalyzer(10, 10, "product_cn",
                definition.getCanonicalJson(), definition.getDigest(), 1, 1233, "root"));

        Database restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(database), Database.class);
        TextAnalyzer analyzer = restored.getTextAnalyzer("PRODUCT_CN");
        Assertions.assertNotNull(analyzer);
        Assertions.assertEquals(10, analyzer.getId());
        Assertions.assertEquals(definition.getDigest(), analyzer.getDigest());
        Assertions.assertEquals(1, restored.getTextAnalyzers().size());
        Assertions.assertNotNull(restored.removeTextAnalyzer("product_cn"));
        Assertions.assertNull(restored.getTextAnalyzer("product_cn"));
    }

    @Test
    public void testCrossDatabaseIndexDependency() {
        Database analyzerDb = new Database(10, "search");
        TextAnalyzer analyzer = new TextAnalyzer(11, 10, "product_cn", "{}", "digest", "root");
        Database tableDb = new Database(20, "serving");
        OlapTable table = new OlapTable();
        table.setName("docs");
        table.setIndexes(List.of(new Index(21, "idx_content", List.of(ColumnId.create("content")),
                IndexDef.IndexType.GIN, "", Map.of(
                        "analyzer", "search.product_cn"))));
        Assertions.assertTrue(tableDb.registerTableUnlocked(table));

        Assertions.assertEquals(List.of("serving.docs.idx_content"),
                TextAnalyzerMgr.findReferencesInDatabases(List.of(analyzerDb, tableDb), analyzerDb, analyzer));
    }

    @Test
    public void testShowCreateTableHidesAnalyzerSnapshot() {
        Index index = new Index(21, "idx_content", List.of(ColumnId.create("content")),
                IndexDef.IndexType.GIN, "", Map.of(
                        "analyzer", "search.product_cn",
                        "analyzer_definition", "{\"tokenizer\":{\"type\":\"standard\"}}",
                        "analyzer_digest", "digest",
                        "support_phrase", "true"));

        String sql = index.toSql(null);
        Assertions.assertTrue(sql.contains("\"analyzer\" = \"search.product_cn\""));
        Assertions.assertTrue(sql.contains("\"support_phrase\" = \"true\""));
        Assertions.assertFalse(sql.contains("analyzer_definition"));
        Assertions.assertFalse(sql.contains("analyzer_digest"));
        Assertions.assertFalse(sql.contains("analyzer_version"));
        Assertions.assertTrue(index.getProperties().containsKey("analyzer_definition"));
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parseSingleStatement(sql, 0);
    }
}
