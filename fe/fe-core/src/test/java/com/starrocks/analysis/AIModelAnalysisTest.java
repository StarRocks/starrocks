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

import com.starrocks.qe.RedirectStatus;
import com.starrocks.sql.analyzer.AIModelAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterAIModelStmt;
import com.starrocks.sql.ast.CreateAIModelStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.sql.formatter.AST2StringVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class AIModelAnalysisTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE AI MODEL ChatModel PROPERTIES ('capability'='CHAT', 'provider'='openai_compatible', "
                    + "'endpoint'='https://models.example.test/v1/chat/completions', 'model'='chat-model', "
                    + "'credential_ref'='TEST_MODEL')",
            "CREATE AI MODEL IF NOT EXISTS EmbeddingModel COMMENT 'embedding model' "
                    + "PROPERTIES ('capability'='TEXT_EMBEDDING', 'provider'='openai_compatible', "
                    + "'endpoint'='https://models.example.test/v1/embeddings', 'model'='embedding-model', "
                    + "'credential_ref'='TEST_EMBEDDING')",
            "ALTER AI MODEL ChatModel SET ('model'='new-model')",
            "ALTER AI MODEL IF EXISTS ChatModel SET ('endpoint'='https://models.example.test/v2/chat/completions')",
            "ALTER AI MODEL ChatModel COMMENT = ''",
            "DROP AI MODEL ChatModel",
            "DROP AI MODEL IF EXISTS ChatModel"
    })
    public void testParseAIModelDdl(String sql) {
        StatementBase statement = Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(sql, 0));
        Assertions.assertInstanceOf(DdlStmt.class, statement);
        Assertions.assertSame(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(statement));
        String formatted = new AST2StringVisitor().visit(statement);
        Assertions.assertEquals(statement.getClass(), SqlParser.parseSingleStatement(formatted, 0).getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SHOW AI MODELS",
            "SHOW AI MODELS LIKE 'Chat%'",
            "SHOW AI MODELS LIKE ''",
            "DESC AI MODEL ChatModel",
            "DESCRIBE AI MODEL ChatModel"
    })
    public void testParseAIModelShow(String sql) {
        StatementBase statement = Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(sql, 0));
        Assertions.assertInstanceOf(ShowStmt.class, statement);
        Assertions.assertSame(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(statement));
        Assertions.assertEquals(statement.getClass(),
                SqlParser.parseSingleStatement(new AST2StringVisitor().visit(statement), 0).getClass());
    }

    @Test
    public void testAIModelKeywordsRemainIdentifiers() {
        Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement("SELECT ai, model, models FROM ai", 0));
    }

    @Test
    public void testImmutableAstAndCommentClear() {
        Map<String, String> properties = new HashMap<>(Map.of("model", "original"));
        CreateAIModelStmt create = new CreateAIModelStmt(true, "MixedCase", properties, null, NodePosition.ZERO);
        properties.put("model", "mutated");
        Assertions.assertEquals("original", create.getProperties().get("model"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> create.getProperties().clear());
        Assertions.assertEquals("", create.getComment());
        AlterAIModelStmt set = (AlterAIModelStmt) SqlParser.parseSingleStatement(
                "ALTER AI MODEL MixedCase SET ('model'='next')", 0);
        AlterAIModelStmt clear = (AlterAIModelStmt) SqlParser.parseSingleStatement(
                "ALTER AI MODEL MixedCase COMMENT = ''", 0);
        Assertions.assertNull(set.getComment());
        Assertions.assertEquals("", clear.getComment());
        Assertions.assertEquals("MixedCase", set.getName());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> set.getProperties().clear());
        AlterAIModelStmt roundTrip = (AlterAIModelStmt) SqlParser.parseSingleStatement(
                new AST2StringVisitor().visit(clear), 0);
        Assertions.assertEquals("", roundTrip.getComment());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER AI MODEL m SET ('api_key'='do-not-expose')",
            "ALTER AI MODEL m SET ('endpoint'='https://user:do-not-expose@models.example.test/v1')",
            "ALTER AI MODEL m SET ('endpoint'='https://models.example.test/v1?key=do-not-expose')",
            "ALTER AI MODEL m SET ('unknown'='do-not-expose')"
    })
    public void testRejectedSensitivePropertiesAreSafeInAudit(String sql) {
        StatementBase statement = SqlParser.parseSingleStatement(sql, 0);
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> AIModelAnalyzer.analyze(statement, null));
        Assertions.assertFalse(error.getMessage().contains("do-not-expose"));
        Assertions.assertTrue(AuditEncryptionChecker.needEncrypt(statement));
        String rendered = new AST2StringVisitor().visit(statement);
        Assertions.assertFalse(rendered.contains("do-not-expose"));
        Assertions.assertTrue(rendered.contains("*XXX"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE AI MODEL m PROPERTIES ('model'='missing-required-properties')",
            "ALTER AI MODEL m SET ('provider'='openai')",
            "ALTER AI MODEL m SET ('capability'='IMAGE')",
            "ALTER AI MODEL m SET ('credential_ref'='not-a-ref')",
            "DROP AI MODEL 'bad name'"
    })
    public void testInvalidMetadataFailsAnalysis(String sql) {
        StatementBase statement = SqlParser.parseSingleStatement(sql, 0);
        Assertions.assertThrows(SemanticException.class, () -> AIModelAnalyzer.analyze(statement, null));
    }
}
