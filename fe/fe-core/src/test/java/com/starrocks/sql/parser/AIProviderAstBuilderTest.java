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

package com.starrocks.sql.parser;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.aiprovider.AlterAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.CreateAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DescAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DropAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.SetDefaultAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.ShowAIProvidersStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Grammar/AstBuilder coverage for the unified CREATE/ALTER/DROP/SHOW/DESC/SET DEFAULT AI PROVIDER. */
public class AIProviderAstBuilderTest {
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    private Object parse(String sql) {
        return SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());
    }

    @Test
    public void testCreateRerank() {
        CreateAIProviderStmt stmt = (CreateAIProviderStmt) parse(
                "CREATE AI PROVIDER r TYPE rerank PROPERTIES("
                        + "\"endpoint\"=\"https://openrouter.ai/api/v1/rerank\","
                        + "\"model\"=\"cohere/rerank-4-fast\",\"api_key\"=\"sk-x\")");
        Assertions.assertEquals("r", stmt.getName());
        Assertions.assertEquals("rerank", stmt.getType());
        Assertions.assertFalse(stmt.isIfNotExists());
        Assertions.assertEquals("cohere/rerank-4-fast", stmt.getProperties().get("model"));
    }

    @Test
    public void testCreateEmbeddingIfNotExists() {
        CreateAIProviderStmt stmt = (CreateAIProviderStmt) parse(
                "CREATE AI PROVIDER IF NOT EXISTS e TYPE embedding PROPERTIES("
                        + "\"endpoint\"=\"https://api.openai.com/v1/embeddings\",\"model\"=\"m\",\"dimensions\"=\"1536\")");
        Assertions.assertTrue(stmt.isIfNotExists());
        Assertions.assertEquals("embedding", stmt.getType());
        Assertions.assertEquals("1536", stmt.getProperties().get("dimensions"));
    }

    @Test
    public void testAlter() {
        AlterAIProviderStmt stmt = (AlterAIProviderStmt) parse(
                "ALTER AI PROVIDER r SET (\"api_key\"=\"sk-new\")");
        Assertions.assertEquals("r", stmt.getName());
        Assertions.assertEquals("sk-new", stmt.getProperties().get("api_key"));
    }

    @Test
    public void testDropIfExists() {
        DropAIProviderStmt stmt = (DropAIProviderStmt) parse("DROP AI PROVIDER IF EXISTS r");
        Assertions.assertEquals("r", stmt.getName());
        Assertions.assertTrue(stmt.isIfExists());
    }

    @Test
    public void testShowAllAndByType() {
        ShowAIProvidersStmt all = (ShowAIProvidersStmt) parse("SHOW AI PROVIDERS");
        Assertions.assertEquals("", all.getTypeFilter());
        ShowAIProvidersStmt byType = (ShowAIProvidersStmt) parse("SHOW AI PROVIDERS TYPE rerank");
        Assertions.assertEquals("rerank", byType.getTypeFilter());
    }

    @Test
    public void testDescAndSetDefault() {
        DescAIProviderStmt desc = (DescAIProviderStmt) parse("DESC AI PROVIDER r");
        Assertions.assertEquals("r", desc.getName());
        SetDefaultAIProviderStmt setd = (SetDefaultAIProviderStmt) parse("SET r AS DEFAULT AI PROVIDER");
        Assertions.assertEquals("r", setd.getName());
    }
}
