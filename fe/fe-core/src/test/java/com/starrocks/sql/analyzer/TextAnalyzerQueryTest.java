// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TextAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableFunctionRelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TextAnalyzerQueryTest {
    private static final String ANALYZER_NAME = "detail_analyzer";

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.initWithoutTableAndDb(RunMode.SHARED_NOTHING);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(AnalyzeTestUtil.getDbName());
        TextAnalyzer.Definition definition = TextAnalyzer.canonicalize(
                "{\"tokenizer\":{\"type\":\"standard\"}}");
        db.putTextAnalyzer(new TextAnalyzer(100, db.getId(), ANALYZER_NAME,
                definition.getCanonicalJson(), definition.getDigest(), 1, 1, "root"));
    }

    @Test
    public void testResolveNamedAnalyzerForTableFunction() {
        StatementBase statement = AnalyzeTestUtil.analyzeWithoutTestView(
                "SELECT t.* FROM (SELECT 1) s, tokenize_detail('" + ANALYZER_NAME + "', 'StarRocks') t");
        SelectRelation select = (SelectRelation) ((QueryStatement) statement).getQueryRelation();
        JoinRelation join = (JoinRelation) select.getRelation();
        TableFunctionRelation tableFunction = (TableFunctionRelation) join.getRight();
        StringLiteral resolved = (StringLiteral) tableFunction.getChildExpressions().get(0);

        Assertions.assertTrue(resolved.getValue().startsWith("{\"spec_version\":1"));
        Assertions.assertTrue(resolved.getValue().contains("\"type\":\"standard\""));
    }

    @Test
    public void testDuplicateNameTakesPrecedenceOverDefinitionValidation() {
        AnalyzeTestUtil.analyzeFail(
                "CREATE TEXT ANALYZER " + ANALYZER_NAME
                        + " PROPERTIES (\"definition\" = 'not-json')",
                "already exists");
    }
}
