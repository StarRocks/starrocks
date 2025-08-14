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

import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class SecurityIntegrationTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    @Disabled
    public void test() {
        CreateSecurityIntegrationStatement createSecurityIntegrationStatement =
                (CreateSecurityIntegrationStatement) analyzeSuccess("create security integration test " +
                "properties(\"type\"=\"oidc\", \"oidc_jwks_url\"=\"jwks.json\", \"oidc_principal_field\"=\"sub\")");
        Assertions.assertEquals("test", createSecurityIntegrationStatement.getName());
        Assertions.assertEquals("oidc", createSecurityIntegrationStatement.getPropertyMap().get("type"));

        AlterSecurityIntegrationStatement alterSecurityIntegrationStatement =
                (AlterSecurityIntegrationStatement) analyzeSuccess("alter security integration test " +
                        "set (\"type\" = \"oidc\")");
        Assertions.assertEquals("test", alterSecurityIntegrationStatement.getName());
        Assertions.assertEquals("oidc", alterSecurityIntegrationStatement.getProperties().get("type"));

        DropSecurityIntegrationStatement dropSecurityIntegrationStatement =
                (DropSecurityIntegrationStatement) analyzeSuccess("drop security integration test");
        Assertions.assertEquals("test", dropSecurityIntegrationStatement.getName());

        ShowSecurityIntegrationStatement  showSecurityIntegrationStatement =
                (ShowSecurityIntegrationStatement) analyzeSuccess("show security integrations");
        Assertions.assertNotNull(new ShowResultMetaFactory().getMetadata(showSecurityIntegrationStatement));

        ShowCreateSecurityIntegrationStatement showCreateSecurityIntegrationStatement =
                (ShowCreateSecurityIntegrationStatement) analyzeSuccess("show create security integration test");
        Assertions.assertNotNull(new ShowResultMetaFactory().getMetadata(showCreateSecurityIntegrationStatement));
        Assertions.assertEquals("test", showCreateSecurityIntegrationStatement.getName());
    }
}
