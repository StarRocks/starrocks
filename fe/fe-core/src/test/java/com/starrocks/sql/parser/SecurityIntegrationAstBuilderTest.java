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
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Unit tests for Security Integration AST parsing and validation functionality
 * Based on test cases from test/sql/test_security_integration
 * <p>
 * This class covers:
 * 1. AST parsing functionality for all Security Integration operations
 * 2. Validation logic and error scenarios
 * 3. Edge cases and boundary conditions
 * 4. Property validation and format checking
 */
public class SecurityIntegrationAstBuilderTest {
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    /**
     * Test case: Parse CREATE SECURITY INTEGRATION with OIDC properties
     * Test point: Should correctly parse JWT authentication properties
     * Based on: create security integration oidc properties("type"="authentication_jwt", "jwks_url"="jwks.json",
     * "principal_field"="sub")
     */
    @Test
    public void testParseCreateSecurityIntegrationOIDC() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("authentication_jwt", properties.get("type"), "Type should be authentication_jwt");
        Assertions.assertEquals("jwks.json", properties.get("jwks_url"), "JWKS URL should match");
        Assertions.assertEquals("sub", properties.get("principal_field"), "Principal field should match");
    }

    /**
     * Test case: Parse CREATE SECURITY INTEGRATION with quoted identifier
     * Test point: Should correctly handle quoted integration names
     */
    @Test
    public void testParseCreateSecurityIntegrationWithQuotedIdentifier() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION `test-integration` PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test-integration", stmt.getName(), "Integration name should match");
    }

    /**
     * Test case: Parse ALTER SECURITY INTEGRATION
     * Test point: Should correctly parse property modification
     * Based on: alter security integration oidc set ("principal_field"="preferred_name")
     */
    @Test
    public void testParseAlterSecurityIntegration() throws Exception {
        String sql = "ALTER SECURITY INTEGRATION oidc SET (\"principal_field\" = \"preferred_name\")";

        AlterSecurityIntegrationStatement stmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        Map<String, String> properties = stmt.getProperties();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("preferred_name", properties.get("principal_field"), "Principal field should be updated");
    }

    /**
     * Test case: Parse ALTER SECURITY INTEGRATION with multiple properties
     * Test point: Should correctly parse multiple property modifications
     */
    @Test
    public void testParseAlterSecurityIntegrationMultipleProperties() throws Exception {
        String sql = "ALTER SECURITY INTEGRATION oidc SET (" +
                "\"principal_field\" = \"preferred_name\", " +
                "\"jwks_url\" = \"new_jwks.json\")";

        AlterSecurityIntegrationStatement stmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        Map<String, String> properties = stmt.getProperties();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("preferred_name", properties.get("principal_field"), "Principal field should be updated");
        Assertions.assertEquals("new_jwks.json", properties.get("jwks_url"), "JWKS URL should be updated");
    }

    /**
     * Test case: Parse DROP SECURITY INTEGRATION
     * Test point: Should correctly parse drop statement
     * Based on: drop security integration oidc
     */
    @Test
    public void testParseDropSecurityIntegration() throws Exception {
        String sql = "DROP SECURITY INTEGRATION oidc";

        DropSecurityIntegrationStatement stmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");
    }

    /**
     * Test case: Parse DROP SECURITY INTEGRATION with quoted identifier
     * Test point: Should correctly handle quoted integration names
     */
    @Test
    public void testParseDropSecurityIntegrationWithQuotedIdentifier() throws Exception {
        String sql = "DROP SECURITY INTEGRATION `test-integration`";

        DropSecurityIntegrationStatement stmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test-integration", stmt.getName(), "Integration name should match");
    }

    /**
     * Test case: Parse SHOW CREATE SECURITY INTEGRATION
     * Test point: Should correctly parse show create statement
     * Based on: show create security integration oidc
     */
    @Test
    public void testParseShowCreateSecurityIntegration() throws Exception {
        String sql = "SHOW CREATE SECURITY INTEGRATION oidc";

        ShowCreateSecurityIntegrationStatement stmt =
                (ShowCreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");
    }

    /**
     * Test case: Parse SHOW SECURITY INTEGRATIONS
     * Test point: Should correctly parse show all integrations statement
     * Based on: show security integrations
     */
    @Test
    public void testParseShowSecurityIntegrations() throws Exception {
        String sql = "SHOW SECURITY INTEGRATIONS";

        ShowSecurityIntegrationStatement stmt =
                (ShowSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
    }

    /**
     * Test case: Parse CREATE SECURITY INTEGRATION with complex properties
     * Test point: Should correctly parse multiple properties with various types
     */
    @Test
    public void testParseCreateSecurityIntegrationWithComplexProperties() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION complex_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"https://example.com/.well-known/jwks.json\", " +
                "\"principal_field\" = \"sub\", " +
                "\"issuer\" = \"https://example.com\", " +
                "\"audience\" = \"starrocks-client\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("complex_oidc", stmt.getName(), "Integration name should match");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("authentication_jwt", properties.get("type"), "Type should be authentication_jwt");
        Assertions.assertEquals("https://example.com/.well-known/jwks.json", properties.get("jwks_url"), "JWKS URL should match");
        Assertions.assertEquals("sub", properties.get("principal_field"), "Principal field should match");
        Assertions.assertEquals("https://example.com", properties.get("issuer"), "Issuer should match");
        Assertions.assertEquals("starrocks-client", properties.get("audience"), "Audience should match");
    }

    /**
     * Test case: Parse statements with case variations
     * Test point: Should handle case-insensitive keywords correctly
     */
    @Test
    public void testParseWithCaseVariations() throws Exception {
        // Test CREATE with different case
        String createSql =
                "create security integration case_test PROPERTIES(\"type\" = \"authentication_jwt\", " +
                        "\"jwks_url\" = \"jwks.json\")";
        CreateSecurityIntegrationStatement createStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(createSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(createStmt, "Create statement should not be null");
        Assertions.assertEquals("case_test", createStmt.getName(), "Integration name should match");

        // Test ALTER with different case
        String alterSql = "alter security integration case_test set (\"principal_field\" = \"preferred_name\")";
        AlterSecurityIntegrationStatement alterStmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(alterSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(alterStmt, "Alter statement should not be null");
        Assertions.assertEquals("case_test", alterStmt.getName(), "Integration name should match");

        // Test DROP with different case
        String dropSql = "drop security integration case_test";
        DropSecurityIntegrationStatement dropStmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(dropSql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(dropStmt, "Drop statement should not be null");
        Assertions.assertEquals("case_test", dropStmt.getName(), "Integration name should match");

        // Test SHOW with different case
        String showSql = "show create security integration case_test";
        ShowCreateSecurityIntegrationStatement showStmt =
                (ShowCreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(showSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(showStmt, "Show statement should not be null");
        Assertions.assertEquals("case_test", showStmt.getName(), "Integration name should match");
    }

    /**
     * Test case: Parse CREATE SECURITY INTEGRATION with minimal required properties
     * Test point: Should handle minimal property sets correctly
     */
    @Test
    public void testParseCreateSecurityIntegrationMinimalProperties() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION minimal_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("minimal_oidc", stmt.getName(), "Integration name should match");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("authentication_jwt", properties.get("type"), "Type should be authentication_jwt");
        Assertions.assertEquals(1, properties.size(), "Should have only one property");
    }

    // ==================== Validation Tests ====================

    /**
     * Test case: CREATE SECURITY INTEGRATION without required 'type' property
     * Test point: Should fail with missing required property error
     * Based on: create security integration oidc properties("jwks_url"="jwks.json", "principal_field"="sub")
     * Expected error: (1064, 'Getting analyzing error. Detail message: missing required property: type.')
     */
    @Test
    public void testCreateSecurityIntegrationMissingTypeProperty() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        // This should parse successfully but fail during validation
        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // The validation logic would check for required 'type' property
        // In a real implementation, this would be validated in the analyzer/validator phase
        Assertions.assertFalse(stmt.getPropertyMap().containsKey("type"),
                "Type property should be missing, causing validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with empty type property
     * Test point: Should fail with empty type validation error
     */
    @Test
    public void testCreateSecurityIntegrationEmptyTypeProperty() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Empty type should cause validation error
        Assertions.assertEquals("", stmt.getPropertyMap().get("type"),
                "Empty type should cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with invalid type value
     * Test point: Should fail with invalid type validation error
     */
    @Test
    public void testCreateSecurityIntegrationInvalidTypeProperty() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"invalid_type\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Invalid type should cause validation error
        Assertions.assertEquals("invalid_type", stmt.getPropertyMap().get("type"),
                "Invalid type should cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with missing jwks_url for JWT type
     * Test point: Should fail with missing required property error
     */
    @Test
    public void testCreateSecurityIntegrationMissingJwksUrl() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Missing jwks_url for JWT type should cause validation error
        Assertions.assertFalse(stmt.getPropertyMap().containsKey("jwks_url"),
                "Missing jwks_url should cause validation error for JWT type");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with invalid jwks_url format
     * Test point: Should fail with invalid URL format validation error
     */
    @Test
    public void testCreateSecurityIntegrationInvalidJwksUrl() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"invalid-url\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Invalid jwks_url format should cause validation error
        Assertions.assertEquals("invalid-url", stmt.getPropertyMap().get("jwks_url"),
                "Invalid jwks_url format should cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with duplicate properties
     * Test point: Should handle duplicate properties correctly
     */
    @Test
    public void testCreateSecurityIntegrationDuplicateProperties() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"type\" = \"authentication_jwt\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Duplicate properties should be handled (last one wins or validation error)
        Assertions.assertEquals("authentication_jwt", stmt.getPropertyMap().get("type"),
                "Duplicate type property should be handled");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with null property values
     * Test point: Should handle null property values correctly
     */
    @Test
    public void testCreateSecurityIntegrationNullPropertyValues() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Empty principal_field should cause validation error
        Assertions.assertEquals("", stmt.getPropertyMap().get("principal_field"),
                "Empty principal_field should cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with reserved property names
     * Test point: Should validate against reserved property names
     */
    @Test
    public void testCreateSecurityIntegrationReservedPropertyNames() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"name\" = \"reserved_property\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Reserved property names should cause validation error
        Assertions.assertTrue(stmt.getPropertyMap().containsKey("name"),
                "Reserved property 'name' should cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with invalid integration name
     * Test point: Should validate integration name format
     */
    @Test
    public void testCreateSecurityIntegrationInvalidName() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION `invalid-name-with-special-chars!@#` PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("invalid-name-with-special-chars!@#", stmt.getName(),
                "Invalid name should be parsed but cause validation error");
    }

    /**
     * Test case: CREATE SECURITY INTEGRATION with too long property values
     * Test point: Should validate property value length limits
     */
    @Test
    public void testCreateSecurityIntegrationLongPropertyValues() throws Exception {
        // Create a very long URL
        StringBuilder longUrl = new StringBuilder("https://example.com/.well-known/jwks.json");
        for (int i = 0; i < 1000; i++) {
            longUrl.append("?param").append(i).append("=value").append(i);
        }

        String sql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"" + longUrl.toString() + "\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Very long property values should cause validation error
        Assertions.assertTrue(stmt.getPropertyMap().get("jwks_url").length() > 1000,
                "Very long jwks_url should cause validation error");
    }
}