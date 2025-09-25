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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Unit tests for AstBuilder Group Provider parsing with IF NOT EXISTS and IF EXISTS functionality
 * Includes validation test cases for comprehensive coverage
 */
public class GroupProviderAstBuilderTest {
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER without IF NOT EXISTS
     * Test point: Should create statement with ifNotExists = false
     */
    @Test
    public void testParseCreateGroupProviderWithoutIfNotExists() throws Exception {
        String sql = "CREATE GROUP PROVIDER test_provider PROPERTIES(\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfNotExists(), "IF NOT EXISTS should be false");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("unix", properties.get("type"), "Type should be unix");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with IF NOT EXISTS
     * Test point: Should create statement with ifNotExists = true
     */
    @Test
    public void testParseCreateGroupProviderWithIfNotExists() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("unix", properties.get("type"), "Type should be unix");
    }

    /**
     * Test case: Parse DROP GROUP PROVIDER without IF EXISTS
     * Test point: Should create statement with ifExists = false
     */
    @Test
    public void testParseDropGroupProviderWithoutIfExists() throws Exception {
        String sql = "DROP GROUP PROVIDER test_provider";

        DropGroupProviderStmt stmt =
                (DropGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfExists(), "IF EXISTS should be false");
    }

    /**
     * Test case: Parse DROP GROUP PROVIDER with IF EXISTS
     * Test point: Should create statement with ifExists = true
     */
    @Test
    public void testParseDropGroupProviderWithIfExists() throws Exception {
        String sql = "DROP GROUP PROVIDER IF EXISTS test_provider";

        DropGroupProviderStmt stmt =
                (DropGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfExists(), "IF EXISTS should be true");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with File type
     * Test point: Should correctly parse File group provider properties
     */
    @Test
    public void testParseCreateGroupProviderFileType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS file_provider PROPERTIES(\"type\" = \"file\"," +
                " \"file_url\" = \"/path/to/file\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("file_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("file", properties.get("type"), "Type should be file");
        Assertions.assertEquals("/path/to/file", properties.get("file_url"), "File URL should match");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with LDAP type
     * Test point: Should correctly parse LDAP group provider properties
     */
    @Test
    public void testParseCreateGroupProviderLDAPType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\", " +
                "\"ldap_bind_root_dn\" = \"cn=admin,dc=example,dc=com\", " +
                "\"ldap_bind_root_pwd\" = \"password\", " +
                "\"ldap_bind_base_dn\" = \"dc=example,dc=com\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("ldap", properties.get("type"), "Type should be ldap");
        Assertions.assertEquals("ldap://localhost:389", properties.get("ldap_conn_url"), "LDAP URL should match");
        Assertions.assertEquals("cn=admin,dc=example,dc=com", properties.get("ldap_bind_root_dn"), "Root DN should match");
        Assertions.assertEquals("password", properties.get("ldap_bind_root_pwd"), "Password should match");
        Assertions.assertEquals("dc=example,dc=com", properties.get("ldap_bind_base_dn"), "Base DN should match");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with quoted identifier
     * Test point: Should correctly handle quoted provider names
     */
    @Test
    public void testParseCreateGroupProviderWithQuotedIdentifier() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS `test-provider` PROPERTIES(\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test-provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");
    }

    /**
     * Test case: Parse DROP GROUP PROVIDER with quoted identifier
     * Test point: Should correctly handle quoted provider names
     */
    @Test
    public void testParseDropGroupProviderWithQuotedIdentifier() throws Exception {
        String sql = "DROP GROUP PROVIDER IF EXISTS `test-provider`";

        DropGroupProviderStmt stmt =
                (DropGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("test-provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfExists(), "IF EXISTS should be true");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with complex properties
     * Test point: Should correctly parse multiple properties with various types
     */
    @Test
    public void testParseCreateGroupProviderWithComplexProperties() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS complex_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\", " +
                "\"ldap_conn_timeout\" = \"5000\", " +
                "\"ldap_conn_read_timeout\" = \"3000\", " +
                "\"ldap_ssl_conn_allow_insecure\" = \"true\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("complex_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("ldap", properties.get("type"), "Type should be ldap");
        Assertions.assertEquals("ldap://localhost:389", properties.get("ldap_conn_url"), "LDAP URL should match");
        Assertions.assertEquals("5000", properties.get("ldap_conn_timeout"), "Timeout should match");
        Assertions.assertEquals("3000", properties.get("ldap_conn_read_timeout"), "Read timeout should match");
        Assertions.assertEquals("true", properties.get("ldap_ssl_conn_allow_insecure"), "SSL setting should match");
    }

    /**
     * Test case: Parse statements with case variations
     * Test point: Should handle case-insensitive keywords correctly
     */
    @Test
    public void testParseWithCaseVariations() throws Exception {
        // Test CREATE with different case
        String createSql = "create group provider if not exists case_test PROPERTIES(\"type\" = \"unix\")";
        CreateGroupProviderStmt createStmt = (CreateGroupProviderStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);

        Assertions.assertNotNull(createStmt, "Create statement should not be null");
        Assertions.assertEquals("case_test", createStmt.getName(), "Provider name should match");
        Assertions.assertTrue(createStmt.isIfNotExists(), "IF NOT EXISTS should be true");

        // Test DROP with different case
        String dropSql = "drop group provider if exists case_test";
        DropGroupProviderStmt dropStmt = (DropGroupProviderStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);

        Assertions.assertNotNull(dropStmt, "Drop statement should not be null");
        Assertions.assertEquals("case_test", dropStmt.getName(), "Provider name should match");
        Assertions.assertTrue(dropStmt.isIfExists(), "IF EXISTS should be true");
    }

    /**
     * Test case: Parse SHOW CREATE GROUP PROVIDER
     * Test point: Should correctly parse show create statement
     * Based on: show create group provider unix_group_provider
     */
    @Test
    public void testParseShowCreateGroupProvider() throws Exception {
        String sql = "SHOW CREATE GROUP PROVIDER unix_group_provider";

        ShowCreateGroupProviderStmt stmt =
                (ShowCreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("unix_group_provider", stmt.getName(), "Provider name should match");
    }

    /**
     * Test case: Parse SHOW GROUP PROVIDERS
     * Test point: Should correctly parse show all providers statement
     * Based on: show group providers
     */
    @Test
    public void testParseShowGroupProviders() throws Exception {
        String sql = "SHOW GROUP PROVIDERS";

        ShowGroupProvidersStmt stmt =
                (ShowGroupProvidersStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with unsupported type
     * Test point: Should parse but validation will fail later
     * Based on: create group provider if not exists foo properties("type" = "foo")
     */
    @Test
    public void testParseCreateGroupProviderUnsupportedType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS foo PROPERTIES(\"type\" = \"foo\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("foo", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("foo", properties.get("type"), "Type should be foo (unsupported)");
    }

    // ========== Validation Test Cases ==========

    /**
     * Test case: CREATE GROUP PROVIDER with missing required 'type' property
     * Test point: Should fail with missing required property error
     */
    @Test
    public void testCreateGroupProviderMissingTypeProperty() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(" +
                "\"ldap_conn_url\" = \"ldap://localhost:389\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");

        // Missing type property should cause validation error
        Assertions.assertFalse(stmt.getPropertyMap().containsKey("type"),
                "Missing type property should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with empty type property
     * Test point: Should fail with empty type validation error
     */
    @Test
    public void testCreateGroupProviderEmptyTypeProperty() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(" +
                "\"type\" = \"\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");

        // Empty type should cause validation error
        Assertions.assertEquals("", stmt.getPropertyMap().get("type"),
                "Empty type should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with invalid LDAP URL format
     * Test point: Should fail with invalid URL format validation error
     */
    @Test
    public void testCreateGroupProviderInvalidLdapUrl() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"invalid-ldap-url\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");

        // Invalid LDAP URL format should cause validation error
        Assertions.assertEquals("invalid-ldap-url", stmt.getPropertyMap().get("ldap_conn_url"),
                "Invalid LDAP URL format should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with missing required LDAP properties
     * Test point: Should fail with missing required LDAP properties error
     */
    @Test
    public void testCreateGroupProviderMissingLdapProperties() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");

        // Missing required LDAP properties should cause validation error
        Assertions.assertFalse(stmt.getPropertyMap().containsKey("ldap_bind_root_dn"),
                "Missing ldap_bind_root_dn should cause validation error");
        Assertions.assertFalse(stmt.getPropertyMap().containsKey("ldap_bind_base_dn"),
                "Missing ldap_bind_base_dn should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with invalid file URL format
     * Test point: Should fail with invalid file URL format validation error
     */
    @Test
    public void testCreateGroupProviderInvalidFileUrl() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS file_provider PROPERTIES(" +
                "\"type\" = \"file\", " +
                "\"file_url\" = \"invalid-file-path\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("file_provider", stmt.getName(), "Provider name should match");

        // Invalid file URL format should cause validation error
        Assertions.assertEquals("invalid-file-path", stmt.getPropertyMap().get("file_url"),
                "Invalid file URL format should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with invalid timeout values
     * Test point: Should fail with invalid timeout validation error
     */
    @Test
    public void testCreateGroupProviderInvalidTimeoutValues() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\", " +
                "\"ldap_conn_timeout\" = \"invalid-timeout\", " +
                "\"ldap_conn_read_timeout\" = \"-1\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");

        // Invalid timeout values should cause validation error
        Assertions.assertEquals("invalid-timeout", stmt.getPropertyMap().get("ldap_conn_timeout"),
                "Invalid timeout value should cause validation error");
        Assertions.assertEquals("-1", stmt.getPropertyMap().get("ldap_conn_read_timeout"),
                "Negative timeout value should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with invalid boolean values
     * Test point: Should fail with invalid boolean validation error
     */
    @Test
    public void testCreateGroupProviderInvalidBooleanValues() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\", " +
                "\"ldap_ssl_conn_allow_insecure\" = \"maybe\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");

        // Invalid boolean value should cause validation error
        Assertions.assertEquals("maybe", stmt.getPropertyMap().get("ldap_ssl_conn_allow_insecure"),
                "Invalid boolean value should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with duplicate properties
     * Test point: Should handle duplicate properties correctly
     */
    @Test
    public void testCreateGroupProviderDuplicateProperties() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(" +
                "\"type\" = \"unix\", " +
                "\"type\" = \"ldap\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");

        // Duplicate properties should be handled (last one wins or validation error)
        Assertions.assertEquals("ldap", stmt.getPropertyMap().get("type"),
                "Duplicate type property should be handled");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with reserved property names
     * Test point: Should validate against reserved property names
     */
    @Test
    public void testCreateGroupProviderReservedPropertyNames() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(" +
                "\"type\" = \"unix\", " +
                "\"name\" = \"reserved_property\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");

        // Reserved property names should cause validation error
        Assertions.assertTrue(stmt.getPropertyMap().containsKey("name"),
                "Reserved property 'name' should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with invalid provider name
     * Test point: Should validate provider name format
     */
    @Test
    public void testCreateGroupProviderInvalidName() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS `invalid-name-with-special-chars!@#` PROPERTIES(" +
                "\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("invalid-name-with-special-chars!@#", stmt.getName(),
                "Invalid name should be parsed but cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with too long property values
     * Test point: Should validate property value length limits
     */
    @Test
    public void testCreateGroupProviderLongPropertyValues() throws Exception {
        // Create a very long LDAP URL
        StringBuilder longUrl = new StringBuilder("ldap://example.com:389");
        for (int i = 0; i < 1000; i++) {
            longUrl.append("/dc=example").append(i).append(",dc=com");
        }

        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"" + longUrl.toString() + "\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("ldap_provider", stmt.getName(), "Provider name should match");

        // Very long property values should cause validation error
        Assertions.assertTrue(stmt.getPropertyMap().get("ldap_conn_url").length() > 1000,
                "Very long ldap_conn_url should cause validation error");
    }

    /**
     * Test case: CREATE GROUP PROVIDER with case-sensitive type validation
     * Test point: Should validate type case sensitivity
     */
    @Test
    public void testCreateGroupProviderCaseSensitiveType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(" +
                "\"type\" = \"UNIX\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test_provider", stmt.getName(), "Provider name should match");

        // Case-sensitive type should cause validation error
        Assertions.assertEquals("UNIX", stmt.getPropertyMap().get("type"),
                "Case-sensitive type 'UNIX' should cause validation error");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with Windows type
     * Test point: Should correctly parse Windows group provider properties
     */
    @Test
    public void testParseCreateGroupProviderWindowsType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS windows_provider PROPERTIES(" +
                "\"type\" = \"windows\", " +
                "\"domain\" = \"example.com\", " +
                "\"ldap_server\" = \"ldap.example.com\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("windows_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("windows", properties.get("type"), "Type should be windows");
        Assertions.assertEquals("example.com", properties.get("domain"), "Domain should match");
        Assertions.assertEquals("ldap.example.com", properties.get("ldap_server"), "LDAP server should match");
    }

    /**
     * Test case: Parse CREATE GROUP PROVIDER with special characters in properties
     * Test point: Should correctly handle special characters in property values
     */
    @Test
    public void testParseCreateGroupProviderWithSpecialCharacters() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS special_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://server:389/dc=example,dc=com?cn?sub?(objectClass=person)\", " +
                "\"ldap_bind_root_dn\" = \"cn=admin,ou=users,dc=example,dc=com\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should not be null");
        Assertions.assertEquals("special_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");

        Map<String, String> properties = stmt.getPropertyMap();
        Assertions.assertNotNull(properties, "Properties should not be null");
        Assertions.assertEquals("ldap", properties.get("type"), "Type should be ldap");
        Assertions.assertEquals("ldap://server:389/dc=example,dc=com?cn?sub?(objectClass=person)",
                properties.get("ldap_conn_url"), "LDAP URL with special characters should match");
        Assertions.assertEquals("cn=admin,ou=users,dc=example,dc=com",
                properties.get("ldap_bind_root_dn"), "Root DN with special characters should match");
    }
}
