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
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Unit tests for AstBuilder Group Provider parsing with IF NOT EXISTS and IF EXISTS functionality
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
}
