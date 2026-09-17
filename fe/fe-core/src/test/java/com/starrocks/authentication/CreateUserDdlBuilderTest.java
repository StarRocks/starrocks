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

package com.starrocks.authentication;

import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Pair;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserLockOption;
import com.starrocks.sql.ast.UserPasswordOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateUserDdlBuilderTest {
    // MySQL native digest of "123456"
    private static final String DIGEST_123456 = "*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9";

    private static final UserRef JACK = new UserRef("jack", "%");
    private static final UserIdentity JACK_IDENTITY = new UserIdentity("jack", "%");

    private static UserAuthenticationInfo info(UserAuthOption authOption,
                                               UserPasswordOption passwordOption,
                                               UserLockOption lockOption) {
        return new UserAuthenticationInfo(JACK, authOption, passwordOption, lockOption);
    }

    private static UserAuthOption plainPassword(String password) {
        return new UserAuthOption(null, password, true, NodePosition.ZERO);
    }

    private static String build(UserAuthenticationInfo authenticationInfo, List<String> defaultRoles,
                                UserProperty userProperty, boolean showSecret) {
        return CreateUserDdlBuilder.build(JACK_IDENTITY, authenticationInfo, defaultRoles, userProperty, showSecret);
    }

    @Test
    public void testNoPasswordNoRoleNoProperty() {
        String ddl = build(info(null, null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'%'", ddl);
    }

    @Test
    public void testPasswordVisibleWithShowSecret() {
        String ddl = build(info(plainPassword("123456"), null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "IDENTIFIED WITH mysql_native_password AS '" + DIGEST_123456 + "'", ddl);
    }

    @Test
    public void testPasswordMaskedWithoutShowSecret() {
        String ddl = build(info(plainPassword("123456"), null, null), null, null, false);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "IDENTIFIED WITH mysql_native_password AS '<secret>'", ddl);
        Assertions.assertFalse(ddl.contains(DIGEST_123456));
    }

    @Test
    public void testEmptyPasswordEmitsNoAuthClause() {
        String ddl = build(info(plainPassword(""), null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'%'", ddl);
    }

    @Test
    public void testAuthPluginAuthStringMasked() {
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.toString(),
                "uid=jack,ou=people,dc=example,dc=com", false, NodePosition.ZERO);

        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                        + "IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=people,dc=example,dc=com'",
                build(info(authOption, null, null), null, null, true));

        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                        + "IDENTIFIED WITH authentication_ldap_simple AS '<secret>'",
                build(info(authOption, null, null), null, null, false));
    }

    @Test
    public void testOauth2AuthStringMasked() {
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.AUTHENTICATION_OAUTH2.toString(),
                "{\"oauth2_client_secret\":\"topsecret\"}", false, NodePosition.ZERO);

        String masked = build(info(authOption, null, null), null, null, false);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "IDENTIFIED WITH authentication_oauth2 AS '<secret>'", masked);
        Assertions.assertFalse(masked.contains("topsecret"));
    }

    @Test
    public void testAuthPluginWithoutAuthStringEmitsNoAsClause() {
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.AUTHENTICATION_KERBEROS.toString(), null, false, NodePosition.ZERO);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "IDENTIFIED WITH authentication_kerberos", build(info(authOption, null, null), null, null, true));
    }

    @Test
    public void testDefaultRolesAreSorted() {
        String ddl = build(info(null, null, null), Lists.newArrayList("r2", "r1", "r10"), null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "DEFAULT ROLE 'r1', 'r10', 'r2'", ddl);
    }

    @Test
    public void testExpirePasswordAndLockAreBothEmitted() {
        UserAuthenticationInfo authenticationInfo =
                info(null, new UserPasswordOption(true), new UserLockOption(true));
        String ddl = build(authenticationInfo, null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "EXPIRE_PASSWORD = true\n"
                + "LOCK", ddl);
    }

    @Test
    public void testOnlyNonDefaultPropertiesAreEmitted() {
        UserProperty userProperty = new UserProperty();
        Assertions.assertEquals("CREATE USER 'jack'@'%'",
                build(info(null, null, null), null, userProperty, true));

        userProperty.updateForReplayJournal(Lists.newArrayList(
                Pair.create("max_user_connections", "100"),
                Pair.create("catalog", "hive_catalog"),
                Pair.create("database", "db1"),
                Pair.create("session.query_timeout", "600")));

        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                        + "PROPERTIES (\"max_user_connections\" = \"100\", \"catalog\" = \"hive_catalog\", "
                        + "\"database\" = \"db1\", \"session.query_timeout\" = \"600\")",
                build(info(null, null, null), null, userProperty, true));
    }

    @Test
    public void testQuoteInHostIsEscaped() {
        // A host is only validated by compiling it as a MySQL pattern, so it can carry a quote
        UserIdentity quotedHost = new UserIdentity("jack", "foo'bar");
        String ddl = CreateUserDdlBuilder.build(quotedHost, info(null, null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'foo''bar'", ddl);
    }

    @Test
    public void testBackslashInHostIsEscaped() {
        UserIdentity backslashHost = new UserIdentity("jack", "foo\\bar");
        String ddl = CreateUserDdlBuilder.build(backslashHost, info(null, null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@'foo\\\\bar'", ddl);
    }

    @Test
    public void testDomainHostIsRenderedWithBrackets() {
        UserIdentity domain = new UserIdentity("jack", "example.com", true);
        String ddl = CreateUserDdlBuilder.build(domain, info(null, null, null), null, null, true);
        Assertions.assertEquals("CREATE USER 'jack'@['example.com']", ddl);
    }

    @Test
    public void testAllClausesTogether() {
        UserProperty userProperty = new UserProperty();
        userProperty.updateForReplayJournal(Lists.newArrayList(Pair.create("max_user_connections", "100")));

        String ddl = build(info(plainPassword("123456"), new UserPasswordOption(true), new UserLockOption(true)),
                Lists.newArrayList("db_admin"), userProperty, true);

        Assertions.assertEquals("CREATE USER 'jack'@'%'\n"
                + "IDENTIFIED WITH mysql_native_password AS '" + DIGEST_123456 + "'\n"
                + "DEFAULT ROLE 'db_admin'\n"
                + "EXPIRE_PASSWORD = true\n"
                + "LOCK\n"
                + "PROPERTIES (\"max_user_connections\" = \"100\")", ddl);
    }
}
