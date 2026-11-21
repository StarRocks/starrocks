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

package com.starrocks.persist.image;

import com.starrocks.authentication.FileGroupProvider;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.JWTSecurityIntegration;
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.authentication.OAuth2SecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.authentication.UnixGroupProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authentication.UserProperty;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.GroupProviderPB;
import com.starrocks.persist.proto.SecurityIntegrationPB;
import com.starrocks.persist.proto.UserAuthenticationInfoPB;
import com.starrocks.persist.proto.UserPropertyPB;
import com.starrocks.sql.ast.UserRef;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthenticationProtoTest {
    static UserAuthenticationInfo authentication(String user, String host) {
        UserAuthenticationInfo info = new UserAuthenticationInfo(new UserRef(user, host, false), null);
        setField(info, "password", new byte[] {0, 1, -1, 127, -128});
        setField(info, "authPlugin", "AUTHENTICATION_LDAP_SIMPLE");
        setField(info, "authString", "cn=stored-user,dc=example");
        return info;
    }

    static UserProperty property() {
        UserProperty property = new UserProperty();
        setField(property, "maxConn", 345L);
        setField(property, "database", "db");
        setField(property, "catalog", "hive");
        property.getSessionVariables().put("query_timeout", "123");
        property.getSessionVariables().put("time_zone", "Asia/Shanghai");
        return property;
    }

    static List<GroupProvider> providers() {
        return List.of(new FileGroupProvider("file", new HashMap<>(Map.of("type", "file", "group_file_url", "groups"))),
                new UnixGroupProvider("unix", new HashMap<>(Map.of("type", "unix"))),
                new LDAPGroupProvider("ldap", new HashMap<>(Map.of("type", "ldap", "ldap_conn_url", "ldap://unused"))));
    }

    static List<SecurityIntegration> integrations() {
        return List.of(new JWTSecurityIntegration("jwt", new HashMap<>(Map.of("type", "authentication_jwt"))),
                new SimpleLDAPSecurityIntegration("ldap", new HashMap<>(Map.of("type", "authentication_ldap_simple"))),
                new OAuth2SecurityIntegration("oauth", new HashMap<>(Map.of("type", "authentication_oauth2"))));
    }

    @Test
    public void testStoredCredentialsAndPatterns() throws Exception {
        UserAuthenticationInfo source = authentication("alice", "10.2.%");
        UserAuthenticationInfo loaded = UserAuthenticationInfo.fromProto(
                UserAuthenticationInfoPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, UserAuthenticationInfo.class), loaded);
        assertArrayEquals(source.getPassword(), loaded.getPassword());
        assertEquals(source.getAuthPlugin(), loaded.getAuthPlugin());
        assertEquals(source.getAuthString(), loaded.getAuthString());
        assertTrue(loaded.matchUser("alice"));
        assertFalse(loaded.matchUser("bob"));
        assertTrue(loaded.matchHost("10.2.8.9"));
        assertFalse(loaded.matchHost("10.3.8.9"));
        loaded.getPassword()[0] = 5;
        assertEquals(0, source.getPassword()[0]);
    }

    @Test
    public void testWildcardUserAndHost() {
        UserAuthenticationInfo loaded = UserAuthenticationInfo.fromProto(authentication("%", "%").toProto());
        assertTrue(loaded.matchUser("anyone"));
        assertTrue(loaded.matchHost("192.168.1.7"));
        loaded = UserAuthenticationInfo.fromProto(authentication("a_ice", "10.0._.1").toProto());
        assertTrue(loaded.matchUser("alice"));
        assertFalse(loaded.matchUser("allice"));
        assertTrue(loaded.matchHost("10.0.2.1"));
    }

    @Test
    public void testNullVersusEmptyCredentials() {
        UserAuthenticationInfo source = authentication("alice", "%");
        for (String field : List.of("password", "authPlugin", "authString")) {
            setField(source, field, null);
        }
        UserAuthenticationInfo loaded = UserAuthenticationInfo.fromProto(source.toProto());
        assertNull(loaded.getPassword());
        assertNull(loaded.getAuthPlugin());
        assertNull(loaded.getAuthString());
        setField(source, "password", new byte[0]);
        setField(source, "authString", "");
        loaded = UserAuthenticationInfo.fromProto(source.toProto());
        assertArrayEquals(new byte[0], loaded.getPassword());
        assertEquals("", loaded.getAuthString());
    }

    @Test
    public void testUserPropertiesAndDefaults() throws Exception {
        UserProperty source = property();
        UserProperty loaded = UserProperty.fromProto(UserPropertyPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, UserProperty.class), loaded);
        assertEquals(345, loaded.getMaxConn());
        assertEquals("hive", loaded.getCatalog());
        assertEquals("db", loaded.getDatabase());
        assertEquals("123", loaded.getSessionVariables().get("query_timeout"));
        loaded.getSessionVariables().clear();
        assertEquals(2, source.getSessionVariables().size());
        GsonEquivalence.assertEquivalent(new UserProperty(), UserProperty.fromProto(UserPropertyPB.getDefaultInstance()));
    }

    @Test
    public void testAllGroupProviderSubtypes() throws Exception {
        for (GroupProvider source : providers()) {
            GroupProvider loaded = GroupProvider.fromProto(GroupProviderPB.parseFrom(
                    assertInstances(source.toProto()).toByteArray()));
            assertEquals(source.getClass(), loaded.getClass());
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, GroupProvider.class), loaded);
            loaded.getProperties().put("comment", "new");
            assertFalse(source.getProperties().containsKey("comment"));
        }
    }

    @Test
    public void testAllSecurityIntegrationSubtypes() throws Exception {
        for (SecurityIntegration source : integrations()) {
            SecurityIntegration loaded = SecurityIntegration.fromProto(
                    SecurityIntegrationPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
            assertEquals(source.getClass(), loaded.getClass());
            GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, SecurityIntegration.class), loaded);
            loaded.getPropertyMap().put("comment", "new");
            assertFalse(source.getPropertyMap().containsKey("comment"));
        }
    }

    @Test
    public void testSubtypeComesFromKindAndDoesNotValidateDdlProperties() {
        GroupProvider source = new FileGroupProvider("file", new HashMap<>(Map.of("type", "ldap")));
        assertEquals(FileGroupProvider.class, GroupProvider.fromProto(source.toProto()).getClass());
        SecurityIntegration integration = new JWTSecurityIntegration("jwt", new HashMap<>());
        assertEquals(JWTSecurityIntegration.class, SecurityIntegration.fromProto(integration.toProto()).getClass());
    }

    @Test
    public void testNullPropertyValuesFollowV2() {
        GroupProvider source = providers().get(0);
        source.getProperties().put("absent", null);
        GroupProvider loaded = GroupProvider.fromProto(source.toProto());
        assertFalse(loaded.getProperties().containsKey("absent"));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, GroupProvider.class), loaded);
    }

    @Test
    public void testUnknownFields() {
        UserAuthenticationInfo source = authentication("alice", "%");
        GsonEquivalence.assertEquivalent(source, UserAuthenticationInfo.fromProto(source.toProto().toBuilder()
                .setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build()));
        GroupProvider provider = providers().get(0);
        GsonEquivalence.assertEquivalent(provider, GroupProvider.fromProto(provider.toProto().toBuilder()
                .setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build()));
    }

    @Test
    public void testInvalidSubtype() {
        for (int kind : List.of(0, 999)) {
            Class<? extends RuntimeException> expected = kind == 0
                    ? IllegalArgumentException.class : SubtypeNotFoundException.class;
            assertThrows(expected, () -> GroupProvider.fromProto(
                    GroupProviderPB.newBuilder().setKindValue(kind).setName("invalid").build()));
            assertThrows(expected, () -> SecurityIntegration.fromProto(
                    SecurityIntegrationPB.newBuilder().setKindValue(kind).setName("invalid").build()));
        }
    }
}
