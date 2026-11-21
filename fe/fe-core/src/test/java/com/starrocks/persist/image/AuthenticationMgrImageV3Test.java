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

import com.google.protobuf.Message;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.FileGroupProvider;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserProperty;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.persist.proto.AuthenticationMgrPB;
import com.starrocks.persist.proto.AuthenticationUserAuthenticationEntryPB;
import com.starrocks.persist.proto.AuthenticationUserPropertyEntryPB;
import com.starrocks.persist.proto.GroupProviderPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthenticationMgrImageV3Test {
    @TempDir
    Path dir;

    static AuthenticationMgr populated() {
        AuthenticationMgr source = new AuthenticationMgr();
        for (UserIdentity user : List.of(new UserIdentity("alice", "%", false),
                new UserIdentity("alice", "10.0.0.1", false), new UserIdentity("alice", "host.example", true))) {
            source.getUserToAuthenticationInfo().put(user,
                    AuthenticationProtoTest.authentication(user.getUser(), user.getHost()));
        }
        Map<String, UserProperty> properties = getField(source, "userNameToProperty");
        properties.put("alice", AuthenticationProtoTest.property());
        properties.put("property_only", AuthenticationProtoTest.property());
        Map<String, GroupProvider> providers = getField(source, "nameToGroupProviderMap");
        AuthenticationProtoTest.providers().forEach(provider -> providers.put(provider.getName(), provider));
        Map<String, SecurityIntegration> integrations = getField(source, "nameToSecurityIntegrationMap");
        AuthenticationProtoTest.integrations().forEach(integration -> integrations.put(integration.getName(), integration));
        return source;
    }

    static void mockProviderInit(Map<String, Integer> calls) {
        new MockUp<FileGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                calls.merge("file", 1, Integer::sum);
                throw new DdlException("test init failure");
            }
        };
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() {
                calls.merge("ldap", 1, Integer::sum);
            }
        };
    }

    static void equivalent(AuthenticationMgr expected, AuthenticationMgr actual) {
        StatisticsSecurityTestSupport.assertMaps(expected, actual, "userNameToProperty", "userToAuthenticationInfo",
                "nameToGroupProviderMap", "nameToSecurityIntegrationMap");
    }

    @Test
    public void testAuthenticationAndProviderInitializationMatchV2() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        Map<String, Integer> calls = new HashMap<>();
        mockProviderInit(calls);
        AuthenticationMgr source = populated();
        AuthenticationMgr v2 = new AuthenticationMgr();
        StatisticsSecurityTestSupport.v2(source::saveV2, v2::loadV2);
        assertEquals(Map.of("file", 1, "ldap", 1), calls);
        calls.clear();
        AuthenticationMgr loaded = new AuthenticationMgr();
        Path file = LoadImageTestSupport.save(dir, source::save);
        assertTrue(calls.isEmpty());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        equivalent(v2, loaded);
        assertEquals(4, sections.size());
        assertEquals(13, sections.stream().mapToLong(IndexEntryPB::getNumEntries).sum());
        assertTrue(loaded.isLoaded());
        assertEquals(Map.of("file", 1, "ldap", 1), calls);
        assertEquals(345, loaded.getMaxConn("alice"));
        assertEquals(345, loaded.getMaxConn("property_only"));
        assertFalse(loaded.doesUserExist(new UserIdentity("property_only", "%", false)));
        assertEquals(3, loaded.getAllGroupProviders().size());
        assertEquals(3, loaded.getAllSecurityIntegrations().size());

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testIpDomainAndWildcardPrecedence() throws Exception {
        mockProviderInit(new HashMap<>());
        AuthenticationMgr source = populated();
        AuthenticationMgr loaded = new AuthenticationMgr();
        LoadImageTestSupport.load(LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        loaded.setHostnameToIpSet(Map.of("host.example", Set.of("10.0.0.1", "10.0.0.2")));
        assertEquals(new UserIdentity("alice", "10.0.0.1", false),
                loaded.getBestMatchedUserIdentity("alice", "10.0.0.1").getKey());
        assertEquals(new UserIdentity("alice", "host.example", true),
                loaded.getBestMatchedUserIdentity("alice", "10.0.0.2").getKey());
        assertEquals(new UserIdentity("alice", "%", false),
                loaded.getBestMatchedUserIdentity("alice", "10.0.0.3").getKey());
        assertNull(loaded.getBestMatchedUserIdentity("bob", "10.0.0.1"));
        assertArrayEquals(AuthenticationProtoTest.authentication("alice", "%").getPassword(),
                loaded.getBestMatchedUserIdentity("alice", "10.0.0.3").getValue().getPassword());

    }

    @Test
    public void testExplicitEmptyImageDoesNotInventRoot() throws Exception {
        AuthenticationMgr source = new AuthenticationMgr();
        source.getUserToAuthenticationInfo().clear();
        ((Map<?, ?>) getField(source, "userNameToProperty")).clear();
        AuthenticationMgr v2 = new AuthenticationMgr();
        StatisticsSecurityTestSupport.v2(source::saveV2, v2::loadV2);
        AuthenticationMgr loaded = new AuthenticationMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        equivalent(v2, loaded);
        assertEquals(4, sections.size());
        assertTrue(sections.stream().allMatch(section -> section.getNumEntries() == 0));
        assertFalse(loaded.doesUserExist(UserIdentity.ROOT));
        assertTrue(loaded.isLoaded());

    }

    @Test
    public void testMalformedImageDoesNotInitializeOrInstall() throws Exception {
        Map<String, Integer> calls = new HashMap<>();
        mockProviderInit(calls);
        Message provider = AuthenticationProtoTest.providers().get(0).toProto();
        for (Message invalid : List.of(AuthenticationUserAuthenticationEntryPB.getDefaultInstance(),
                AuthenticationUserPropertyEntryPB.newBuilder().setUserName("missing").build(),
                GroupProviderPB.newBuilder().setKindValue(0).setName("invalid").build())) {
            Path file = StatisticsSecurityTestSupport.sections(dir,
                    AuthenticationMgrPB.getDefaultInstance(), List.of(provider, invalid));
            AuthenticationMgr loaded = new AuthenticationMgr();
            assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
            assertFalse(loaded.isLoaded());
            assertTrue(loaded.doesUserExist(UserIdentity.ROOT));
            assertTrue(loaded.getAllGroupProviders().isEmpty());
            assertTrue(calls.isEmpty());
        }
    }
}
