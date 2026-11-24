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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.Message;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.ActionSet;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PolicyFCEntryObject;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeCollectionV2;
import com.starrocks.authorization.PrivilegeEntry;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.authorization.RolePrivilegeCollectionV2.RoleFlags;
import com.starrocks.authorization.UserPrivilegeCollectionV2;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.proto.AuthorizationGroupRolesEntryPB;
import com.starrocks.persist.proto.AuthorizationMgrPB;
import com.starrocks.persist.proto.AuthorizationRoleHeaderEntryPB;
import com.starrocks.persist.proto.AuthorizationRoleNameIdEntryPB;
import com.starrocks.persist.proto.AuthorizationRolePrivilegeEntryPB;
import com.starrocks.persist.proto.AuthorizationUserHeaderEntryPB;
import com.starrocks.persist.proto.AuthorizationUserPrivilegeEntryPB;
import com.starrocks.persist.proto.PrivilegeEntryPB;
import com.starrocks.persist.proto.PrivilegeObjectPB;
import com.starrocks.persist.proto.RolePrivilegeHeaderPB;
import com.starrocks.persist.proto.UserPrivilegeHeaderPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizationMgrImageV3Test {
    static final UserIdentity ALICE = new UserIdentity("alice", "%", false);
    static final UserIdentity EMPTY_USER = new UserIdentity("empty", "%", false);
    static final long ROLE = 100;
    static final long PARENT = 101;
    @TempDir
    Path dir;

    @BeforeEach
    public void setUp() {
        global();
    }

    static GlobalStateMgr global() {
        GlobalStateMgr global = LoadImageTestSupport.global();
        Mockito.when(global.getAuthorizer()).thenReturn(new Authorizer(null));
        return global;
    }

    static AuthorizationMgr manager() {
        return new AuthorizationMgr(new DefaultAuthorizationProvider());
    }

    static AuthorizationMgr populated() throws Exception {
        AuthorizationMgr source = manager();
        UserPrivilegeCollectionV2 user = new UserPrivilegeCollectionV2();
        user.grantRole(ROLE);
        user.setDefaultRoleIds(new HashSet<>(Set.of(ROLE)));
        user.grant(ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.of(PrivilegeProtoTest.table(20)), true);
        user.grant(ObjectType.AI_PROVIDER, List.of(PrivilegeType.USAGE),
                List.of(PrivilegeProtoTest.object("AIProviderPEntryObject", "\"id\":\"provider-uuid\"")), true);
        user.grant(ObjectType.AI_FUNCTION, List.of(PrivilegeType.USAGE),
                List.of(PrivilegeProtoTest.object("AIFunctionPEntryObject", "\"name\":\"ai_query\"")), false);
        user.grant(ObjectType.SYSTEM, List.of(PrivilegeType.OPERATE), Collections.singletonList(null), false);
        Map<UserIdentity, UserPrivilegeCollectionV2> users = getField(source, "userToPrivilegeCollection");
        users.put(ALICE, user);
        users.put(EMPTY_USER, new UserPrivilegeCollectionV2());
        RolePrivilegeCollectionV2 role = new RolePrivilegeCollectionV2("analyst", "role comment",
                RoleFlags.MUTABLE, RoleFlags.REMOVABLE);
        role.addParentRole(PARENT);
        role.grant(ObjectType.TABLE, List.of(PrivilegeType.INSERT), List.of(PrivilegeProtoTest.table(20)), false);
        RolePrivilegeCollectionV2 parent = new RolePrivilegeCollectionV2("parent", "parent comment", RoleFlags.MUTABLE);
        parent.addSubRole(ROLE);
        parent.grant(ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.of(PrivilegeProtoTest.table(21)), false);
        Map<Long, RolePrivilegeCollectionV2> roles = getField(source, "roleIdToPrivilegeCollection");
        roles.put(ROLE, role);
        roles.put(PARENT, parent);
        roles.put(102L, new RolePrivilegeCollectionV2("empty_role", "empty", RoleFlags.MUTABLE));
        Map<String, Long> names = getField(source, "roleNameToId");
        names.put("analyst", ROLE);
        names.put("parent", PARENT);
        names.put("empty_role", 102L);
        names.put("retained_alias", ROLE);
        Map<String, Set<Long>> groups = getField(source, "groupToRoleList");
        groups.put("analysts", new HashSet<>(Set.of(ROLE)));
        groups.put("empty_group", new HashSet<>());
        source.getUserPrivilegeCollection(UserIdentity.ROOT).grantRole(ROLE);
        source.getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID).addSubRole(ROLE);
        return source;
    }

    static void equivalent(AuthorizationMgr expected, AuthorizationMgr actual) {
        GsonEquivalence.assertEquivalent(expected, actual);
        StatisticsSecurityTestSupport.assertMaps(expected, actual, "userToPrivilegeCollection", "roleIdToPrivilegeCollection");
    }

    private AuthorizationMgr roundTrip(AuthorizationMgr source) throws Exception {
        AuthorizationMgr loaded = manager();
        LoadImageTestSupport.load(LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        return loaded;
    }

    private PrivilegeCollectionV2 merged(AuthorizationMgr manager, UserIdentity user, Set<String> groups, Set<Long> roles) {
        return invoke(manager, "loadPrivilegeCollection", user, groups, roles);
    }

    @Test
    public void testUsersRolesAndMappingsMatchV2() throws Exception {
        AuthorizationMgr source = populated();
        AuthorizationMgr v2 = manager();
        StatisticsSecurityTestSupport.v2(source::saveV2, v2::loadV2);
        AuthorizationMgr loaded = roundTrip(source);
        equivalent(v2, loaded);
        for (AuthorizationMgr restored : List.of(v2, loaded)) {
            UserPrivilegeCollectionV2 privileges = restored.getUserPrivilegeCollection(ALICE);
            assertTrue(privileges.check(ObjectType.AI_PROVIDER, PrivilegeType.USAGE,
                    PrivilegeProtoTest.object("AIProviderPEntryObject", "\"id\":\"provider-uuid\"")));
            assertFalse(privileges.check(ObjectType.AI_PROVIDER, PrivilegeType.USAGE,
                    PrivilegeProtoTest.object("AIProviderPEntryObject", "\"id\":\"another-provider\"")));
            assertTrue(privileges.check(ObjectType.AI_FUNCTION, PrivilegeType.USAGE,
                    PrivilegeProtoTest.object("AIFunctionPEntryObject", "\"name\":\"ai_query\"")));
        }
        assertTrue(loaded.isLoaded());
        assertEquals(Set.of(ROLE), loaded.getUserPrivilegeCollection(ALICE).getDefaultRoleIds());
        assertEquals(Set.of(PARENT), loaded.getRolePrivilegeCollection(ROLE).getParentRoleIds());
        assertEquals(Set.of(ROLE), loaded.getRolePrivilegeCollection(PARENT).getSubRoleIds());
        assertEquals(Set.of(ROLE), loaded.getRoleIdListByGroup("analysts"));
        assertTrue(loaded.getRoleIdListByGroup("empty_group").isEmpty());
        assertEquals("role comment", loaded.getRolePrivilegeCollection(ROLE).getComment());
        assertTrue(loaded.getUserPrivilegeCollection(EMPTY_USER).isEmpty());
        assertTrue(loaded.getRolePrivilegeCollection(102).isEmpty());
        assertEquals(ROLE, (long) ((Map<String, Long>) getField(loaded, "roleNameToId")).get("retained_alias"));

    }

    @Test
    public void testActualPermissionsAndGrantOptionAfterLoad() throws Exception {
        AuthorizationMgr loaded = roundTrip(populated());
        PrivilegeCollectionV2 active = merged(loaded, ALICE, Set.of(), Set.of(ROLE));
        assertTrue(active.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(20)));
        assertTrue(active.check(ObjectType.TABLE, PrivilegeType.INSERT, PrivilegeProtoTest.table(20)));
        assertTrue(active.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(21)));
        assertFalse(active.check(ObjectType.TABLE, PrivilegeType.DELETE, PrivilegeProtoTest.table(20)));
        assertFalse(active.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(22)));
        assertTrue(active.check(ObjectType.SYSTEM, PrivilegeType.OPERATE, null));
        assertTrue(active.allowGrant(ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.of(PrivilegeProtoTest.table(20))));
        assertFalse(active.allowGrant(ObjectType.TABLE, List.of(PrivilegeType.INSERT),
                List.of(PrivilegeProtoTest.table(20))));
        PrivilegeCollectionV2 inactive = merged(loaded, ALICE, Set.of(), Set.of());
        assertTrue(inactive.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(20)));
        assertFalse(inactive.check(ObjectType.TABLE, PrivilegeType.INSERT, PrivilegeProtoTest.table(20)));
        assertFalse(inactive.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(21)));
        PrivilegeCollectionV2 group = merged(loaded, EMPTY_USER, Set.of("analysts"), Set.of());
        assertTrue(group.check(ObjectType.TABLE, PrivilegeType.INSERT, PrivilegeProtoTest.table(20)));
        assertTrue(group.check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(21)));
        assertFalse(group.check(ObjectType.TABLE, PrivilegeType.DELETE, PrivilegeProtoTest.table(20)));

    }

    @Test
    public void testRootAndImmutableRoleUseCurrentBuiltinGrants() throws Exception {
        AuthorizationMgr source = populated();
        UserPrivilegeCollectionV2 root = source.getUserPrivilegeCollection(UserIdentity.ROOT);
        root.getTypeToPrivilegeEntryList().put(ObjectType.TABLE, new ArrayList<>(List.of(
                new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)), PrivilegeProtoTest.table(999), false))));
        root.setDefaultRoleIds(new HashSet<>(Set.of(ROLE)));
        source.getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID).getTypeToPrivilegeEntryList().clear();
        AuthorizationMgr v2 = manager();
        StatisticsSecurityTestSupport.v2(source::saveV2, v2::loadV2);
        AuthorizationMgr loaded = roundTrip(source);
        equivalent(v2, loaded);
        UserPrivilegeCollectionV2 loadedRoot = loaded.getUserPrivilegeCollection(UserIdentity.ROOT);
        assertTrue(loadedRoot.getAllRoles().containsAll(Set.of(ROLE, PrivilegeBuiltinConstants.ROOT_ROLE_ID)));
        assertEquals(manager().getUserPrivilegeCollection(UserIdentity.ROOT).getDefaultRoleIds(), loadedRoot.getDefaultRoleIds());
        assertFalse(loadedRoot.getTypeToPrivilegeEntryList().containsKey(ObjectType.TABLE));
        PrivilegeCollectionV2 privileges = merged(loaded, UserIdentity.ROOT, Set.of(), loadedRoot.getAllRoles());
        assertTrue(privileges.check(ObjectType.TABLE, PrivilegeType.DELETE, PrivilegeProtoTest.table(999)));
        assertEquals(Set.of(ROLE), loaded.getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID).getSubRoleIds());
    }

    @Test
    public void testBuiltinGrantsAreOmittedFromFile() throws Exception {
        Path file = LoadImageTestSupport.save(dir, populated()::save);
        List<Long> headers = new ArrayList<>();
        List<Long> owners = new ArrayList<>();
        LoadImageTestSupport.load(file, List.of(
                Pair.create(SectionId.AUTHORIZATION_ROLE_HEADER, reader -> reader.readEntries(
                        AuthorizationRoleHeaderEntryPB.parser(), entry -> headers.add(entry.getRoleId()))),
                Pair.create(SectionId.AUTHORIZATION_ROLE_PRIVILEGE, reader -> reader.readEntries(
                        AuthorizationRolePrivilegeEntryPB.parser(), entry -> owners.add(entry.getRoleId())))));
        assertTrue(headers.containsAll(PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS));
        assertTrue(Collections.disjoint(owners, PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS));
        assertTrue(owners.containsAll(List.of(ROLE, PARENT)));
    }

    @Test
    public void testLargeUserAndRolePrivilegesAreSeparateEntries() throws Exception {
        AuthorizationMgr source = populated();
        List<PrivilegeEntry> entries = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            entries.add(new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)),
                    PrivilegeProtoTest.table(1000 + i), false));
        }
        source.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().put(ObjectType.TABLE, entries);
        source.getRolePrivilegeCollection(ROLE).getTypeToPrivilegeEntryList().put(ObjectType.TABLE, new ArrayList<>(entries));
        Path file = LoadImageTestSupport.save(dir, source::save);
        List<AuthorizationUserPrivilegeEntryPB> users = new ArrayList<>();
        List<AuthorizationRolePrivilegeEntryPB> roles = new ArrayList<>();
        LoadImageTestSupport.load(file, List.of(
                Pair.create(SectionId.AUTHORIZATION_USER_PRIVILEGE, reader ->
                        reader.readEntries(AuthorizationUserPrivilegeEntryPB.parser(), users::add)),
                Pair.create(SectionId.AUTHORIZATION_ROLE_PRIVILEGE, reader ->
                        reader.readEntries(AuthorizationRolePrivilegeEntryPB.parser(), roles::add))));
        assertEquals(1024, users.stream().filter(entry -> entry.getObjectType() == ObjectType.TABLE.getId()).count());
        assertEquals(1024, roles.stream().filter(entry -> entry.getRoleId() == ROLE).count());
        AuthorizationMgr loaded = manager();
        LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        for (long table : List.of(1000L, 1512L, 2023L)) {
            assertTrue(loaded.getUserPrivilegeCollection(ALICE)
                    .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(table)));
            assertTrue(loaded.getRolePrivilegeCollection(ROLE)
                    .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(table)));
        }
        assertFalse(loaded.getUserPrivilegeCollection(ALICE)
                .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(2024)));

    }

    static Message userHeader(UserIdentity user) {
        return AuthorizationUserHeaderEntryPB.newBuilder()
                .setUser(UserIdentityUtils.toProto(user)).setHeader(UserPrivilegeHeaderPB.getDefaultInstance()).build();
    }

    static Message roleHeader(long role) {
        return AuthorizationRoleHeaderEntryPB.newBuilder()
                .setRoleId(role).setHeader(RolePrivilegeHeaderPB.newBuilder().setName("role")).build();
    }

    static PrivilegeEntryPB privilege(boolean unknown) {
        PrivilegeEntryPB.Builder entry = new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)),
                PrivilegeProtoTest.table(20), true).toProto().toBuilder();
        if (unknown) {
            entry.setObject(PrivilegeObjectPB.newBuilder().setKindValue(999));
        }
        return entry.build();
    }

    static Message userPrivilege(UserIdentity user, PrivilegeEntryPB entry) {
        return AuthorizationUserPrivilegeEntryPB.newBuilder()
                .setUser(UserIdentityUtils.toProto(user)).setObjectType(ObjectType.TABLE.getId()).setPrivilege(entry).build();
    }

    static Message rolePrivilege(long role, PrivilegeEntryPB entry) {
        return AuthorizationRolePrivilegeEntryPB.newBuilder()
                .setRoleId(role).setObjectType(ObjectType.TABLE.getId()).setPrivilege(entry).build();
    }

    private AuthorizationMgr loadEntries(List<Message> entries) throws Exception {
        AuthorizationMgr loaded = manager();
        Path file = StatisticsSecurityTestSupport.sections(dir,
                AuthorizationMgrPB.newBuilder().setPluginId(new DefaultAuthorizationProvider().getPluginId())
                        .setPluginVersion(new DefaultAuthorizationProvider().getPluginVersion()).build(), entries);
        LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        return loaded;
    }

    @Test
    public void testUnknownSubtypeRejectsByDefault() throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = false;
        try {
            assertThrows(ImageFormatException.class, () -> loadEntries(List.of(
                    userHeader(ALICE), userPrivilege(ALICE, privilege(false)), userPrivilege(ALICE, privilege(true)))));
            assertThrows(ImageFormatException.class, () -> loadEntries(List.of(
                    roleHeader(ROLE), rolePrivilege(ROLE, privilege(false)), rolePrivilege(ROLE, privilege(true)))));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @Test
    public void testUnknownSubtypeSkipsOnlyGrantAndKeepsBuiltinRules() throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = true;
        try {
            List<Message> entries = new ArrayList<>();
            for (UserIdentity user : List.of(ALICE, UserIdentity.ROOT)) {
                entries.add(userHeader(user));
                entries.add(userPrivilege(user, privilege(false)));
                entries.add(userPrivilege(user, privilege(true)));
                entries.add(userPrivilege(user, privilege(false)));
            }
            for (long role : List.of(ROLE, PrivilegeBuiltinConstants.ROOT_ROLE_ID)) {
                entries.add(roleHeader(role));
                entries.add(rolePrivilege(role, privilege(false)));
                entries.add(rolePrivilege(role, privilege(true)));
                entries.add(rolePrivilege(role, privilege(false)));
            }
            entries.add(userHeader(EMPTY_USER));
            entries.add(userPrivilege(EMPTY_USER, privilege(false)));
            AuthorizationMgr loaded = loadEntries(entries);
            assertEquals(2, loaded.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().get(ObjectType.TABLE).size());
            assertEquals(2, loaded.getRolePrivilegeCollection(ROLE).getTypeToPrivilegeEntryList().get(ObjectType.TABLE).size());
            GsonEquivalence.assertEquivalent(manager().getUserPrivilegeCollection(UserIdentity.ROOT),
                    loaded.getUserPrivilegeCollection(UserIdentity.ROOT));
            RolePrivilegeCollectionV2 rootRole = loaded.getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID);
            assertEquals("role", rootRole.getName());
            assertEquals("", rootRole.getComment());
            StatisticsSecurityTestSupport.assertMap(
                    manager().getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID).getTypeToPrivilegeEntryList(),
                    rootRole.getTypeToPrivilegeEntryList());
            assertTrue(loaded.getUserPrivilegeCollection(EMPTY_USER)
                    .check(ObjectType.TABLE, PrivilegeType.SELECT, PrivilegeProtoTest.table(20)));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUnknownGrantSkipMatchesActualV2ReadMap(boolean ignore) throws Exception {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = ignore;
        try {
            AuthorizationMgr source = manager();
            UserPrivilegeCollectionV2 user = new UserPrivilegeCollectionV2();
            user.grant(ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.of(PrivilegeProtoTest.table(20)), true);
            JsonObject unknown = GsonUtils.GSON.toJsonTree(user).getAsJsonObject();
            JsonArray grants = unknown.getAsJsonArray("m2").get(0).getAsJsonArray().get(1).getAsJsonArray();
            JsonObject futureGrant = grants.get(0).deepCopy().getAsJsonObject();
            futureGrant.getAsJsonObject("o").addProperty("clazz", "FuturePEntryObject");
            grants.add(futureGrant);
            grants.add(grants.get(0).deepCopy());
            StatisticsSecurityTestSupport.configureV2GsonReader();
            AuthorizationMgr v2 = manager();
            StatisticsSecurityTestSupport.SaveV2 save = writer -> {
                SRMetaBlockWriter block = writer.getBlockWriter(SRMetaBlockID.AUTHORIZATION_MGR, 5);
                block.writeJson(source);
                block.writeInt(1);
                block.writeJson(ALICE);
                block.writeJson(unknown);
                block.writeInt(0);
                block.close();
            };
            if (!ignore) {
                assertThrows(SubtypeNotFoundException.class, () -> StatisticsSecurityTestSupport.v2(save, v2::loadV2));
                assertThrows(ImageFormatException.class, () -> loadEntries(List.of(userHeader(ALICE),
                        userPrivilege(ALICE, privilege(true)))));
                return;
            }
            StatisticsSecurityTestSupport.v2(save, v2::loadV2);
            AuthorizationMgr loaded = loadEntries(List.of(userHeader(ALICE), userPrivilege(ALICE, privilege(false)),
                    userPrivilege(ALICE, privilege(true)), userPrivilege(ALICE, privilege(false))));
            equivalent(v2, loaded);
            assertEquals(2, loaded.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().get(ObjectType.TABLE).size());
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @Test
    public void testKnownMalformedPayloadIsNeverIgnored() {
        boolean old = Config.metadata_ignore_unknown_subtype;
        Config.metadata_ignore_unknown_subtype = true;
        try {
            PrivilegeEntryPB malformed = privilege(false).toBuilder()
                    .setObject(PrivilegeObjectPB.newBuilder().setKind(PrivilegeObjectPB.Kind.TABLE)).build();
            assertThrows(ImageFormatException.class,
                    () -> loadEntries(List.of(userHeader(ALICE), userPrivilege(ALICE, malformed))));
        } finally {
            Config.metadata_ignore_unknown_subtype = old;
        }
    }

    @Test
    public void testPolicyPlaceholderCleanupMatchesV2() throws Exception {
        AuthorizationMgr source = populated();
        UserPrivilegeCollectionV2 user = source.getUserPrivilegeCollection(ALICE);
        user.getTypeToPrivilegeEntryList().put(ObjectType.CATALOG, new ArrayList<>(List.of(
                new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.USAGE)), new PolicyFCEntryObject(), false))));
        user.getTypeToPrivilegeEntryList().get(ObjectType.TABLE).add(
                new PrivilegeEntry(new ActionSet(List.of(PrivilegeType.SELECT)), new PolicyFCEntryObject(), false));
        AuthorizationMgr v2 = manager();
        StatisticsSecurityTestSupport.v2(source::saveV2, v2::loadV2);
        AuthorizationMgr loaded = roundTrip(source);
        equivalent(v2, loaded);
        assertFalse(loaded.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().containsKey(ObjectType.CATALOG));
        assertEquals(1, loaded.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().get(ObjectType.TABLE).size());
    }

    @Test
    public void testUnknownObjectTypeAndActionBitsSurvive() throws Exception {
        AuthorizationMgr source = populated();
        ObjectType type = GsonUtils.GSON.fromJson("{\"id\":29999}", ObjectType.class);
        ActionSet actions = GsonUtils.GSON.fromJson("{\"b\":-1}", ActionSet.class);
        source.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().put(type,
                new ArrayList<>(List.of(new PrivilegeEntry(actions, PrivilegeProtoTest.table(20), true))));
        AuthorizationMgr loaded = roundTrip(source);
        PrivilegeEntry entry = loaded.getUserPrivilegeCollection(ALICE).getTypeToPrivilegeEntryList().get(type).get(0);
        assertEquals(-1L, (long) getField(entry.getActionSet(), "bitSet"));
        assertTrue(entry.isWithGrantOption());
        assertEquals(PrivilegeProtoTest.table(20), entry.getObject());
    }

    @Test
    public void testEmptySectionRestoresBuiltins() throws Exception {
        AuthorizationMgr loaded = loadEntries(List.of());
        AuthorizationMgr expected = manager();
        StatisticsSecurityTestSupport.assertMaps(expected, loaded, "userToPrivilegeCollection", "roleIdToPrivilegeCollection");
        assertTrue(loaded.isLoaded());
    }

    @Test
    public void testMalformedEntryAndMissingOwner() {
        for (Message invalid : List.of(userPrivilege(ALICE, privilege(false)), rolePrivilege(ROLE, privilege(false)),
                AuthorizationUserHeaderEntryPB.getDefaultInstance(),
                AuthorizationRoleHeaderEntryPB.getDefaultInstance(),
                AuthorizationGroupRolesEntryPB.getDefaultInstance(),
                AuthorizationRoleNameIdEntryPB.getDefaultInstance())) {
            assertThrows(ImageFormatException.class, () -> loadEntries(List.of(invalid)));
        }
    }

    @Test
    public void testLatePrivilegeFailureDoesNotInstallEarlierSections() throws Exception {
        AuthorizationMgr destination = populated();
        AuthorizationMgr expected = populated();
        UserIdentity staged = new UserIdentity("staged", "%");
        Path file = StatisticsSecurityTestSupport.sections(dir, AuthorizationMgrPB.getDefaultInstance(), List.of(
                userHeader(staged), userPrivilege(staged, privilege(false)),
                roleHeader(ROLE), AuthorizationRolePrivilegeEntryPB.newBuilder().setRoleId(ROLE).build()));
        assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, destination.getImageV3Loaders()));
        equivalent(expected, destination);
    }

    @Test
    public void testPluginMetadataAndShortRange() throws Exception {
        for (short value : new short[] {Short.MIN_VALUE, 0, Short.MAX_VALUE}) {
            AuthorizationMgr source = manager();
            setField(source, "pluginId", value);
            setField(source, "pluginVersion", value);
            AuthorizationMgr loaded = roundTrip(source);
            assertEquals(value, (short) getField(loaded, "pluginId"));
            assertEquals(value, (short) getField(loaded, "pluginVersion"));
        }
        Path file = StatisticsSecurityTestSupport.sections(dir,
                AuthorizationMgrPB.newBuilder().setPluginVersion(32768).build(), List.of());
        AuthorizationMgr loaded = manager();
        assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
        assertFalse(loaded.isLoaded());
    }
}
