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

package com.starrocks.authorization;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.sys.GrantsTo;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.persist.DropAIProviderLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.thrift.TGetGrantsToRolesOrUserItem;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGrantsToType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AIPrivilegeSchemaTest {
    private static final String ROLE = "ai_schema_role";
    private final DefaultAuthorizationProvider authorizationProvider = new DefaultAuthorizationProvider();
    private final List<String> createdProviderIds = new ArrayList<>();
    private ConnectContext context;
    private AuthorizationMgr authorizationMgr;
    private AIProviderMgr providerMgr;

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void afterAll() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        context = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        providerMgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        authorizationMgr.createRole(new CreateRoleStmt(List.of(ROLE), false, null));
    }

    @AfterEach
    public void tearDown() {
        createdProviderIds.forEach(id -> providerMgr.replayDropProvider(new DropAIProviderLog(id)));
    }

    @Test
    public void testGlobalFunctionGrantSyntax() {
        GrantPrivilegeStmt stmt = grantStatement("USE AI FUNCTIONS ON SYSTEM");
        Assertions.assertEquals(ObjectType.SYSTEM, stmt.getObjectType());
        Assertions.assertEquals("USE AI FUNCTIONS", stmt.getPrivilegeTypes().get(0).name());
        Assertions.assertEquals(29, stmt.getPrivilegeTypes().get(0).getId());
        Assertions.assertEquals("GRANT USE AI FUNCTIONS ON SYSTEM TO ROLE '" + ROLE + "'",
                AstToSQLBuilder.toSQL(stmt));
    }

    @Test
    public void testPublicFunctionFamiliesShareCanonicalGrantObjects() throws Exception {
        ObjectType type = objectType("AI FUNCTION");
        Assertions.assertEquals(15, type.getId());
        Set<String> families = GlobalStateMgr.getCurrentState().getBuiltinFunctions().stream()
                .filter(Function::isAi).filter(Function::isUserVisible)
                .map(Function::functionName).collect(Collectors.toSet());
        Assertions.assertFalse(families.isEmpty());
        for (String family : families) {
            PEntryObject object = authorizationProvider.generateObject(type, List.of(family));
            PEntryObject upper = authorizationProvider.generateObject(type, List.of(family.toUpperCase(Locale.ROOT)));
            Assertions.assertEquals(object, upper);
            Assertions.assertEquals(object.hashCode(), upper.hashCode());
            Assertions.assertEquals(0, object.compareTo(upper));
            Assertions.assertEquals(object, object.clone());
            Assertions.assertNotSame(object, object.clone());
            Assertions.assertTrue(object.validate());
            Assertions.assertFalse(object.isFuzzyMatching());
            Assertions.assertEquals(family, object.toString());
            Assertions.assertEquals(object, GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(object, PEntryObject.class), PEntryObject.class));
        }
        for (List<String> tokens : List.of(List.of("*"), List.of("ai_query"), List.of("lower"),
                List.of("missing_ai_function"), List.of("db", "ai_complete"))) {
            Assertions.assertThrows(PrivilegeException.class, () -> authorizationProvider.generateObject(type, tokens));
        }
    }

    @Test
    public void testProviderObjectUsesUuidAndExactName() throws Exception {
        ObjectType type = objectType("AI PROVIDER");
        Assertions.assertEquals(16, type.getId());
        String id = createProvider("ai_schema_Chat");
        PEntryObject original = authorizationProvider.generateObject(type, List.of("ai_schema_Chat"));
        Assertions.assertTrue(GsonUtils.GSON.toJson(original, PEntryObject.class).contains(id));
        Assertions.assertFalse(GsonUtils.GSON.toJson(original, PEntryObject.class).contains("secret-test-key"));
        Assertions.assertEquals(original, original.clone());
        Assertions.assertEquals(original.hashCode(), original.clone().hashCode());
        Assertions.assertEquals(0, original.compareTo(original.clone()));
        Assertions.assertTrue(original.validate());
        Assertions.assertThrows(PrivilegeException.class,
                () -> authorizationProvider.generateObject(type, List.of("ai_schema_chat")));
        providerMgr.alterProvider("ai_schema_Chat", Map.of(AIProvider.PROPERTY_MODEL, "changed"), false);
        Assertions.assertEquals(original, authorizationProvider.generateObject(type, List.of("ai_schema_Chat")));
        providerMgr.dropProvider("ai_schema_Chat", false);
        createProvider("ai_schema_Chat");
        PEntryObject replacement = authorizationProvider.generateObject(type, List.of("ai_schema_Chat"));
        Assertions.assertFalse(original.validate());
        Assertions.assertNotEquals(original, replacement);
        Assertions.assertFalse(replacement.match(original));
    }

    @Test
    public void testProviderWildcardRequiresExplicitAllSyntax() throws Exception {
        createProvider("*");
        createProvider("ai_schema_chat");
        GrantPrivilegeStmt all = grantStatement("USAGE ON ALL AI PROVIDERS");
        PEntryObject wildcard = all.getObjectList().get(0);
        PEntryObject named = grantStatement("USAGE ON AI PROVIDER ai_schema_chat").getObjectList().get(0);
        Assertions.assertTrue(all.isGrantOnALL());
        Assertions.assertTrue(wildcard.isFuzzyMatching());
        Assertions.assertTrue(named.match(wildcard));
        Assertions.assertFalse(wildcard.match(named));
        Assertions.assertTrue(wildcard.compareTo(named) < 0);
        for (String object : List.of("*", "'*'", "ai_schema_chat, '*'")) {
            Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                    "GRANT USAGE ON AI PROVIDER " + object + " TO ROLE " + ROLE, context));
        }
        for (String object : List.of("ALL AI FUNCTIONS", "AI FUNCTION *", "AI FUNCTION ai_complete(VARCHAR)",
                "AI FUNCTION db.ai_complete", "ALL AI PROVIDERS IN DATABASE db")) {
            Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                    "GRANT USAGE ON " + object + " TO ROLE " + ROLE, context));
        }
    }

    @Test
    public void testInvalidActionsCannotGrantAiObjects() throws Exception {
        createProvider("ai_schema_chat");
        for (String clause : List.of("SELECT ON AI FUNCTION ai_complete", "ALTER ON AI PROVIDER ai_schema_chat",
                "OPERATE ON AI PROVIDER ai_schema_chat", "USE AI FUNCTIONS ON AI PROVIDER ai_schema_chat",
                "USAGE ON SYSTEM", "USE AI FUNCTIONS ON RESOURCE ai_schema_chat")) {
            Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                    "GRANT " + clause + " TO ROLE " + ROLE, context));
        }
        Assertions.assertEquals(List.of(PrivilegeType.USAGE),
                grantStatement("ALL ON AI FUNCTION ai_complete").getPrivilegeTypes());
        Assertions.assertEquals(List.of(PrivilegeType.USAGE),
                grantStatement("ALL ON ALL AI PROVIDERS").getPrivilegeTypes());
    }

    @Test
    public void testGrantOptionDoesNotCrossObjectTypes() throws Exception {
        createProvider("ai_schema_chat");
        GrantPrivilegeStmt global = grantStatement("USE AI FUNCTIONS ON SYSTEM");
        GrantPrivilegeStmt function = grantStatement("USAGE ON AI FUNCTION ai_complete");
        GrantPrivilegeStmt provider = grantStatement("USAGE ON AI PROVIDER ai_schema_chat");
        GrantPrivilegeStmt all = grantStatement("USAGE ON ALL AI PROVIDERS");
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        collection.grant(global.getObjectType(), global.getPrivilegeTypes(), global.getObjectList(), true);
        Assertions.assertTrue(collection.allowGrant(global.getObjectType(), global.getPrivilegeTypes(), global.getObjectList()));
        Assertions.assertFalse(collection.allowGrant(function.getObjectType(), function.getPrivilegeTypes(),
                function.getObjectList()));
        Assertions.assertFalse(collection.allowGrant(provider.getObjectType(), provider.getPrivilegeTypes(),
                provider.getObjectList()));
        collection.grant(provider.getObjectType(), provider.getPrivilegeTypes(), provider.getObjectList(), true);
        Assertions.assertTrue(collection.allowGrant(provider.getObjectType(), provider.getPrivilegeTypes(),
                provider.getObjectList()));
        Assertions.assertFalse(collection.allowGrant(all.getObjectType(), all.getPrivilegeTypes(), all.getObjectList()));
        collection.grant(all.getObjectType(), all.getPrivilegeTypes(), all.getObjectList(), true);
        Assertions.assertTrue(collection.allowGrant(all.getObjectType(), all.getPrivilegeTypes(), all.getObjectList()));
    }

    @Test
    public void testGrantRevokeRoundTripQuotesProviderNames() throws Exception {
        ShowGrantsStmt show = (ShowGrantsStmt) UtFrameUtils.parseStmtWithNewParser("SHOW GRANTS FOR ROLE " + ROLE, context);
        for (String name : List.of("ai schema`provider", "ai schema'provider", "ai schema\\'provider",
                "ai schema\"provider", "ai schema\\\\provider")) {
            createProvider(name);
            String quotedName = "'" + SqlUtils.escapeSqlString(name) + "'";
            GrantPrivilegeStmt stmt = grantStatement("USAGE ON AI PROVIDER " + quotedName, true);
            authorizationMgr.grant(stmt);
            List<List<String>> rows = ShowExecutor.execute(show, context).getResultRows();
            Assertions.assertEquals(1, rows.size());
            GrantPrivilegeStmt reparsed = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(rows.get(0).get(2), context);
            Assertions.assertEquals(stmt.getObjectList(), reparsed.getObjectList());
            Assertions.assertTrue(reparsed.isWithGrantOption());
            RevokePrivilegeStmt revoke = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                    "REVOKE USAGE ON AI PROVIDER " + quotedName + " FROM ROLE " + ROLE, context);
            authorizationMgr.revoke(revoke);
            Assertions.assertTrue(ShowExecutor.execute(show, context).getResultRows().isEmpty());
        }
    }

    @Test
    public void testShowGrantsSkipsDroppedProviderBeforeCleanup() throws Exception {
        createProvider("ai_schema_chat");
        GrantPrivilegeStmt provider = grantStatement("USAGE ON AI PROVIDER ai_schema_chat");
        GrantPrivilegeStmt function = grantStatement("USAGE ON AI FUNCTION ai_complete");
        authorizationMgr.grant(provider);
        authorizationMgr.grant(function);
        ShowGrantsStmt show = (ShowGrantsStmt) UtFrameUtils.parseStmtWithNewParser("SHOW GRANTS FOR ROLE " + ROLE, context);
        Assertions.assertEquals(2, ShowExecutor.execute(show, context).getResultRows().size());

        providerMgr.dropProvider("ai_schema_chat", false);

        Assertions.assertTrue(authorizationMgr.getRolePrivilegeCollection(ROLE)
                .check(provider.getObjectType(), PrivilegeType.USAGE, provider.getObjectList().get(0)));
        List<List<String>> rows = ShowExecutor.execute(show, context).getResultRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(AstToSQLBuilder.toSQL(function), rows.get(0).get(2));
    }

    @Test
    public void testImageAndRoleJournalPreserveAiGrants() throws Exception {
        createProvider("ai_schema_chat");
        List<GrantPrivilegeStmt> grants = List.of(grantStatement("USE AI FUNCTIONS ON SYSTEM"),
                grantStatement("USAGE ON AI FUNCTION ai_complete"),
                grantStatement("USAGE ON AI PROVIDER ai_schema_chat", true));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        AuthorizationMgr follower = new AuthorizationMgr(new DefaultAuthorizationProvider());
        for (GrantPrivilegeStmt grant : grants) {
            authorizationMgr.grant(grant);
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            follower.replayUpdateRolePrivilegeCollection(info);
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        authorizationMgr.saveV2(image.getImageWriter());
        AuthorizationMgr restored = new AuthorizationMgr(new DefaultAuthorizationProvider());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        restored.loadV2(reader);
        reader.close();
        for (AuthorizationMgr manager : List.of(follower, restored)) {
            for (GrantPrivilegeStmt grant : grants) {
                Assertions.assertTrue(manager.getRolePrivilegeCollection(ROLE).check(grant.getObjectType(),
                        grant.getPrivilegeTypes().get(0), grant.getObjectList().get(0)));
            }
            Assertions.assertTrue(manager.getRolePrivilegeCollection(ROLE).allowGrant(grants.get(2).getObjectType(),
                    grants.get(2).getPrivilegeTypes(), grants.get(2).getObjectList()));
        }
    }

    @Test
    public void testOrdinaryUserGrantAndRevokeJournalReplay() throws Exception {
        createProvider("ai_schema_chat");
        CreateUserStmt createUser = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER ai_schema_user", context);
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().createUser(createUser);
        UserIdentity user = new UserIdentity("ai_schema_user", "%");
        try {
            UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
            AuthorizationMgr follower = new AuthorizationMgr(new DefaultAuthorizationProvider());
            List<String> clauses = List.of("USE AI FUNCTIONS ON SYSTEM", "USAGE ON AI FUNCTION ai_complete",
                    "USAGE ON AI PROVIDER ai_schema_chat");
            List<GrantPrivilegeStmt> grants = new ArrayList<>();
            for (String clause : clauses) {
                GrantPrivilegeStmt grant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                        "GRANT " + clause + " TO USER ai_schema_user WITH GRANT OPTION", context);
                grants.add(grant);
                authorizationMgr.grant(grant);
                replayNextUserPrivilegeJournal(follower);
                Assertions.assertTrue(follower.getUserPrivilegeCollection(user).allowGrant(grant.getObjectType(),
                        grant.getPrivilegeTypes(), grant.getObjectList()));
            }
            UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
            authorizationMgr.saveV2(image.getImageWriter());
            AuthorizationMgr restored = new AuthorizationMgr(new DefaultAuthorizationProvider());
            SRMetaBlockReader reader = image.getMetaBlockReader();
            restored.loadV2(reader);
            reader.close();
            for (GrantPrivilegeStmt grant : grants) {
                Assertions.assertTrue(restored.getUserPrivilegeCollection(user).allowGrant(grant.getObjectType(),
                        grant.getPrivilegeTypes(), grant.getObjectList()));
            }
            for (int i = 0; i < clauses.size(); i++) {
                RevokePrivilegeStmt revoke = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                        "REVOKE " + clauses.get(i) + " FROM USER ai_schema_user", context);
                authorizationMgr.revoke(revoke);
                replayNextUserPrivilegeJournal(follower);
                GrantPrivilegeStmt grant = grants.get(i);
                Assertions.assertFalse(follower.getUserPrivilegeCollection(user).check(grant.getObjectType(),
                        grant.getPrivilegeTypes().get(0), grant.getObjectList().get(0)));
            }
        } finally {
            GlobalStateMgr.getCurrentState().getAuthenticationMgr().dropUser((DropUserStmt)
                    UtFrameUtils.parseStmtWithNewParser("DROP USER IF EXISTS ai_schema_user", context));
        }
    }

    @Test
    public void testBuiltinRolesAreRebuiltWhenLoadingImageAndJournalWithoutAiGrants() throws Exception {
        GrantPrivilegeStmt global = grantStatement("USE AI FUNCTIONS ON SYSTEM");
        GrantPrivilegeStmt providers = grantStatement("USAGE ON ALL AI PROVIDERS");
        for (String role : List.of(PrivilegeBuiltinConstants.ROOT_ROLE_NAME, PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME)) {
            for (GrantPrivilegeStmt grant : List.of(global, providers)) {
                Assertions.assertTrue(authorizationMgr.getRolePrivilegeCollection(role).check(grant.getObjectType(),
                        grant.getPrivilegeTypes().get(0), grant.getObjectList().get(0)));
            }
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        // Built-in permission entries are deliberately omitted, just as in an image predating AI RBAC.
        authorizationMgr.saveV2(image.getImageWriter());
        AuthorizationMgr restored = new AuthorizationMgr(new DefaultAuthorizationProvider());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        restored.loadV2(reader);
        reader.close();
        AuthorizationMgr follower = new AuthorizationMgr(new DefaultAuthorizationProvider());
        for (String role : List.of(PrivilegeBuiltinConstants.ROOT_ROLE_NAME, PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME)) {
            RolePrivilegeCollectionInfo emptyRoleUpdate = new RolePrivilegeCollectionInfo(
                    Map.of(authorizationMgr.getRoleIdByNameNoLock(role), new RolePrivilegeCollectionV2(role)),
                    authorizationProvider.getPluginId(), authorizationProvider.getPluginVersion());
            follower.replayUpdateRolePrivilegeCollection(GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(emptyRoleUpdate), RolePrivilegeCollectionInfo.class));
        }
        for (AuthorizationMgr manager : List.of(restored, follower)) {
            for (GrantPrivilegeStmt grant : List.of(global, providers)) {
                for (String role : List.of(PrivilegeBuiltinConstants.ROOT_ROLE_NAME,
                        PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME)) {
                    Assertions.assertTrue(manager.getRolePrivilegeCollection(role).check(grant.getObjectType(),
                            grant.getPrivilegeTypes().get(0), grant.getObjectList().get(0)));
                }
                Assertions.assertFalse(manager.getRolePrivilegeCollection(PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME)
                        .check(grant.getObjectType(), grant.getPrivilegeTypes().get(0), grant.getObjectList().get(0)));
            }
        }
    }

    @Test
    public void testSysGrantsAndInvalidObjectCleanup() throws Exception {
        createProvider("ai_schema_chat");
        GrantPrivilegeStmt named = grantStatement("USAGE ON AI PROVIDER ai_schema_chat", true);
        GrantPrivilegeStmt function = grantStatement("USAGE ON AI FUNCTION ai_complete");
        authorizationMgr.grant(named);
        authorizationMgr.grant(function);
        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.ROLE);
        List<TGetGrantsToRolesOrUserItem> rows = GrantsTo.getGrantsTo(request).getGrants_to();
        Assertions.assertTrue(rows.stream().anyMatch(row -> "AI PROVIDER".equals(row.getObject_type())
                && "ai_schema_chat".equals(row.getObject_name()) && row.isIs_grantable()));
        Assertions.assertTrue(rows.stream().anyMatch(row -> "AI FUNCTION".equals(row.getObject_type())
                && "ai_complete".equals(row.getObject_name())));
        providerMgr.dropProvider("ai_schema_chat", false);
        rows = GrantsTo.getGrantsTo(request).getGrants_to();
        Assertions.assertFalse(rows.stream().anyMatch(row -> "AI PROVIDER".equals(row.getObject_type())));
        authorizationMgr.removeInvalidObject();
        Assertions.assertFalse(authorizationMgr.getRolePrivilegeCollection(ROLE)
                .check(named.getObjectType(), PrivilegeType.USAGE, named.getObjectList().get(0)));
        authorizationMgr.grant(grantStatement("USAGE ON ALL AI PROVIDERS"));
        createProvider("ai_schema_future");
        rows = GrantsTo.getGrantsTo(request).getGrants_to();
        Assertions.assertTrue(rows.stream().anyMatch(row -> "AI PROVIDER".equals(row.getObject_type())
                && "ai_schema_future".equals(row.getObject_name())));
    }

    private String createProvider(String name) throws Exception {
        String id = providerMgr.createProvider(name, AIProviderType.CHAT,
                Map.of(AIProvider.PROPERTY_ENDPOINT, "https://example.invalid/v1/chat/completions",
                        AIProvider.PROPERTY_MODEL, "test-model", AIProvider.PROPERTY_API_KEY, "secret-test-key"), "");
        createdProviderIds.add(id);
        return id;
    }

    private void replayNextUserPrivilegeJournal(AuthorizationMgr follower) throws Exception {
        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        follower.replayUpdateUserPrivilegeCollection(info.getUserIdentity(), info.getPrivilegeCollection(),
                info.getPluginId(), info.getPluginVersion());
    }

    private ObjectType objectType(String name) {
        return Assertions.assertDoesNotThrow(() -> authorizationProvider.getObjectType(name));
    }

    private GrantPrivilegeStmt grantStatement(String privilegeAndObject) {
        return grantStatement(privilegeAndObject, false);
    }

    private GrantPrivilegeStmt grantStatement(String privilegeAndObject, boolean withGrantOption) {
        return Assertions.assertDoesNotThrow(() -> (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT " + privilegeAndObject + " TO ROLE " + ROLE + (withGrantOption ? " WITH GRANT OPTION" : ""), context));
    }
}
