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

import com.starrocks.catalog.AIModel;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.AIModelMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AIModelBindings;
import com.starrocks.sql.common.MetaNotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AIModelPrivilegeTest {
    private static AIModel model(long id) throws Exception {
        return AIModel.create(id, "same_model_name", Map.of(
                "capability", "CHAT", "provider", "openai_compatible",
                "endpoint", "https://models.example.test/v1/chat/completions",
                "model", "remote-model", "credential_ref", "TEST_MODEL"), "");
    }

    private static ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("model_user", "%"));
        context.setCurrentRoleIds(Set.of());
        return context;
    }

    @Test
    public void testNativeChecksCapturedIdentityWithoutNameResolution() throws Exception {
        AIModel captured = model(101);
        AIModel replacement = model(102);
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        collection.grant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE),
                List.of(AIModelPEntryObject.forId(captured.getId())), false);
        AuthorizationMgr manager = Mockito.mock(AuthorizationMgr.class);
        manager.provider = new DefaultAuthorizationProvider();
        Mockito.when(manager.mergePrivilegeCollection(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(collection);
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAuthorizationMgr()).thenReturn(manager);
        ConnectContext context = context();
        NativeAccessController controller = new NativeAccessController();

        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            Assertions.assertDoesNotThrow(() -> controller.checkAIModelAction(context, captured, PrivilegeType.USAGE));
            Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnAIModel(context, captured));
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkAIModelAction(context, replacement, PrivilegeType.USAGE));
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkAIModelAction(context, captured, PrivilegeType.ALTER));
            collection.grant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE),
                    List.of(AIModelPEntryObject.generate(List.of("*"))), false);
            Assertions.assertDoesNotThrow(() -> controller.checkAIModelAction(context, replacement, PrivilegeType.USAGE));
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkAIModelAction(context, null, PrivilegeType.USAGE));
            Mockito.verify(state, Mockito.never()).getAIModelMgr();
        }
    }

    @Test
    public void testNativePrivilegeLookupErrorsFailClosed() throws Exception {
        AuthorizationMgr manager = Mockito.mock(AuthorizationMgr.class);
        Mockito.when(manager.mergePrivilegeCollection(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new PrivObjNotFoundException("privilege collection is unavailable"));
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAuthorizationMgr()).thenReturn(manager);
        ConnectContext context = context();
        AIModel model = model(101);
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            NativeAccessController controller = new NativeAccessController();
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkAIModelAction(context, model, PrivilegeType.USAGE));
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkAnyActionOnAIModel(context, model));
        }
    }

    @Test
    public void testDefaultControllerDeniesAIModelActions() throws Exception {
        AccessController controller = new AccessController() { };
        ConnectContext context = context();
        AIModel model = model(101);
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkAIModelAction(context, model, PrivilegeType.USAGE));
        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkAnyActionOnAIModel(context, model));
    }

    @Test
    public void testPrivilegeIdentityAndWildcardSurviveGson() throws Exception {
        AIModelPEntryObject specific = AIModelPEntryObject.forId(101);
        AIModelPEntryObject wildcard = AIModelPEntryObject.generate(List.of("*"));
        Assertions.assertTrue(specific.match(wildcard));
        Assertions.assertFalse(wildcard.match(specific));
        Assertions.assertFalse(specific.match(AIModelPEntryObject.forId(102)));
        Assertions.assertEquals(specific, specific.clone());
        Assertions.assertEquals(0, specific.compareTo(AIModelPEntryObject.forId(101)));
        Assertions.assertTrue(wildcard.compareTo(specific) < 0);
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIModelPEntryObject.forId(-1));

        for (PEntryObject object : List.of(specific, wildcard)) {
            String json = GsonUtils.GSON.toJson(object, PEntryObject.class);
            PEntryObject restored = GsonUtils.GSON.fromJson(json, PEntryObject.class);
            Assertions.assertInstanceOf(AIModelPEntryObject.class, restored);
            Assertions.assertEquals(object, restored);
            Assertions.assertEquals(object.isFuzzyMatching(), restored.isFuzzyMatching());
            Assertions.assertFalse(json.contains("credential"));
            Assertions.assertFalse(json.contains("endpoint"));
        }
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        collection.grant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE), List.of(specific), true);
        PrivilegeCollectionV2 restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(collection), PrivilegeCollectionV2.class);
        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        Assertions.assertTrue(provider.check(ObjectType.AI_MODEL, PrivilegeType.USAGE, specific, restored));
        Assertions.assertTrue(provider.allowGrant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE),
                List.of(specific), restored));
        Assertions.assertFalse(provider.check(ObjectType.AI_MODEL, PrivilegeType.USAGE,
                AIModelPEntryObject.forId(102), restored));
    }

    @Test
    public void testWildcardGrantOptionIsNotHiddenBySpecificGrant() throws Exception {
        AIModelPEntryObject specific = AIModelPEntryObject.forId(101);
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        collection.grant(ObjectType.AI_MODEL, List.of(PrivilegeType.ALTER), List.of(specific), true);
        collection.grant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE),
                List.of(AIModelPEntryObject.generate(List.of("*"))), true);
        Assertions.assertTrue(collection.allowGrant(ObjectType.AI_MODEL, List.of(PrivilegeType.USAGE), List.of(specific)));
        Assertions.assertTrue(collection.allowGrant(ObjectType.AI_MODEL, List.of(PrivilegeType.ALTER), List.of(specific)));
        Assertions.assertFalse(collection.allowGrant(ObjectType.AI_MODEL, List.of(PrivilegeType.DROP), List.of(specific)));
    }

    @Test
    public void testAuthorizerUsesCapturedModelAndInternalController() throws Exception {
        AIModel model = model(101);
        AccessController internal = Mockito.mock(AccessController.class);
        AccessController external = Mockito.mock(AccessController.class);
        AuthorizerStmtVisitor visitor = Mockito.mock(AuthorizerStmtVisitor.class);
        AccessControlProvider provider = new AccessControlProvider(visitor, internal);
        provider.setAccessControl("external_catalog", external);
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAuthorizer()).thenReturn(new Authorizer(provider));
        ConnectContext context = context();
        context.setCurrentCatalog("external_catalog");
        StatementBase statement = Mockito.mock(StatementBase.class);
        AIModelBindings bindings = new AIModelBindings(Map.of(model.getName(), model));
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            Authorizer.check(statement, context, bindings);
            Mockito.verify(visitor).check(statement, context);
            Mockito.verify(internal).checkAIModelAction(context, model, PrivilegeType.USAGE);
            Mockito.verifyNoInteractions(external);
            Mockito.verify(state, Mockito.never()).getAIModelMgr();
            Assertions.assertSame(internal, Authorizer.getInstance()
                    .getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME));
            Assertions.assertThrows(NullPointerException.class, () -> Authorizer.check(statement, context, null));
        }
    }

    @Test
    public void testGrantGenerationAndDisplayNeverRetargetDroppedIdentity() throws Exception {
        AIModel model = model(101);
        AIModelMgr manager = Mockito.mock(AIModelMgr.class);
        Mockito.when(manager.getByName(model.getName())).thenReturn(model);
        Mockito.when(manager.getById(model.getId())).thenReturn(model);
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAIModelMgr()).thenReturn(manager);
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            AIModelPEntryObject object = AIModelPEntryObject.generate(List.of(model.getName()));
            Assertions.assertEquals(101, object.getId());
            Assertions.assertTrue(object.validate());
            Assertions.assertEquals("`same_model_name`", object.toString());
            Assertions.assertThrows(PrivilegeException.class,
                    () -> AIModelPEntryObject.generate(List.of("db", model.getName())));
            Assertions.assertThrows(PrivObjNotFoundException.class,
                    () -> AIModelPEntryObject.generate(List.of("missing_model")));
            Mockito.when(manager.getByName(model.getName())).thenReturn(model(102));
            Mockito.when(manager.getById(model.getId())).thenReturn(null);
            Assertions.assertFalse(object.validate());
            Assertions.assertThrows(MetaNotFoundException.class, object::toString);
            Assertions.assertTrue(AIModelPEntryObject.generate(List.of("*")).validate());
        }
    }

    @Test
    public void testBuiltinRolesDoNotGrantModelUsageToPublic() throws Exception {
        AuthorizationMgr manager = new AuthorizationMgr(new DefaultAuthorizationProvider());
        Assertions.assertFalse(manager.getTypeToPrivilegeEntryListByRole(PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME)
                .containsKey(ObjectType.AI_MODEL));
        Assertions.assertTrue(manager.getTypeToPrivilegeEntryListByRole(PrivilegeBuiltinConstants.ROOT_ROLE_NAME)
                .containsKey(ObjectType.AI_MODEL));
        Assertions.assertTrue(manager.getTypeToPrivilegeEntryListByRole(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME)
                .containsKey(ObjectType.AI_MODEL));
        Assertions.assertTrue(manager.provider.getAvailablePrivType(ObjectType.SYSTEM).contains(PrivilegeType.CREATE_AI_MODEL));
        Assertions.assertEquals(Set.of(PrivilegeType.USAGE, PrivilegeType.ALTER, PrivilegeType.DROP),
                Set.copyOf(manager.provider.getAvailablePrivType(ObjectType.AI_MODEL)));
    }
}
