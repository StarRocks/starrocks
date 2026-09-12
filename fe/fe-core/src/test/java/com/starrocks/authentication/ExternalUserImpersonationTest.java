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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivObjNotFoundException;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.UserPEntryObject;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * `EXECUTE AS` on a user that was never created with CREATE USER.
 *
 * <p>Impersonation is how airflow / zeppelin / superset run a query as the person who asked for it, so
 * every one of those people used to need a CREATE USER plus a per-user IMPERSONATE grant before they
 * could be impersonated at all. A member of an allowed AD group now needs neither: the executor gives it
 * an ephemeral identity whose authorization comes entirely from the roles mapped to its groups.
 *
 * <p>Where the decision is made matters as much as what it decides. Admission happens in the executor,
 * after the IMPERSONATE check and immediately before the session is switched, which is what makes the
 * refusal indistinguishable to an unprivileged caller and the approved group set identical to the one the
 * session runs with. The analyzer only rejects when the feature is off, exactly as it did before.
 */
public class ExternalUserImpersonationTest {

    private static final String PROVIDER_NAME = "ad";
    private static final String ALLOWED_GROUP = "zeppelin-public";

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr auth;
    @Mocked
    private AuthorizationMgr authorizationMgr;

    private String[] savedGroupProvider;
    private String[] savedAllowedGroups;

    /**
     * Answers group membership from a map instead of a directory, so {@link AuthenticationHandler#getGroups}
     * still runs for real - provider lookup, null-provider skip, union across providers.
     */
    private static class FakeGroupProvider extends GroupProvider {
        private final Map<String, Set<String>> userToGroups;

        FakeGroupProvider(Map<String, Set<String>> userToGroups) {
            super(PROVIDER_NAME, Map.of(GROUP_PROVIDER_PROPERTY_TYPE_KEY, "fake"));
            this.userToGroups = userToGroups;
        }

        @Override
        public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
            return userToGroups.getOrDefault(distinguishedName, Set.of());
        }

        @Override
        public void checkProperty() {
        }
    }

    @BeforeEach
    public void setUp() {
        savedGroupProvider = Config.group_provider;
        savedAllowedGroups = Config.execute_as_external_user_allowed_groups;
        Config.group_provider = new String[] {PROVIDER_NAME};

        SqlParser sqlParser = new SqlParser(AstBuilder.getInstance());
        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                // Recorded on its own rather than as GlobalStateMgr.getCurrentState().getAuthenticationMgr():
                // in a chained recording `minTimes = 0` binds to the last call only, which would make
                // getCurrentState() mandatory - and the UserPEntryObject tests below deliberately never
                // reach it, because skipping that lookup for an ephemeral identity is the point.
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuthenticationMgr();
                minTimes = 0;
                result = auth;

                globalStateMgr.getSqlParser();
                minTimes = 0;
                result = sqlParser;

                globalStateMgr.getAnalyzer();
                minTimes = 0;
                result = analyzer;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        // Config is process-global; leaving it set would leak into whatever runs next in this JVM.
        Config.group_provider = savedGroupProvider;
        Config.execute_as_external_user_allowed_groups = savedAllowedGroups;
    }

    /**
     * @param directory username -> groups, as the group providers see it
     */
    private void givenDirectory(Map<String, Set<String>> directory) {
        FakeGroupProvider provider = new FakeGroupProvider(directory);
        new Expectations(auth) {
            {
                auth.getGroupProvider(PROVIDER_NAME);
                minTimes = 0;
                result = provider;
            }
        };
    }

    private void givenNoRegisteredUsers() {
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = false;
            }
        };
    }

    private void givenEveryUserRegistered() {
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;

                auth.getUserProperty(anyString);
                minTimes = 0;
                result = new UserProperty();
            }
        };
    }

    /**
     * The caller holds `IMPERSONATE ON ALL USERS`, which is what covers an ephemeral target. Needed by every
     * test where an external `EXECUTE AS` is expected to succeed, because the executor re-asks the
     * IMPERSONATE question before switching the session and the real Authorizer would refuse here.
     */
    private void givenImpersonatePermitted() {
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                        PrivilegeType privilegeType) {
            }
        };
    }

    private void givenGroupsMapToNoRoles() {
        new Expectations(authorizationMgr) {
            {
                authorizationMgr.getRoleIdListByGroup(anyString);
                minTimes = 0;
                result = new HashSet<Long>();
            }
        };
    }

    private static ExecuteAsStmt analyzeExecuteAs(String user, ConnectContext ctx) {
        // The sqlMode overload, not the SessionVariable one: ConnectContext builds its SessionVariable
        // from the mocked VariableMgr, and the cascaded mock answers getSqlDialect() with null.
        ExecuteAsStmt stmt = (ExecuteAsStmt) SqlParser.parse(
                "execute as " + user + " with no revert", 1).get(0);
        Analyzer.analyze(stmt, ctx);
        return stmt;
    }

    // ---------------------------------------------------------------------------------------------
    // What must not change
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testFeatureOffKeepsTheExistingFailureInTheAnalyzer() {
        // The default is an empty allow-list, and then this whole feature must be invisible: an
        // unregistered target fails during analysis with the same message and at the same phase as
        // before. If this regresses, an upgrade silently changes when EXECUTE AS fails.
        Config.execute_as_external_user_allowed_groups = new String[] {};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));

        ConnectContext ctx = new ConnectContext();
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> analyzeExecuteAs("alice", ctx));
        Assertions.assertTrue(e.getMessage().contains("cannot find user"), e.getMessage());

        // And no group provider is consulted: with the feature off this costs nothing and observes nothing.
        new Verifications() {
            {
                auth.getGroupProvider(anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testRegisteredUserTakesTheNativePath() {
        // A registered user must keep the native path even when the directory would also vouch for it: the
        // external branch skips UserProperty and the session-variable restore, so mistaking a real user for
        // an external one would quietly drop its per-user connection limit.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenEveryUserRegistered();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));
        givenGroupsMapToNoRoles();

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        Assertions.assertDoesNotThrow(() -> ExecuteAsExecutor.execute(stmt, ctx));

        Assertions.assertFalse(ctx.getCurrentUserIdentity().isEphemeral(),
                "a user that exists in AuthenticationMgr must not become an ephemeral identity");
    }

    @Test
    public void testAnalyzerDoesNotProbeTheDirectoryWhenTheFeatureIsOn() {
        // The analyzer must not resolve groups: it runs before Authorizer.check, so anything it decides is
        // decided for a caller who has not been shown to hold any privilege at all.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));

        ConnectContext ctx = new ConnectContext();
        // Neither a member nor a non-member is rejected here - admission has not happened yet.
        Assertions.assertDoesNotThrow(() -> analyzeExecuteAs("alice", ctx));
        Assertions.assertDoesNotThrow(() -> analyzeExecuteAs("nobody", ctx));

        new Verifications() {
            {
                auth.getGroupProvider(anyString);
                times = 0;
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // The IMPERSONATE check comes first, and it cannot be used to probe membership
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testUnregisteredTargetIsCheckedAsEphemeralSoOnlyAllUsersCanCoverIt() {
        // NativeAccessController turns the identity into a UserPEntryObject. Passing a non-ephemeral one for
        // a name with no account would let a leftover named grant match it; passing the ephemeral one means
        // only `IMPERSONATE ON ALL USERS` can.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));

        UserIdentity[] checked = new UserIdentity[1];
        PrivilegeType[] want = new PrivilegeType[1];
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                        PrivilegeType privilegeType) {
                checked[0] = impersonateUser;
                want[0] = privilegeType;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        new AuthorizerStmtVisitor().visitExecuteAsStatement(stmt, ctx);

        Assertions.assertEquals(PrivilegeType.IMPERSONATE, want[0]);
        Assertions.assertNotNull(checked[0]);
        Assertions.assertTrue(checked[0].isEphemeral(),
                "a target with no account must be checked as an ephemeral identity");
    }

    @Test
    public void testRegisteredTargetIsCheckedAsItself() {
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenEveryUserRegistered();

        UserIdentity[] checked = new UserIdentity[1];
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                        PrivilegeType privilegeType) {
                checked[0] = impersonateUser;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        new AuthorizerStmtVisitor().visitExecuteAsStatement(stmt, ctx);

        Assertions.assertFalse(checked[0].isEphemeral(),
                "a registered target must still be coverable by a grant naming it");
    }

    @Test
    public void testImpersonateCheckIsIdenticalForMembersAndNonMembers() {
        // The membership oracle this closes: an unprivileged caller must not be able to tell an allowed
        // member from a non-member, and the IMPERSONATE check is the only thing they can reach.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("member", Set.of(ALLOWED_GROUP), "outsider", Set.of("finance-only")));

        UserIdentity[] checked = new UserIdentity[2];
        int[] calls = new int[1];
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                        PrivilegeType privilegeType) {
                checked[calls[0]++] = impersonateUser;
            }
        };

        ConnectContext ctx = new ConnectContext();
        new AuthorizerStmtVisitor().visitExecuteAsStatement(analyzeExecuteAs("member", ctx), ctx);
        new AuthorizerStmtVisitor().visitExecuteAsStatement(analyzeExecuteAs("outsider", ctx), ctx);

        Assertions.assertEquals(2, calls[0]);
        Assertions.assertTrue(checked[0].isEphemeral());
        Assertions.assertTrue(checked[1].isEphemeral());
        // Nothing consulted the directory on the way to the privilege check.
        new Verifications() {
            {
                auth.getGroupProvider(anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testAccountDroppedBetweenTheCheckAndTheSwitchIsRefused() {
        // AuthorizerStmtVisitor picks the identity to check by asking AuthenticationMgr, and the executor
        // asks the same question again a moment later. A DROP USER landing in between would otherwise let a
        // check that passed against a NAMED identity be spent on an EPHEMERAL one - a grant naming a user
        // authorizing an accountless namesake, which is exactly what UserPEntryObject.match() refuses to do
        // for a grant that outlives its user. The executor closes the transient version by re-asking.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));
        givenGroupsMapToNoRoles();

        boolean[] dropped = {false};
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = new Delegate<Boolean>() {
                    @SuppressWarnings("unused")
                    boolean doesUserExist(UserIdentity ignored) {
                        return !dropped[0];
                    }
                };
            }
        };

        // The caller holds `IMPERSONATE ON USER 'alice'` and nothing more: it covers the registered alice,
        // and an ephemeral identity needs ON ALL USERS.
        List<UserIdentity> checked = new ArrayList<>();
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
                checked.add(impersonateUser);
                if (impersonateUser.isEphemeral()) {
                    throw new AccessDeniedException();
                }
            }
        };

        ConnectContext ctx = new ConnectContext();
        UserIdentity impersonator = new UserIdentity("svc_datasvc", "%");
        ctx.setCurrentUserIdentity(impersonator);
        ctx.setGroups(new HashSet<>(Set.of("service-accounts")));
        ctx.setCurrentRoleIds(new HashSet<>(Set.of(42L)));

        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        new AuthorizerStmtVisitor().visitExecuteAsStatement(stmt, ctx);
        Assertions.assertEquals(1, checked.size());
        Assertions.assertFalse(checked.get(0).isEphemeral(), "alice still had an account at this point");

        dropped[0] = true;   // DROP USER alice

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> ExecuteAsExecutor.execute(stmt, ctx));
        // Not a euphemism: the account really did just disappear.
        Assertions.assertEquals("cannot find user 'alice'@'%'!", e.getMessage());

        Assertions.assertEquals(2, checked.size(), "the executor must ask again for the identity it will use");
        Assertions.assertTrue(checked.get(1).isEphemeral(),
                "the second question has to be about the identity the session would actually become");
        Assertions.assertEquals(impersonator, ctx.getCurrentUserIdentity());
        Assertions.assertEquals(Set.of("service-accounts"), ctx.getGroups());
        Assertions.assertEquals(Set.of(42L), ctx.getCurrentRoleIds());
    }

    // ---------------------------------------------------------------------------------------------
    // Admission, in the executor
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testMemberOfAllowedGroupGetsAnEphemeralIdentityCarryingItsGroups() {
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of("some-other-group", ALLOWED_GROUP)));
        givenGroupsMapToNoRoles();
        givenImpersonatePermitted();

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        Assertions.assertDoesNotThrow(() -> ExecuteAsExecutor.execute(stmt, ctx));

        UserIdentity current = ctx.getCurrentUserIdentity();
        Assertions.assertEquals(new UserIdentity("alice", "%"), current);
        Assertions.assertTrue(current.isEphemeral(),
                "an unregistered target has no privilege collection to merge, so it must be ephemeral");
        // This is the whole point: group-based Ranger policies and group-mapped roles are evaluated against
        // the impersonated person's groups, not the service account's.
        Assertions.assertEquals(Set.of("some-other-group", ALLOWED_GROUP), ctx.getGroups());
    }

    @Test
    public void testNonMemberIsRefusedWithTheSameWordingAsAnUnknownUser() {
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("bob", Set.of("finance-only", "payroll-admins")));

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("bob", ctx);
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> ExecuteAsExecutor.execute(stmt, ctx));

        // Indistinguishable from the plain unknown-user rejection, and it names neither the groups nor
        // the allow-list.
        Assertions.assertEquals("cannot find user 'bob'@'%'!", e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("finance-only"), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains(ALLOWED_GROUP), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("execute_as_external_user_allowed_groups"),
                e.getMessage());
    }

    @Test
    public void testRefusalLeavesTheSessionUntouched() {
        // Atomicity: nothing is applied until the target is admitted and its roles computed, so a refusal
        // must not leave the session running as the target while carrying the caller's groups and roles.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("bob", Set.of("finance-only")));

        ConnectContext ctx = new ConnectContext();
        UserIdentity impersonator = new UserIdentity("svc_datasvc", "%");
        ctx.setCurrentUserIdentity(impersonator);
        ctx.setGroups(new HashSet<>(Set.of("service-accounts")));
        ctx.setCurrentRoleIds(new HashSet<>(Set.of(42L)));

        ExecuteAsStmt stmt = analyzeExecuteAs("bob", ctx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(stmt, ctx));

        Assertions.assertEquals(impersonator, ctx.getCurrentUserIdentity());
        Assertions.assertFalse(ctx.getCurrentUserIdentity().isEphemeral());
        Assertions.assertEquals(Set.of("service-accounts"), ctx.getGroups());
        Assertions.assertEquals(Set.of(42L), ctx.getCurrentRoleIds());
    }

    @Test
    public void testGroupResolutionFailureLeavesTheSessionUntouched() {
        // Same guarantee when the provider throws rather than the target being refused.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        new Expectations(auth) {
            {
                auth.getGroupProvider(PROVIDER_NAME);
                minTimes = 0;
                result = new IllegalStateException("group provider is unavailable");
            }
        };

        ConnectContext ctx = new ConnectContext();
        UserIdentity impersonator = new UserIdentity("svc_datasvc", "%");
        ctx.setCurrentUserIdentity(impersonator);
        ctx.setGroups(new HashSet<>(Set.of("service-accounts")));
        ctx.setCurrentRoleIds(new HashSet<>(Set.of(42L)));

        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        // The provider's own failure, not a DdlException: AuthenticationHandler.getGroups() does not catch
        // and GroupProvider.getGroup() declares nothing checked, so it escapes execute() as it was thrown.
        // Asserting the type and message is what proves the group-resolution path actually ran - a broader
        // Exception.class would also be satisfied by a mock that never took effect.
        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class,
                () -> ExecuteAsExecutor.execute(stmt, ctx));
        Assertions.assertEquals("group provider is unavailable", thrown.getMessage());

        Assertions.assertEquals(impersonator, ctx.getCurrentUserIdentity());
        Assertions.assertFalse(ctx.getCurrentUserIdentity().isEphemeral());
        Assertions.assertEquals(Set.of("service-accounts"), ctx.getGroups());
        Assertions.assertEquals(Set.of(42L), ctx.getCurrentRoleIds());
    }

    @Test
    public void testUserUnknownToTheDirectoryIsRefused() {
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("nobody", ctx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(stmt, ctx));
    }

    @Test
    public void testGroupProviderThatCannotAnswerRefuses() {
        // No provider registered under the configured name: getGroups() skips it and returns nothing. The
        // gate has to read that as "refused", never as "unrestricted".
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        new Expectations(auth) {
            {
                auth.getGroupProvider(anyString);
                minTimes = 0;
                result = null;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(stmt, ctx));
    }

    @Test
    public void testUnresolvableSecurityIntegrationRefusesInsteadOfFallingBack() {
        // getGroupProviderList falls back to Config.group_provider when the session names a security
        // integration that cannot be resolved. For the registered-user refresh that is the existing behaviour,
        // but admission must not accept it: the default provider is a different directory from the one the
        // session was told to trust. The mocked AuthenticationMgr answers getSecurityIntegration with null,
        // which is exactly the shape of an integration that was deleted.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));
        givenGroupsMapToNoRoles();
        givenImpersonatePermitted();

        ConnectContext ctx = new ConnectContext();
        ctx.setSecurityIntegration("partner_oauth");
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(stmt, ctx));

        // And the strictness is scoped: the same user is admitted when no integration is named, because
        // then Config.group_provider is the configuration rather than a fallback. Without this half, the
        // assertion above would also pass if admission had simply stopped working.
        ConnectContext nativeCtx = new ConnectContext();
        ExecuteAsStmt nativeStmt = analyzeExecuteAs("alice", nativeCtx);
        Assertions.assertDoesNotThrow(() -> ExecuteAsExecutor.execute(nativeStmt, nativeCtx));
        Assertions.assertTrue(nativeCtx.getCurrentUserIdentity().isEphemeral());
    }

    @Test
    public void testExplicitHostIsRefusedForAnUnregisteredTarget() {
        // RangerStarRocksAccessRequest uses the identity's host as the request's client IP, so allowing an
        // explicit host for a name with no account would let SQL text choose the value Ranger policies
        // match on and audit records show. Only 'user' and 'user'@'%' are eligible.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));
        givenGroupsMapToNoRoles();
        givenImpersonatePermitted();

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt pinned = analyzeExecuteAs("'alice'@'10.0.0.1'", ctx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(pinned, ctx));

        ConnectContext domainCtx = new ConnectContext();
        ExecuteAsStmt domain = analyzeExecuteAs("alice@['corp.example.com']", domainCtx);
        Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(domain, domainCtx));

        // The wildcard forms stay eligible.
        ConnectContext wildcardCtx = new ConnectContext();
        ExecuteAsStmt wildcard = analyzeExecuteAs("'alice'@'%'", wildcardCtx);
        Assertions.assertDoesNotThrow(() -> ExecuteAsExecutor.execute(wildcard, wildcardCtx));
        Assertions.assertTrue(wildcardCtx.getCurrentUserIdentity().isEphemeral());
    }

    @Test
    public void testGroupRolesAreAppliedAsAMutableSet() {
        // AuthorizationMgr.loadPrivilegeCollection's ephemeral branch aliases the context's role id set
        // (validRoleIds = roleIdsSpecified) and then addAll()s the group roles into it, so an immutable set
        // would arm an UnsupportedOperationException inside an authorization check.
        Config.execute_as_external_user_allowed_groups = new String[] {ALLOWED_GROUP};
        givenNoRegisteredUsers();
        givenDirectory(Map.of("alice", Set.of(ALLOWED_GROUP)));
        new Expectations(authorizationMgr) {
            {
                authorizationMgr.getRoleIdListByGroup(ALLOWED_GROUP);
                minTimes = 0;
                result = new HashSet<>(Set.of(7L));
            }
        };
        givenImpersonatePermitted();

        ConnectContext ctx = new ConnectContext();
        ExecuteAsStmt stmt = analyzeExecuteAs("alice", ctx);
        Assertions.assertDoesNotThrow(() -> ExecuteAsExecutor.execute(stmt, ctx));

        Assertions.assertEquals(Set.of(7L), ctx.getCurrentRoleIds(),
                "the roles mapped to the admitted groups are what authorize the session");
        Assertions.assertDoesNotThrow(() -> ctx.getCurrentRoleIds().add(1L));
        Assertions.assertDoesNotThrow(() -> ctx.getGroups().add("late-arriving-group"));
    }

    // ---------------------------------------------------------------------------------------------
    // UserPEntryObject: the privilege object of a user that does not exist
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testEphemeralPrivilegeObjectSkipsTheExistenceCheck() throws PrivilegeException {
        givenNoRegisteredUsers();

        UserPEntryObject object = UserPEntryObject.generate(
                UserIdentity.createEphemeralUserIdent("alice", "%"));

        Assertions.assertNotNull(object);
        Assertions.assertEquals(new UserIdentity("alice", "%"), object.getUserIdentity());
    }

    @Test
    public void testNonEphemeralUnknownUserIsStillRejectedAsAPrivilegeObject() {
        // The grant path builds non-ephemeral identities, so loosening the check must not reach it:
        // GRANT IMPERSONATE ON USER 'typo' has to keep failing.
        givenNoRegisteredUsers();

        Assertions.assertThrows(PrivObjNotFoundException.class,
                () -> UserPEntryObject.generate(new UserIdentity("typo", "%")));
    }

    @Test
    public void testOnlyAllUsersCoversAnEphemeralTarget() throws PrivilegeException {
        givenEveryUserRegistered();

        UserPEntryObject requested = UserPEntryObject.generate(
                UserIdentity.createEphemeralUserIdent("alice", "%"));
        UserPEntryObject grantedOnAllUsers = UserPEntryObject.generate(null);
        UserPEntryObject grantedOnSameName = UserPEntryObject.generate(new UserIdentity("alice", "%"));
        UserPEntryObject grantedOnSomeoneElse = UserPEntryObject.generate(new UserIdentity("bob", "%"));

        // GRANT IMPERSONATE ON ALL USERS - the only grant that can cover a user nobody created.
        Assertions.assertTrue(requested.match(grantedOnAllUsers));
        // A grant naming that same name can only be a leftover from a dropped namesake. It must not come
        // back to life just because someone with no account now answers to the name.
        Assertions.assertFalse(requested.match(grantedOnSameName));
        // Checked the other way too, because PrivilegeCollectionV2.searchObject() swaps the arguments.
        Assertions.assertFalse(grantedOnSameName.match(requested));
        Assertions.assertFalse(requested.match(grantedOnSomeoneElse));

        // A registered target is still covered by a grant naming it.
        UserPEntryObject registered = UserPEntryObject.generate(new UserIdentity("alice", "%"));
        Assertions.assertTrue(registered.match(grantedOnSameName));
        Assertions.assertTrue(registered.match(grantedOnAllUsers));
    }

    @Test
    public void testAllUsersObjectDoesNotMatchANamedRequest() throws PrivilegeException {
        // The class documents "this(ALL), other(userx) -> false" but used to dereference a null
        // userIdentity to get there.
        givenEveryUserRegistered();

        UserPEntryObject allUsers = UserPEntryObject.generate(null);
        UserPEntryObject named = UserPEntryObject.generate(new UserIdentity("alice", "%"));

        Assertions.assertFalse(Assertions.assertDoesNotThrow(() -> allUsers.match(named)));
    }
}
