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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for Group Provider statements with IF NOT EXISTS and IF EXISTS functionality
 */
public class GroupProviderStatementTest {

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        // Real EditLog on an auto-committing pseudo journal (shields BDB): journal writes complete so the
        // WALApplier.apply() inside logJsonObject() still runs and the DDL takes effect in memory.
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private ConnectContext ctx;
    private AuthenticationMgr authenticationMgr;
    private static final String TEST_PROVIDER_NAME = "test_provider";
    private static final String NON_EXISTENT_PROVIDER_NAME = "non_existent_provider";
    private static final String OTHER_PROVIDER_NAME = "other_test_provider";

    @BeforeEach
    public void setUp() throws Exception {

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // Clean up any existing test providers
        cleanupTestProviders();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanupTestProviders();
    }

    /**
     * Test case: Create Group Provider with IF NOT EXISTS when provider does not exist
     * Test point: Should successfully create the provider without error
     */
    @Test
    public void testCreateGroupProviderIfNotExistsWhenNotExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.createGroupProviderStatement(stmt, ctx);

        // Verify provider was created
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should be created successfully");
        Assertions.assertEquals(TEST_PROVIDER_NAME, provider.getName());
        Assertions.assertEquals("unix", provider.getType());
    }

    /**
     * Test case: Create Group Provider with IF NOT EXISTS when provider already exists
     * Test point: Should silently return without error and not modify existing provider
     */
    @Test
    public void testCreateGroupProviderIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Get the original provider for comparison
        GroupProvider originalProvider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(originalProvider, "Original provider should exist");

        // Try to create again with IF NOT EXISTS
        CreateGroupProviderStmt secondStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.createGroupProviderStatement(secondStmt, ctx);

        // Verify provider still exists and is unchanged
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should still exist");
        Assertions.assertEquals(originalProvider.getName(), provider.getName());
        Assertions.assertEquals(originalProvider.getType(), provider.getType());
    }

    /**
     * Test case: Create Group Provider without IF NOT EXISTS when provider already exists
     * Test point: Should throw DdlException with appropriate error message
     */
    @Test
    public void testCreateGroupProviderWithoutIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Try to create again without IF NOT EXISTS
        CreateGroupProviderStmt secondStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.createGroupProviderStatement(secondStmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("Group provider '" + TEST_PROVIDER_NAME + "' already exists"),
                "Error message should indicate provider already exists: " + exception.getMessage());
    }

    /**
     * Test case: Drop Group Provider with IF EXISTS when provider does not exist
     * Test point: Should silently return without error
     */
    @Test
    public void testDropGroupProviderIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.dropGroupProviderStatement(stmt, ctx);

        // Verify provider still does not exist
        GroupProvider provider = authenticationMgr.getGroupProvider(NON_EXISTENT_PROVIDER_NAME);
        Assertions.assertNull(provider, "Group provider should not exist");
    }

    /**
     * Test case: Drop Group Provider with IF EXISTS when provider exists
     * Test point: Should successfully drop the provider
     */
    @Test
    public void testDropGroupProviderIfExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        // Verify provider exists
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should exist before drop");

        // Drop with IF EXISTS
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);

        // Verify provider was dropped
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Group provider should be dropped successfully");
    }

    /**
     * Test case: Drop Group Provider without IF EXISTS when provider does not exist
     * Test point: Should throw DdlException with appropriate error message
     */
    @Test
    public void testDropGroupProviderWithoutIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, false, NodePosition.ZERO);

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.dropGroupProviderStatement(stmt, ctx);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("Group provider '" + NON_EXISTENT_PROVIDER_NAME + "' does not exist"),
                "Error message should indicate provider does not exist: " + exception.getMessage());
    }

    /**
     * Test case: Test CreateGroupProviderStmt constructor and getters
     * Test point: Verify proper initialization of statement properties
     */
    @Test
    public void testCreateGroupProviderStmtProperties() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");
    }

    /**
     * Test case: Test DropGroupProviderStmt constructor and getters
     * Test point: Verify proper initialization of statement properties
     */
    @Test
    public void testDropGroupProviderStmtProperties() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfExists(), "IF EXISTS should be true");
    }

    /**
     * Test case: Test CreateGroupProviderStmt without IF NOT EXISTS
     * Test point: Verify default behavior when IF NOT EXISTS is false
     */
    @Test
    public void testCreateGroupProviderStmtWithoutIfNotExists() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertFalse(stmt.isIfNotExists(), "IF NOT EXISTS should be false");
    }

    /**
     * Test case: Test DropGroupProviderStmt without IF EXISTS
     * Test point: Verify default behavior when IF EXISTS is false
     */
    @Test
    public void testDropGroupProviderStmtWithoutIfExists() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, false, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfExists(), "IF EXISTS should be false");
    }

    /**
     * Test case: Test CreateGroupProviderStmt with legacy constructor
     * Test point: Verify backward compatibility with existing constructor
     */
    @Test
    public void testCreateGroupProviderStmtLegacyConstructor() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertFalse(stmt.isIfNotExists(), "IF NOT EXISTS should default to false");
    }

    /**
     * Test case: Test DropGroupProviderStmt with legacy constructor
     * Test point: Verify backward compatibility with existing constructor
     */
    @Test
    public void testDropGroupProviderStmtLegacyConstructor() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfExists(), "IF EXISTS should default to false");
    }

    /**
     * Test case: Test multiple Group Provider operations in sequence
     * Test point: Verify proper handling of multiple create/drop operations
     */
    @Test
    public void testMultipleGroupProviderOperations() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // Create provider
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        // Verify exists
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Provider should exist after creation");

        // Try to create again with IF NOT EXISTS (should not error)
        CreateGroupProviderStmt createAgainStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createAgainStmt, ctx);

        // Verify still exists
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Provider should still exist after IF NOT EXISTS create");

        // Drop with IF EXISTS
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
        authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);

        // Verify dropped
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Provider should be dropped");

        // Try to drop again with IF EXISTS (should not error)
        DropGroupProviderStmt dropAgainStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
        authenticationMgr.dropGroupProviderStatement(dropAgainStmt, ctx);

        // Verify still does not exist
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Provider should still not exist after IF EXISTS drop");
    }

    /**
     * Test case: ALTER GROUP PROVIDER merges the delta and replaces the runtime instance
     * Test point: unlisted properties are preserved, listed ones are updated, and the map entry
     *             is swapped to a new instance under the same name
     */
    @Test
    public void testAlterGroupProviderMergesAndReplaces() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        properties.put("keep", "original");
        // ALTER only accepts a property the type defines or one the provider already has, and a unix
        // provider defines none - so the property this delta updates has to come from the CREATE.
        properties.put("changed", "before");
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("changed", "value");
        authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps);

        GroupProvider after = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(after, "Provider should still exist after alter");
        Assertions.assertNotSame(before, after, "Alter should swap in a new instance");
        Assertions.assertEquals(TEST_PROVIDER_NAME, after.getName(), "Name is unchanged");
        Assertions.assertEquals("unix", after.getType(), "Type is preserved");
        Assertions.assertEquals("original", after.getProperties().get("keep"), "Unlisted property is preserved");
        Assertions.assertEquals("value", after.getProperties().get("changed"), "Listed property is applied");
    }

    /**
     * Test case: ALTER GROUP PROVIDER on a name that does not exist
     * Test point: Should throw DdlException reporting the provider was not found
     */
    @Test
    public void testAlterGroupProviderNotFound() {
        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("added", "value");

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(NON_EXISTENT_PROVIDER_NAME, alterProps));
        Assertions.assertTrue(
                exception.getMessage().contains("Group Provider '" + NON_EXISTENT_PROVIDER_NAME + "' not found"),
                "Error message should indicate provider not found: " + exception.getMessage());
    }

    /**
     * Test case: replayAlterGroupProvider installs the journaled configuration on a follower
     * Test point: the record carries the provider's complete property map, so replay rebuilds the provider
     *             from it instead of merging onto this node's own copy - a node whose earlier replay failed
     *             would otherwise keep merging later records onto a stale base and diverge silently.
     */
    @Test
    public void testReplayAlterGroupProvider() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        Map<String, String> journaledProps = new HashMap<>(createUnixGroupProviderProperties());
        journaledProps.put("added", "value");
        authenticationMgr.replayAlterGroupProvider(TEST_PROVIDER_NAME, journaledProps);

        GroupProvider after = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(after, "Provider should exist after replay");
        Assertions.assertEquals("unix", after.getType(), "Type is preserved on replay");
        Assertions.assertEquals("value", after.getProperties().get("added"), "The journaled properties are installed");
    }

    /**
     * Test case: ALTER GROUP PROVIDER where the new configuration fails synchronous validation
     * Test point: prepareForActivation() throwing must fail the ALTER fast - the old provider
     *             stays in the map and no EditLog is written (the swap inside the WAL applier never runs)
     */
    @Test
    public void testAlterGroupProviderValidationFailureKeepsOldProvider() throws Exception {
        List<GroupProvider> destroyed = new ArrayList<>();
        // An LDAP provider whose synchronous validation fails. init()/checkProperty() are stubbed so
        // the test needs no real LDAP server; prepareForActivation() simulates an unusable config.
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() {
            }

            @Mock
            public void checkProperty() {
            }

            @Mock
            public void prepareForActivation() throws DdlException {
                throw new DdlException("simulated: new LDAP configuration is not usable");
            }

            @Mock
            public void destroy(Invocation invocation) {
                destroyed.add(invocation.getInvokedInstance());
            }
        };

        Map<String, String> ldapProps = new HashMap<>();
        ldapProps.put("type", "ldap");
        ldapProps.put("ldap_bind_root_pwd", "oldpwd");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, ldapProps);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(before);

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_bind_root_pwd", "newpwd");
        Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps),
                "ALTER must fail when the new configuration does not validate");

        // The old provider is untouched: no swap happened, so it is still the instance serving lookups
        // and it still carries the old properties. Under UtFrameUtils.setUpForPersistTest() the EditLog is a
        // real object whose WAL applier does run, so "the map was not touched" is exactly "no journal was written".
        Assertions.assertSame(before, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "Validation failure must not swap in the new provider");
        Assertions.assertEquals("oldpwd", before.getProperties().get("ldap_bind_root_pwd"),
                "The live provider must keep its original properties");
        // Failing inside prepareForActivation() happens before anything is started, so nothing is torn down:
        // in particular the old provider must not be destroyed.
        Assertions.assertTrue(destroyed.isEmpty(),
                "A validation failure must not destroy any provider, got " + destroyed.size());
    }

    /**
     * Test case: ALTER GROUP PROVIDER where the EditLog write fails after validation succeeds
     * Test point: the new provider's runtime is torn down and the old provider stays in the map,
     *             so a failed persist does not leave a half-applied change
     */
    @Test
    public void testAlterGroupProviderEditLogFailureKeepsOldProvider() throws Exception {
        List<GroupProvider> destroyed = new ArrayList<>();
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() {
            }

            @Mock
            public void checkProperty() {
            }

            @Mock
            public void prepareForActivation() {
                // validation succeeds
            }

            @Mock
            public void destroy(Invocation invocation) {
                destroyed.add(invocation.getInvokedInstance());
            }
        };

        Map<String, String> ldapProps = new HashMap<>();
        ldapProps.put("type", "ldap");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, ldapProps);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(before);

        // Make the EditLog write fail at the alter step. The pseudo journal is shared by the whole test
        // class, so the spy has to be removed again in the finally block below.
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterGroupProvider(any(GroupProviderLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            Map<String, String> alterProps = new HashMap<>();
            alterProps.put("ldap_bind_root_pwd", "newpwd");
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());

            // The map still holds the original provider; the new one was torn down in the finally block
            Assertions.assertSame(before, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                    "A failed EditLog write must not swap in the new provider");
            // Exactly the new provider is destroyed: its runtime was already started by init(), and leaving it
            // running would keep refreshing a configuration that never took effect.
            Assertions.assertEquals(1, destroyed.size(),
                    "Exactly one provider (the new one) must be destroyed, got " + destroyed.size());
            Assertions.assertNotSame(before, destroyed.get(0),
                    "The provider still serving lookups must not be the one that was destroyed");
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    /**
     * Test case: ALTER GROUP PROVIDER to an LDAP configuration that cannot be reached
     * Test point: prepareForActivation() really connects, so an unusable configuration fails the
     *             statement itself (fail-fast) instead of being swapped in and failing silently in the
     *             background. Nothing is mocked here: the URL points at a closed local port.
     */
    @Test
    public void testAlterGroupProviderUnreachableLdapFailsFast() throws Exception {
        // Put the provider in place through the replay path: it does a cold init() only, so no LDAP round
        // trip is needed to get there. The long refresh interval keeps its background refresh out of the way.
        Map<String, String> ldapProps = unreachableLdapProperties();
        ldapProps.put("ldap_cache_refresh_interval", "3600");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, ldapProps);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(before, "Provider should exist before the alter");

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_bind_root_pwd", "another-secret");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps),
                "ALTER must fail when the new configuration cannot talk to the directory");
        Assertions.assertTrue(
                exception.getMessage().contains("failed to apply the new configuration to group provider"),
                "Error message should come from prepareForActivation(): " + exception.getMessage());

        Assertions.assertSame(before, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "The live provider must be left in place when validation fails");
        Assertions.assertEquals("secret", before.getProperties().get("ldap_bind_root_pwd"),
                "The live provider must keep its original password");
    }

    /**
     * Test case: CREATE -> ALTER -> SHOW CREATE, all through the SQL path
     * Test point: the whole chain (parser -> analyzer -> DDLStmtExecutor -> AuthenticationMgr) is wired up,
     *             and SHOW CREATE reports the merged properties: only the listed property changed.
     */
    @Test
    public void testAlterGroupProviderThroughSqlPath() throws Exception {
        executeDdl("CREATE GROUP PROVIDER " + TEST_PROVIDER_NAME
                + " PROPERTIES(\"type\" = \"unix\", \"keep\" = \"original\", \"change\" = \"before\")");

        executeDdl("ALTER GROUP PROVIDER " + TEST_PROVIDER_NAME + " SET (\"change\" = \"after\")");

        String createStatement = showCreateGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertTrue(createStatement.contains("\"change\" = \"after\""),
                "SHOW CREATE should report the new value: " + createStatement);
        Assertions.assertTrue(createStatement.contains("\"keep\" = \"original\""),
                "SHOW CREATE should still report the untouched property: " + createStatement);
        Assertions.assertTrue(createStatement.contains("\"type\" = \"unix\""),
                "SHOW CREATE should still report the type: " + createStatement);
    }

    /**
     * Test case: ALTER then DROP through the SQL path
     * Test point: the instance ALTER swapped into the map is a normal manageable object - DROP still finds
     *             it and removes it.
     */
    @Test
    public void testAlterThenDropGroupProviderThroughSqlPath() throws Exception {
        executeDdl("CREATE GROUP PROVIDER " + TEST_PROVIDER_NAME
                + " PROPERTIES(\"type\" = \"unix\", \"some_property\" = \"before\")");
        executeDdl("ALTER GROUP PROVIDER " + TEST_PROVIDER_NAME + " SET (\"some_property\" = \"value\")");
        Assertions.assertNotNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        executeDdl("DROP GROUP PROVIDER " + TEST_PROVIDER_NAME);
        Assertions.assertNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "DROP should remove the provider that ALTER swapped in");
    }

    /**
     * Test case: ALTER a file group provider to a different, readable file
     * Test point: the file implementation has no prepareForActivation() override - it validates and loads
     *             inside its own synchronous init(), which still runs before the EditLog write. So the
     *             swapped-in provider is already loaded and the first lookup after ALTER sees the new file,
     *             the same property the LDAP implementation gets from the warm-up.
     */
    @Test
    public void testAlterFileGroupProviderSwapsInTheNewFile() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "file");
        properties.put("group_file_url", createGroupFile("readers:alice\n"));
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        UserIdentity alice = UserIdentity.createEphemeralUserIdent("alice", "%");
        Assertions.assertEquals(Set.of("readers"),
                AuthenticationHandler.resolveGroupsFromProviders(alice, null, List.of(TEST_PROVIDER_NAME)),
                "Sanity check: the original file is in effect");

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("group_file_url", createGroupFile("writers:alice\n"));
        authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps);

        Assertions.assertEquals(Set.of("writers"),
                AuthenticationHandler.resolveGroupsFromProviders(alice, null, List.of(TEST_PROVIDER_NAME)),
                "The first lookup after ALTER must already read the new file");
    }

    /**
     * Test case: ALTER a unix group provider
     * Test point: the unix implementation reads no property at all (it asks the OS through UGI), so an
     *             ALTER only rewrites the stored property map - it must still succeed, swap the instance,
     *             and leave group resolution working exactly as before.
     */
    @Test
    public void testAlterUnixGroupProviderKeepsResolvingGroups() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        // The unix type defines no property of its own, so the only thing an ALTER can change on such a
        // provider is a property its CREATE put there.
        properties.put("comment_like_property", "before");
        CreateGroupProviderStmt createStmt = new CreateGroupProviderStmt(
                TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);

        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        Set<String> groupsBefore = AuthenticationHandler.resolveGroupsFromProviders(user, null, List.of(TEST_PROVIDER_NAME));

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("comment_like_property", "value");
        authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps);

        GroupProvider after = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotSame(before, after, "ALTER should swap in a new instance for unix too");
        Assertions.assertEquals("value", after.getProperties().get("comment_like_property"),
                "The delta is stored even though the unix implementation reads no property");
        Assertions.assertEquals(groupsBefore,
                AuthenticationHandler.resolveGroupsFromProviders(user, null, List.of(TEST_PROVIDER_NAME)),
                "Group resolution must be unaffected by an ALTER on a unix provider");
    }

    /**
     * Test case: ALTER a file group provider to a file that cannot be read
     * Test point: the file implementation validates inside its own synchronous init(), which runs before
     *             the EditLog write - so the statement fails and the old provider keeps serving. This is
     *             the third implementation's failure path (unix has none, ldap fails in prepareForActivation).
     */
    @Test
    public void testAlterFileGroupProviderWithUnreadableFileKeepsOldProvider() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "file");
        properties.put("group_file_url", createGroupFile("readers:alice\n"));
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(before);

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("group_file_url", "/tmp/definitely-not-a-group-file-" + System.nanoTime());
        Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps),
                "ALTER must fail when the new group file cannot be read");

        Assertions.assertSame(before, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "A file that cannot be read must not swap in the new provider");
        Assertions.assertEquals(properties.get("group_file_url"),
                before.getProperties().get("group_file_url"),
                "The live provider must keep pointing at the old file");
    }

    /**
     * Test case: ALTER to a property combination that checkProperty() rejects
     * Test point: property validation happens on the merged set before anything is started, so an
     *             individually valid delta that becomes invalid once merged still fails the statement.
     *             Here the provider already has ldap_group_dn and the delta adds ldap_group_filter -
     *             the two are mutually exclusive.
     */
    @Test
    public void testAlterGroupProviderInvalidMergedPropertiesKeepsOldProvider() throws Exception {
        Map<String, String> properties = unreachableLdapProperties();
        properties.put("ldap_cache_refresh_interval", "3600");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, properties);
        GroupProvider before = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(before);

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_group_filter", "(&(objectClass=groupOfNames)(cn=readers))");
        // checkProperty() reports bad input with an unchecked SemanticException; ALTER converts it so the
        // user sees the same clean DDL error CREATE gives, instead of "Maybe our bug or wrong input
        // parameters" plus a stack trace from StmtExecutor's catch-all.
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(TEST_PROVIDER_NAME, alterProps),
                "ALTER must fail when the merged properties do not validate");
        Assertions.assertTrue(
                exception.getMessage().contains("ldap_group_dn and ldap_group_filter"),
                "Error should come from checkProperty(): " + exception.getMessage());

        Assertions.assertSame(before, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "A property validation failure must not swap in the new provider");
        Assertions.assertNull(before.getProperties().get("ldap_group_filter"),
                "The live provider must not have picked up the rejected property");
    }

    /**
     * Test case: SHOW CREATE GROUP PROVIDER must not echo credentials
     * Test point: the LDAP bind password and the trust store password are printed as *** so that any
     *             user allowed to read the definition (and every place the output is copied into) does
     *             not receive the secrets in plain text.
     */
    @Test
    public void testShowCreateGroupProviderMasksCredentials() throws Exception {
        Map<String, String> properties = unreachableLdapProperties();
        properties.put("ldap_ssl_conn_trust_store_pwd", "trust-store-secret");
        properties.put("ldap_cache_refresh_interval", "3600");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, properties);

        String createStatement = showCreateGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertFalse(createStatement.contains("secret"),
                "No credential value may appear in SHOW CREATE output: " + createStatement);
        Assertions.assertTrue(createStatement.contains("\"ldap_bind_root_pwd\" = \"***\""),
                "The bind password should be masked: " + createStatement);
        Assertions.assertTrue(createStatement.contains("\"ldap_ssl_conn_trust_store_pwd\" = \"***\""),
                "The trust store password should be masked: " + createStatement);
        Assertions.assertTrue(createStatement.contains("\"ldap_bind_root_dn\" = \"cn=admin,dc=example,dc=com\""),
                "Non-sensitive properties should still be reported: " + createStatement);
    }

    /**
     * Helper method to run a DDL statement through parser + analyzer + DDLStmtExecutor
     */
    private void executeDdl(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
    }

    /**
     * Helper method returning the statement text reported by SHOW CREATE GROUP PROVIDER
     */
    private String showCreateGroupProvider(String name) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("SHOW CREATE GROUP PROVIDER " + name, ctx);
        ShowResultSet resultSet = ShowExecutor.execute((ShowStmt) stmt, ctx);
        Assertions.assertEquals(1, resultSet.getResultRows().size(), "SHOW CREATE should return one row");
        return resultSet.getResultRows().get(0).get(1);
    }

    /**
     * Helper method writing a group file and returning the value to put in group_file_url.
     * FileGroupProvider resolves a non-URL value under $STARROCKS_HOME/conf, so when that variable is set
     * (it is in the build and in a real FE) the file has to live there and the property is just its name.
     */
    private String createGroupFile(String content) throws Exception {
        String starRocksHome = System.getenv("STARROCKS_HOME");
        java.nio.file.Path dir = starRocksHome != null
                ? java.nio.file.Paths.get(starRocksHome, "conf")
                : java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"));
        java.nio.file.Files.createDirectories(dir);
        java.nio.file.Path file = java.nio.file.Files.createTempFile(dir, "group-provider-test", ".txt");
        java.nio.file.Files.writeString(file, content);
        // $STARROCKS_HOME/conf is the repo's tracked conf/ directory when the tests run through
        // run-fe-ut.sh, and it is not gitignored. deleteOnExit() only fires on a clean JVM exit, so a
        // surefire fork timeout or crash would leave these files in the working tree; delete them in
        // @AfterEach instead.
        createdGroupFiles.add(file);
        return starRocksHome != null ? file.getFileName().toString() : file.toAbsolutePath().toString();
    }

    private final java.util.List<java.nio.file.Path> createdGroupFiles = new java.util.ArrayList<>();

    @AfterEach
    public void deleteGroupFiles() {
        for (java.nio.file.Path file : createdGroupFiles) {
            try {
                java.nio.file.Files.deleteIfExists(file);
            } catch (Exception e) {
                // best effort: the assertion result matters more than a leftover temp file
            }
        }
        createdGroupFiles.clear();
    }

    /**
     * Test case: CREATE parked in a slow init() next to unrelated group provider DDL
     * Test point: init() reads the provider's source - FileGroupProvider.init() calls URL.openStream() on an
     *             http(s) group_file_url with no timeout at all - so it runs outside the DDL lock. Holding
     *             the lock across it would park every other CREATE / DROP / ALTER behind one unreachable
     *             endpoint, including the DROP an operator would reach for to get out of it.
     *             The interleaving is forced with latches rather than a sleep, so the case either exercises
     *             the overlap or fails.
     */
    @Test
    public void testSlowCreateDoesNotBlockUnrelatedGroupProviderDdl() throws Exception {
        // A second provider to operate on while the CREATE is parked - created before the fake init() is in
        // place, so it is not the call that parks.
        authenticationMgr.replayCreateGroupProvider(OTHER_PROVIDER_NAME, createUnixGroupProviderProperties());

        CountDownLatch initStarted = new CountDownLatch(1);
        CountDownLatch initMayFinish = new CountDownLatch(1);
        mockUnixInitPausing(initStarted, initMayFinish);

        List<Throwable> failures = new ArrayList<>();
        Thread slowCreate = new Thread(() -> {
            try {
                authenticationMgr.createGroupProviderStatement(new CreateGroupProviderStmt(
                        TEST_PROVIDER_NAME, createUnixGroupProviderProperties(), false, NodePosition.ZERO), ctx);
            } catch (Throwable t) {
                synchronized (failures) {
                    failures.add(t);
                }
            }
        });
        slowCreate.start();
        Assertions.assertTrue(initStarted.await(30, TimeUnit.SECONDS), "The CREATE should have reached init()");

        authenticationMgr.dropGroupProviderStatement(
                new DropGroupProviderStmt(OTHER_PROVIDER_NAME, false, NodePosition.ZERO), ctx);
        Assertions.assertNull(authenticationMgr.getGroupProvider(OTHER_PROVIDER_NAME),
                "An unrelated DROP must not wait for a CREATE that is still reading its source");

        initMayFinish.countDown();
        slowCreate.join(30000);
        Assertions.assertFalse(slowCreate.isAlive(), "The CREATE should finish once init() returns");
        Assertions.assertTrue(failures.isEmpty(), "The CREATE should succeed, got: " + failures);
        Assertions.assertNotNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));
    }

    /**
     * Test case: the name is taken while CREATE is still initialising
     * Test point: init() no longer runs under the lock, so the name this statement checked can be taken in
     *             the meantime. The re-check before journaling has to report that instead of overwriting the
     *             provider that got there first - and the candidate it built must be torn down, not leaked.
     */
    @Test
    public void testCreateLosingTheNameRaceReportsItAndKeepsTheWinner() throws Exception {
        CountDownLatch initStarted = new CountDownLatch(1);
        CountDownLatch initMayFinish = new CountDownLatch(1);
        List<GroupProvider> destroyed = mockUnixInitPausing(initStarted, initMayFinish);

        List<Throwable> failures = new ArrayList<>();
        Thread losingCreate = new Thread(() -> {
            try {
                authenticationMgr.createGroupProviderStatement(new CreateGroupProviderStmt(
                        TEST_PROVIDER_NAME, createUnixGroupProviderProperties(), false, NodePosition.ZERO), ctx);
            } catch (Throwable t) {
                synchronized (failures) {
                    failures.add(t);
                }
            }
        });
        losingCreate.start();
        Assertions.assertTrue(initStarted.await(30, TimeUnit.SECONDS), "The CREATE should have reached init()");

        // Someone else takes the name while the statement above is still initialising.
        Map<String, String> winnerProps = createUnixGroupProviderProperties();
        winnerProps.put("winner", "yes");
        authenticationMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, winnerProps);
        GroupProvider winner = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);

        initMayFinish.countDown();
        losingCreate.join(30000);
        Assertions.assertFalse(losingCreate.isAlive(), "The losing CREATE should finish");

        Assertions.assertEquals(1, failures.size(), "The losing CREATE must report the taken name");
        Assertions.assertTrue(failures.get(0).getMessage().contains("already exists"),
                "Expected an already-exists error, got: " + failures.get(0));
        Assertions.assertSame(winner, authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME),
                "The provider that got the name first must stay in the map");
        Assertions.assertEquals(1, destroyed.size(),
                "The candidate that lost the race must be destroyed, not leaked");
    }

    /**
     * Fakes UnixGroupProvider's init() so that it parks until released, standing in for any init() that
     * reads a remote source (the file provider's http(s) group_file_url, the LDAP one's directory).
     * Returns the list that records every destroy()ed instance.
     */
    private List<GroupProvider> mockUnixInitPausing(CountDownLatch initStarted, CountDownLatch initMayFinish) {
        List<GroupProvider> destroyed = new ArrayList<>();
        AtomicInteger inits = new AtomicInteger();
        new MockUp<UnixGroupProvider>() {
            @Mock
            public void init() throws InterruptedException {
                // Only the first init() parks - that is the statement under test. Anything the test sets up
                // afterwards (the second provider, the winner of the name race) must not block.
                if (inits.incrementAndGet() != 1) {
                    return;
                }
                initStarted.countDown();
                Assertions.assertTrue(initMayFinish.await(30, TimeUnit.SECONDS),
                        "The paused init() should have been released by the test");
            }

            @Mock
            public void destroy(Invocation invocation) {
                synchronized (destroyed) {
                    destroyed.add(invocation.getInvokedInstance());
                }
            }
        };
        return destroyed;
    }

    /**
     * Helper method to create LDAP Group Provider properties pointing at a closed local port.
     * checkProperty() accepts them (all four required properties are present, and exactly one of
     * ldap_group_dn / ldap_group_filter), but any actual connection attempt fails right away.
     */
    private Map<String, String> unreachableLdapProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put("ldap_conn_url", "ldap://127.0.0.1:1");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=example,dc=com");
        properties.put("ldap_bind_root_pwd", "secret");
        properties.put("ldap_bind_base_dn", "dc=example,dc=com");
        properties.put("ldap_group_dn", "cn=group1,dc=example,dc=com");
        properties.put("ldap_conn_timeout", "100");
        return properties;
    }

    /**
     * Helper method to create Unix Group Provider properties
     */
    private Map<String, String> createUnixGroupProviderProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unix");
        return properties;
    }

    /**
     * Helper method to clean up test providers
     */
    private void cleanupTestProviders() {
        try {
            // Try to drop test providers if they exist
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        try {
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        try {
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(OTHER_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}
