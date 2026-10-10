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
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ExtensionGroupProviderTest {
    private static final String PROVIDER_NAME = "test_extension_provider";
    private static final UserIdentity TEST_USER = new UserIdentity("test_user", "%");
    private static final String TEST_DN = "test_user";

    @BeforeEach
    public void resetFactoryCounters() {
        CountingFactory.COUNTER.set(0);
        CountingFactory.last = null;
        FailingFactory.ATTEMPTS.set(0);
    }

    private static ExtensionGroupProvider providerFor(String factoryClassName) {
        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, ExtensionGroupProvider.TYPE);
        if (factoryClassName != null) {
            properties.put(ExtensionGroupProvider.PROVIDER_FACTORY_CLASS_PROPERTY, factoryClassName);
        }
        return new ExtensionGroupProvider(PROVIDER_NAME, properties);
    }

    // ---------------------------------------- delegation ----------------------------------------

    @Test
    public void testGetGroupIsDelegated() throws Exception {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());

        Assertions.assertEquals(Set.of("test_group"), provider.getGroup(TEST_USER, TEST_DN));
    }

    @Test
    public void testCheckPropertyIsDelegated() {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());

        Assertions.assertDoesNotThrow(provider::checkProperty);
    }

    @Test
    public void testInitIsDelegated() throws Exception {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());
        provider.init();

        Assertions.assertNotNull(CountingFactory.last);
        Assertions.assertTrue(CountingFactory.last.initialized, "delegate should have been initialized");
    }

    /**
     * The delegate must not be resolved by the constructor: a definition is also reconstructed
     * while metadata is loaded on a frontend that may not have the extension deployed.
     */
    @Test
    public void testConstructorDoesNotResolveDelegate() {
        Assertions.assertDoesNotThrow(() -> providerFor("com.example.DoesNotExist"));
        Assertions.assertEquals(0, CountingFactory.COUNTER.get());
    }

    // ---------------------------------- failures during analysis --------------------------------

    @Test
    public void testMissingFactoryClassFailsAnalysis() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> providerFor(null).checkProperty());

        Assertions.assertTrue(e.getMessage().contains(ExtensionGroupProvider.PROVIDER_FACTORY_CLASS_PROPERTY),
                e.getMessage());
    }

    @Test
    public void testUnknownFactoryClassFailsAnalysis() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> providerFor("com.example.DoesNotExist").checkProperty());

        Assertions.assertTrue(e.getMessage().contains("class not found"), e.getMessage());
    }

    @Test
    public void testClassThatIsNotAFactoryFailsAnalysis() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> providerFor(String.class.getName()).checkProperty());

        Assertions.assertTrue(e.getMessage().contains("does not implement"), e.getMessage());
    }

    @Test
    public void testFactoryReturningNullFailsAnalysis() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> providerFor(NullReturningFactory.class.getName()).checkProperty());

        Assertions.assertTrue(e.getMessage().contains("returned null"), e.getMessage());
    }

    // -------------------------------- failures outside analysis ---------------------------------

    /**
     * The type matters, not just the failure: AuthenticationMgr.replayCreateGroupProvider catches
     * DdlException only. A SemanticException here would abort journal replay and stop a frontend
     * that does not have the extension deployed from starting at all.
     */
    @Test
    public void testInitReportsDdlException() {
        Assertions.assertThrows(DdlException.class,
                () -> providerFor("com.example.DoesNotExist").init());
    }

    /**
     * Reachable when init() failed while an image was loaded: AuthenticationMgr logs the failure
     * but keeps the provider registered, so the next login resolves groups against it.
     */
    @Test
    public void testGetGroupYieldsNoGroupsWhenUnresolvable() {
        ExtensionGroupProvider provider = providerFor("com.example.DoesNotExist");

        Assertions.assertEquals(Set.of(), provider.getGroup(TEST_USER, TEST_DN));
        Assertions.assertEquals(Set.of(), provider.getGroup(TEST_USER, TEST_DN, new AccessControlContext()));
    }

    @Test
    public void testResolutionFailureIsNotRetried() {
        ExtensionGroupProvider provider = providerFor(FailingFactory.class.getName());

        Assertions.assertThrows(DdlException.class, provider::init);
        Assertions.assertThrows(DdlException.class, provider::init);
        Assertions.assertEquals(1, FailingFactory.ATTEMPTS.get(),
                "a provider known to be unresolvable must not be resolved again");
    }

    // ------------------------------------ context awareness -------------------------------------

    @Test
    public void testContextAwareDelegateReceivesContext() {
        ExtensionGroupProvider provider = providerFor(ContextAwareFactory.class.getName());
        Set<String> groups = provider.getGroup(TEST_USER, TEST_DN, new AccessControlContext());

        Assertions.assertEquals(Set.of("context_group"), groups);
    }

    @Test
    public void testContextAwareDelegateFallsBackWithoutContext() {
        ExtensionGroupProvider provider = providerFor(ContextAwareFactory.class.getName());

        Assertions.assertEquals(Set.of("no_context_group"), provider.getGroup(TEST_USER, TEST_DN));
    }

    @Test
    public void testContextIsIgnoredByADelegateThatIsNotAware() {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());
        Set<String> groups = provider.getGroup(TEST_USER, TEST_DN, new AccessControlContext());

        Assertions.assertEquals(Set.of("test_group"), groups);
    }

    // ----------------------------------------- teardown -----------------------------------------

    @Test
    public void testDestroyDoesNotResolveDelegate() {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());
        provider.destroy();

        Assertions.assertEquals(0, CountingFactory.COUNTER.get(),
                "a provider that was never used must not be created just to be torn down");
    }

    @Test
    public void testDestroyIsDelegatedOnceResolved() throws Exception {
        ExtensionGroupProvider provider = providerFor(CountingFactory.class.getName());
        provider.init();
        provider.destroy();

        Assertions.assertTrue(CountingFactory.last.destroyed, "delegate should have been destroyed");
    }

    // ------------------------------------- test doubles ------------------------------------------

    public static class CountingFactory implements ExtensionGroupProviderFactory {
        static final AtomicInteger COUNTER = new AtomicInteger();
        static volatile RecordingGroupProvider last;

        @Override
        public GroupProvider create(String name, Map<String, String> properties) {
            COUNTER.incrementAndGet();
            last = new RecordingGroupProvider(name, properties);
            return last;
        }
    }

    public static class ContextAwareFactory implements ExtensionGroupProviderFactory {
        @Override
        public GroupProvider create(String name, Map<String, String> properties) {
            return new ContextAwareGroupProvider(name, properties);
        }
    }

    public static class NullReturningFactory implements ExtensionGroupProviderFactory {
        @Override
        public GroupProvider create(String name, Map<String, String> properties) {
            return null;
        }
    }

    public static class FailingFactory implements ExtensionGroupProviderFactory {
        static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public GroupProvider create(String name, Map<String, String> properties) throws DdlException {
            ATTEMPTS.incrementAndGet();
            throw new DdlException("factory refuses to create a provider");
        }
    }

    public static class RecordingGroupProvider extends GroupProvider {
        boolean initialized;
        boolean destroyed;

        public RecordingGroupProvider(String name, Map<String, String> properties) {
            super(name, properties);
        }

        @Override
        public void init() {
            initialized = true;
        }

        @Override
        public void destroy() {
            destroyed = true;
        }

        @Override
        public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
            return Set.of("test_group");
        }

        @Override
        public void checkProperty() {
        }
    }

    public static class ContextAwareGroupProvider extends GroupProvider
            implements AccessControlContextAwareGroupProvider {
        public ContextAwareGroupProvider(String name, Map<String, String> properties) {
            super(name, properties);
        }

        @Override
        public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
            return Set.of("no_context_group");
        }

        @Override
        public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName,
                                    AccessControlContext accessControlContext) {
            return Set.of("context_group");
        }

        @Override
        public void checkProperty() {
        }
    }
}
