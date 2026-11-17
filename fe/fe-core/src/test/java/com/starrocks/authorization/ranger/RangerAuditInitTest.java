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

package com.starrocks.authorization.ranger;

import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.DummyAuditProvider;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

/**
 * Ranger initializes its process-wide audit factory inside {@link RangerBasePlugin#init()},
 * before the policy engine. A destination class missing from the classpath used to fail the
 * controller (so the first catalog access after every FE start) and then silently drop all
 * audit. These tests run the real {@code init()}; only the policy refresher, which talks to
 * Ranger Admin, is stubbed.
 */
public class RangerAuditInitTest {
    private static int refresherStarts;

    @BeforeEach
    public void setUp() throws IllegalAccessException {
        // The audit factory is a JVM-wide singleton; start every test from a fresh one.
        resetAuditFactory();
        FieldUtils.writeDeclaredStaticField(RangerAccessController.class, "auditInitFailed", false, true);
        refresherStarts = 0;

        new MockUp<PolicyRefresher>() {
            @Mock
            void $init(RangerBasePlugin plugin) {
            }

            @Mock
            void startRefresher() {
                refresherStarts++;
            }
        };
    }

    private static void resetAuditFactory() throws IllegalAccessException {
        FieldUtils.writeDeclaredStaticField(AuditProviderFactory.class, "sFactory", null, true);
    }

    private static class AuditConfiguredController extends RangerStarRocksAccessController {
        // Read by buildRangerPluginContext, which the super constructor calls before this
        // class's fields exist.
        private static Map<String, String> auditProperties;

        static AuditConfiguredController create(Map<String, String> properties) {
            auditProperties = properties;
            return new AuditConfiguredController();
        }

        @Override
        protected RangerPluginConfig buildRangerPluginContext(String serviceType, String serviceName) {
            RangerPluginConfig config = super.buildRangerPluginContext(serviceType, serviceName);
            config.set("xasecure.audit.is.enabled", "true");
            auditProperties.forEach(config::set);
            return config;
        }
    }

    @Test
    public void testUnshippedAuditDestinationOnlyDisablesAudit() {
        // log4j is not shipped since Ranger 2.8 (ranger-audit-core has only the file destination).
        RangerAccessController controller =
                AuditConfiguredController.create(Map.of("xasecure.audit.destination.log4j", "true"));

        Assertions.assertTrue(RangerAccessController.isAuditInitFailed());
        // The policy engine was still built: init() ran past the audit step.
        Assertions.assertEquals(1, refresherStarts);
        Assertions.assertNotNull(controller.getRangerPlugin());
        // Ranger left the factory marked initialized with its no-op provider, so nothing
        // retries or half-works.
        AuditProviderFactory factory = AuditProviderFactory.getInstance();
        Assertions.assertTrue(factory.isInitDone());
        Assertions.assertInstanceOf(DummyAuditProvider.class, factory.getAuditProvider());

        // A second catalog's controller neither fails nor re-initializes audit.
        AuditConfiguredController.create(Map.of("xasecure.audit.destination.log4j", "true"));
        Assertions.assertEquals(2, refresherStarts);
    }

    @Test
    public void testSolrAuditDestinationIsShipped() throws IllegalAccessException {
        // Guards the FE packaging: Ranger 2.8 moved Solr audit into ranger-audit-dest-solr,
        // which a dependency bump once dropped. Both the v2 keys (SolrAuditProvider) and the
        // v3 keys (SolrAuditDestination) must initialize.
        AuditConfiguredController.create(Map.of(
                "xasecure.audit.solr.is.enabled", "true",
                "xasecure.audit.solr.solr_url", "http://127.0.0.1:1/solr/ranger_audits"));
        Assertions.assertFalse(RangerAccessController.isAuditInitFailed());
        Assertions.assertFalse(AuditProviderFactory.getInstance().getAuditProvider() instanceof DummyAuditProvider);

        resetAuditFactory();
        AuditConfiguredController.create(Map.of(
                "xasecure.audit.destination.solr", "true",
                "xasecure.audit.destination.solr.urls", "http://127.0.0.1:1/solr/ranger_audits"));
        Assertions.assertFalse(RangerAccessController.isAuditInitFailed());
        Assertions.assertFalse(AuditProviderFactory.getInstance().getAuditProvider() instanceof DummyAuditProvider);
    }

    @Test
    public void testFileAuditDestinationIsShipped(@TempDir Path dir) {
        // The docs offer the file destination besides Solr. It has only the v3 keys and lives in
        // ranger-audit-core, so it needs neither solrj nor jetty.
        AuditConfiguredController.create(Map.of(
                "xasecure.audit.destination.file", "true",
                "xasecure.audit.destination.file.dir", dir.toString()));
        Assertions.assertFalse(RangerAccessController.isAuditInitFailed());
        Assertions.assertFalse(AuditProviderFactory.getInstance().getAuditProvider() instanceof DummyAuditProvider);
    }

    @Test
    public void testSolrjCanFinishASolrRequest() throws ClassNotFoundException {
        // HttpSolrClient calls Utils.consumeFully after reading every Solr response, and Utils
        // references ZooKeeper classes. Without them each Solr audit send fails with
        // NoClassDefFoundError after Solr stored the events.
        Class.forName("org.apache.solr.common.util.Utils");
    }

    @Test
    public void testNonAuditInitFailureStillFailsController() {
        // Only audit is isolated. Failing to load policies must keep failing the catalog,
        // never fall back to allow.
        new MockUp<PolicyRefresher>() {
            @Mock
            void startRefresher() {
                throw new IllegalStateException("cannot load policies");
            }
        };
        Assertions.assertThrows(IllegalStateException.class,
                () -> AuditConfiguredController.create(Map.of("xasecure.audit.destination.file", "false")));
        Assertions.assertFalse(RangerAccessController.isAuditInitFailed());
    }
}
