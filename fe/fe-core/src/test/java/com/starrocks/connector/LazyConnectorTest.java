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

package com.starrocks.connector;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessControllerFactory;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class LazyConnectorTest {
    private static final String CATALOG_NAME = "test_catalog";

    private static class TestExternalAccessController extends ExternalAccessController {
        private boolean catalogActionChecked;

        @Override
        public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType) {
            catalogActionChecked = true;
        }
    }

    @Test
    public void testExtensionAccessControllerFactory() throws AccessDeniedException {
        ConnectorContext context = new ConnectorContext(
                CATALOG_NAME, "hive", Map.of("catalog.access.control", "extension"));
        AccessControlProvider provider = new AccessControlProvider(
                Mockito.mock(AuthorizerStmtVisitor.class), Mockito.mock(ExternalAccessController.class));
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        TestExternalAccessController controller = new TestExternalAccessController();
        Connector delegate = Mockito.mock(Connector.class);

        Mockito.when(factory.createAccessController(context)).thenReturn(controller);

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class);
                MockedStatic<ExtensionManager> extensions = Mockito.mockStatic(ExtensionManager.class);
                MockedStatic<ConnectorFactory> connectors = Mockito.mockStatic(ConnectorFactory.class)) {
            authorizer.when(Authorizer::getInstance).thenReturn(provider);
            extensions.when(() -> ExtensionManager.hasComponent(AccessControllerFactory.class)).thenReturn(true);
            extensions.when(() -> ExtensionManager.getComponent(AccessControllerFactory.class)).thenReturn(factory);
            connectors.when(() -> ConnectorFactory.createRealConnector(context)).thenReturn(delegate);

            new LazyConnector(context).initIfNeeded();

            Mockito.verify(factory).createAccessController(context);
            provider.getAccessControlOrDefault(CATALOG_NAME)
                    .checkCatalogAction(null, CATALOG_NAME, PrivilegeType.SELECT);
            Assertions.assertTrue(controller.catalogActionChecked);
        }
    }

    @Test
    public void testExtensionAccessControllerFactoryReturningNullFails() {
        ConnectorContext context = new ConnectorContext(
                CATALOG_NAME, "hive", Map.of("catalog.access.control", "extension"));
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);

        try (MockedStatic<ExtensionManager> extensions = Mockito.mockStatic(ExtensionManager.class)) {
            extensions.when(() -> ExtensionManager.hasComponent(AccessControllerFactory.class)).thenReturn(true);
            extensions.when(() -> ExtensionManager.getComponent(AccessControllerFactory.class)).thenReturn(factory);

            StarRocksConnectorException exception = Assertions.assertThrows(
                    StarRocksConnectorException.class, () -> new LazyConnector(context).initIfNeeded());
            Assertions.assertTrue(exception.getMessage().contains("AccessControllerFactory returned null"));
        }
    }

    @Test
    public void testMissingExtensionAccessControllerFactoryFails() {
        ConnectorContext context = new ConnectorContext(
                CATALOG_NAME, "hive", Map.of("catalog.access.control", "extension"));

        try (MockedStatic<ExtensionManager> extensions = Mockito.mockStatic(ExtensionManager.class)) {
            extensions.when(() -> ExtensionManager.hasComponent(AccessControllerFactory.class)).thenReturn(false);

            StarRocksConnectorException exception = Assertions.assertThrows(
                    StarRocksConnectorException.class, () -> new LazyConnector(context).initIfNeeded());
            Assertions.assertTrue(exception.getMessage().contains("No AccessControllerFactory is registered"));
        }
    }
}
