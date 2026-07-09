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
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessControllerFactory;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.sql.analyzer.Authorizer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class LazyConnectorTest {
    @Test
    public void testExtensionAccessControllerFactory() {
        ConnectorContext context = new ConnectorContext(
                "test_catalog", "hive", Map.of("catalog.access.control", "extension"));
        AccessControlProvider provider = Mockito.mock(AccessControlProvider.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessController controller = Mockito.mock(AccessController.class);
        Connector delegate = Mockito.mock(Connector.class);

        Mockito.when(factory.createAccessController(context)).thenReturn(controller);

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class);
                MockedStatic<ExtensionManager> extensions = Mockito.mockStatic(ExtensionManager.class);
                MockedStatic<ConnectorFactory> connectors = Mockito.mockStatic(ConnectorFactory.class)) {
            authorizer.when(Authorizer::getInstance).thenReturn(provider);
            extensions.when(() -> ExtensionManager.getComponent(AccessControllerFactory.class)).thenReturn(factory);
            connectors.when(() -> ConnectorFactory.createRealConnector(context)).thenReturn(delegate);

            new LazyConnector(context).initIfNeeded();

            Mockito.verify(factory).createAccessController(context);
            Mockito.verify(provider).setAccessControl("test_catalog", controller);
        }
    }
}
