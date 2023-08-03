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

import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Constructor;

public class ConnectorFactory {
    private static final Logger LOG = LogManager.getLogger(ConnectorFactory.class);

    /**
     * create a connector instance
     *
     * @param context - encapsulate all information needed to create a connector
     * @return a connector instance
     */
    public static ConnectorService createConnector(ConnectorContext context) {
        if (null == context || !ConnectorType.isSupport(context.getType())) {
            return null;
        }

        ConnectorType connectorType = ConnectorType.from(context.getType());
        Class<Connector> connectorClass = connectorType.getConnectorClass();
        Class<ConnectorConfig> ctConfigClass = connectorType.getConfigClass();
        try {
            Constructor connectorConstructor = connectorClass.getDeclaredConstructor(ConnectorContext.class);
            Connector connector = (Connector) connectorConstructor.newInstance(new Object[] {context});

            // init config, then load config
            if (null != connector && null != ctConfigClass) {
                ConnectorConfig connectorConfig = ctConfigClass.newInstance();
                connectorConfig.loadConfig(context.getProperties());
                connector.bindConfig(connectorConfig);
            }

            InformationSchemaMetadata informationSchemaMetadata =
                    new InformationSchemaMetadata(context.getCatalogName());
            return new ConnectorService(connector, informationSchemaMetadata);
        } catch (Exception e) {
            LOG.error("can't create connector for type: " + context.getType(), e);
            return null;
        }
    }
}
