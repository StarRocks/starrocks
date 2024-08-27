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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.informationschema.InformationSchemaConnector;
import com.starrocks.connector.metadata.TableMetaConnector;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ConnectorFactory {
    private static final Logger LOG = LogManager.getLogger(ConnectorFactory.class);

    /**
     * create a connector instance
     *
     * @param context - encapsulate all information needed to create a connector
     * @return a connector instance
     */
    public static CatalogConnector createConnector(ConnectorContext context, boolean isReplay)
            throws StarRocksConnectorException {
        if (null == context || !ConnectorType.isSupport(context.getType())) {
            return null;
        }

        try {
            LazyConnector lazyConnector = new LazyConnector(context);
            if (!isReplay) {
                lazyConnector.initIfNeeded();
            }

            InformationSchemaConnector informationSchemaConnector =
                    new InformationSchemaConnector(context.getCatalogName());
            TableMetaConnector tableMetaConnector = new TableMetaConnector(context.getCatalogName(), context.getType());
            return new CatalogConnector(lazyConnector, informationSchemaConnector, tableMetaConnector);
        } catch (Exception e) {
            LOG.error(String.format("create [%s] connector failed", context.getType()), e);
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    public static Connector createRealConnector(ConnectorContext context)
            throws StarRocksConnectorException {
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

            return connector;
        } catch (InvocationTargetException e) {
            LOG.error(String.format("create [%s] connector failed", context.getType()), e);
            Throwable rootCause = ExceptionUtils.getCause(e);
            throw new StarRocksConnectorException(rootCause.getMessage(), rootCause);
        } catch (Exception e1) {
            LOG.error(String.format("create [%s] connector failed", context.getType()), e1);
            throw new StarRocksConnectorException(e1.getMessage(), e1);
        }
    }
}
