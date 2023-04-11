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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.elasticsearch.ElasticsearchConnector;
import com.starrocks.connector.elasticsearch.EsConfig;
import com.starrocks.connector.hive.HiveConnector;
import com.starrocks.connector.hudi.HudiConnector;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.connector.jdbc.JDBCConnector;

import java.lang.reflect.Constructor;
import java.util.Map;

import static com.starrocks.connector.ConnectorType.ELASTICSEARCH;
import static com.starrocks.connector.ConnectorType.HIVE;
import static com.starrocks.connector.ConnectorType.HUDI;
import static com.starrocks.connector.ConnectorType.ICEBERG;
import static com.starrocks.connector.ConnectorType.JDBC;

public class ConnectorFactory {
    public static Map<ConnectorType, Class> SUPPORT_CONNECTOR_TYPE = ImmutableMap.of(
            HIVE, HiveConnector.class,
            HUDI, HudiConnector.class,
            ICEBERG, IcebergConnector.class,
            JDBC, JDBCConnector.class,
            ELASTICSEARCH, ElasticsearchConnector.class);

    // TODO extract other ConnectorType
    public static Map<ConnectorType, Class> SUPPORT_CONNECTOR_CONFIG_TYPE = ImmutableMap.of(
            ELASTICSEARCH, EsConfig.class);

    /**
     * create a connector instance
     *
     * @param context - encapsulate all information needed to create a connector
     * @return a connector instance
     */
    public static Connector createConnector(ConnectorContext context) {
        Preconditions.checkState(ConnectorType.isSupport(context.getCatalogName()),
                "not support create connector for " + context.getType());

        Class<Connector> connectorClass = SUPPORT_CONNECTOR_TYPE.get(ConnectorType.valueOf(context.getType()));
        Class<ConnectorConfig> ctConfigClass = SUPPORT_CONNECTOR_CONFIG_TYPE.get(ConnectorType.valueOf(context.getType()));
        try {
            Constructor connectorConstructor =
                    connectorClass.getDeclaredConstructor(new Class[] {ConnectorContext.class});
            Connector connector = (Connector) connectorConstructor.newInstance(new Object[] {context});

            // init config, then load config
            if (null != ctConfigClass) {
                ConnectorConfig connectorConfig = ctConfigClass.newInstance();
                connectorConfig.loadConfig(context.getProperties());
                connector.loadConfig(connectorConfig);
            }

            return connector;
        } catch (Exception e) {
            throw new RuntimeException("can't create connector for type" + context.getType(), e);
        }
    }
}
