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

import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.elasticsearch.ElasticsearchConnector;
import com.starrocks.connector.elasticsearch.EsConfig;
import com.starrocks.connector.hive.HiveConnector;
import com.starrocks.connector.hudi.HudiConnector;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.connector.jdbc.JDBCConnector;
import com.starrocks.connector.paimon.PaimonConnector;

import java.util.Set;

public enum ConnectorType {

    ELASTICSEARCH("es", ElasticsearchConnector.class, EsConfig.class),
    HIVE("hive", HiveConnector.class, null),
    ICEBERG("iceberg", IcebergConnector.class, null),
    JDBC("jdbc", JDBCConnector.class, null),
    HUDI("hudi", HudiConnector.class, null),
    DELTALAKE("deltalake", DeltaLakeConnector.class, null),
    PAIMON("paimon", PaimonConnector.class, null);

    public static Set<String> SUPPORT_TYPE_SET = ImmutableSet.of(
            ELASTICSEARCH.getName(),
            HIVE.getName(),
            ICEBERG.getName(),
            JDBC.getName(),
            HUDI.getName(),
            DELTALAKE.getName(),
            PAIMON.getName()
    );

    ConnectorType(String name, Class connectorClass, Class configClass) {
        this.name = name;
        this.connectorClass = connectorClass;
        this.configClass = configClass;
    }

    private String name;
    private Class<Connector> connectorClass;
    private Class<ConnectorConfig> configClass;

    public String getName() {
        return name;
    }

    public Class getConnectorClass() {
        return connectorClass;
    }

    public Class<ConnectorConfig> getConfigClass() {
        return configClass;
    }

    public static boolean isSupport(String name) {
        return SUPPORT_TYPE_SET.contains(name);
    }

    public static ConnectorType from(String name) {
        switch (name) {
            case "es":
                return ELASTICSEARCH;
            case "hive":
                return HIVE;
            case "iceberg":
                return ICEBERG;
            case "jdbc":
                return JDBC;
            case "hudi":
                return HUDI;
            case "deltalake":
                return DELTALAKE;
            case "paimon":
                return PAIMON;
            default:
                throw new IllegalStateException("Unexpected value: " + name);
        }
    }

}
