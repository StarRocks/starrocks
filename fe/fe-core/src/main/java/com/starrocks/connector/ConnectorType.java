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
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.elasticsearch.ElasticsearchConnector;
import com.starrocks.connector.elasticsearch.EsConfig;
import com.starrocks.connector.hive.HiveConnector;
import com.starrocks.connector.hudi.HudiConnector;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.connector.jdbc.JDBCConnector;
import com.starrocks.connector.odps.OdpsConnector;
import com.starrocks.connector.paimon.PaimonConnector;
import com.starrocks.connector.unified.UnifiedConnector;
import org.apache.commons.lang3.EnumUtils;

import java.util.EnumSet;
import java.util.Set;

public enum ConnectorType {

    ES("es", ElasticsearchConnector.class, EsConfig.class),
    HIVE("hive", HiveConnector.class, null),
    ICEBERG("iceberg", IcebergConnector.class, null),
    JDBC("jdbc", JDBCConnector.class, null),
    HUDI("hudi", HudiConnector.class, null),
    DELTALAKE("deltalake", DeltaLakeConnector.class, null),
    PAIMON("paimon", PaimonConnector.class, null),
    ODPS("odps", OdpsConnector.class, null),
    UNIFIED("unified", UnifiedConnector.class, null);

    public static Set<ConnectorType> SUPPORT_TYPE_SET = EnumSet.of(
            ES,
            HIVE,
            ICEBERG,
            JDBC,
            HUDI,
            DELTALAKE,
            PAIMON,
            ODPS,
            UNIFIED
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
        ConnectorType type = EnumUtils.getEnumIgnoreCase(ConnectorType.class, name);
        return type != null && SUPPORT_TYPE_SET.contains(type);
    }

    public static ConnectorType from(String name) {
        ConnectorType res = EnumUtils.getEnumIgnoreCase(ConnectorType.class, name);
        if (res == null) {
            throw new IllegalStateException("unsupported catalog type: " + name);
        }
        return res;
    }

}
