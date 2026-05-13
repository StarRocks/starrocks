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

package com.starrocks.connector.fluss;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlussConnector implements Connector {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String LAKE_PAIMON_PREFIX = "table.datalake.paimon.";
    private final Connection connection;
    private final Admin admin;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<String, String> tableProperties;

    public FlussConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        Map<String, String> properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);

        String bootstrapServers = properties.get(BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new StarRocksConnectorException("The property %s must be set.", BOOTSTRAP_SERVERS);
        }

        Configuration conf = new Configuration();
        conf.setString(BOOTSTRAP_SERVERS, bootstrapServers);
        String keyPrefix = "fluss.option.";
        Set<String> optionKeys = properties.keySet().stream()
                .filter(k -> k.startsWith(keyPrefix)).collect(Collectors.toSet());
        for (String k : optionKeys) {
            String key = k.substring(keyPrefix.length());
            conf.setString(key, properties.get(k));
        }

        this.tableProperties = properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(LAKE_PAIMON_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new FlussMetadata(catalogName, hdfsEnvironment, this.connection, this.admin, tableProperties);
    }

    @Override
    public void shutdown() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
