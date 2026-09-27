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

public class FlussConnector implements Connector {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String FLUSS_OPTION_PREFIX = "fluss.option.";
    private static final String LAKE_PROPERTY_PREFIX = "table.datalake.";
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Configuration flussClientConf;
    private final Configuration catalogConf;
    private Connection connection;
    private Admin admin;

    public FlussConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        Map<String, String> properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);

        String bootstrapServers = properties.get(BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new StarRocksConnectorException("The property %s must be set.", BOOTSTRAP_SERVERS);
        }

        this.flussClientConf = new Configuration();
        flussClientConf.setString(BOOTSTRAP_SERVERS, bootstrapServers);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(FLUSS_OPTION_PREFIX)) {
                flussClientConf.setString(key.substring(FLUSS_OPTION_PREFIX.length()), entry.getValue());
            }
        }

        this.catalogConf = Configuration.fromMap(flussClientConf.toMap());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(LAKE_PROPERTY_PREFIX)) {
                this.catalogConf.setString(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public synchronized ConnectorMetadata getMetadata() {
        initConnection();
        return new FlussMetadata(catalogName, hdfsEnvironment, this.admin, catalogConf);
    }

    private void initConnection() {
        if (connection != null) {
            return;
        }

        Connection newConnection = ConnectionFactory.createConnection(flussClientConf);
        try {
            Admin newAdmin = newConnection.getAdmin();
            this.connection = newConnection;
            this.admin = newAdmin;
        } catch (RuntimeException e) {
            try {
                newConnection.close();
            } catch (Exception closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    @Override
    public synchronized void shutdown() {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
