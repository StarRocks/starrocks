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

package com.starrocks.connector.kudu;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class KuduConnector implements Connector {
    private static final String HIVE = "hive";
    private static final String GLUE = "glue";
    private static final String KUDU = "kudu";
    private static final Set<String> SUPPORTED_METASTORE_TYPE = Sets.newHashSet(HIVE, GLUE, KUDU);
    public static final String KUDU_MASTER = "kudu.master";
    public static final String KUDU_CATALOG_TYPE = "kudu.catalog.type";
    public static final String KUDU_SCHEMA_EMULATION_ENABLED = "kudu.schema-emulation.enabled";
    public static final String KUDU_SCHEMA_EMULATION_PREFIX = "kudu.schema-emulation.prefix";
    public static final String DEFAULT_KUDU_MASTER = "localhost:7051";
    private final String catalogName;
    private final String kuduMaster;
    private final String catalogType;
    private final String metastoreUris;
    private final Boolean schemaEmulationEnabled;
    private final String schemaEmulationPrefix;
    private final HdfsEnvironment hdfsEnvironment;
    private final Map<String, String> properties;
    private final Optional<IHiveMetastore> hiveMetastoreClient;

    public KuduConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.kuduMaster = properties.getOrDefault(KUDU_MASTER, DEFAULT_KUDU_MASTER);
        this.catalogType = getPropertyOrThrow(KUDU_CATALOG_TYPE).toLowerCase();
        this.metastoreUris = properties.get(HIVE_METASTORE_URIS);
        this.schemaEmulationEnabled = Boolean.parseBoolean(properties.get(KUDU_SCHEMA_EMULATION_ENABLED));
        this.schemaEmulationPrefix = properties.getOrDefault(KUDU_SCHEMA_EMULATION_PREFIX, StringUtils.EMPTY);

        validateCatalogType(catalogType);
        validateMetastoreUrisIfNecessary(catalogType, metastoreUris);

        if (HIVE.equals(catalogType) || GLUE.equals(catalogType)) {
            HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(hdfsEnvironment, properties);
            MetastoreType metastoreType = MetastoreType.get(catalogType);
            hiveMetastoreClient = Optional.of(new HiveMetastore(metaClient, catalogName, metastoreType));
        } else {
            hiveMetastoreClient = Optional.empty();
        }
    }

    private String getPropertyOrThrow(String propertyName) {
        String propertyValue = properties.get(propertyName);
        if (Strings.isNullOrEmpty(propertyValue)) {
            throw new StarRocksConnectorException("The property %s must be set.", propertyName);
        }
        return propertyValue;
    }

    private void validateCatalogType(String catalogType) {
        if (!SUPPORTED_METASTORE_TYPE.contains(catalogType)) {
            throw new StarRocksConnectorException("kudu catalog type [%s] is not supported", catalogType);
        }
    }

    private void validateMetastoreUrisIfNecessary(String catalogType, String metastoreUris) {
        if (HIVE.equals(catalogType) && Strings.isNullOrEmpty(metastoreUris)) {
            throw new StarRocksConnectorException("The property %s must be set if kudu catalog is hive.",
                    HIVE_METASTORE_URIS);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new KuduMetadata(catalogName, hdfsEnvironment, kuduMaster, schemaEmulationEnabled, schemaEmulationPrefix,
                hiveMetastoreClient);
    }
}
