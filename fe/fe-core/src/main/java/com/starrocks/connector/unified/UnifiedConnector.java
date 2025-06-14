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

package com.starrocks.connector.unified;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Table;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.hive.HiveConnector;
import com.starrocks.connector.hudi.HudiConnector;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.connector.kudu.KuduConnector;
import com.starrocks.connector.paimon.PaimonConnector;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Table.TableType.DELTALAKE;
import static com.starrocks.catalog.Table.TableType.HIVE;
import static com.starrocks.catalog.Table.TableType.HUDI;
import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.catalog.Table.TableType.KUDU;
import static com.starrocks.catalog.Table.TableType.PAIMON;
import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_TYPE;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.kudu.KuduConnector.KUDU_CATALOG_TYPE;
import static com.starrocks.connector.paimon.PaimonConnector.PAIMON_CATALOG_TYPE;
import static com.starrocks.connector.paimon.PaimonConnector.PAIMON_CATALOG_WAREHOUSE;

public class UnifiedConnector implements Connector {
    public static final String UNIFIED_METASTORE_TYPE = "unified.metastore.type";
    public static final List<String> SUPPORTED_METASTORE_TYPE = ImmutableList.of("hive", "glue");
    private Map<Table.TableType, Connector> connectorMap;

    public UnifiedConnector(ConnectorContext context) {
        String metastoreType = context.getProperties().get(UNIFIED_METASTORE_TYPE);
        if (!SUPPORTED_METASTORE_TYPE.contains(metastoreType)) {
            throw new SemanticException("Unified catalog only supports hive and glue as metastore.");
        }

        ImmutableMap.Builder<String, String> derivedProperties = ImmutableMap.builder();
        derivedProperties.putAll(context.getProperties());
        derivedProperties.put(HIVE_METASTORE_TYPE, metastoreType);
        derivedProperties.put(ICEBERG_CATALOG_TYPE, metastoreType);
        derivedProperties.put(PAIMON_CATALOG_TYPE, metastoreType);
        derivedProperties.put(KUDU_CATALOG_TYPE, metastoreType);

        ConnectorContext derivedContext = new ConnectorContext(context.getCatalogName(), context.getType(),
                derivedProperties.build());

        ImmutableMap.Builder<Table.TableType, Connector> builder = ImmutableMap.builder();

        builder.put(HIVE, new HiveConnector(derivedContext))
                .put(ICEBERG, new IcebergConnector(derivedContext))
                .put(HUDI, new HudiConnector(derivedContext))
                .put(DELTALAKE, new DeltaLakeConnector(derivedContext))
                .put(KUDU, new KuduConnector(derivedContext));
        boolean containsPaimon = null != context.getProperties().get(PAIMON_CATALOG_WAREHOUSE);
        if (containsPaimon) {
            builder.put(PAIMON, new PaimonConnector(derivedContext));
        }
        connectorMap = builder.build();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        Map<Table.TableType, ConnectorMetadata> metadataMap = new HashMap<>();
        connectorMap.forEach((k, v) -> metadataMap.put(k, v.getMetadata()));

        return new UnifiedMetadata(metadataMap);
    }

    @Override
    public void shutdown() {
        connectorMap.forEach((k, v) -> v.shutdown());
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        connectorMap.forEach((k, v) -> v.bindConfig(config));
    }
}