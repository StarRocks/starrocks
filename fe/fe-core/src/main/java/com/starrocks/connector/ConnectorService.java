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

import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.credential.CloudConfiguration;

import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

// ConnectorService is a wrapper class of connector interface.
// For now, it maintains information schema and query-level metadata cache.
public class ConnectorService {
    private final Connector connector;
    private final ConnectorMetadata informationSchemaMetadata;

    // query-level metadata cache
    private final ConcurrentHashMap<String, ConnectorMetadata> memoizedMetadata = new ConcurrentHashMap<>();

    public ConnectorService(Connector connector, InformationSchemaMetadata informationSchemaMetadata) {
        requireNonNull(connector, "connector is null");
        requireNonNull(informationSchemaMetadata, "informationSchemaMetadata is null");
        this.connector = connector;
        this.informationSchemaMetadata = informationSchemaMetadata;
    }

    public ConnectorMetadata getMetadata() {
        return new CatalogConnectorMetadata(
                connector.getMetadata(),
                informationSchemaMetadata
        );
    }

    public ConnectorMetadata getCachedMetadata(String queryID) {
        memoizedMetadata.putIfAbsent(queryID, new CatalogConnectorMetadata(
                connector.getMetadata(),
                informationSchemaMetadata
        ));
        return memoizedMetadata.get(queryID);
    }

    public void removeMetadata(String queryID) {
        ConnectorMetadata metadata = memoizedMetadata.remove(queryID);
        if (metadata != null) {
            metadata.clear();
        }
    }

    public void shutdown() {
        connector.shutdown();
    }

    public CloudConfiguration getCloudConfiguration() {
        return connector.getCloudConfiguration();
    }
}
