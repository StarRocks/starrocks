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

import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

public class ConnectorProperties {
    public static final String ENABLE_GET_STATS_FROM_EXTERNAL_METADATA = "enable_get_stats_from_external_metadata";

    private final ConnectorType connectorType;
    private final Map<String, String> properties;

    public ConnectorProperties(ConnectorType connectorType) {
        this.connectorType = connectorType;
        this.properties = Map.of();
    }

    public ConnectorProperties(ConnectorType connectorType, Map<String, String> properties) {
        this.connectorType = connectorType;
        this.properties = properties;
    }

    public boolean enableGetTableStatsFromExternalMetadata() {
        // For Iceberg and DeltaLake, we don't get table stats from metadata by default.
        boolean defaultValue = connectorType != ConnectorType.ICEBERG && connectorType != ConnectorType.DELTALAKE;
        return PropertyUtil.propertyAsBoolean(properties, ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA,
                defaultValue);
    }
}
