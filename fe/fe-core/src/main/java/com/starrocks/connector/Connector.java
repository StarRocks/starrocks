// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import java.util.Map;

public interface Connector {
    class Context {
        private final Map<String, String> properties;

        Context(Map<String, String> properties) {
            this.properties = properties;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }

    /**
     * Get the connector meta of connector
     * @return a ConnectorMetadata instance of connector
     */
    ConnectorMetadata getMetadata();
}
