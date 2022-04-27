// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

public interface Connector {
    /**
     * Get the connector meta of connector
     * @return a ConnectorMetadata instance of connector
     */
    ConnectorMetadata getMetadata();
}
