// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

public interface Connector {
    /**
     * Get the connector meta of connector
     *
     * @return a ConnectorMetadata instance of connector
     */
    ConnectorMetadata getMetadata() throws Exception;

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    default void shutdown() {}
}
