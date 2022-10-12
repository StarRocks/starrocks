// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

public interface ConnectorFactory {
    /**
     * create a connector instance
     *
     * @param context - encapsulate all information needed to create a connector
     * @return a connector instance
     */
    Connector createConnector(ConnectorContext context);

    /**
     * a unique string represents a kinds of connector
     *
     * @return a string of connector name
     */
    String name();
}
