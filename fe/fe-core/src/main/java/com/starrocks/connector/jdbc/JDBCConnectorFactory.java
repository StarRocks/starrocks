// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.jdbc;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;

public class JDBCConnectorFactory implements ConnectorFactory {

    @Override
    public Connector createConnector(ConnectorContext context) {
        return new JDBCConnector(context);
    }

    @Override
    public String name() {
        return "jdbc";
    }
}

