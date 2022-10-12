// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;

public class HiveConnectorFactory implements ConnectorFactory {
    @Override
    public Connector createConnector(ConnectorContext context) {
        return new HiveConnector(context);
    }

    @Override
    public String name() {
        return "hive";
    }
}
