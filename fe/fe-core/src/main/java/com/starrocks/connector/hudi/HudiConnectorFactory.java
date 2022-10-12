// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;

public class HudiConnectorFactory implements ConnectorFactory {
    @Override
    public Connector createConnector(ConnectorContext context) {
        return new HudiConnector(context);
    }

    @Override
    public String name() {
        return "hudi";
    }
}
