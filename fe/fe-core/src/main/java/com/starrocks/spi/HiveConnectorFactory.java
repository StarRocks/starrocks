package com.starrocks.spi;

import java.util.Map;

public class HiveConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "hive";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config) {
        return new HiveConnector(catalogName, config);
    }


}
