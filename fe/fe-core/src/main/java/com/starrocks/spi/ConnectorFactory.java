package com.starrocks.spi;

import java.util.Map;

public interface ConnectorFactory {
    String getName();

    Connector create(String catalogName, Map<String, String> config);
}
