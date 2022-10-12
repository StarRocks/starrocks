// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import java.util.Map;

public class ConnectorContext {
    private final String catalogName;
    private final String type;
    private final Map<String, String> properties;

    public ConnectorContext(String catalogName, String type, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.type = type;
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getType() {
        return type;
    }
}

