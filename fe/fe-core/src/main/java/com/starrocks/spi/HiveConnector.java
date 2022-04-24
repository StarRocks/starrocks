package com.starrocks.spi;

import java.util.Map;

public class HiveConnector implements Connector {
    private Map<String, String> properties;
    private String name;
    private String resourceName;
    private Metadata metadata;

    public HiveConnector(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        validateProperties();
    }

    private void validateProperties() {
        // check properties
        resourceName = properties.get("hive.metastore.url");
    }

    @Override
    public Metadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new HiveMetadata(resourceName);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        return metadata;
    }

    @Override
    public ScanRangerProvider getScanRangeProvider() {
        return null;
    }

    @Override
    public RuleProvider getRuleProvider() {
        return null;
    }
}
