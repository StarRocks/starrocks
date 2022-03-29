package com.starrocks.spi;

import com.starrocks.common.io.Writable;

import java.util.Map;

public abstract class Catalog implements Writable {
    public enum CatalogType {
        INTERNAL,
        HIVE,
        ICEBERG,
        HUDI
    }

    private final String name;
    private final Map<String, String> properties;
    private final CatalogType type;

    public Catalog(String name, Map<String, String> properties, CatalogType type) {
        this.name = name;
        this.properties = properties;
        this.type = type;
    }



}
