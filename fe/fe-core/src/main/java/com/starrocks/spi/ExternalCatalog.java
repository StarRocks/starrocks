package com.starrocks.spi;

import com.starrocks.common.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ExternalCatalog extends Catalog {
    private String catalogName;
    private Map<String, String> properties;

    public ExternalCatalog(String catalogName, Map<String, String> properties, Catalog.CatalogType type) {
        super(catalogName, properties, type);
    }


    @Override
    public void write(DataOutput out) throws IOException {

    }
}
