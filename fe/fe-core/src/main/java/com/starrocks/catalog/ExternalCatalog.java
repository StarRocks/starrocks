// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;

import java.util.Map;

public class ExternalCatalog extends Catalog {
    private static final String CATALOG_TYPE = "type";
    private final Map<String, String> config;
    private final String type;

    public ExternalCatalog(String name, Map<String, String> config) {
        super(name);
        this.config = config;
        this.type = Preconditions.checkNotNull(config.get(CATALOG_TYPE));
    }

    public String getType() {
        return type;
    }
}
