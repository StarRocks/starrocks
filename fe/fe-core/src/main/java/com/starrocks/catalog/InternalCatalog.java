// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import java.util.Map;

public class InternalCatalog extends Catalog {
    public static final String DEFAULT_INTERNAL_CATALOG_NAME = "default_catalog";

    public InternalCatalog(String name, Map<String, String> config, String comment) {
        super(DEFAULT_INTERNAL_CATALOG_NAME, config, comment);
    }

}
