// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

public class InternalCatalog extends Catalog {
    private static final String INTERNAL_CATALOG_NAME = "default";

    public InternalCatalog() {
        super(CatalogName.of(INTERNAL_CATALOG_NAME));
    }
}
