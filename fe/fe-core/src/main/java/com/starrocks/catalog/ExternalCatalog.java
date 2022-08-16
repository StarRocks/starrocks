// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;

import java.util.Map;

public class ExternalCatalog extends Catalog {

    public ExternalCatalog(long id, String name, String comment, Map<String, String> config) {
        super(id, name, config, comment);
        Preconditions.checkNotNull(config.get(CATALOG_TYPE));
    }
}
