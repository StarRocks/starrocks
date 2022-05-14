// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.proc.BaseProcResult;

public class InternalCatalog extends Catalog {
    public static final String DEFAULT_INTERNAL_CATALOG_NAME = "default";

    public InternalCatalog() {
        super(DEFAULT_INTERNAL_CATALOG_NAME, "");
    }

    public InternalCatalog(String comment) {
        super(DEFAULT_INTERNAL_CATALOG_NAME, comment);
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(DEFAULT_INTERNAL_CATALOG_NAME,
                DEFAULT_INTERNAL_CATALOG_NAME, DEFAULT_INTERNAL_CATALOG_NAME));
    }
}
