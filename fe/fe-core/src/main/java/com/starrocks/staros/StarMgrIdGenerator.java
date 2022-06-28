// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.util.IdGenerator;
import com.starrocks.catalog.CatalogIdGenerator;

// wrapper for star mgr to use catalog id generator
public class StarMgrIdGenerator extends IdGenerator {
    private CatalogIdGenerator catalogIdGenerator;

    public StarMgrIdGenerator(CatalogIdGenerator generator) {
        this.catalogIdGenerator = generator;
    }

    @Override
    public long getNextId() {
        return catalogIdGenerator.getNextId();
    }
}
