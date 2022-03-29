package com.starrocks.spi;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;

public class InternalCatalog extends Catalog {
    public InternalCatalog() {
        super("default", Collections.emptyMap(), CatalogType.INTERNAL);
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }
}
