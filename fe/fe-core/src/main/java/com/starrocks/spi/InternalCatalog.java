package com.starrocks.spi;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;

public class InternalCatalog extends Catalog {
    public static final String DEFAULT_CATALOG = "default";



    public InternalCatalog() {
        super("default", Collections.emptyMap(), "default");
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

}
