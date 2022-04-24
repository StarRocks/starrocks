package com.starrocks.spi;

import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ExternalCatalog extends Catalog {
    public ExternalCatalog(String catalogName, Map<String, String> properties, String type) {
        super(catalogName, properties, type);
    }



}
