package com.starrocks.spi;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static com.starrocks.spi.InternalCatalog.DEFAULT_CATALOG;

public class Catalog implements Writable {
    @SerializedName(value = "name")
    private final String name;
    @SerializedName(value = "properties")
    private final Map<String, String> properties;
    @SerializedName(value = "type")
    private final String type;

    public static boolean isInternalCatalog(String catalogName) {
        return catalogName.equals(DEFAULT_CATALOG);
    }

    public Catalog(String name, Map<String, String> properties, String type) {
        this.name = name;
        this.properties = properties;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getType() {
        return type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Catalog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Catalog.class);
    }
}
