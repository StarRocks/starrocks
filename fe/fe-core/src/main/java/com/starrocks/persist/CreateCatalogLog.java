// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class CreateCatalogLog implements Writable {

    @SerializedName(value = "catalogName")
    private String catalogName;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public CreateCatalogLog(String catalogName, String comment, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.comment = comment;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCatalogType() {
        String catalogType = properties.get("type");
        return catalogType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateCatalogLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), CreateCatalogLog.class);
    }
}
